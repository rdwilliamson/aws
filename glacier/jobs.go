package glacier

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/rdwilliamson/aws"
)

type jobRequest struct {
	Type        string
	ArchiveId   string `json:",omitempty"` // required for archive retrieval
	Description string `json:",omitempty"`
	Format      string `json:",omitempty"`
	SNSTopic    string `json:",omitempty"`
}

// An archive is any object, such as a photo, video, or document, that you
// store in a vault. It is a base unit of storage in Amazon Glacier. Each
// archive has a unique ID and an optional description. When you upload an
// archive, Amazon Glacier returns a response that includes an archive ID.
// This archive ID is unique in the region in which the archive is stored. The
// following is an example archive ID. Archive IDs are 138 bytes long. When
// you upload an archive, you can provide an optional description. You can
// retrieve an archive using its ID but not its description.
type Archive struct {
	ArchiveId          string
	ArchiveDescription string
	CreationDate       time.Time
	Size               int64
	SHA256TreeHash     string
}

// Amazon Glacier updates a vault inventory approximately once a day, starting
// on the day you first upload an archive to the vault. When you initiate a
// job for a vault inventory, Amazon Glacier returns the last inventory it
// generated, which is a point-in-time snapshot and not realtime data. Note
// that after Amazon Glacier creates the first inventory for the vault, it
// typically takes half a day and up to a day before that inventory is
// available for retrieval.You might not find it useful to retrieve a vault
// inventory for each archive upload. However, suppose you maintain a database
// on the client-side associating metadata about the archives you upload to
// Amazon Glacier. Then, you might find the vault inventory useful to
// reconcile information, as needed, in your database with the actual vault
// inventory.
type Inventory struct {
	VaultARN      string
	InventoryDate time.Time
	ArchiveList   []Archive
}

// Retrieving an archive or a vault inventory are asynchronous operations that
// require you to initiate a job. It is a two-step process:
// 1. Initiate a retrieval job.
// 2. After the job completes, download the bytes.
// The retrieval request is executed asynchronously. When you initiate a
// retrieval job, Amazon Glacier creates a job and returns a job ID in the
// response. When Amazon Glacier completes the job, you can get the job output
// (archive or inventory data).
// The job must complete before you can get its output. To determine when a
// job is complete, you have the following options:
// * Use Amazon SNS Notification— You can specify an Amazon Simple Notification
// Service (Amazon SNS) topic to which Amazon Glacier can post a notification
// after the job is completed. You can specify an SNS topic per job request.
// The notification is sent only after Amazon Glacier completes the job. In
// addition to specifying an SNS topic per job request, you can configure
// vault notifications for a vault so that job notifications are sent for all
// retrievals.
// * Get job details- Use DescribeJob.
type Job struct {
	Action               string
	ArchiveId            string
	ArchiveSizeInBytes   int64
	Completed            bool
	CompletionDate       time.Time
	CreationDate         time.Time
	InventorySizeInBytes int
	JobDescription       string
	JobId                string
	SHA256TreeHash       string
	SNSTopic             string
	StatusCode           string
	StatusMessage        string
	VaultARN             string
}

type jobList struct {
	Marker  *string
	JobList []job
}

type job struct {
	Action               string
	ArchiveId            *string
	ArchiveSizeInBytes   *int64
	Completed            bool
	CompletionDate       *string
	CreationDate         string
	InventorySizeInBytes *int
	JobDescription       *string
	JobId                string
	SHA256TreeHash       *string
	SNSTopic             *string
	StatusCode           string
	StatusMessage        *string
	VaultARN             string
}

// Initiate an archive retrieval job with both the vault name where the
// archive resides and the archive ID you wish to download. You can also
// provide an optional job description when you initiate these jobs. If you
// specify a topic, Amazon Glacier sends notifications to both the supplied
// topic and the vault's ArchiveRetrievalCompleted notification topic.
//
// Returns the job ID or the first error encountered.
func (c *Connection) InitiateRetrievalJob(vault, archive, topic, description string) (string, error) {
	// Build request.
	j := jobRequest{Type: "archive-retrieval", ArchiveId: archive, Description: description, SNSTopic: topic}
	body, _ := json.Marshal(j)

	request, err := http.NewRequest("POST", c.vault(vault)+"/jobs", nil)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, aws.MemoryPayload(body))

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusAccepted {
		return "", aws.ParseError(response)
	}

	// Parse success response.
	return response.Header.Get("x-amz-job-id"), nil
}

// Initiate an vault inventory job with the vault name. You can also provide
// an optional job description when you initiate these jobs. If you specify a
// topic, Amazon Glacier sends notifications to both the supplied topic and
// the vault's ArchiveRetrievalCompleted notification topic.
//
// Returns the job ID or the first error encountered.
func (c *Connection) InitiateInventoryJob(vault, topic, description string) (string, error) {
	// Build request.
	j := jobRequest{Type: "inventory-retrieval", Description: description, SNSTopic: topic}
	body, _ := json.Marshal(j)

	request, err := http.NewRequest("POST", c.vault(vault)+"/jobs", nil)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, aws.MemoryPayload(body))

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusAccepted {
		return "", aws.ParseError(response)
	}

	// Parse success response.
	return response.Header.Get("x-amz-job-id"), nil
}

// This operation returns information about a job you previously initiated,
// see Job type.
//
// Returns the job and the first error, if any, encountered.
func (c *Connection) DescribeJob(vault, jobId string) (*Job, error) {
	// Build request.
	request, err := http.NewRequest("GET", c.vault(vault)+"/jobs/"+jobId, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, aws.ParseError(response)
	}

	// Parse success response.
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var j job
	err = json.Unmarshal(body, &j)
	if err != nil {
		return nil, err
	}

	var result Job
	if j.ArchiveId != nil {
		result.ArchiveId = *j.ArchiveId
	}
	if j.ArchiveSizeInBytes != nil {
		result.ArchiveSizeInBytes = *j.ArchiveSizeInBytes
	}
	result.Completed = j.Completed
	if j.CompletionDate != nil {
		result.CompletionDate, err = time.Parse(time.RFC3339, *j.CompletionDate)
		if err != nil {
			return nil, err
		}
	}
	result.CreationDate, err = time.Parse(time.RFC3339, j.CreationDate)
	if err != nil {
		return nil, err
	}
	if j.InventorySizeInBytes != nil {
		result.InventorySizeInBytes = *j.InventorySizeInBytes
	}
	if j.JobDescription != nil {
		result.JobDescription = *j.JobDescription
	}
	result.JobId = j.JobId
	if j.SHA256TreeHash != nil {
		result.SHA256TreeHash = *j.SHA256TreeHash
	}
	if j.SNSTopic != nil {
		result.SNSTopic = *j.SNSTopic
	}
	result.StatusCode = j.StatusCode
	if j.StatusMessage != nil {
		result.StatusMessage = *j.StatusMessage
	}
	result.VaultARN = j.VaultARN

	return &result, nil
}

// You can download all the job output or download a portion of the output by
// specifying a byte range. In the case of an archive retrieval job, depending
// on the byte range you specify, Amazon Glacier returns the checksum for the
// portion of the data.You can compute the checksum on the client and verify
// that the values match to ensure the portion you downloaded is the correct
// data.
//
// Returns a ReadCloser containing the requested data, a tree hash or the
// first error encountered.
// It is the caller's reposonsibility to call Close on the returned value. The
// tree hash is only returned under the following conditions:
// * You get the entire range of the archive.
// * You request a byte range of the archive that starts and ends on a multiple
// of 1 MB. For example, if you have a 3.1 MB archive and you specify a range
// to return that starts at 1 MB and ends at 2 MB, then the x-amz-sha256-tree-
// hash is returned as a response header.
// * You request a byte range that starts on a multiple of 1 MB and goes to the
// end of the archive. For example, if you have a 3.1 MB archive and you
// specify a range that starts at 2 MB and ends at 3.1 MB (the end of the
// archive), then the x-amz-sha256-tree-hash is returned as a response header.
func (c *Connection) GetRetrievalJob(vault, job string, start, end int64) (io.ReadCloser, string, error) {
	// Build request.
	request, err := http.NewRequest("GET", c.vault(vault)+"/jobs/"+job+"/output", nil)
	if err != nil {
		return nil, "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")
	if end > 0 {
		request.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, "", err
	}

	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		err := aws.ParseError(response)
		response.Body.Close()
		return nil, "", err
	}

	// Parse success response.
	return response.Body, response.Header.Get("x-amz-sha256-tree-hash"), nil
}

// Amazon Glacier updates a vault inventory approximately once a day, starting
// on the day you first upload an archive to the vault. When you initiate a job
// for a vault inventory, Amazon Glacier returns the last inventory it
// generated, which is a point-in-time snapshot and not realtime data. Note that
// after Amazon Glacier creates the first inventory for the vault, it typically
// takes half a day and up to a day before that inventory is available for
// retrieval.You might not find it useful to retrieve a vault inventory for each
// archive upload. However, suppose you maintain a database on the client-side
// associating metadata about the archives you upload to Amazon Glacier. Then,
// you might find the vault inventory useful to reconcile information, as
// needed, in your database with the actual vault inventory.
func (c *Connection) GetInventoryJob(vault, job string) (*Inventory, error) {
	// Build request.
	request, err := http.NewRequest("GET", c.vault(vault)+"/jobs/"+job+"/output", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, aws.ParseError(response)
	}

	// Parse success response.
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var i struct {
		VaultARN      string
		InventoryDate string
		ArchiveList   []struct {
			ArchiveId          string
			ArchiveDescription string
			CreationDate       string
			Size               int64
			SHA256TreeHash     string
		}
	}
	err = json.Unmarshal(body, &i)
	if err != nil {
		return nil, err
	}

	var result Inventory
	result.VaultARN = i.VaultARN
	result.InventoryDate, err = time.Parse(time.RFC3339, i.InventoryDate)
	if err != nil {
		return nil, err
	}
	result.ArchiveList = make([]Archive, len(i.ArchiveList))
	for j, v := range i.ArchiveList {
		result.ArchiveList[j].ArchiveId = v.ArchiveId
		result.ArchiveList[j].ArchiveDescription = v.ArchiveDescription
		result.ArchiveList[j].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil {
			return nil, err
		}
		result.ArchiveList[j].Size = v.Size
		result.ArchiveList[j].SHA256TreeHash = v.SHA256TreeHash
	}

	return &result, nil
}

// This operation lists jobs for a vault including jobs that are in-progress and
// jobs that have recently finished.
//
// To retrieve an archive or retrieve a vault inventory from Amazon Glacier, you
// first initiate a job, and after the job completes, you download the data. For
// an archive retrieval, the output is the archive data, and for an inventory
// retrieval, it is the inventory list. The List Job operation returns a list of
// these jobs sorted by job initiation time. The List Jobs operation supports
// pagination. By default, this operation returns up to 1,000 jobs in the
// response.You should always check the response marker field for a marker at
// which to continue the list; if there are no more items the marker field is
// null. To return a list of jobs that begins at a specific job, set the marker
// request parameter to the value you obtained from a previous List Jobs
// request.You can also limit the number of jobs returned in the response by
// specifying the limit parameter in the request.
//
// Additionally, you can filter the jobs list returned by specifying an optional
// statuscode (InProgress, Succeeded, or Failed) and completed (true, false)
// parameter. The statuscode allows you to specify that only jobs that match a
// specified status are returned.The completed parameter allows you to specify
// that only jobs in specific completion state are returned.
//
// Amazon Glacier retains recently completed jobs for a period before deleting
// them; however, it eventually removes completed jobs. The output of completed
// jobs can be retrieved. Retaining completed jobs for a period of time after
// they have completed enables you to get a job output in the event you miss the
// job completion notification or your first attempt to download it fails. For
// example, suppose you start an archive retrieval job to download an archive.
// After the job completes, you start to download the archive but encounter a
// network error. In this scenario, you can retry and download the archive while
// the job exists.
func (c *Connection) ListJobs(vault, completed, statusCode, marker string, limit int) ([]Job, string, error) {
	// Build request.
	get, err := url.Parse(c.vault(vault)+"/jobs"))
	if err != nil {
		return nil, "", err
	}

	query := get.Query()
	if completed != "" {
		// TODO validate, true or false
		query.Add("completed", completed)
	}
	if limit > 0 {
		// TODO validate, 1 - 1000
		query.Add("limit", fmt.Sprint(limit))
	}
	if marker != "" {
		// TODO validate
		query.Add("marker", marker)
	}
	if statusCode != "" {
		// TODO validate, InProgress, Succeeded, or Failed
		query.Add("statuscode", statusCode)
	}
	get.RawQuery = query.Encode()

	request, err := http.NewRequest("GET", get.String(), nil)
	if err != nil {
		return nil, "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return nil, "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, "", aws.ParseError(response)
	}

	// Parse success response.
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, "", err
	}

	var jl jobList
	err = json.Unmarshal(body, &jl)
	if err != nil {
		return nil, "", err
	}

	jobs := make([]Job, len(jl.JobList))
	for i, v := range jl.JobList {
		jobs[i].Action = v.Action
		if v.ArchiveId != nil {
			jobs[i].ArchiveId = *v.ArchiveId
		}
		if v.ArchiveSizeInBytes != nil {
			jobs[i].ArchiveSizeInBytes = *v.ArchiveSizeInBytes
		}
		jobs[i].Completed = v.Completed
		if v.CompletionDate != nil {
			jobs[i].CompletionDate, err = time.Parse(time.RFC3339, *v.CompletionDate)
			if err != nil {
				return nil, "", err
			}
		}
		jobs[i].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil {
			return nil, "", err
		}
		if v.InventorySizeInBytes != nil {
			jobs[i].InventorySizeInBytes = *v.InventorySizeInBytes
		}
		if v.JobDescription != nil {
			jobs[i].JobDescription = *v.JobDescription
		}
		jobs[i].JobId = v.JobId
		if v.SHA256TreeHash != nil {
			jobs[i].SHA256TreeHash = *v.SHA256TreeHash
		}
		if v.SNSTopic != nil {
			jobs[i].SNSTopic = *v.SNSTopic
		}
		jobs[i].StatusCode = v.StatusCode
		if v.StatusMessage != nil {
			jobs[i].StatusMessage = *v.StatusMessage
		}
		jobs[i].VaultARN = v.VaultARN
	}

	var m string
	if jl.Marker != nil {
		m = *jl.Marker
	}

	return jobs, m, nil
}
