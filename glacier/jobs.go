package glacier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rdwilliamson/aws"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
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
	Size               uint64
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
	ArchiveSizeInBytes   uint64
	Completed            bool
	CompletionDate       time.Time
	CreationDate         time.Time
	InventorySizeInBytes uint
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
	ArchiveSizeInBytes   *uint64
	Completed            bool
	CompletionDate       *string
	CreationDate         string
	InventorySizeInBytes *uint
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
	j := jobRequest{Type: "archive-retrieval", ArchiveId: archive, Description: description, SNSTopic: topic}
	rawBody, _ := json.Marshal(j)
	body := bytes.NewReader(rawBody)

	request, err := http.NewRequest("POST", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs", body)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, body, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return "", err
	}

	if response.StatusCode != 202 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return "", err
		}
		response.Body.Close()
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return "", err
		}
		return "", &e
	}

	response.Body.Close()

	return response.Header.Get("x-amz-job-id"), nil
}

func (c *Connection) InitiateInventoryJob(vault, topic, description string) (string, error) {
	j := jobRequest{Type: "inventory-retrieval", Description: description, SNSTopic: topic}
	rawBody, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	body := bytes.NewReader(rawBody)

	request, err := http.NewRequest("POST",
		"https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs", body)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, body, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return "", err
	}

	if response.StatusCode != 202 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return "", err
		}
		err = response.Body.Close()
		if err != nil {
			return "", err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return "", err
		}
		return "", &e
	}

	return response.Header.Get("x-amz-job-id"), response.Body.Close()
}

func (c *Connection) DescribeJob(vault, jobId string) (*Job, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs/"+jobId, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err1 := response.Body.Close()

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, &e
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
		if err != nil && err1 == nil {
			err1 = err
		}
	}
	result.CreationDate, err = time.Parse(time.RFC3339, j.CreationDate)
	if err != nil && err1 == nil {
		err1 = err
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

	return &result, err1
}

func (c *Connection) GetRetrievalJob(vault, job string, start, end uint64) (io.ReadCloser, string, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs/"+job+
		"/output", nil)
	if err != nil {
		return nil, "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")
	if end > 0 {
		request.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, "", err
	}

	if response.StatusCode != 200 && response.StatusCode != 206 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, "", err
		}
		response.Body.Close()

		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, "", err
		}
		return nil, "", &e
	}

	return response.Body, response.Header.Get("x-amz-sha256-tree-hash"), nil
}

func (c *Connection) GetInventoryJob(vault, job string) (*Inventory, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs/"+job+
		"/output", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err1 := response.Body.Close()

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, &e
	}

	var i struct {
		VaultARN      string
		InventoryDate string
		ArchiveList   []struct {
			ArchiveId          string
			ArchiveDescription string
			CreationDate       string
			Size               uint64
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
		if err != nil && err1 == nil {
			err1 = err
		}
		result.ArchiveList[j].Size = v.Size
		result.ArchiveList[j].SHA256TreeHash = v.SHA256TreeHash
	}

	return &result, err1
}

func (c *Connection) ListJobs(vault, completed, statusCode, marker string, limit int) ([]Job, string, error) {
	get, err := url.Parse("https://" + c.Signature.Region.Glacier + "/-/vaults/" + vault + "/jobs")
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

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, "", err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, "", err
	}
	err1 := response.Body.Close()

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, "", err
		}
		return nil, "", &e
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
			if err != nil && err1 == nil {
				err1 = err
			}
		}
		jobs[i].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil && err1 == nil {
			err1 = err
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

	return jobs, m, err1
}
