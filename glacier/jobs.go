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

type Archive struct {
	ArchiveId          string
	ArchiveDescription string
	CreationDate       time.Time
	Size               uint64
	SHA256TreeHash     string
}

type Inventory struct {
	VaultARN      string
	InventoryDate time.Time
	ArchiveList   []Archive
}

// TODO write unmarshaler instead of using these structs
type archive struct {
	ArchiveId          string
	ArchiveDescription string
	CreationDate       string
	Size               uint64
	SHA256TreeHash     string
}
type inventory struct {
	VaultARN      string
	InventoryDate string
	ArchiveList   []archive
}

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

// TODO write unmarshaler instead of using these structs
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
type jobList struct {
	Marker  *string
	JobList []job
}

func (c *Connection) InitiateRetrievalJob(vault, archive, topic,
	description string) (string, error) {
	j := jobRequest{Type: "archive-retrieval", ArchiveId: archive,
		Description: description, SNSTopic: topic}
	rawBody, _ := json.Marshal(j)
	body := bytes.NewReader(rawBody)

	request, err := http.NewRequest("POST",
		"https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs", body)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, body, nil)
	if err != nil {
		return "", err
	}

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
		return "", e
	}

	return response.Header.Get("x-amz-job-id"), nil
}

func (c *Connection) InitiateInventoryJob(vault, description,
	topic string) (string, error) {
	j := jobRequest{Type: "inventory-retrieval", Description: description,
		SNSTopic: topic}
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

	err = c.Signature.Sign(request, body, nil)
	if err != nil {
		return "", err
	}

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
		return "", e
	}

	return response.Header.Get("x-amz-job-id"), nil
}

func (c *Connection) DescribeJob() error {
	return nil
}

func (c *Connection) GetRetrievalJob(vault, job string, start, end uint) (io.ReadCloser, error) {
	request, err := http.NewRequest("GET", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs/"+job+"/output",
		nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")
	if end > 0 {
		request.Header.Add("Range", fmt.Sprintf("bytes %d-%d/*", start, end))
	}

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		response.Body.Close()

		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, e
	}

	// TODO return content range and x-amz-sha256-tree-hash
	return response.Body, nil
}

func (c *Connection) GetInventoryJob(vault, job string) (Inventory, error) {
	request, err := http.NewRequest("GET", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs/"+job+"/output",
		nil)
	if err != nil {
		return Inventory{}, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return Inventory{}, err
	}

	response, err := c.Client.Do(request)
	if err != nil {
		return Inventory{}, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return Inventory{}, err
	}
	err = response.Body.Close()
	if err != nil {
		return Inventory{}, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return Inventory{}, err
		}
		return Inventory{}, e
	}

	var i inventory
	err = json.Unmarshal(body, &i)
	if err != nil {
		return Inventory{}, err
	}

	var result Inventory
	result.VaultARN = i.VaultARN
	result.InventoryDate, err = time.Parse(time.RFC3339, i.InventoryDate)
	if err != nil {
		return Inventory{}, err
	}
	result.ArchiveList = make([]Archive, len(i.ArchiveList))
	for j, v := range i.ArchiveList {
		result.ArchiveList[j].ArchiveId = v.ArchiveId
		result.ArchiveList[j].ArchiveDescription = v.ArchiveDescription
		result.ArchiveList[j].CreationDate, err = time.Parse(time.RFC3339,
			v.CreationDate)
		if err != nil {
			return Inventory{}, err
		}
		result.ArchiveList[j].Size = v.Size
		result.ArchiveList[j].SHA256TreeHash = v.SHA256TreeHash
	}

	return result, nil
}

func (c *Connection) ListJobs(vault, completed, limit, marker,
	statusCode string) ([]Job, string, error) {
	get, err := url.Parse("https://" + c.Signature.Region.Glacier +
		"/-/vaults/" + vault + "/jobs")
	if err != nil {
		return nil, "", err
	}

	query := get.Query()
	if completed != "" {
		// TODO validate, true or false
		query.Add("completed", completed)
	}
	if limit != "" {
		// TODO validate, 1 - 1000
		query.Add("limit", limit)
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
	err = response.Body.Close()
	if err != nil {
		return nil, "", err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, "", err
		}
		return nil, "", e
	}

	var jl jobList
	err = json.Unmarshal(body, &jl)
	if err != nil {
		return nil, "", err
	}

	var err1 error
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
		}
		if err != nil {
			err1 = err
		}
		jobs[i].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil {
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
