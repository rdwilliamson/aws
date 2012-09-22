package glacier

import (
	"bytes"
	"encoding/json"
	"github.com/rdwilliamson/aws"
	"io/ioutil"
	"net/http"
)

type job struct {
	Type        string
	ArchiveId   string `json:",omitempty"` // archive retrieval only
	Description string
	Format      string `json:",omitempty"` // inventory retrieval only
	SNSTopic    string
}

func (c *Connection) InitiateRetrievalJob(vault, archive, topic string) error {
	return nil
}

func (c *Connection) InitiateInventoryJob(vault, description,
	topic string) error {
	j := job{Type: "inventory-retrieval", Description: description,
		SNSTopic: topic}
	rawBody, err := json.Marshal(j)
	if err != nil {
		return err
	}
	body := bytes.NewReader(rawBody)

	request, err := http.NewRequest("POST",
		"https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/jobs", body)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = c.Signature.Sign(request, body, nil)
	if err != nil {
		return err
	}

	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 202 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		err = response.Body.Close()
		if err != nil {
			return err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return err
		}
		return e
	}

	return nil
}

func (c *Connection) DescribeJob() error {
	return nil
}

func (c *Connection) GetRetrievalJob(vault, job string, start, end uint) error {
	return nil
}

func (c *Connection) GetInventoryJob(vault, job string) error {
	return nil
}

func (c *Connection) ListJobs(vault string) error {
	return nil
}
