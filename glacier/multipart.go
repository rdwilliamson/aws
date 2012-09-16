package glacier

import (
	"encoding/json"
	"fmt"
	"github.com/rdwilliamson/aws"
	"io/ioutil"
	"net/http"
)

func (c *Connection) InitiateMultipart(vault string, size uint,
	description string) (string, error) {
	request, err := http.NewRequest("POST", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/multipart-upload", nil)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	// TODO check that size is valid
	request.Header.Add("x-amz-part-size", fmt.Sprint(size))

	if description != "" {
		request.Header.Add("x-amz-archive-description", fmt.Sprint(size))
	}

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		return "", err
	}
	return "", nil

	response, err := c.Client.Do(request)
	if err != nil {
		return "", err
	}

	if response.StatusCode != 201 {
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

	return response.Header.Get("x-amz-multipart-upload-id"), nil
}

// reader or []byte?
func (c *Connection) UploadMultipart(vault, id string, start uint, data []byte) error {
	request, err := http.NewRequest("POST", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/multipart-upload", nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	// TODO check that size is valid
	request.Header.Add("Content-Range", fmt.Sprintf("%d-%d/*", start,
		start+uint(len(data))-1))

	return nil
}

func (c *Connection) CompleteMultipart() error {
	return nil
}

func (c *Connection) AbortMultipart() error {
	return nil
}

func (c *Connection) ListMultipartParts() error {
	return nil
}

func (c *Connection) ListMultipartUploads() error {
	return nil
}
