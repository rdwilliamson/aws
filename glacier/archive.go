package glacier

import (
	"encoding/json"
	"github.com/rdwilliamson/aws"
	"io"
	"io/ioutil"
	"net/http"
)

func (c *Connection) UploadArchive(description string, archive io.ReadSeeker, vault string) (string, error) {
	request, err := http.NewRequest("POST", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/archives",
		archive)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	th := NewTreeHash()
	request.ContentLength, err = io.Copy(th, archive)
	if err != nil {
		return "", err
	}
	th.Close()

	_, err = archive.Seek(0, 0)
	if err != nil {
		return "", err
	}

	request.Header.Add("x-amz-archive-description", description)
	request.Header.Add("x-amz-sha256-tree-hash", th.TreeHash())
	request.Header.Add("x-amz-content-sha256", th.Hash())

	c.Signature.Sign(request, nil, th.HashBytes())

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
		return "", &e
	}

	return response.Header.Get("Location"), nil
}

func (c *Connection) DeleteArchive(vault, archive string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/archives/"+
		archive, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != 204 {
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
		return &e
	}

	return nil
}
