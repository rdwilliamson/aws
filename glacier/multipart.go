package glacier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/rdwilliamson/aws"
	"io/ioutil"
	"net/http"
)

func (c *Connection) InitiateMultipart(vault string, size uint,
	description string) (string, error) {
	request, err := http.NewRequest("POST", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/multipart-uploads", nil)
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
func (c *Connection) UploadMultipart(vault, uploadId string, start uint,
	data []byte) error {
	// TODO check that data size and start location make sense

	body := bytes.NewReader(data)
	request, err := http.NewRequest("PUT", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/multipart-uploads/"+
		uploadId, body)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	request.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/*", start,
		start+uint(len(data))-1))

	ht, hash, err := createTreeHash(body)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-content-sha256", string(toHex(hash)))
	request.Header.Add("x-amz-sha256-tree-hash", string(toHex(ht.Hash[:])))

	err = c.Signature.Sign(request, nil, hash)
	if err != nil {
		return err
	}

	request.ContentLength, err = body.Seek(0, 2)
	if err != nil {
		return err
	}
	_, err = body.Seek(0, 0)
	if err != nil {
		return err
	}

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
		return e
	}

	return nil
}

func (c *Connection) CompleteMultipart(vault, uploadId, treeHash string,
	size uint) (string, error) {
	request, err := http.NewRequest("POST", "https://"+
		c.Signature.Region.Glacier+"/-/vaults/"+vault+"/multipart-uploads/"+
		uploadId, nil)
	if err != nil {
		panic(err)
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	request.Header.Add("x-amz-sha256-tree-hash", treeHash)
	request.Header.Add("x-amz-archive-size", fmt.Sprint(size))

	err = c.Signature.Sign(request, nil, nil)
	if err != nil {
		panic(err)
		return "", err
	}

	response, err := c.Client.Do(request)
	if err != nil {
		panic(err)
		return "", err
	}

	if response.StatusCode != 201 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
			return "", err
		}
		err = response.Body.Close()
		if err != nil {
			panic(err)
			return "", err
		}
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			panic(err)
			return "", err
		}
		return "", e
	}

	return response.Header.Get("x-amz-archive-id"), nil
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