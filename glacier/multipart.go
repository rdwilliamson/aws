package glacier

import (
	"encoding/json"
	"fmt"
	"github.com/rdwilliamson/aws"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type Multipart struct {
	ArchiveDescription string
	CreationDate       time.Time
	MultipartUploadId  string
	PartSizeInBytes    uint
	VaultARN           string
}

type multipart struct {
	ArchiveDescription *string
	CreationDate       string
	MultipartUploadId  string
	PartSizeInBytes    uint
	VaultARN           string
}

type multipartList struct {
	Marker      *string
	UploadsList []multipart
}

type MultipartPart struct {
	RangeInBytes   string
	SHA256TreeHash string
}

type multipartParts struct {
	ArchiveDescription string
	CreationDate       string
	Marker             *string
	MultipartUploadId  string
	PartSizeInBytes    uint
	Parts              []MultipartPart
	VaultARN           string
}

type MultipartParts struct {
	ArchiveDescription string
	CreationDate       time.Time
	Marker             string
	MultipartUploadId  string
	PartSizeInBytes    uint
	Parts              []MultipartPart
	VaultARN           string
}

func (c *Connection) InitiateMultipart(vault string, size uint, description string) (string, error) {
	request, err := http.NewRequest("POST", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads", nil)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	// TODO check that size is valid
	request.Header.Add("x-amz-part-size", fmt.Sprint(size))

	if description != "" {
		request.Header.Add("x-amz-archive-description", description)
	}

	c.Signature.Sign(request, nil, nil)

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

	return response.Header.Get("x-amz-multipart-upload-id"), nil
}

func (c *Connection) UploadMultipart(vault, uploadId string, start int64, body io.ReadSeeker) error {
	// TODO check that data size and start location make sense

	request, err := http.NewRequest("PUT", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads/"+uploadId, body)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	th := NewTreeHash()
	n, err := io.Copy(th, body)
	if err != nil {
		return err
	}
	th.Close()

	_, err = body.Seek(0, 0)
	if err != nil {
		return err
	}

	request.Header.Add("x-amz-content-sha256", th.Hash())
	request.Header.Add("x-amz-sha256-tree-hash", th.TreeHash())
	request.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, start+n-1))
	request.ContentLength = n

	c.Signature.Sign(request, nil, th.HashBytes())

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

func (c *Connection) CompleteMultipart(vault, uploadId, treeHash string, size uint) (string, error) {
	request, err := http.NewRequest("POST", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads/"+uploadId, nil)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	request.Header.Add("x-amz-sha256-tree-hash", treeHash)
	request.Header.Add("x-amz-archive-size", fmt.Sprint(size))

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)
	if err != nil {
		return "", err
	}

	if response.StatusCode != 201 {
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

	return response.Header.Get("x-amz-archive-id"), nil
}

func (c *Connection) AbortMultipart(vault, uploadId string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads/"+uploadId, nil)
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
		response.Body.Close()
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return err
		}
		return e
	}

	return nil
}

func (c *Connection) ListMultipartParts(vault, uploadId, marker string, limit int) (*MultipartParts, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads/"+uploadId, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	// TODO validate limit
	if limit > 0 {
		request.Header.Add("limit", fmt.Sprint(limit))
	}
	if marker != "" {
		request.Header.Add("marker", marker)
	}

	c.Signature.Sign(request, nil, nil)

	response, err := c.Client.Do(request)

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	response.Body.Close()

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, &e
	}

	fmt.Println(string(body))

	var list multipartParts
	err = json.Unmarshal(body, &list)
	if err != nil {
		return nil, err
	}

	var result MultipartParts
	result.ArchiveDescription = list.ArchiveDescription
	result.CreationDate, err = time.Parse(time.RFC3339, list.CreationDate)
	if list.Marker != nil {
		result.Marker = *list.Marker
	}
	result.MultipartUploadId = list.MultipartUploadId
	result.PartSizeInBytes = list.PartSizeInBytes
	result.Parts = list.Parts
	result.VaultARN = list.VaultARN

	return &result, nil
}

func (c *Connection) ListMultipartUploads(vault, marker string, limit int) ([]Multipart, string, error) {
	request, err := http.NewRequest("GET", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads", nil)
	if err != nil {
		return nil, "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	// TODO validate limit
	if limit > 0 {
		request.Header.Add("limit", fmt.Sprint(limit))
	}
	if marker != "" {
		request.Header.Add("marker", marker)
	}

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

	var list multipartList
	err = json.Unmarshal(body, &list)
	if err != nil {
		return nil, "", err
	}

	parts := make([]Multipart, len(list.UploadsList))
	for i, v := range list.UploadsList {
		if v.ArchiveDescription != nil {
			parts[i].ArchiveDescription = *v.ArchiveDescription
		}
		parts[i].CreationDate, err = time.Parse(time.RFC3339, v.CreationDate)
		if err != nil && err1 == nil {
			err1 = err
		}
		parts[i].MultipartUploadId = v.MultipartUploadId
		parts[i].PartSizeInBytes = v.PartSizeInBytes
		parts[i].VaultARN = v.VaultARN
	}

	var m string
	if list.Marker != nil {
		m = *list.Marker
	}

	return parts, m, nil
}
