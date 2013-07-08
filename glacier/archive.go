package glacier

import (
	"encoding/json"
	"github.com/rdwilliamson/aws"
	"io"
	"io/ioutil"
	"net/http"
	"path"
)

// Upload archive to vault with optional description. The entire archive will
// be read in order to create its tree hash before uploading.
//
// Returns the archive ID or the first error encountered.
func (c *Connection) UploadArchive(vault string, archive io.ReadSeeker, description string) (string, error) {
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

	hash := th.Hash()

	request.Header.Add("x-amz-archive-description", description)
	request.Header.Add("x-amz-sha256-tree-hash", string(toHex(th.TreeHash())))
	request.Header.Add("x-amz-content-sha256", string(toHex(hash)))

	c.Signature.Sign(request, aws.HashedPayload(hash))

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

	response.Body.Close()

	_, location := path.Split(response.Header.Get("Location"))
	return location, nil
}

// Deletes archive from vault.
//
// Returns the first error encountered.
func (c *Connection) DeleteArchive(vault, archive string) error {
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/archives/"+
		archive, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

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
		return &e
	}

	response.Body.Close()

	return nil
}
