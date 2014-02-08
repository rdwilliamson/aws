package glacier

import (
	"io"
	"net/http"
	"path"

	"github.com/rdwilliamson/aws"
)

// Upload archive to vault with optional description. The entire archive will
// be read in order to create its tree hash before uploading.
//
// Returns the archive ID or the first error encountered.
func (c *Connection) UploadArchive(vault string, archive io.ReadSeeker, description string) (string, error) {
	// Build reuest.
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

	// Perform request.
	response, err := c.Client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return "", aws.ParseError(response)
	}

	// Parse success response.
	_, location := path.Split(response.Header.Get("Location"))
	return location, nil
}

// Deletes archive from vault.
//
// Returns the first error encountered.
func (c *Connection) DeleteArchive(vault, archive string) error {
	// Build request.
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+"/archives/"+
		archive, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return aws.ParseError(response)
	}

	// Parse success response.
	return nil
}
