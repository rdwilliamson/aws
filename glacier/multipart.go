package glacier

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/rdwilliamson/aws"
)

// Multipart contains all relevant data for a multipart upload.
type Multipart struct {
	ArchiveDescription string
	CreationDate       time.Time
	MultipartUploadId  string
	PartSizeInBytes    int64
	VaultARN           string
}

// MultipartPart contains the range and hash of part of an upload.
type MultipartPart struct {
	RangeInBytes   string
	SHA256TreeHash string
}

// MultipartParts contains contains all relevant data for a multipart upload.
type MultipartParts struct {
	// TODO: document this and all three of these structs
	// better. All three can't be "all relevant data for
	// multipart", otherwise there would only be one of them.

	ArchiveDescription string
	CreationDate       time.Time
	Marker             string
	MultipartUploadId  string
	PartSizeInBytes    int64
	Parts              []MultipartPart
	VaultARN           string
}

// This operation initiates a multipart upload. Amazon Glacier creates a
// multipart upload resource and returns its ID in the response. You use this
// Upload ID in subsequent multipart upload operations.
//
// When you initiate a multipart upload, you specify the part size in number of
// bytes. The part size must be a megabyte (1024 KB) multiplied by a power of
// 2—for example, 1048576 (1 MB), 2097152 (2 MB), 4194304 (4 MB), 8388608 (8
// MB), and so on.The minimum allowable part size is 1 MB, and the maximum is 4
// GB.
//
// Every part you upload using this upload ID, except the last one, must have
// the same size. The last one can be the same size or smaller. For example,
// suppose you want to upload a 16.2 MB file. If you initiate the multipart
// upload with a part size of 4 MB, you will upload four parts of 4 MB each and
// one part of 0.2 MB.
//
// After you complete the multipart upload, Amazon Glacier removes the multipart
// upload resource referenced by the ID. Amazon Glacier will also remove the
// multipart upload resource if you cancel the multipart upload or or it may be
// removed if there is no activity for a period of 24 hours.
//
// Note: You don't need to know the size of the archive when you start a
// multipart upload because Amazon Glacier does not require you to specify the
// overall archive size.
func (c *Connection) InitiateMultipart(vault string, size int64, description string) (string, error) {
	// Build request.
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

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return "", aws.ParseError(response)
	}

	// Parse success response.
	return response.Header.Get("x-amz-multipart-upload-id"), nil
}

// This multipart upload operation uploads a part of an archive.You can upload
// archive parts in any order because in your Upload Part request you specify
// the range of bytes in the assembled archive that will be uploaded in this
// part.You can also upload these parts in parallel.You can upload up to 10,000
// parts for a multipart upload.
//
// Amazon Glacier rejects your upload part request if any of the following
// conditions is true:
// * SHA256 tree hash does not match—To ensure that part data is not corrupted
// in transmission, you compute a SHA256 tree hash of the part and include it in
// your request. Upon receiving the part data, Amazon Glacier also computes a
// SHA256 tree hash. If the two hash values don't match, the operation fails.
// * Part size does not match—The size of each part except the last must match
// the size that is specified in the corresponding Initiate Multipart Upload.
// The size of the last part must be the same size as, or smaller than, the
// specified size. Note: If you upload a part whose size is smaller than the
// part size you specified in your initiate multipart upload request and that
// part is not the last part, then the upload part request will succeed.
// However, the subsequent Complete Multipart Upload request will fail.
// * Range does not align—The byte range value in the request does not align
// with the part size specified in the corresponding initiate request. For
// example, if you specify a part size of 4194304 bytes (4 MB), then 0 to
// 4194303 bytes (4 MB —1) and 4194304 (4 MB) to 8388607 (8 MB —1) are valid
// part ranges. However, if you set a range value of 2 MB to 6 MB, the range
// does not align with the part size and the upload will fail.
//
// This operation is idempotent. If you upload the same part multiple times, the
// data included in the most recent request overwrites the previously uploaded
// data.
func (c *Connection) UploadMultipart(vault, uploadId string, start int64, body io.ReadSeeker) error {
	// TODO check that data size and start location make sense

	// Build request.
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

	hash := th.Hash()

	request.Header.Add("x-amz-content-sha256", toHex(hash))
	request.Header.Add("x-amz-sha256-tree-hash", toHex(th.TreeHash()))
	request.Header.Add("Content-Range", fmt.Sprintf("bytes %d-%d/*", start, start+n-1))
	request.ContentLength = n

	c.Signature.Sign(request, aws.HashedPayload(hash))

	// Perform request.
	response, err := c.client().Do(request)
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

// You call this multipart upload operation to inform Amazon Glacier that all
// the archive parts have been uploaded and Amazon Glacier can now assemble the
// archive from the uploaded parts.
//
// After assembling and saving the archive to the vault, Amazon Glacier returns
// the archive ID of the newly created archive resource. After you upload an
// archive, you should save the archive ID returned to retrieve the archive at a
// later point.
//
// In the request, you must include the computed SHA256 tree hash of the entire
// archive you have uploaded. On the server side, Amazon Glacier also constructs
// the SHA256 tree hash of the assembled archive. If the values match, Amazon
// Glacier saves the archive to the vault; otherwise, it returns an error, and
// the operation fails. It includes checksum information for each uploaded part
// that can be used to debug a bad checksum issue.
//
// Additionally, Amazon Glacier also checks for any missing content ranges.When
// uploading parts, you specify range values identifying where each part fits in
// the final assembly of the archive.When assembling the final archive Amazon
// Glacier checks for any missing content ranges and if there are any missing
// content ranges, Amazon Glacier returns an error and the Complete Multipart
// Upload operation fails.
//
// Complete Multipart Upload is an idempotent operation. After your first
// successful complete multipart upload, if you call the operation again within
// a short period, the operation will succeed and return the same archive ID.
// This is useful in the event you experience a network issue that causes an
// aborted connection or receive a 500 server error, in which case you can
// repeat your Complete Multipart Upload request and get the same archive ID
// without creating duplicate archives. Note, however, that after the multipart
// upload completes, you cannot call the List Parts operation and the multipart
// upload will not appear in List Multipart Uploads response, even if idempotent
// complete is possible.
func (c *Connection) CompleteMultipart(vault, uploadId, treeHash string, size int64) (string, error) {
	// Build request.
	request, err := http.NewRequest("POST", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads/"+uploadId, nil)
	if err != nil {
		return "", err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	request.Header.Add("x-amz-sha256-tree-hash", treeHash)
	request.Header.Add("x-amz-archive-size", fmt.Sprint(size))

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return "", aws.ParseError(response)
	}

	// Parse success response.
	return response.Header.Get("x-amz-archive-id"), nil
}

// This multipart upload operation aborts a multipart upload identified by the upload ID.
//
// After the Abort Multipart Upload request succeeds, you cannot use the upload
// ID to upload any more parts or perform any other operations. Aborting a
// completed multipart upload fails. However, aborting an already-aborted upload
// will succeed, for a short time.
//
// This operation is idempotent.
func (c *Connection) AbortMultipart(vault, uploadId string) error {
	// Build request.
	request, err := http.NewRequest("DELETE", "https://"+c.Signature.Region.Glacier+"/-/vaults/"+vault+
		"/multipart-uploads/"+uploadId, nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	c.Signature.Sign(request, nil)

	// Perform request.
	response, err := c.client().Do(request)
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

// This multipart upload operation lists the parts of an archive that have been
// uploaded in a specific multipart upload identified by an upload ID.
//
// You can make this request at any time during an in-progress multipart upload
// before you complete the multipart upload. Amazon Glacier returns the part
// list sorted by range you specified in each part upload. If you send a List
// Parts request after completing the multipart upload, Amazon Glacier returns
// an error.
//
// The List Parts operation supports pagination. By default, this operation
// returns up to 1,000 uploaded parts in the response.You should always check
// the marker field in the response body for a marker at which to continue the
// list; if there are no more items the marker field is null. If the marker is
// not null, to fetch the next set of parts you sent another List Parts request
// with the marker request parameter set to the marker value Amazon Glacier
// returned in response to your previous List Parts request.
//
// You can also limit the number of parts returned in the response by specifying
// the limit parameter in the request.
func (c *Connection) ListMultipartParts(vault, uploadId, marker string, limit int) (*MultipartParts, error) {
	// Build request.
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

	var list struct {
		ArchiveDescription string
		CreationDate       string
		Marker             *string
		MultipartUploadId  string
		PartSizeInBytes    int64
		Parts              []MultipartPart
		VaultARN           string
	}
	err = json.Unmarshal(body, &list)
	if err != nil {
		return nil, err
	}

	var result MultipartParts
	result.ArchiveDescription = list.ArchiveDescription
	result.CreationDate, err = time.Parse(time.RFC3339, list.CreationDate)
	if err != nil {
		return nil, err
	}
	if list.Marker != nil {
		result.Marker = *list.Marker
	}
	result.MultipartUploadId = list.MultipartUploadId
	result.PartSizeInBytes = list.PartSizeInBytes
	result.Parts = list.Parts
	result.VaultARN = list.VaultARN

	return &result, nil
}

// This multipart upload operation lists in-progress multipart uploads for the
// specified vault. An in-progress multipart upload is a multipart upload that
// has been initiated by an Initiate Multipart Upload request, but has not yet
// been completed or aborted. The list returned in the List Multipart Upload
// response has no guaranteed order.
//
// The List Multipart Uploads operation supports pagination. By default, this
// operation returns up to 1,000 multipart uploads in the response.You should
// always check the marker field in the response body for a marker at which to
// continue the list; if there are no more items the marker field is null.
//
// If the marker is not null, to fetch the next set of multipart uploads you
// sent another List Multipart Uploads request with the marker request parameter
// set to the marker value Amazon Glacier returned in response to your previous
// List Multipart Uploads request.
//
// Note the difference between this operation and the List Parts operation.The
// List Multipart Uploads operation lists all multipart uploads for a vault. The
// List Parts operation returns parts of a specific multipart upload identified
// by an Upload ID.
func (c *Connection) ListMultipartUploads(vault, marker string, limit int) ([]Multipart, string, error) {
	// Build request.
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

	var list struct {
		Marker      *string
		UploadsList []struct {
			ArchiveDescription *string
			CreationDate       string
			MultipartUploadId  string
			PartSizeInBytes    int64
			VaultARN           string
		}
	}
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
		if err != nil {
			return nil, "", err
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
