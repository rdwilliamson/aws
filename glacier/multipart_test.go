package glacier

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"time"
)

const (
	envAWSAccess     = "AWS_ACCESS_KEY"
	envAWSSecret     = "AWS_SECRET_KEY"
	envGlacierVault  = "GLACIER_VAULT"
	envGlacierRegion = "GLACIER_REGION"
)

// aReader returns "a" ad infinitum
type aReader struct{}

func (x *aReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

func TestSmallFewParts(t *testing.T) {
	testUpload(t, rand.Reader, 3, 1<<20)
}

func TestSmallManyParts(t *testing.T) {
	testUpload(t, rand.Reader, 50, 1<<20)
}

func TestMediumParts(t *testing.T) {
	testUpload(t, rand.Reader, 3, 4<<20)
}

func TestHugeParts(t *testing.T) {
	x := aReader{}
	testUpload(t, &x, 2, 256<<20)
}

// testUpload only runs if AWS credentials and Glacier parameters
// are set. It uploads nParts+1 parts; nParts of size partSize, and a final
// one of size (3/4)*partSize.
func testUpload(t *testing.T, r io.Reader, nParts int, partSize int64) {
	conn := testConnection(t)
	vault := testVault(t)

	// Add 3/4ths of a part to simulate a uneven read.
	toRead := int64(nParts)*partSize + partSize>>1 + partSize>>2
	nParts++
	description := fmt.Sprintf("multipart-upload-test-%d", time.Now().UnixNano())
	uploadID, err := conn.InitiateMultipart(vault, partSize, description)
	if err != nil {
		t.Fatal(err)
	}
	var totalRead int64
	for i := 0; i < nParts; i++ { // Upload each part sequentially.
		size := partSize
		if i == nParts-1 { // Last, uneven part
			size = toRead % partSize
		}
		data := make([]byte, 0, size)
		b := bytes.NewBuffer(data)
		read, err := io.CopyN(b, r, size)
		if err != nil {
			t.Fatal(err)
		}
		totalRead += read
		partR := bytes.NewReader(b.Bytes())
		err = conn.UploadMultipart(vault, uploadID, int64(i)*partSize, partR)
		if err != nil {
			t.Fatal(err)
		}
	}
	if totalRead != toRead {
		t.Errorf("Expected %d data read, got %d", toRead, totalRead)
	}
	// Build the treehash from the parts, complete the multipart upload.
	th, err := conn.TreeHashFromMultipartUpload(vault, uploadID)
	if err != nil {
		t.Fatal(err)
	}
	archiveID, err := conn.CompleteMultipart(vault, uploadID, th, toRead)
	// Cleanup by aborting or deleting the archive, depending on completion status.
	if err != nil {
		secondErr := conn.AbortMultipart(vault, uploadID)
		if secondErr != nil {
			t.Fatal(secondErr)
		}
		t.Fatal(err)
	}
	err = conn.DeleteArchive(vault, archiveID)
	if err != nil {
		t.Fatal(err)
	}
}
