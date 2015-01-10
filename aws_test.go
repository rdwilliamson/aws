package aws

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Struct for holding a single request and its "gold standard"
// signature so that we may verify we can produce the same.
//
type awsTestCase struct {
	base    string
	req     []byte
	sreq    []byte
	request *http.Request
	body    io.ReadSeeker
}

// Get a list of the files in the AWS test suite.
//
func getAWSSuiteFiles(dir string) (files []string, err error) {

	d, err := os.Open(dir)
	if err != nil {
		return
	}
	f, err := d.Readdirnames(0)
	if err != nil {
		return
	}

	sort.Strings(f)

	files = make([]string, 0)
	for i := 0; i < len(f)-1; {
		if filepath.Ext(f[i]) == ".req" &&
			filepath.Ext(f[i+1]) == ".sreq" {
			files = append(files, f[i][:len(f[i])-4])
			i += 2
		} else {
			i++
		}
	}
	return

}

// Build a slice of awsTestCase structs based on the "gold standards"
// distributed by Amazon and located in the aws4_testsuite directory.
//
func buildAWSSuite() (tests []*awsTestCase, err error) {

	// Get the list of files in the aws4_testsuite directory
	dir := "aws4_testsuite"
	files, err := getAWSSuiteFiles(dir)
	if err != nil {
		return
	}

	tests = make([]*awsTestCase, 0)
	for _, f := range files {
		d := new(awsTestCase)
		d.base = f

		// Read in the raw request and convert it to go's internal format
		d.req, err = ioutil.ReadFile(dir + "/" + f + ".req")
		if err != nil {
			return
		}
		// Go doesn't like post requests with spaces in them
		if d.base == "post-vanilla-query-nonunreserved" ||
			d.base == "post-vanilla-query-space" ||
			d.base == "get-slashes" {
			// skip tests with spacing in URLs or invalid escapes or
			// trailing slashes
			continue
		} else {

			// Go doesn't like lowercase http
			fixed := bytes.Replace(d.req, []byte("http"), []byte("HTTP"), 1)
			reader := bufio.NewReader(bytes.NewBuffer(fixed))
			d.request, err = http.ReadRequest(reader)
			if err != nil {
				return
			}
			delete(d.request.Header, "User-Agent")
			if i := bytes.Index(d.req, []byte("\n\n")); i != -1 {
				d.body = bytes.NewReader(d.req[i+2:])
				d.request.Body = ioutil.NopCloser(d.body)
			}
		}

		d.sreq, err = ioutil.ReadFile(dir + "/" + f + ".sreq")
		if err != nil {
			return
		}

		tests = append(tests, d)
	}
	return
}

func TestSignature(t *testing.T) {

	date := time.Date(2011, time.September, 9, 0, 0, 0, 0, time.UTC)
	secret := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	access := "AKIDEXAMPLE"
	signature := &Signature{
		access,
		date.Format(ISO8601BasicFormatShort),
		USEast1,
		"host",
		[sha256.Size]byte{},
		nil,
	}
	signature.generateSigningKey(secret)

	// Get a slice of awsTestCase structs based on the files in
	// the aws4_testsuite directory.
	//
	tests, err := buildAWSSuite()
	if err != nil {
		t.Fatal(err)
	}

	// Run each of the tests, for each verifying that we're able
	// to match the signature in awsTestCase.
	//
	for _, f := range tests {
		err := signature.Sign(f.request, nil)
		if err != nil {
			t.Error(err)
			continue
		}

		var sreqBuffer bytes.Buffer
		i := bytes.Index(f.req, []byte("\n\n"))
		_, err = sreqBuffer.Write(f.req[:i+1])
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = sreqBuffer.WriteString(fmt.Sprintf("Authorization: %s\n\n",
			f.request.Header.Get("Authorization")))
		if err != nil {
			t.Error(err)
			continue
		}
		f.body.Seek(0, 0)
		_, err = io.Copy(&sreqBuffer, f.request.Body)
		if err != nil {
			t.Error(err)
			continue
		}
		sreq := sreqBuffer.Bytes()
		if !bytes.Equal(sreq, f.sreq) {
			t.Error(f.base, "signed request")
			t.Logf("got:\n%s", sreq)
			t.Logf("want:\n%s", f.sreq)
		}
	}
}

func BenchmarkNewSignature(b *testing.B) {
	secret := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	access := "AKIDEXAMPLE"
	for i := 0; i < b.N; i++ {
		_ = NewSignature(secret, access, USEast1, "service")
	}
}

func BenchmarkSignatureSign(b *testing.B) {
	b.StopTimer()
	secret := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	access := "AKIDEXAMPLE"
	signature := NewSignature(secret, access, USEast1, "service")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rawRequest := []byte(`POST / HTTP/1.1
Content-Type:application/x-www-form-urlencoded
Date:Mon, 09 Sep 2011 23:36:00 GMT
Host:host.foo.com

foo=bar`)
		reader := bufio.NewReader(bytes.NewBuffer(rawRequest))
		request, err := http.ReadRequest(reader)
		if err != nil {
			b.Fatal(err)
		}
		delete(request.Header, "User-Agent")
		var body *bytes.Reader
		if i := bytes.Index(rawRequest, []byte("\n\n")); i != -1 {
			body = bytes.NewReader(rawRequest[i+2:])
			request.Body = ioutil.NopCloser(body)
		}
		b.StartTimer()
		_ = signature.Sign(request, nil)
	}
}

func TestSignErrors(t *testing.T) {
	secret := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	access := "AKIDEXAMPLE"
	signature := NewSignature(secret, access, USEast1, "service")
	rawRequest := []byte(`POST / HTTP/1.1
Content-Type:application/x-www-form-urlencoded
Date:a
Host:host.foo.com

foo=bar`)
	reader := bufio.NewReader(bytes.NewBuffer(rawRequest))
	request, err := http.ReadRequest(reader)
	if err != nil {
		t.Fatal(err)
	}

	err = signature.Sign(request, nil)
	if err == nil {
		t.Error("expected error but got nil")
	} else {
		if _, ok := err.(*time.ParseError); !ok {
			t.Error("url not *time.ParseError")
		}
	}

	request.URL.RawQuery += "%jk"
	err = signature.Sign(request, nil)
	if err == nil {
		t.Error("expected error but got nil")
	} else {
		if _, ok := err.(url.EscapeError); !ok {
			t.Error("url not url.EscapeError")
		}
	}
}

func TestKeysFromFile(t *testing.T) {
	name := filepath.Join(os.TempDir(), "aws_keys_test")
	tests := []string{
		`secret access`,
		`secret  access`,
		`secret
access`,
		`secret
access
`,
		`
secret	access`,
	}

	for _, v := range tests {
		create, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, err = create.Write([]byte(v))
		if err != nil {
			create.Close()
			t.Fatal(err)
		}
		create.Close()

		secret, access, err := KeysFromFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if secret != "secret" || access != "access" {
			t.Error("unexpected keys from", v)
		}

		err = os.Remove(name)
		if err != nil {
			t.Fatal(err)
		}
	}
}
