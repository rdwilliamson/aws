package main

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

var (
	v4dir             = "aws4_testsuite"
	v4CredentialScope = "20110909/us-east-1/host/aws4_request"
	v4SecretKey       = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
)

type v4TestFiles struct {
	base  string
	req   []byte
	creq  []byte
	sts   []byte
	authz []byte
	sreq  []byte

	request *http.Request
}

// http://docs.amazonwebservices.com/general/latest/gr/signature-v4-test-suite.html
func testFiles(dir string) ([]string, error) {
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	f, err := d.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	sort.Strings(f)

	tests := make([]string, 0)
	for i := 0; i < len(f)-4; {
		if filepath.Ext(f[i]) == ".authz" &&
			filepath.Ext(f[i+1]) == ".creq" &&
			filepath.Ext(f[i+2]) == ".req" &&
			filepath.Ext(f[i+3]) == ".sreq" &&
			filepath.Ext(f[i+4]) == ".sts" {
			tests = append(tests, f[i][:len(f[i])-6])
			i += 5
		} else {
			i++
		}
	}

	return tests, nil
}

func readTestFiles(files []string, t *testing.T) chan *v4TestFiles {
	ch := make(chan *v4TestFiles)
	go func() {
		for _, f := range files {
			var err error
			d := new(v4TestFiles)
			d.base = f

			// read in the raw request and convert it to go's internal format
			d.req, err = ioutil.ReadFile(v4dir + "/" + f + ".req")
			if err != nil {
				t.Error("reading", d.base, err)
				continue
			}
			// go doesn't like post requests with spaces in them
			if d.base == "post-vanilla-query-nonunreserved" ||
				d.base == "post-vanilla-query-space" {
				// skip tests with spacing in URLs or invalid escapes
				continue
			} else {
				// go doesn't like lowercase http
				fixed := bytes.Replace(d.req, []byte("http"), []byte("HTTP"), 1)
				reader := bufio.NewReader(bytes.NewBuffer(fixed))
				d.request, err = http.ReadRequest(reader)
				if err != nil {
					t.Error("parsing", d.base, "request", err)
					continue
				}
				if i := bytes.Index(d.req, []byte("\n\n")); i != -1 {
					d.request.Body = ioutil.NopCloser(bytes.NewBuffer(d.req[i+2:]))
				}
			}

			d.creq, err = ioutil.ReadFile(v4dir + "/" + f + ".creq")
			if err != nil {
				t.Error("reading", d.base, err)
				continue
			}

			d.sts, err = ioutil.ReadFile(v4dir + "/" + f + ".sts")
			if err != nil {
				t.Error("reading", d.base, err)
				continue
			}

			d.authz, err = ioutil.ReadFile(v4dir + "/" + f + ".authz")
			if err != nil {
				t.Error("reading", d.base, err)
				continue
			}

			ch <- d
		}
		close(ch)
	}()
	return ch
}

func TestSignatureVersion4(t *testing.T) {
	files, err := testFiles(v4dir)
	if err != nil {
		t.Fatal(err)
	}
	tests := readTestFiles(files, t)
	var headers []string
	var cr []byte
	for f := range tests {
		cr, headers, err = CreateCanonicalRequest(f.request)
		if err != nil {
			t.Error(f.base, err)
			continue
		}
		if !bytes.Equal(cr, f.creq) {
			t.Error(f.base, "canonical request")
			t.Logf("got:\n%s", string(cr))
			t.Logf("want:\n%s", string(f.creq))
			continue
		}
		var sts []byte
		date, ok := f.request.Header["Date"]
		if ok && len(date) > 0 {
			sts, err = CreateStringToSign(cr, date[0], v4CredentialScope)
			if err != nil {
				t.Error(f.base, err)
				continue
			}
			if !bytes.Equal(sts, f.sts) {
				t.Error(f.base, "string to sign")
				t.Logf("got:\n%s", string(sts))
				t.Logf("want:\n%s", string(f.sts))
				continue
			}
		} else {
			t.Error(f.base, "no date")
			t.Log(f.request)
			continue
		}

		sig, err := CreateSignature("20110909", "us-east-1", "host", sts)
		if err != nil {
			t.Error(err)
			continue
		}
		authz := []byte("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
		authz = append(authz, v4CredentialScope...)
		authz = append(authz, ", SignedHeaders="...)
		for i := range headers {
			if i > 0 {
				authz = append(authz, ';')
			}
			authz = append(authz, headers[i]...)
		}
		authz = append(authz, ", Signature="...)
		authz = append(authz, sig...)

		if !bytes.Equal(authz, f.authz) {
			t.Error(f.base, "signed signature")
			t.Logf("got:\n%s", authz)
			t.Logf("want:\n%s", f.authz)
		}
	}
}
