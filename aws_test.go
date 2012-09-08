package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

var (
	v4dir = "aws4_testsuite"
)

type v4TestFiles struct {
	base  string
	req   []byte
	creq  []byte
	sts   []byte
	authz []byte
	sreq  []byte

	request *http.Request
	body    io.ReadSeeker
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
				d.base == "post-vanilla-query-space" ||
				d.base == "get-slashes" {
				// skip tests with spacing in URLs or invalid escapes or
				// triling slashes
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
				delete(d.request.Header, "User-Agent")
				if i := bytes.Index(d.req, []byte("\n\n")); i != -1 {
					d.body = bytes.NewReader(d.req[i+2:])
					d.request.Body = ioutil.NopCloser(d.body)
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

			d.sreq, err = ioutil.ReadFile(v4dir + "/" + f + ".sreq")
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
	date := time.Date(2011, time.September, 9, 0, 0, 0, 0, time.UTC)

	signature := NewSignature("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
		"20110909/us-east-1/host/aws4_request", date, USEast, "host")

	files, err := testFiles(v4dir)
	if err != nil {
		t.Fatal(err)
	}
	tests := readTestFiles(files, t)
	// var headers []string
	// var cr []byte
	for f := range tests {
		// cr, headers, err = createCanonicalRequest(f.request)
		// if err != nil {
		// 	t.Error(f.base, err)
		// 	continue
		// }
		// if !bytes.Equal(cr, f.creq) {
		// 	t.Error(f.base, "canonical request")
		// 	t.Logf("got:\n%s", string(cr))
		// 	t.Logf("want:\n%s", string(f.creq))
		// 	continue
		// }

		// var sts []byte
		// date, ok := f.request.Header["Date"]
		// if ok && len(date) > 0 {
		// 	sts, err = createStringToSign(cr, date[0], v4CredentialScope)
		// 	if err != nil {
		// 		t.Error(f.base, err)
		// 		continue
		// 	}
		// 	if !bytes.Equal(sts, f.sts) {
		// 		t.Error(f.base, "string to sign")
		// 		t.Logf("got:\n%s", string(sts))
		// 		t.Logf("want:\n%s", string(f.sts))
		// 		continue
		// 	}
		// } else {
		// 	t.Error(f.base, "no date")
		// 	t.Log(f.request)
		// 	continue
		// }

		// sig := signature.signStringToSign(sts)
		// authz := []byte("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
		// authz = append(authz, v4CredentialScope...)
		// authz = append(authz, ", SignedHeaders="...)
		// for i := range headers {
		// 	if i > 0 {
		// 		authz = append(authz, ';')
		// 	}
		// 	authz = append(authz, headers[i]...)
		// }
		// authz = append(authz, ", Signature="...)
		// authz = append(authz, sig...)
		// if !bytes.Equal(authz, f.authz) {
		// 	t.Error(f.base, "signed signature")
		// 	t.Logf("got:\n%s", authz)
		// 	t.Logf("want:\n%s", f.authz)
		// }

		err := signature.Sign(f.request)
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
		// _, err = sreqBuffer.WriteString(fmt.Sprintf("Authorization: %s\n\n",
		// 	authz))
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
	t := time.Now()
	for i := 0; i < b.N; i++ {
		_ = NewSignature("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
			"20110909/us-east-1/host/aws4_request", t, USEast, "service")
	}
}

func BenchmarkSignatureSign(b *testing.B) {
	b.StopTimer()
	date := time.Date(2011, time.September, 9, 0, 0, 0, 0, time.UTC)
	signature := NewSignature("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
		"20110909/us-east-1/host/aws4_request", date, USEast, "service")
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
		if i := bytes.Index(rawRequest, []byte("\n\n")); i != -1 {
			body := bytes.NewReader(rawRequest[i+2:])
			request.Body = ioutil.NopCloser(body)
		}
		b.StartTimer()
		_ = signature.Sign(request)
	}
}
