package aws

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

func TestSignature(t *testing.T) {
	type testData struct {
		base string
		req  []byte
		sreq []byte

		request *http.Request
		body    io.ReadSeeker
	}

	date := time.Date(2011, time.September, 9, 0, 0, 0, 0, time.UTC)
	keys := &Keys{"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE"}
	signature := NewSignature(keys, date, USEast, "host")
	dir := "aws4_testsuite"

	d, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Readdirnames(0)
	if err != nil {
		t.Fatal(err)
	}

	sort.Strings(f)

	files := make([]string, 0)
	for i := 0; i < len(f)-1; {
		if filepath.Ext(f[i]) == ".req" &&
			filepath.Ext(f[i+1]) == ".sreq" {
			files = append(files, f[i][:len(f[i])-4])
			i += 2
		} else {
			i++
		}
	}

	tests := make([]*testData, 0)
	for _, f := range files {
		var err error
		d := new(testData)
		d.base = f

		// read in the raw request and convert it to go's internal format
		d.req, err = ioutil.ReadFile(dir + "/" + f + ".req")
		if err != nil {
			t.Error("reading", d.base, err)
			continue
		}
		// go doesn't like post requests with spaces in them
		if d.base == "post-vanilla-query-nonunreserved" ||
			d.base == "post-vanilla-query-space" ||
			d.base == "get-slashes" {
			// skip tests with spacing in URLs or invalid escapes or
			// trailing slashes
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

		d.sreq, err = ioutil.ReadFile(dir + "/" + f + ".sreq")
		if err != nil {
			t.Error("reading", d.base, err)
			continue
		}

		tests = append(tests, d)
	}

	for _, f := range tests {
		err := signature.Sign(f.request,
			"20110909/us-east-1/host/aws4_request")
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
	keys := &Keys{"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE"}
	t := time.Now()
	for i := 0; i < b.N; i++ {
		_ = NewSignature(keys, t, USEast, "service")
	}
}

func BenchmarkSignatureSign(b *testing.B) {
	b.StopTimer()
	keys := &Keys{"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "AKIDEXAMPLE"}
	date := time.Date(2011, time.September, 9, 0, 0, 0, 0, time.UTC)
	signature := NewSignature(keys, date, USEast, "service")
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
		_ = signature.Sign(request,
			"20110909/us-east-1/host/aws4_request")
	}
}
