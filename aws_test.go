package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
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
				// manually parse
				reader := bufio.NewReader(bytes.NewBuffer(d.req))
				requestLine, prefix, err := reader.ReadLine()
				if prefix {
					t.Error("parsing special", d.base, "readline prefix")
					continue
				}
				if err != nil {
					t.Error("parsing", d.base, err)
					continue
				}
				dateLine, prefix, err := reader.ReadLine()
				if prefix {
					t.Error("parsing special", d.base, "readline prefix")
					continue
				}
				if err != nil {
					t.Error("parsing", d.base, err)
					continue
				}
				hostLine, prefix, err := reader.ReadLine()
				if prefix {
					t.Error("parsing special", d.base, "readline prefix")
					continue
				}
				if err != nil {
					t.Error("parsing", d.base, err)
					continue
				}
				i0 := 0
				for i0 < len(requestLine) {
					if requestLine[i0] == ' ' {
						break
					}
					i0++
				}
				method := string(requestLine[:i0])
				i0++
				i1, i2 := i0, i0
				for i2 < len(requestLine) {
					if requestLine[i2] == ' ' {
						i1 = i2
					}
					i2++
				}
				urlStr := string(requestLine[i0:i1])
				d.request, err = http.NewRequest(method, urlStr, nil)
				if err != nil {
					t.Error("parsing", d.base, err)
					continue
				}
				t.Log(string(hostLine), string(dateLine))
				// continue
			} else {
				// go doesn't like lowercase http
				fixed := bytes.Replace(d.req, []byte("http"), []byte("HTTP"), 1)
				reader := bufio.NewReader(bytes.NewBuffer(fixed))
				d.request, err = http.ReadRequest(reader)
				if err != nil {
					t.Error("parsing", d.base, "request", err)
					continue
				}
			}

			ch <- d
		}
		close(ch)
	}()
	return ch
}

func TestCreateCanonicalRequest(t *testing.T) {
	files, err := testFiles(v4dir)
	if err != nil {
		t.Fatal(err)
	}
	tests := readTestFiles(files, t)
	for f := range tests {
		fmt.Println(f.base)
		fmt.Println(f.request)
	}
}
