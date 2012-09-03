package main

import (
	"fmt"
	"io/ioutil"
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
			d.req, err = ioutil.ReadFile(v4dir + "/" + f + ".req")
			if err != nil {
				t.Error(err)
				continue
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
		// fmt.Println(string(f.req))
	}
}
