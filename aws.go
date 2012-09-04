package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

var (
	unreserved = make([]bool, 128)
	hex        = "0123456789ABCDEF"
)

func init() {
	// RFC3986
	u := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890-_.~"
	for _, c := range u {
		unreserved[c] = true
	}
}

// from https://launchpad.net/goamz
func encode(s string) string {
	encode := false
	for i := 0; i != len(s); i++ {
		c := s[i]
		if c > 127 || !unreserved[c] {
			encode = true
			break
		}
	}
	if !encode {
		return s
	}
	e := make([]byte, len(s)*3)
	ei := 0
	for i := 0; i != len(s); i++ {
		c := s[i]
		if c > 127 || !unreserved[c] {
			e[ei] = '%'
			e[ei+1] = hex[c>>4]
			e[ei+2] = hex[c&0xF]
			ei += 3
		} else {
			e[ei] = c
			ei += 1
		}
	}
	return string(e[:ei])
}

// http://docs.amazonwebservices.com/general/latest/gr/sigv4-create-canonical-request.html
func CreateCanonicalRequest(r *http.Request) ([]byte, error) {
	var crb bytes.Buffer // canonical request buffer

	// 1
	_, err := crb.WriteString(r.Method)
	if err != nil {
		return nil, err
	}
	err = crb.WriteByte('\n')
	if err != nil {
		return nil, err
	}

	// 2
	_, err = crb.WriteString(r.URL.Path)
	if err != nil {
		return nil, err
	}
	err = crb.WriteByte('\n')
	if err != nil {
		return nil, err
	}

	// 3
	// TODO another buffer to avoid all the string allocation
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0)
	for i := range query {
		keys = append(keys, i)
	}
	sort.Strings(keys)
	var cqs string // canonical query string
	for i := range keys {
		if i > 0 {
			cqs = cqs + "&"
		}
		parameters := query[keys[i]]
		sort.Strings(parameters)
		for j := range parameters {
			if j > 0 {
				cqs = cqs + "&"
			}
			cqs = cqs + encode(keys[i]) + "=" + encode(parameters[j])
		}
	}
	if len(cqs) > 0 {
		_, err = crb.WriteString(cqs)
		if err != nil {
			return nil, err
		}
		err = crb.WriteByte('\n')
		if err != nil {
			return nil, err
		}
	}

	// 4
	// TODO check for data and add if required
	headers := make([]string, 0)
	headersMap := make(map[string]string)
	for i := range r.Header {
		header := strings.ToLower(strings.TrimSpace(i))
		headers = append(headers, header)
		headersMap[header] = i
	}
	sort.Strings(headers)
	for i := range headers {
		_, err = crb.WriteString(headers[i])
		if err != nil {
			return nil, err
		}
		err = crb.WriteByte(':')
		if err != nil {
			return nil, err
		}
		value := strings.Join(r.Header[headersMap[headers[i]]], ",")
		_, err := crb.WriteString(value)
		err = crb.WriteByte('\n')
		if err != nil {
			return nil, err
		}
	}

	// 5
	err = crb.WriteByte('\n')
	if err != nil {
		return nil, err
	}
	_, err = crb.WriteString(strings.Join(headers, ";"))
	if err != nil {
		return nil, err
	}
	err = crb.WriteByte('\n')
	if err != nil {
		return nil, err
	}

	// 6
	hash := sha256.New()
	_, err = io.Copy(hash, r.Body)
	if err != nil {
		return nil, err
	}
	var hashed [sha256.Size]byte
	_, err = fmt.Fprintf(&crb, "%x", hash.Sum(hashed[:0]))
	if err != nil {
		return nil, err
	}

	return crb.Bytes(), nil
}
