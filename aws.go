package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
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
	// go's path.Clean will remove the trailing slash, if one exists, check if
	// it will need to be readded
	var ts bool
	if r.URL.Path[len(r.URL.Path)-1] == '/' {
		for i := len(r.URL.Path) - 2; i > 0; i-- {
			if r.URL.Path[i] != '/' && r.URL.Path[i] != '.' {
				ts = true
				break
			}
		}
	}
	var cp string // canonical path
	parts := strings.Split(path.Clean(r.URL.Path)[1:], "/")
	for i := range parts {
		cp += "/" + encode(parts[i])
	}
	if ts {
		cp += "/"
	}
	_, err = crb.WriteString(cp)
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
	}
	err = crb.WriteByte('\n')
	if err != nil {
		return nil, err
	}

	// 4
	// TODO check for date and add if required
	headers := make([]string, 0)
	headersMap := make(map[string]string)
	for i := range r.Header {
		header := strings.ToLower(strings.TrimSpace(i))
		headers = append(headers, header)
		headersMap[header] = i
	}
	headers = append(headers, "host")
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
		var value string
		if headers[i] == "host" {
			value = r.Host
		} else {
			values := r.Header[headersMap[headers[i]]]
			sort.Strings(values)
			value = strings.Join(values, ",")
		}
		_, err := crb.WriteString(value)
		err = crb.WriteByte('\n')
		if err != nil {
			return nil, err
		}
	}
	err = crb.WriteByte('\n')
	if err != nil {
		return nil, err
	}

	// 5
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

func CreateStringToSign(cr []byte, date, cs string) ([]byte, error) {
	var sts bytes.Buffer

	// 1
	_, err := sts.WriteString("AWS4-HMAC-SHA256\n")
	if err != nil {
		return nil, err
	}

	// 2
	// TODO get date into correct format
	_, err = sts.WriteString(date + "\n")
	if err != nil {
		return nil, err
	}

	// 3
	_, err = sts.WriteString(cs)
	if err != nil {
		return nil, err
	}
	err = sts.WriteByte('\n')
	if err != nil {
		return nil, err
	}

	hash := sha256.New()
	hash.Write(cr)
	var hashed [sha256.Size]byte
	_, err = fmt.Fprintf(&sts, "%x", hash.Sum(hashed[:0]))
	if err != nil {
		return nil, err
	}
	return sts.Bytes(), nil
}
