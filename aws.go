package aws

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	ISO8601BasicFormat      = "20060102T150405Z"
	ISO8601BasicFormatShort = "20060102"
)

var (
	unreserved = make([]bool, 128)
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
	hex := "0123456789ABCDEF"
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

func toHex(x []byte) []byte {
	hex := "0123456789abcdef"
	z := make([]byte, 2*len(x))
	for i, v := range x {
		z[2*i] = hex[(v&0xf0)>>4]
		z[2*i+1] = hex[v&0x0f]
	}
	return z
}

type Keys struct {
	Secret string
	Access string
}

func KeysFromEnviroment() *Keys {
	return &Keys{os.Getenv("AWS_SECRET_KEY"), os.Getenv("AWS_ACCESS_KEY")}
}

type Signature struct {
	access string
	hash   [sha256.Size]byte
}

func NewSignature(k *Keys, t time.Time, r *Region, service string) *Signature {
	var s Signature
	h := hmac.New(sha256.New, []byte("AWS4"+k.Secret))
	h.Write([]byte(t.Format(ISO8601BasicFormatShort)))
	h = hmac.New(sha256.New, h.Sum(s.hash[:0]))
	h.Write([]byte(r.Name))
	h = hmac.New(sha256.New, h.Sum(s.hash[:0]))
	h.Write([]byte(service))
	h = hmac.New(sha256.New, h.Sum(s.hash[:0]))
	h.Write([]byte("aws4_request"))
	h.Sum(s.hash[:0])
	s.access = k.Access
	return &s
}

func (s *Signature) Sign(r *http.Request, access string) error {
	// TODO check all error cases first
	// TODO compare request date to signature date? or have signature update
	// when it expires? or have whatever is using signature check it?

	// create canonical request
	var crb bytes.Buffer // canonical request buffer

	// 1
	crb.WriteString(r.Method)
	crb.WriteByte('\n')

	// 2
	var cp bytes.Buffer // canonical path
	parts := strings.Split(path.Clean(r.URL.Path)[1:], "/")
	for i := range parts {
		cp.WriteByte('/')
		cp.WriteString(encode(parts[i]))
	}
	crb.Write(cp.Bytes())
	crb.WriteByte('\n')

	// 3
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(query))
	for i := range query {
		keys = append(keys, i)
	}
	sort.Strings(keys)
	var cqs bytes.Buffer // canonical query string
	for i := range keys {
		if i > 0 {
			cqs.WriteByte('&')
		}
		parameters := query[keys[i]]
		sort.Strings(parameters)
		for j := range parameters {
			if j > 0 {
				cqs.WriteByte('&')
			}
			cqs.WriteString(encode(keys[i]))
			cqs.WriteByte('=')
			cqs.WriteString(encode(parameters[j]))
		}
	}
	if cqs.Len() > 0 {
		crb.Write(cqs.Bytes())
	}
	crb.WriteByte('\n')

	// 4
	// TODO check for date and add if required
	headers := make([]string, 0, len(r.Header)+1)
	headersMap := make(map[string]string)
	for i := range r.Header {
		header := strings.ToLower(i)
		headers = append(headers, header)
		headersMap[header] = i
	}
	headers = append(headers, "host")
	sort.Strings(headers)
	for i := range headers {
		crb.WriteString(headers[i])
		crb.WriteByte(':')
		var value string
		if headers[i] == "host" {
			value = r.Host
		} else {
			values := r.Header[headersMap[headers[i]]]
			sort.Strings(values)
			value = strings.Join(values, ",")
		}
		crb.WriteString(value)
		crb.WriteByte('\n')
	}
	crb.WriteByte('\n')

	// 5
	crb.WriteString(strings.Join(headers, ";"))
	crb.WriteByte('\n')

	// 6
	hash := sha256.New()
	if r.Body != nil {
		io.Copy(hash, r.Body)
	}
	var hashed [sha256.Size]byte
	crb.Write(toHex(hash.Sum(hashed[:0])))

	// create string to sign
	var sts bytes.Buffer

	// 1
	sts.WriteString("AWS4-HMAC-SHA256\n")

	// 2
	var dateTime time.Time
	dates, ok := r.Header["Date"]
	if !ok || len(dates) < 1 {
		dateTime = time.Now().UTC()
		r.Header.Set("Date", dateTime.Format(time.RFC3339))
	} else {
		dateTime, err = time.Parse(time.RFC1123, dates[0])
		if err != nil {
			return err
		}
	}
	sts.WriteString(dateTime.Format(ISO8601BasicFormat))
	sts.WriteByte('\n')

	// 3
	sts.WriteString(access)
	sts.WriteByte('\n')

	// 4
	hash.Reset()
	hash.Write(crb.Bytes())
	sts.Write(toHex(hash.Sum(hashed[:0])))

	// sign string and write to authorization header
	var authz bytes.Buffer
	authz.WriteString("AWS4-HMAC-SHA256 Credential=")
	authz.WriteString(s.access)
	authz.WriteByte('/')
	authz.WriteString(access)
	authz.WriteString(", SignedHeaders=")
	for i := range headers {
		if i > 0 {
			authz.WriteByte(';')
		}
		authz.WriteString(headers[i])
	}
	authz.WriteString(", Signature=")
	h := hmac.New(sha256.New, s.hash[:])
	h.Write(sts.Bytes())
	authz.Write(toHex(h.Sum(hashed[:0])))

	r.Header.Add("Authorization", authz.String())

	return nil
}
