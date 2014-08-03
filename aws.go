package aws

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
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

// Tests if a character requires encoding in a URI.
//
func charRequiresEncoding(c byte) bool {
	return c > 127 || !unreserved[c]
}

// Tests if a character requires encoding in a URI.
//
func stringRequiresEncoding(s string) bool {
	for _, c := range []byte(s) {
		if charRequiresEncoding(c) {
			return true
		}
	}
	return false
}

// URI encode a string.  Characters that are not in `unreserved` are
// replaced with their hex encoding preceded by a '%' character.
//
// Note:
//  * Based on https://launchpad.net/goamz
//
func uriEncodeString(s string) string {

	// If `s` does not require encoding, we're done.
	if !stringRequiresEncoding(s) {
		return s
	}

	// Encode `s`.  `e` is initialized to 3x the size of `s`
	// because that is the max possible length of the hex
	// encoded copy of `s`, assuming each character needed to
	// be encoded.
	e := make([]byte, len(s)*3)
	ei := 0
	const hex = "0123456789ABCDEF"
	for _, c := range []byte(s) {

		if charRequiresEncoding(c) {

			// This character requires encoding.
			e[ei] = '%'
			e[ei+1] = hex[c>>4]
			e[ei+2] = hex[c&0xF]
			ei += 3

		} else {

			// This character does not require encoding.
			e[ei] = c
			ei += 1
		}
	}
	return string(e[:ei])
}

// Return a new copy of the input byte array that is
// hex encoded.
//
func toHex(x []byte) []byte {
	z := make([]byte, 2*len(x))
	hex.Encode(z, x)
	return z
}

// Get secret and access ID keys (in that order) from environment variables
// AWS_SECRET_KEY and AWS_ACCESS_KEY.
func KeysFromEnviroment() (string, string) {
	return os.Getenv("AWS_SECRET_KEY"), os.Getenv("AWS_ACCESS_KEY")
}

// Get secret and access ID keys (in that order) from a file.
func KeysFromFile(name string) (string, string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	var secret, access string
	_, err = fmt.Fscan(file, &secret, &access)
	return secret, access, err
}

// Signature contains the access ID key, UTC date in YYYYMMDD format, region,
// service name, and the signing key.
type Signature struct {
	AccessID   string
	Date       string
	Region     *Region
	Service    string
	SigningKey [sha256.Size]byte
	NewKeys    func() (string, string) // function to get keys if date changes
}

// NewSignature creates a new signature from the secret key, access key,
// region, and service with the date set to UTC now.
func NewSignature(secret, access string, r *Region, service string) *Signature {
	var s Signature

	s.AccessID = access
	s.Date = time.Now().UTC().Format(ISO8601BasicFormatShort)
	s.Region = r
	s.Service = service
	s.generateSigningKey(secret)

	return &s
}

// AWS signature Version 4 requires that you sign your message using a key that
// is derived from your secret access key rather than using the secret access
// key directly. See
// http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
//
// Note:
//  * This is a separate function so that test suite can set a custom date.
//
func (s *Signature) generateSigningKey(secret string) {

	// Get an HMAC digest of the date using a key that
	// is our AWS secret prepended with the string "AWS4".
	h := hmac.New(sha256.New, []byte("AWS4"+secret))
	h.Write([]byte(s.Date))

	// Get an HMAC digest of the region name using a key that
	// is the HMAC digest computed in the previous step.
	h = hmac.New(sha256.New, h.Sum(nil))
	h.Write([]byte(s.Region.Name))

	// Repeat for service name.
	h = hmac.New(sha256.New, h.Sum(nil))
	h.Write([]byte(s.Service))

	// Repeat for the string "aws4_request".
	h = hmac.New(sha256.New, h.Sum(nil))
	h.Write([]byte("aws4_request"))

	// Copy this HMAC into the s.SigningKey byte array.
	h.Sum(s.SigningKey[:0])
}

// Sign uses signature s to sign the HTTP request. It sets the Authorization
// header and sets / overwrites the Date header for now.
// If the signature was created on a different UTC day the signing will be
// invalid.
// The optional payload allows for various methods to hash the request's body.
// If no payload is supplied the body is copyed into memory to be hashed.
//
// Possible errors are an invalid URL query parameters (url.EscapeError), if
// the date header isn't in time.RFC1123 format (*time.ParseError), or an error
// when calculating the payload's hash.
func (s *Signature) Sign(r *http.Request, payload Payload) error {
	// TODO check if header already has hash instead of parameter
	// TODO check all error cases first

	// If the date has changed and we sill have access to the secret and access
	// keys create a new signing key.
	//
	if today := time.Now().UTC().Format(ISO8601BasicFormatShort); s.NewKeys != nil && s.Date != today {
		access, secret := s.NewKeys()
		s.AccessID = access
		s.Date = today
		s.generateSigningKey(secret)
	}

	// Create the canonical request, which is the string we must sign
	// with our derived key. See
	// http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
	//
	var crb bytes.Buffer // canonical request buffer

	// 1 - Start with the HTTP request method (GET, PUT, POST, etc.).
	//
	crb.WriteString(r.Method)
	crb.WriteByte('\n')

	// 2 - Add the CanonicalURI parameter. This is the URI-encoded version
	// of the absolute path component of the URI—everything from the HTTP host
	// header to the question mark character ('?') that begins the query string
	// parameters.
	//
	var cp bytes.Buffer // canonical path
	parts := strings.Split(path.Clean(r.URL.Path)[1:], "/")
	for i := range parts {
		cp.WriteByte('/')
		cp.WriteString(uriEncodeString(parts[i]))
	}
	crb.Write(cp.Bytes())
	crb.WriteByte('\n')

	// 3 - Add the CanonicalQueryString parameter. If the request does not
	// include a query string, set the value of CanonicalQueryString to an empty
	// string.
	//
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
			cqs.WriteString(uriEncodeString(keys[i]))
			cqs.WriteByte('=')
			cqs.WriteString(uriEncodeString(parameters[j]))
		}
	}
	if cqs.Len() > 0 {
		crb.Write(cqs.Bytes())
	}
	crb.WriteByte('\n')

	// 4 - Add the CanonicalHeaders parameter, which is a list of all the HTTP
	// headers for the request. You must include a valid host header. Any other
	// required headers are described by the service you're using.
	//
	// TODO:
	//   * check for date and add if required
	//
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

	// 5 - Add the SignedHeaders parameter, which is the list of HTTP headers
	// that you included in the canonical headers. You must include a list of
	// signed headers because extra headers are often added to the request by
	// the transport layers. The list of signed headers enables AWS to determine
	// which headers are part of your original request.
	//
	crb.WriteString(strings.Join(headers, ";"))
	crb.WriteByte('\n')

	// 6 - Add the payload, which you derive from the body of the HTTP
	// or HTTPS request.
	//
	if payload == nil {
		var mem []byte
		if r.Body != nil {
			mem, err = ioutil.ReadAll(r.Body)
			if err != nil {
				return err
			}
			err = r.Body.Close()
			if err != nil {
				return err
			}
		}
		payload = MemoryPayload(mem)
	}
	body, hash, err := payload.Payload()
	if err != nil {
		return err
	}
	if body != nil {
		r.Body = body
	}
	crb.Write(toHex(hash))

	// Create the string to sign, which includes meta information about our
	// request and the canonical request that we just created. See
	// http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
	//
	var sts bytes.Buffer // string to sign

	// 1 - Start with the Algorithm designation, followed by a newline
	// character.
	//
	sts.WriteString("AWS4-HMAC-SHA256\n")

	// 2 - Append the RequestDate value, which is specified by using the ISO8601
	// Basic format via the x-amz-date header in the YYYYMMDD'T'HHMMSS'Z'
	// format. This value must match the value you used in any previous steps.
	//
	// Note:
	//  * Parsing data just for test suite.
	//
	// TODO:
	//  * Could be different day than signature. We should probably return an
	//    error here instead of incurring the cost of sending the request to aws
	//    and have it return an error.
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

	// 3 - Append the CredentialScope value, which is a string that includes the
	// date, the region you are targeting, the service you are requesting, and a
	// termination string ("aws4_request") in lowercase characters. The region
	// and service name strings must be UTF-8 encoded.
	//
	credential := s.Date + "/" + s.Region.Name + "/" + s.Service + "/aws4_request"
	sts.WriteString(credential)
	sts.WriteByte('\n')

	// 4 - Append the hashed canonical request. The hashed canonical request
	// must be lowercase base-16 encoded, as defined by Section 8 of RFC 4648.
	//
	hasher := sha256.New()
	var hashed [sha256.Size]byte
	hasher.Write(crb.Bytes())
	sts.Write(toHex(hasher.Sum(hashed[:0])))

	// Add authorization parameters that AWS uses to ensure the validity and
	// authenticity of the request.
	//
	var authz bytes.Buffer

	// Algorithm: The method used to sign the request. For signature version 4,
	// use the value AWS4-HMAC-SHA256.
	//
	authz.WriteString("AWS4-HMAC-SHA256")

	// Credential: A slash('/')-separated string that is formed by concatenating
	// your Access Key ID and your credential scope components. Credential scope
	// comprises the date (YYYYMMDD), the AWS region, the service name, and a
	// special termination string (aws4_request).
	//
	authz.WriteString(" Credential=")
	authz.WriteString(s.AccessID)
	authz.WriteByte('/')
	authz.WriteString(credential)

	// SignedHeaders: A semicolon(';')-delimited list of HTTP headers to include
	// in the signature.
	//
	authz.WriteString(", SignedHeaders=")
	for i := range headers {
		if i > 0 {
			authz.WriteByte(';')
		}
		authz.WriteString(headers[i])
	}

	// Signature: A hexadecimal-encoded string from your derived signing key and
	// your string to sign as inputs to the keyed hash function that you use to
	// calculate the signature.
	//
	authz.WriteString(", Signature=")
	h := hmac.New(sha256.New, s.SigningKey[:])
	h.Write(sts.Bytes())
	authz.Write(toHex(h.Sum(hashed[:0])))

	r.Header.Add("Authorization", authz.String())
	return nil
}

// Provides control over how the payload hash is calculated. If a payload is
// supplied to Sign the returned ReadCloser (if one is returned) is the new
// requests body.
//
// The payload of an AWS request must be hashed and the http requests body is
// only a reader, there is no way to rewind it.
type Payload interface {
	Payload() (io.ReadCloser, []byte, error)
}

type memoryPayload struct {
	mem []byte
}

// Returns a payload that hashes the memory and then uses it as the requests
// body.
func MemoryPayload(m []byte) Payload {
	return &memoryPayload{m}
}

func (v *memoryPayload) Payload() (io.ReadCloser, []byte, error) {
	hasher := sha256.New()
	hasher.Write(v.mem)
	return ioutil.NopCloser(bytes.NewReader(v.mem)), hasher.Sum(nil), nil
}

type readSeekerPayload struct {
	rs io.ReadSeeker
}

// Returns a payload that reads the ReadSeeker to hash it and then reads it
// again when sending the request.
func ReadSeekerPayload(rs io.ReadSeeker) Payload {
	return &readSeekerPayload{rs}
}

func (v *readSeekerPayload) Payload() (io.ReadCloser, []byte, error) {
	_, err := v.rs.Seek(0, 0)
	if err != nil {
		return nil, nil, err
	}
	hasher := sha256.New()
	_, err = io.Copy(hasher, v.rs)
	if err != nil {
		return nil, nil, err
	}
	_, err = v.rs.Seek(0, 0)
	if err != nil {
		return nil, nil, err
	}
	return ioutil.NopCloser(v.rs), hasher.Sum(nil), nil
}

type hashedPayload struct {
	sum []byte
}

// Returns a payload that just returns the precomputed hash. The requests body
// is untouched.
func HashedPayload(h []byte) Payload {
	return &hashedPayload{h}
}

func (v *hashedPayload) Payload() (io.ReadCloser, []byte, error) {
	return nil, v.sum, nil
}
