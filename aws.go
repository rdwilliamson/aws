package aws

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

// Get secret and access ID keys (in that order) from enviroment variables
// AWS_SECRET_KEY and AWS_ACCESS_KEY.
func KeysFromEnviroment() (string, string) {
	return os.Getenv("AWS_SECRET_KEY"), os.Getenv("AWS_ACCESS_KEY")
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
// is derived from your secret access key rather than using the secret access key
// directly.  See  http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html.
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
// If a hash of the body is provided it is used and the body of the request is
// left alone. If no hash is provided one is created from the ReadSeeker, this
// reads the entire body and then resets it to the beginning. If there is no
// body then neither a ReadSeeker or hash is required.
//
// Possible errors are an invalid URL query parameters (url.EscapeError) or if
// the date header isn't in time.RFC1123 format (*time.ParseError).
func (s *Signature) Sign(r *http.Request, rs io.ReadSeeker, hash []byte) error {
	// TODO check if header already has hash instead of parameter
	// TODO check all error cases first

	if today := time.Now().UTC().Format(ISO8601BasicFormatShort); s.NewKeys != nil && s.Date != today {
		access, secret := s.NewKeys()
		s.AccessID = access
		s.Date = today
		s.generateSigningKey(secret)
	}

	credential := s.Date + "/" + s.Region.Name + "/" + s.Service + "/aws4_request"

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
	// include a query string, set the value of CanonicalQueryString to an empty string.
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

	// 4 - Add the CanonicalHeaders parameter, which is a list of all
	// the HTTP headers for the request. You must include a valid host
	// header. Any other required headers are described by the service
	// you're using.
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

	// 5 - Add the SignedHeaders parameter, which is the list of HTTP
	// headers that you included in the canonical headers. You must include
	// a list of signed headers because extra headers are often added to the
	// request by the transport layers. The list of signed headers enables
	// AWS to determine which headers are part of your original request.
	//
	crb.WriteString(strings.Join(headers, ";"))
	crb.WriteByte('\n')

	// 6 - Add the payload, which you derive from the body of the HTTP
	// or HTTPS request.
	//
	hasher := sha256.New()
	var hashed [sha256.Size]byte
	if hash == nil {
		if rs != nil {
			io.Copy(hasher, rs)
			rs.Seek(0, 0)
		}
		crb.Write(toHex(hasher.Sum(hashed[:0])))
		hasher.Reset()
	} else {
		crb.Write(toHex(hash))
	}

	// create string to sign
	var sts bytes.Buffer

	// 1
	sts.WriteString("AWS4-HMAC-SHA256\n")

	// 2
	// TODO parsing dates just to pass test suite, implement such that the date
	// is just overwritten if it exists
	var dateTime time.Time
	dates, ok := r.Header["Date"]
	if !ok || len(dates) < 1 {
		dateTime = time.Now().UTC() // TODO could be different day than signature
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
	sts.WriteString(credential)
	sts.WriteByte('\n')

	// 4
	hasher.Write(crb.Bytes())
	sts.Write(toHex(hasher.Sum(hashed[:0])))

	// sign string and write to authorization header
	var authz bytes.Buffer
	authz.WriteString("AWS4-HMAC-SHA256 Credential=")
	authz.WriteString(s.AccessID)
	authz.WriteByte('/')
	authz.WriteString(credential)
	authz.WriteString(", SignedHeaders=")
	for i := range headers {
		if i > 0 {
			authz.WriteByte(';')
		}
		authz.WriteString(headers[i])
	}
	authz.WriteString(", Signature=")
	h := hmac.New(sha256.New, s.SigningKey[:])
	h.Write(sts.Bytes())
	authz.Write(toHex(h.Sum(hashed[:0])))

	r.Header.Add("Authorization", authz.String())

	return nil
}
