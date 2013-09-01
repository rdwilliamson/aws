package aws

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Type + ": " + e.Message
}

// Attempts to parse an AWS error out of memory. See
// http://docs.aws.amazon.com/amazonglacier/latest/dev/api-error-responses.html
//
func ParseError(body []byte) error {
	awsErr := new(Error)
	err := json.Unmarshal(body, awsErr)
	if err != nil {
		return err
	}
	return awsErr
}

// Attempts to parse an AWS error out of a http response, it is still the
// caller's responsibility to close the body. See
// http://docs.aws.amazon.com/amazonglacier/latest/dev/api-error-responses.html
//
func ParseResponseError(r *http.Response) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return ParseError(body)
}
