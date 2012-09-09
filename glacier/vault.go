package glacier

import (
	"../../aws"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"
)

func (gc *GlacierConnection) ListVaults(limit int, marker string) ([]byte, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("limit must be 1 through 1000")
	}

	request, err := http.NewRequest("GET",
		"https://"+aws.USEast.Glacier+"/-/vaults", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	access := time.Now().UTC().Format(aws.ISO8601BasicFormatShort) + "/" +
		gc.Region.Name + "/glacier/aws4_request"
	err = gc.Signature.Sign(request, access)
	if err != nil {
		return nil, err
	}

	response, err := gc.Client.Do(request)
	if err != nil {
		return nil, err
	}

	// TODO log x-amzn-RequestId

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = response.Body.Close()
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body, &e)
		if err != nil {
			return nil, err
		}
		return nil, e
	}

	// TODO unmarshal json into stuct
	return body, nil
}
