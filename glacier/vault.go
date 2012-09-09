package glacier

import (
	"../../aws"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

type Instance struct {
	Signature *aws.Signature
	Region    *aws.Region
}

func List() error {
	keys := aws.KeysFromEnviroment()
	if keys.Secret == "" || keys.Access == "" {
		return errors.New("could not get keys from enviroment variables")
	}

	signature := aws.NewSignature(keys, now, aws.USEast, "glacier")
	access := now.Format(aws.ISO8601BasicFormatShort) + "/" + aws.USEast.Name +
		"/glacier/aws4_request"

	now := time.Now().UTC()

	request, err := http.NewRequest("GET",
		"https://"+aws.USEast.Glacier+"/-/vaults", nil)
	if err != nil {
		return err
	}
	request.Header.Add("x-amz-glacier-version", "2012-06-01")

	err = signature.Sign(request, access)
	if err != nil {
		return err
	}

	request.Write(os.Stdout)

	var c http.Client
	response, err := c.Do(request)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(response)
	body, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		fmt.Println(err)
	}

	if response.StatusCode != 200 {
		var e aws.Error
		err = json.Unmarshal(body[0:len(body)], &e)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(e.Code)
		fmt.Println(e.Type)
		fmt.Println(e.Message)
	}

	var indentedBody bytes.Buffer
	json.Indent(&indentedBody, body, "", "\t")
	fmt.Println(indentedBody.String())

	fmt.Println()

	return nil
}
