package glacier

import (
	"../../aws"
	"bytes"
	"encoding/json"
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
	request, err := http.NewRequest("GET", "https://"+aws.USEast.Glacier+"/-/vaults", nil)
	request.Header.Add("x-amz-glacier-version", "2012-06-01")
	if err != nil {
		fmt.Println(err)
		return err
	}
	signature := aws.NewSignature("secret",
		"access", time.Now().UTC(), aws.USEast, "glacier")
	access := time.Now().UTC().Format(aws.ISO8601BasicFormatShort) + "/" +
		aws.USEast.Name + "/glacier/aws4_request"
	err = signature.Sign(request, access)
	if err != nil {
		fmt.Println(err)
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
