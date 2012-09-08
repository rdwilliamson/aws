package glacier

import (
	"../../aws"
	"fmt"
	"net/http"
	"os"
	"time"
)

type Instance struct {
	Signature *aws.Signature
	Region    *aws.Region
}

func List() error {
	request, err := http.NewRequest("GET", "/-/vaults", nil)
	request.Header.Del("User-Agent")
	if err != nil {
		fmt.Println(err)
		return err
	}
	signature := aws.NewSignature("a", "b", time.Now(), aws.USEast, "glacier")
	err = signature.Sign(request)
	if err != nil {
		fmt.Println(err)
		return err
	}

	request.Write(os.Stdout)

	return nil
}
