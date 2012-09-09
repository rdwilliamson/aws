package glacier

import (
	"../../aws"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestList(t *testing.T) {
	connection := NewGlacierConnection(aws.KeysFromEnviroment(), aws.USEast)
	result, err := connection.ListVaults(1000, "")
	if err != nil {
		t.Fatal(err)
	}
	var indentedBody bytes.Buffer
	json.Indent(&indentedBody, result, "", "\t")
	fmt.Println(indentedBody.String())
	fmt.Println()
}
