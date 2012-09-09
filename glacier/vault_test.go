package glacier

import (
	"../../aws"
	"fmt"
	"testing"
)

func TestList(t *testing.T) {
	connection := NewGlacierConnection(aws.KeysFromEnviroment(), aws.USEast)
	_, result, err := connection.ListVaults(1000, "")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result)
}
