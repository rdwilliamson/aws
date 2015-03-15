package glacier

import (
	"os"
	"testing"

	"github.com/rdwilliamson/aws"
)

func testConnection(t *testing.T) *Connection {
	secret, access := aws.KeysFromEnviroment()
	if secret == "" || access == "" {
		t.Skipf("%s or %s is not provided.", envAWSAccess, envAWSSecret)
	}
	regionStr := os.Getenv(envGlacierRegion)
	if regionStr == "" {
		t.Skipf("%s is not provided.", envGlacierRegion)
	}
	var region *aws.Region
	for _, r := range aws.Regions {
		if r.Name == regionStr {
			region = r
			break
		}
	}
	if region == nil {
		t.Skipf("%s is invalid.", envGlacierRegion)
	}
	return NewConnection(secret, access, region)
}

func testVault(t *testing.T) string {
	vault := os.Getenv(envGlacierVault)
	if vault == "" {
		t.Skipf("%s is not provided.", envGlacierVault)
	}
	return vault
}
