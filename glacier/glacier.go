package glacier

import (
	"github.com/rdwilliamson/aws"
	"net/http"
)

var (
	defaultClient http.Client
)

// A Connection stores and reuses a connection to an Amazon region for a user.
type Connection struct {
	Client    http.Client
	Signature *aws.Signature
}

// Creates a connection object using the default http client and a signature
// for glacier from the secret key, access key, and region.
func NewConnection(secret, access string, r *aws.Region) *Connection {
	// TODO a go routine to create a new signature when the date changes?
	return &Connection{defaultClient, aws.NewSignature(secret, access,
		r, "glacier")}
}

// TODO method to log things such as x-amzn-RequestId

func toHex(x []byte) []byte {
	hex := "0123456789abcdef"
	z := make([]byte, 2*len(x))
	for i, v := range x {
		z[2*i] = hex[(v&0xf0)>>4]
		z[2*i+1] = hex[v&0x0f]
	}
	return z
}
