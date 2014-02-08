package glacier

import (
	"encoding/hex"
	"net/http"

	"github.com/rdwilliamson/aws"
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
	z := make([]byte, 2*len(x))
	hex.Encode(z, x)
	return z
}
