// Package glacier is a client library for the Amazon Glacier service.
package glacier

import (
	"fmt"
	"net/http"

	"github.com/rdwilliamson/aws"
)

// A Connection specifies the means and parameters of accessing Glacier.
type Connection struct {
	// Client optionally specifies the HTTP client to use. If nil,
	// http.DefaultClient is used.
	Client *http.Client

	Signature *aws.Signature
}

func (c *Connection) client() *http.Client {
	if c.Client == nil {
		return http.DefaultClient
	}
	return c.Client
}

// vault returns the URL prefix of the named vault, without a trailing slash.
func (c *Connection) vault(vault string) string {
	return "https://" + c.Signature.Region.Glacier + "/-/vaults/" + vault
}

// NewConnection returns a Connection with an initialized signature
// based on the provided access credentials and region.
func NewConnection(secret, access string, r *aws.Region) *Connection {
	return &Connection{
		Signature: aws.NewSignature(secret, access, r, "glacier"),
	}
}

// TODO method to log things such as x-amzn-RequestId

// toHex returns the lowercase hex encoding of x.
func toHex(x []byte) string {
	return fmt.Sprintf("%x", x)
}
