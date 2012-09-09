package glacier

import (
	"../../aws"
	"net/http"
)

var (
	defaultClient http.Client
)

// A GlacierConnection stores and reuses a connection to an Amazon region for
// a user.
type GlacierConnection struct {
	Client    http.Client
	Signature *aws.Signature
}

func NewGlacierConnection(k *aws.Keys, r *aws.Region) *GlacierConnection {
	// TODO a go routine to create a new signature when the date changes?
	return &GlacierConnection{defaultClient, aws.NewSignature(k, r, "glacier")}
}

// TODO method to log things such as x-amzn-RequestId
