package glacier

import (
	"../../aws"
	"net/http"
	"time"
)

var (
	defaultClient http.Client
)

type GlacierConnection struct {
	Client    http.Client
	Region    *aws.Region
	Signature *aws.Signature
}

func NewGlacierConnection(k *aws.Keys, r *aws.Region) *GlacierConnection {
	// TODO a go routine to create a new signature when the date changes?
	return &GlacierConnection{defaultClient, r, aws.NewSignature(k,
		time.Now().UTC(), r, "glacier")}
}

// TODO method to log things such as x-amzn-RequestId
