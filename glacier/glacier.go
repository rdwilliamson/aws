package glacier

import (
	"encoding/gob"
	"fmt"
	"github.com/rdwilliamson/aws"
	"io"
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

func NewConnection(secret, access string, r *aws.Region) *Connection {
	// TODO a go routine to create a new signature when the date changes?
	return &Connection{defaultClient, aws.NewSignature(secret, access,
		r, "glacier")}
}

func GetTreeHash(r io.Reader) (string, string, error) {
	treeHash, hash, err := createTreeHash(r)
	if err != nil {
		return "", "", err
	}
	return string(toHex(treeHash.Hash[:])), string(toHex(hash)), nil
}

func CreateTreeHash(r io.Reader, w io.Writer) error {
	th, _, err := createTreeHash(r)
	if err != nil {
		return err
	}
	printTreeHash(th, 0)

	enc := gob.NewEncoder(w)
	err = enc.Encode(th)
	if err != nil {
		return err
	}

	return nil
}

func ReadTreeHash(r io.Reader) error {
	dec := gob.NewDecoder(r)
	var th treeHash
	err := dec.Decode(&th)
	if err != nil {
		return err
	}
	printTreeHash(&th, 0)
	return nil
}

func printTreeHash(th *treeHash, level int) {
	if th.Left == nil && th.Right == nil {
		fmt.Println("leaf", string(toHex(th.Hash[:])))
	} else {
		fmt.Println(level, string(toHex(th.Hash[:])))
		level++
		printTreeHash(th.Left, level)
		printTreeHash(th.Right, level)
	}
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
