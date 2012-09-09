package glacier

import (
	"bytes"
	"fmt"
	"testing"
)

func TestTreeHash(t *testing.T) {
	var in bytes.Buffer
	in.WriteString("Hello World")
	out := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	tree, err := createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", tree.Hash); got != out {
		t.Fatal("wanted:", out, "got:", got)
	}

	in.Reset()
	for i := 0; i < MiB; i++ {
		in.WriteByte('a')
	}
	out = "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", tree.Hash); got != out {
		t.Fatal("wanted:", out, "got:", got)
	}

	in.Reset()
	for i := 0; i < MiB; i++ {
		in.WriteByte('a')
	}
	in.WriteString("Hello World")
	out = "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", tree.Hash); got != out {
		t.Fatal("wanted:", out, "got:", got)
	}
}
