package glacier

import (
	"bytes"
	"testing"
)

func TestTreeHash(t *testing.T) {
	var in bytes.Buffer
	in.WriteString("Hello World")
	out1 := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	tree, err := createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(toHex(tree.Hash[:])); got != out1 {
		t.Fatal("wanted:", out1, "got:", got)
	}

	in.Reset()
	for i := 0; i < MiB; i++ {
		in.WriteByte('a')
	}
	out2 := "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(toHex(tree.Hash[:])); got != out2 {
		t.Fatal("wanted:", out2, "got:", got)
	}

	in.Reset()
	for i := 0; i < MiB*2; i++ {
		in.WriteByte('a')
	}
	out4 := "560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(toHex(tree.Hash[:])); got != out4 {
		t.Fatal("wanted:", out4, "got:", got)
	}
}
