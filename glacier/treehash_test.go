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
	for i := 0; i < MiB; i++ {
		in.WriteByte('a')
	}
	in.WriteString("Hello World")
	out3 := "7a398c79d8fc266cde4766b105d56a49361b22142aaa35a22ef505660c7edf59"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(toHex(tree.Hash[:])); got != out3 {
		t.Fatal("wanted:", out3, "got:", got)
	}

	in.Reset()
	for i := 0; i < MiB*2; i++ {
		in.WriteByte('a')
	}
	out4 := "3d95be8a6b55f83b93db329b8657ef6e8496361cb7b9a882b263fb2fb6197564"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(toHex(tree.Hash[:])); got != out4 {
		t.Fatal("wanted:", out4, "got:", got)
	}

	in.Reset()
	for i := 0; i < MiB*2; i++ {
		in.WriteByte('a')
	}
	in.WriteString("Hello World")
	out5 := "7cf44f7e83180f709ad6f8376dd704609d28a117f3a1878c301bc9e78c870344"
	tree, err = createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(toHex(tree.Hash[:])); got != out5 {
		t.Fatal("wanted:", out5, "got:", got)
	}
}
