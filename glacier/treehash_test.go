package glacier

import (
	"bytes"
	"testing"
)

func TestTreeHash(t *testing.T) {
	var in bytes.Buffer
	in.WriteString("Hello World")
	out1 := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	out2 := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	tree, file, err := createTreeHash(&in)
	if err != nil {
		t.Fatal(err)
	}
	treeString := string(toHex(tree.Hash[:]))
	fileString := string(toHex(file))
	if out1 != treeString {
		t.Fatal("tree hash, wanted:", out1, "got:", treeString)
	}
	if out2 != fileString {
		t.Fatal("hash of entire file, wanted:", out2, "got:", fileString)
	}

	th := NewTreeHash()
	th.Write([]byte("Hello World"))
	th.Close()
	if out1 != th.TreeHash() {
		t.Fatal("tree hash, wanted:", out1, "got:", th.TreeHash())
	}
	if out2 != th.Hash() {
		t.Fatal("hash of entire file, wanted:", out2, "got:", th.Hash())
	}

	in.Reset()
	for i := 0; i < MiB; i++ {
		in.WriteByte('a')
	}
	out3 := "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	out4 := "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	tree, file, err = createTreeHash(&in)
	treeString = string(toHex(tree.Hash[:]))
	fileString = string(toHex(file))
	if out3 != treeString {
		t.Fatal("tree hash, wanted:", out3, "got:", treeString)
	}
	if out4 != fileString {
		t.Fatal("hash of entire file, wanted:", out4, "got:", fileString)
	}

	th.Reset()
	for i := 0; i < MiB; i++ {
		th.Write([]byte{'a'})
	}
	th.Close()
	if out3 != th.TreeHash() {
		t.Fatal("tree hash, wanted:", out3, "got:", th.TreeHash())
	}
	if out4 != th.Hash() {
		t.Fatal("hash of entire file, wanted:", out4, "got:", th.Hash())
	}

	in.Reset()
	for i := 0; i < MiB*2; i++ {
		in.WriteByte('a')
	}
	out5 := "560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa"
	out6 := "5256ec18f11624025905d057d6befb03d77b243511ac5f77ed5e0221ce6d84b5"
	tree, file, err = createTreeHash(&in)
	treeString = string(toHex(tree.Hash[:]))
	fileString = string(toHex(file))
	if out5 != treeString {
		t.Fatal("tree hash, wanted:", out5, "got:", treeString)
	}
	if out6 != fileString {
		t.Fatal("hash of entire file, wanted:", out6, "got:", fileString)
	}

	th.Reset()
	data := make([]byte, 2*MiB)
	for i := range data {
		data[i] = 'a'
	}
	n, _ := th.Write(data)
	if n != len(data) {
		t.Fatal("didn't write", 2*MiB, "wrote", n)
	}
	th.Close()
	if out5 != th.TreeHash() {
		t.Fatal("tree hash, wanted:", out5, "got:", th.TreeHash())
	}
	if out6 != th.Hash() {
		t.Fatal("hash of entire file, wanted:", out6, "got:", th.Hash())
	}
}
