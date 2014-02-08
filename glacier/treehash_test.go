package glacier

import (
	"testing"
)

const (
	MiB = 1024 * 1024
)

func TestTreeHash(t *testing.T) {
	out1 := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	out2 := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	th := NewTreeHash()
	th.Write([]byte("Hello World"))
	th.Close()
	if result := toHex(th.TreeHash()); out1 != result {
		t.Fatal("tree hash, wanted:", out1, "got:", result)
	}
	if result := toHex(th.Hash()); out2 != result {
		t.Fatal("hash of entire file, wanted:", out2, "got:", result)
	}

	out3 := "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	out4 := "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360"
	th.Reset()
	for i := 0; i < MiB; i++ {
		th.Write([]byte{'a'})
	}
	th.Close()
	if result := toHex(th.TreeHash()); out3 != result {
		t.Fatal("tree hash, wanted:", out3, "got:", result)
	}
	if result := toHex(th.Hash()); out4 != result {
		t.Fatal("hash of entire file, wanted:", out4, "got:", result)
	}

	out5 := "560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa"
	out6 := "5256ec18f11624025905d057d6befb03d77b243511ac5f77ed5e0221ce6d84b5"
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
	if result := toHex(th.TreeHash()); out5 != result {
		t.Fatal("tree hash, wanted:", out5, "got:", result)
	}
	if result := toHex(th.Hash()); out6 != result {
		t.Fatal("hash of entire file, wanted:", out6, "got:", result)
	}
}

func BenchmarkTreeHash(b *testing.B) {
	b.StopTimer()
	data := make([]byte, 1024)
	for i := range data {
		data[i] = 'a'
	}
	th := NewTreeHash()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		n, err := th.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(n))
	}
	th.Close()
}
