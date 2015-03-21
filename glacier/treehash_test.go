package glacier

import (
	"crypto/sha256"
	"testing"
)

type thTestCase struct {
	treeHash    string
	linearHash  string
	iterations  int
	dataPerIter byte
}

type mthTestCase struct {
	perPartHash string
	parts       int
	treeHash    string
}

func TestMultiTreeHasher(t *testing.T) {
	testCases := []mthTestCase{
		mthTestCase{
			perPartHash: "7d10631c4d690a8dadf9ef848c7d04307e3c1dfcbb8e0d8443dcd03f480ee9c4",
			treeHash:    "7d10631c4d690a8dadf9ef848c7d04307e3c1dfcbb8e0d8443dcd03f480ee9c4",
			parts:       1,
		},
		mthTestCase{
			perPartHash: "7d10631c4d690a8dadf9ef848c7d04307e3c1dfcbb8e0d8443dcd03f480ee9c4",
			treeHash:    "d6d88996813a8a871bc0b0a4c067bdb806bc20beeb81f365b0b7307d7dd8f103",
			parts:       2,
		},
		mthTestCase{
			perPartHash: "7d10631c4d690a8dadf9ef848c7d04307e3c1dfcbb8e0d8443dcd03f480ee9c4",
			treeHash:    "45fd17a746d04f8ade481994600f29320e181144b2aee43899cff89196fbbb6b",
			parts:       3,
		},
		mthTestCase{
			perPartHash: "7d10631c4d690a8dadf9ef848c7d04307e3c1dfcbb8e0d8443dcd03f480ee9c4",
			treeHash:    "de28773aaed0cc4ff1e41d8808f129c3ab1c98c6c8078d55fdb9ff4963cd9cad",
			parts:       4,
		},
	}

	for _, testCase := range testCases {
		mth := MultiTreeHasher{}
		for i := 0; i < testCase.parts; i++ {
			mth.Add(testCase.perPartHash)
		}
		computed := mth.CreateHash()
		if testCase.treeHash != computed {
			t.Errorf("Expected tree hash %s; got %s", testCase.treeHash, computed)
		}
	}
}

func TestTreeHash(t *testing.T) {
	th := NewTreeHash()
	th.Write([]byte("Hello World"))
	th.Close()
	treeHash := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	linearHash := "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e"
	if treeHash != toHex(th.TreeHash()) {
		t.Fatalf("Expected treehash %s; got %s", treeHash, toHex(th.TreeHash()))
	}
	if linearHash != toHex(th.Hash()) {
		t.Fatalf("Expected linear hash %s; got %s", linearHash, toHex(th.Hash()))
	}

	testCases := []thTestCase{
		thTestCase{
			treeHash:    "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360",
			linearHash:  "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360",
			iterations:  1 << 20,
			dataPerIter: 'a',
		},
		thTestCase{
			treeHash:    "560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa",
			linearHash:  "5256ec18f11624025905d057d6befb03d77b243511ac5f77ed5e0221ce6d84b5",
			iterations:  2 << 20,
			dataPerIter: 'a',
		},
		thTestCase{
			treeHash:    "70239f4f2ead7561f69d48b956b547edef52a1280a93c262c0b582190be7db17",
			linearHash:  "6f850bc94ae6f7de14297c01616c36d712d22864497b28a63b81d776b035e656",
			iterations:  3 << 20,
			dataPerIter: 'a',
		},
		thTestCase{
			treeHash:    "9491cb2ed1d4e7cd53215f4017c23ec4ad21d7050a1e6bb636c4f67e8cddb844",
			linearHash:  "299285fc41a44cdb038b9fdaf494c76ca9d0c866672b2b266c1a0c17dda60a05",
			iterations:  4 << 20,
			dataPerIter: 'a',
		},
		thTestCase{
			treeHash:    "daede4eb580f914dacd5e0bdf7015c937fd615c1e6c6552d25cb04a8b7219828",
			linearHash:  "34c8bdd269f89a091cf17d5d23503940e0abf61c4b6544e42854b9af437f31bb",
			iterations:  3<<20 + 1<<19,
			dataPerIter: 'a',
		},
	}

	th = NewTreeHash()
	for _, testCase := range testCases {
		th.Reset()
		data := make([]byte, testCase.iterations)
		for i := range data {
			data[i] = testCase.dataPerIter
		}
		n, err := th.Write(data)
		if err != nil {
			t.Fatal(err)
		}
		if n != testCase.iterations {
			t.Fatalf("treehash wrote %d, should have written %d", n, testCase.iterations)
		}
		th.Close()
		if result := toHex(th.TreeHash()); testCase.treeHash != result {
			t.Fatal("tree hash, wanted:", testCase.treeHash, "got:", result)
		}
		if result := toHex(th.Hash()); testCase.linearHash != result {
			t.Fatal("hash of entire file, wanted:", testCase.linearHash, "got:", result)
		}
	}
}

func TestTreeHashCloseEmpty(t *testing.T) {
	th := NewTreeHash()
	err := th.Close()
	if err != nil {
		t.Error(err)
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

func BenchmarkTreeHashClose(b *testing.B) {
	nodes := make([][sha256.Size]byte, 4)
	for i := range nodes {
		nodes[i] = sha256.Sum256([]byte{byte(i)})
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		treeHash(nodes)
	}
}
