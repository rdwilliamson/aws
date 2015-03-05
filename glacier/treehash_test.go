package glacier

import (
	"testing"
)

type thTestCase struct {
	treeHash    string
	linearHash  string
	iterations  int
	dataPerIter string
}

func TestTreeHash(t *testing.T) {
	testCases := []thTestCase{
		thTestCase{
			treeHash:    "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e",
			linearHash:  "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e",
			iterations:  1,
			dataPerIter: "Hello World",
		},
		thTestCase{
			treeHash:    "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360",
			linearHash:  "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360",
			iterations:  1 << 20,
			dataPerIter: "a",
		},
		thTestCase{
			treeHash:    "560c2c9333c719cb00cfdffee3ba293db17f58743cdd1f7e4055373ae6300afa",
			linearHash:  "5256ec18f11624025905d057d6befb03d77b243511ac5f77ed5e0221ce6d84b5",
			iterations:  2 << 20,
			dataPerIter: "a",
		},
		thTestCase{
			treeHash:    "70239f4f2ead7561f69d48b956b547edef52a1280a93c262c0b582190be7db17",
			linearHash:  "6f850bc94ae6f7de14297c01616c36d712d22864497b28a63b81d776b035e656",
			iterations:  3 << 20,
			dataPerIter: "a",
		},
		thTestCase{
			treeHash:    "daede4eb580f914dacd5e0bdf7015c937fd615c1e6c6552d25cb04a8b7219828",
			linearHash:  "34c8bdd269f89a091cf17d5d23503940e0abf61c4b6544e42854b9af437f31bb",
			iterations:  3<<20 + 1<<19,
			dataPerIter: "a",
		},
	}

	th := NewTreeHash()
	for _, testCase := range testCases {
		th.Reset()
		written := 0
		nDataPerIteration := len([]byte(testCase.dataPerIter))
		toBeWritten := testCase.iterations * nDataPerIteration
		for i := 0; i < testCase.iterations; i++ {
			n, _ := th.Write([]byte(testCase.dataPerIter))
			written += n
		}
		if written != toBeWritten {
			t.Fatal("didn't write", toBeWritten, "wrote", written)
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
