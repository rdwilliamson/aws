package glacier

import (
	"crypto/sha256"
	"fmt"
	"io"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
)

type treeHash struct {
	Hash  [sha256.Size]byte
	Left  *treeHash
	Right *treeHash
}

func createTreeHash(r io.Reader) (*treeHash, error) {
	hasher := sha256.New()
	hashes := make([]treeHash, 0)
	i := 0

	// generate hashes for 1 MiB chunks
	n, err := io.CopyN(hasher, r, MiB)
	for err == nil {
		hashes = append(hashes, treeHash{})
		hasher.Sum(hashes[i].Hash[:0])
		hasher.Reset()
		i++
		fmt.Printf("%x (%d)\n", hashes[i-1].Hash[:4], i-1)
		n, err = io.CopyN(hasher, r, MiB)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n > 0 {
		hashes = append(hashes, treeHash{})
		hasher.Sum(hashes[i].Hash[:0])
		hasher.Reset()
		i++
		fmt.Printf("%x (%d)\n", hashes[i-1].Hash[:4], i-1)
	}

	// build tree
	j := 0
	added := 0
	children := i
	for children = i; children > 1; children -= 2 {
		hashes = append(hashes, newTreeHash(&hashes[j], &hashes[j+1]))
		j += 2
		i++
		added++
		fmt.Printf("%x (%d -> %d %d)\n", hashes[i-1].Hash[:4], i-1, j-2, j-1)
	}
	if children > 0 {
		remaining = 1
		j++
		fmt.Println("one left", i, j)
	}
	fmt.Println("i, j, added, remaining", i, j, added, remaining)

	return &hashes[i-1], nil
}

func newTreeHash(left, right *treeHash) treeHash {
	h := sha256.New()
	h.Write(toHex(left.Hash[:]))
	h.Write(toHex(right.Hash[:]))
	var hash [sha256.Size]byte
	h.Sum(hash[:0])
	return treeHash{hash, left, right}
}
