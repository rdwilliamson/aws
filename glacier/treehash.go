package glacier

import (
	"crypto/sha256"
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

	n, err := io.CopyN(hasher, r, MiB)
	for err == nil {
		hashes = append(hashes, treeHash{})
		hasher.Sum(hashes[i].Hash[:0])
		hasher.Reset()
		i++
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
	}

	// TODO return top of tree
	return &hashes[0], nil
}
