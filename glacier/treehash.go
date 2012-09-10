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
	outIndex := 0

	// generate hashes for 1 MiB chunks
	n, err := io.CopyN(hasher, r, MiB)
	for err == nil {
		hashes = append(hashes, treeHash{})
		hasher.Sum(hashes[outIndex].Hash[:0])
		hasher.Reset()
		outIndex++
		n, err = io.CopyN(hasher, r, MiB)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n > 0 {
		hashes = append(hashes, treeHash{})
		hasher.Sum(hashes[outIndex].Hash[:0])
		hasher.Reset()
		outIndex++
	}

	// build tree
	// TODO calculate levels remaining and grow once
	childIndex := 0
	added := outIndex
	remainderIndex := -1
	for added > 1 || remainderIndex != -1 {
		children := added
		added = 0
		// pair up 
		for children > 1 {
			hashes = append(hashes, treeHash{})
			hashes[outIndex].Left = &hashes[childIndex]
			hashes[outIndex].Right = &hashes[childIndex+1]
			hasher.Write(toHex(hashes[childIndex].Hash[:]))
			hasher.Write(toHex(hashes[childIndex+1].Hash[:]))
			hasher.Sum(hashes[outIndex].Hash[:0])
			hasher.Reset()
			childIndex += 2
			children -= 2
			outIndex++
			added++
		}
		if children == 1 {
			// have a remainder that couldn't be paired up
			if remainderIndex == -1 {
				// hold on to remainder for later
				remainderIndex = childIndex
				childIndex++
			} else {
				// join with remainder from a previous level
				hashes = append(hashes, treeHash{})
				hashes[outIndex].Left = &hashes[childIndex]
				hashes[outIndex].Right = &hashes[remainderIndex]
				hasher.Write(toHex(hashes[childIndex].Hash[:]))
				hasher.Write(toHex(hashes[remainderIndex].Hash[:]))
				hasher.Sum(hashes[outIndex].Hash[:0])
				hasher.Reset()
				outIndex++
				remainderIndex = -1
				added++
				childIndex++
			}
		}
	}

	return &hashes[outIndex-1], nil
}
