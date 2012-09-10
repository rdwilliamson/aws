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
	outIndex := 0

	// generate hashes for 1 MiB chunks
	n, err := io.CopyN(hasher, r, MiB)
	for err == nil {
		hashes = append(hashes, treeHash{})
		hasher.Sum(hashes[outIndex].Hash[:0])
		hasher.Reset()
		outIndex++
		fmt.Printf("%x (%d)\n", hashes[outIndex-1].Hash[:4], outIndex-1)
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
		fmt.Printf("%x (%d)\n", hashes[outIndex-1].Hash[:4], outIndex-1)
	}

	// build tree
	childIndex := 0
	added := outIndex
	remainderIndex := -1
	for added > 1 || remainderIndex != -1 {
		children := added
		fmt.Println("children", children)
		added = 0
		// pair up 
		for children > 1 {
			hashes = append(hashes, newTreeHash(&hashes[childIndex], &hashes[childIndex+1]))
			childIndex += 2
			children -= 2
			outIndex++
			added++
			fmt.Printf("%x (%d -> %d %d)\n", hashes[outIndex-1].Hash[:4], outIndex-1, childIndex-2, childIndex-1)
		}
		if children == 1 {
			if remainderIndex == -1 {
				remainderIndex = childIndex
				fmt.Println("added remainder", remainderIndex)
				childIndex++
			} else {
				fmt.Println("1 child and remainder")
				hashes = append(hashes, newTreeHash(&hashes[childIndex], &hashes[remainderIndex]))
				outIndex++
				fmt.Printf("%x (%d -> %d %d)\n", hashes[outIndex-1].Hash[:4], outIndex-1, childIndex, remainderIndex)
				remainderIndex = -1
				added++
				childIndex++
			}
		}
	}
	fmt.Println()

	return &hashes[outIndex-1], nil
}

func newTreeHash(left, right *treeHash) treeHash {
	h := sha256.New()
	h.Write(toHex(left.Hash[:]))
	h.Write(toHex(right.Hash[:]))
	var hash [sha256.Size]byte
	h.Sum(hash[:0])
	return treeHash{hash, left, right}
}
