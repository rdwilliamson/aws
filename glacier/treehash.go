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

// TODO hash entire file at the same time
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
			hasher.Write(hashes[childIndex].Hash[:])
			hasher.Write(hashes[childIndex+1].Hash[:])
			hasher.Sum(hashes[outIndex].Hash[:0])
			hasher.Reset()
			outIndex++

			children -= 2
			childIndex += 2
			added++
		}
		if children == 1 {
			// have a remainder that couldn't be paired up
			if remainderIndex == -1 {
				// hold on to remainder for later
				remainderIndex = childIndex
				childIndex++
			} else {
				// join with existing remainder
				hashes = append(hashes, treeHash{})
				hashes[outIndex].Left = &hashes[childIndex]
				hashes[outIndex].Right = &hashes[remainderIndex]
				hasher.Write(hashes[childIndex].Hash[:])
				hasher.Write(hashes[remainderIndex].Hash[:])
				hasher.Sum(hashes[outIndex].Hash[:0])
				hasher.Reset()
				outIndex++

				remainderIndex = -1
				childIndex++
				added++
			}
		}
	}

	return &hashes[outIndex-1], nil
}

func (t *treeHash) node() string {
	name := fmt.Sprintf("\"%p\"", t)
	label := fmt.Sprintf("\tlabel = \"%s\"\n", string(toHex(t.Hash[:4])))

	node := name + " [\n" + label + "];\n"

	var edges string
	if t.Left != nil {
		left := fmt.Sprintf("\"%p\"", t.Left)
		edges += name + " -> " + left + ";\n"
	}
	if t.Right != nil {
		right := fmt.Sprintf("\"%p\"", t.Right)
		edges += name + " -> " + right + ";\n"
	}

	return node + edges
}

func (t *treeHash) dot() string {
	digraph := "digraph g {\n"
	digraph += "node [\n\tshape = box\n];\n"

	var recurse func(t *treeHash)
	recurse = func(t *treeHash) {
		if t.Left != nil {
			recurse(t.Left)
		}
		if t.Right != nil {
			recurse(t.Right)
		}
		digraph += t.node()
	}
	recurse(t)

	digraph += "{\n\trank=same;\n"
	recurse = func(t *treeHash) {
		if t.Left == nil && t.Right == nil {
			digraph += fmt.Sprintf("\t\"%p\"\n", t)
		} else {
			if t.Left != nil {
				recurse(t.Left)
			}
			if t.Right != nil {
				recurse(t.Right)
			}
		}
	}
	recurse(t)
	digraph += "}\n"

	digraph += "}\n"
	return digraph
}
