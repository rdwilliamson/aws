package glacier

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
)

type treeHashNode struct {
	Hash  [sha256.Size]byte
	Left  *treeHashNode
	Right *treeHashNode
}

type TreeHash struct {
	whole   hash.Hash
	part    hash.Hash
	hashers io.Writer
	written int
	nodes   []treeHashNode
}

func NewTreeHash() *TreeHash {
	var result TreeHash
	result.whole = sha256.New()
	result.part = sha256.New()
	result.hashers = io.MultiWriter(result.whole, result.part)
	result.nodes = make([]treeHashNode, 0)
	return &result
}

func (th *TreeHash) Write(p []byte) (n int, err error) {
	// check if we can't fill up remaining chunk
	if len(p) < 1024*1024-th.written {
		n, err = th.hashers.Write(p)
		th.written += n
		return
	}

	// fill remaining chunk
	nn, _ := th.hashers.Write(p[:1024*1024-th.written])
	n += nn
	p = p[nn:]
	th.nodes = append(th.nodes, treeHashNode{})
	th.part.Sum(th.nodes[len(th.nodes)-1].Hash[:0])
	th.part.Reset()

	// write all full chunks
	th.written = 0
	for len(p) > 1024*1024 {
		nn, _ = th.hashers.Write(p[:1024*1024-th.written])
		n += nn
		p = p[nn:]
		th.nodes = append(th.nodes, treeHashNode{})
		th.part.Sum(th.nodes[len(th.nodes)-1].Hash[:0])
		th.part.Reset()
	}

	th.written, _ = th.hashers.Write(p)
	n += th.written

	return
}

func (th *TreeHash) Close() error {
	// create last node
	th.nodes = append(th.nodes, treeHashNode{})
	th.part.Sum(th.nodes[len(th.nodes)-1].Hash[:0])
	th.part.Reset()

	// create tree
	outIndex := len(th.nodes)
	childIndex := 0
	added := outIndex
	var remainder *treeHashNode
	for added > 1 || remainder != nil {
		children := added
		added = 0
		// pair up 
		for children > 1 {
			th.nodes = append(th.nodes, treeHashNode{})
			th.nodes[outIndex].Left = &th.nodes[childIndex]
			th.nodes[outIndex].Right = &th.nodes[childIndex+1]
			th.part.Write(th.nodes[childIndex].Hash[:])
			th.part.Write(th.nodes[childIndex+1].Hash[:])
			th.part.Sum(th.nodes[outIndex].Hash[:0])
			th.part.Reset()
			outIndex++

			children -= 2
			childIndex += 2
			added++
		}
		if children == 1 {
			// have a child that couldn't be paired up
			if remainder == nil {
				// hold on to child as remainder for later
				remainder = &th.nodes[childIndex]
				childIndex++
			} else {
				// join with existing remainder
				th.nodes = append(th.nodes, treeHashNode{})
				th.nodes[outIndex].Left = &th.nodes[childIndex]
				th.nodes[outIndex].Right = remainder
				th.part.Write(th.nodes[childIndex].Hash[:])
				th.part.Write(remainder.Hash[:])
				th.part.Sum(th.nodes[outIndex].Hash[:0])
				th.part.Reset()
				outIndex++

				remainder = nil
				childIndex++
				added++
			}
		}
	}
	return nil
}

func (th *TreeHash) TreeHash() string {
	return string(toHex(th.nodes[len(th.nodes)-1].Hash[:]))
}

func (th *TreeHash) Hash() string {
	return string(toHex(th.whole.Sum(nil)))
}

func (th *TreeHash) Reset() {
	th.whole.Reset()
	th.part.Reset()
	th.written = 0
	th.nodes = th.nodes[:0]
}

// TODO hash entire file at the same time
func createTreeHash(r io.Reader) (*treeHashNode, []byte, error) {
	wholeHash := sha256.New()
	partHash := sha256.New()
	hashers := io.MultiWriter(partHash, wholeHash)
	hashes := make([]treeHashNode, 0)
	outIndex := 0

	// generate hashes for 1 MiB chunks
	n, err := io.CopyN(hashers, r, MiB)
	for err == nil {
		hashes = append(hashes, treeHashNode{})
		partHash.Sum(hashes[outIndex].Hash[:0])
		partHash.Reset()
		outIndex++
		n, err = io.CopyN(hashers, r, MiB)
	}
	if err != nil && err != io.EOF {
		return nil, nil, err
	}
	if n > 0 {
		hashes = append(hashes, treeHashNode{})
		partHash.Sum(hashes[outIndex].Hash[:0])
		partHash.Reset()
		outIndex++
	}

	// build tree
	// TODO calculate levels remaining and grow once
	childIndex := 0
	added := outIndex
	var remainder *treeHashNode
	for added > 1 || remainder != nil {
		children := added
		added = 0
		// pair up 
		for children > 1 {
			hashes = append(hashes, treeHashNode{})
			hashes[outIndex].Left = &hashes[childIndex]
			hashes[outIndex].Right = &hashes[childIndex+1]
			partHash.Write(hashes[childIndex].Hash[:])
			partHash.Write(hashes[childIndex+1].Hash[:])
			partHash.Sum(hashes[outIndex].Hash[:0])
			partHash.Reset()
			outIndex++
			children -= 2
			childIndex += 2
			added++
		}
		if children == 1 {
			// have a remainder that couldn't be paired up
			if remainder == nil {
				// hold on to remainder for later
				remainder = &hashes[childIndex]
				childIndex++
			} else {
				// join with existing remainder
				hashes = append(hashes, treeHashNode{})
				hashes[outIndex].Left = &hashes[childIndex]
				hashes[outIndex].Right = remainder
				partHash.Write(hashes[childIndex].Hash[:])
				partHash.Write(remainder.Hash[:])
				partHash.Sum(hashes[outIndex].Hash[:0])
				partHash.Reset()
				outIndex++
				remainder = nil
				childIndex++
				added++
			}
		}
	}

	return &hashes[outIndex-1], wholeHash.Sum(nil), nil
}

func (t *treeHashNode) node() string {
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

func (t *treeHashNode) dot() string {
	digraph := "digraph g {\n"
	digraph += "node [\n\tshape = box\n];\n"

	var recurse func(t *treeHashNode)
	recurse = func(t *treeHashNode) {
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
	recurse = func(t *treeHashNode) {
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
