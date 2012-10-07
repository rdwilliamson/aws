package glacier

import (
	"crypto/sha256"
	"hash"
	"io"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
)

type treeHashNode struct {
	hash  [sha256.Size]byte
	left  *treeHashNode
	right *treeHashNode
}

type TreeHash struct {
	whole   hash.Hash
	part    hash.Hash
	hashers io.Writer
	nodes   []treeHashNode
	written int
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
		n, _ = th.hashers.Write(p)
		th.written += n
		return
	}

	// fill remaining chunk
	th.written, _ = th.hashers.Write(p[:1024*1024-th.written])
	n += th.written
	p = p[th.written:]
	th.nodes = append(th.nodes, treeHashNode{})
	th.part.Sum(th.nodes[len(th.nodes)-1].hash[:0])
	th.part.Reset()
	th.written = 0

	// write all full chunks
	for len(p) > 1024*1024 {
		th.written, _ = th.hashers.Write(p[:1024*1024-th.written])
		n += th.written
		p = p[th.written:]
		th.nodes = append(th.nodes, treeHashNode{})
		th.part.Sum(th.nodes[len(th.nodes)-1].hash[:0])
		th.part.Reset()
	}

	// write remaining
	th.written, _ = th.hashers.Write(p)
	n += th.written

	return
}

func (th *TreeHash) Close() error {
	// create last node
	if th.written > 0 {
		th.nodes = append(th.nodes, treeHashNode{})
		th.part.Sum(th.nodes[len(th.nodes)-1].hash[:0])
		th.part.Reset()
	}

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
			th.nodes[outIndex].left = &th.nodes[childIndex]
			th.nodes[outIndex].right = &th.nodes[childIndex+1]
			th.part.Write(th.nodes[childIndex].hash[:])
			th.part.Write(th.nodes[childIndex+1].hash[:])
			th.part.Sum(th.nodes[outIndex].hash[:0])
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
				th.nodes[outIndex].left = &th.nodes[childIndex]
				th.nodes[outIndex].right = remainder
				th.part.Write(th.nodes[childIndex].hash[:])
				th.part.Write(remainder.hash[:])
				th.part.Sum(th.nodes[outIndex].hash[:0])
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
	return string(toHex(th.nodes[len(th.nodes)-1].hash[:]))
}

func (th *TreeHash) Hash() string {
	return string(toHex(th.whole.Sum(nil)))
}

func (th *TreeHash) HashBytes() []byte {
	return th.whole.Sum(nil)
}

func (th *TreeHash) Reset() {
	th.whole.Reset()
	th.part.Reset()
	th.written = 0
	th.nodes = th.nodes[:0]
}
