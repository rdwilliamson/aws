package glacier

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

// MultiTreeHasher is used to calculate tree hashes for multi-part uploads
// Call Add sequentially on hashes you have calculated them for
// parts individually, and CreateHash to get the resulting root-level
// hash to use in a CompleteMultipart request.
type MultiTreeHasher struct {
	nodes [][sha256.Size]byte
}

// Add appends the hex-encoded hash to the treehash as a new node
// Add must be called sequentially on parts.
func (t *MultiTreeHasher) Add(hash string) {
	var b [sha256.Size]byte
	hex.Decode(b[:], []byte(hash))
	t.nodes = append(t.nodes, b)
}

// CreateHash returns the root-level hex-encoded hash to send in the
// CompleteMultipart request.
func (t *MultiTreeHasher) CreateHash() string {
	if len(t.nodes) == 0 {
		return ""
	}
	rootHash := treeHash(t.nodes)
	return hex.EncodeToString(rootHash[:])
}

// treeHash calculates the root-level treeHash given sequential
// leaf nodes.
func treeHash(nodes [][sha256.Size]byte) [sha256.Size]byte {
	curLevel := make([][sha256.Size]byte, len(nodes))
	copy(curLevel, nodes)
	for len(curLevel) > 1 {
		nextLevel := make([][sha256.Size]byte, 0)
		for i := 0; i < len(curLevel); i++ {
			if i%2 == 1 { // Concat with previous node and promote
				var sum [sha256.Size * 2]byte
				copy(sum[:sha256.Size], curLevel[i-1][:])
				copy(sum[sha256.Size:], curLevel[i][:])
				concat := sha256.Sum256(sum[:])
				nextLevel = append(nextLevel, concat)
				continue
			}
			if i == len(curLevel)-1 { // Promote last node in an odd-length level
				nextLevel = append(nextLevel, curLevel[i])
			}
		}
		curLevel = nextLevel
	}
	return curLevel[0]
}

// TreeHash is used to calculate the tree hash and regular sha256 hash of the
// data written to it. These values are needed when uploading an archive or
// verifying an aligned download. First each 1 MiB chunk of data is hashed.
// Second each consecutive child node's hashes are concatenated then hashed (if
// there is a single node left it is promoted to the next level). The second
// step is repeated until there is only a single node, this is the tree hash.
// See docs.aws.amazon.com/amazonglacier/latest/dev/checksum-calculations.html
type TreeHash struct {
	nodes       [][sha256.Size]byte
	remaining   []byte
	runningHash hash.Hash         // linear
	treeHash    [sha256.Size]byte // computed
	linearHash  []byte            //computed
}

// NewTreeHash returns an new, initialized tree hasher.
func NewTreeHash() *TreeHash {
	result := &TreeHash{}
	result.Reset()
	return result
}

// Reset the tree hash's state allowing it to be reused.
func (th *TreeHash) Reset() {
	th.runningHash = sha256.New()
	th.remaining = make([]byte, 0)
	th.nodes = make([][sha256.Size]byte, 0)
	th.treeHash = [sha256.Size]byte{}
	th.linearHash = make([]byte, 0)
}

// Write writes all of p, storing every 1 MiB of data's hash.
func (th *TreeHash) Write(p []byte) (n int, err error) {
	n = len(p)
	th.remaining = append(th.remaining, p...)

	// Append one-megabyte increments to the hashes.
	for len(th.remaining) >= (1 << 20) {
		th.nodes = append(th.nodes, sha256.Sum256(th.remaining[:1<<20]))
		th.runningHash.Write(th.remaining[:1<<20])
		th.remaining = th.remaining[1<<20:]
	}
	return
}

// Close closes the the remaing chunks of data and then calculates the tree hash.
func (th *TreeHash) Close() error {
	// create last node; it is impossible that it has a size > 1 MB
	if len(th.remaining) > 0 {
		th.nodes = append(th.nodes, sha256.Sum256(th.remaining))
		th.runningHash.Write(th.remaining)
		th.remaining = make([]byte, 0)
	}
	// Calculate the tree and linear hashes
	if len(th.nodes) > 0 {
		th.treeHash = treeHash(th.nodes)
	}
	th.linearHash = th.runningHash.Sum(nil)
	return nil
}

// TreeHash returns the root-level tree hash of everything written.
func (th *TreeHash) TreeHash() []byte {
	return th.treeHash[:]
}

// Hash returns the linear sha256 checksum of everything written.
func (th *TreeHash) Hash() []byte {
	return th.linearHash[:]
}
