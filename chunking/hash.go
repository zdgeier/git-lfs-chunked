package chunking

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"

	"github.com/zeebo/blake3"
)

// ChunkHash names the function that produces chunk IDs. Object IDs are
// always SHA-256 (they are LFS OIDs); only the chunk IDs are negotiable, so
// a server can address chunks with whatever its store already uses.
type ChunkHash string

const (
	// SHA256 is the default and the only hash a peer that predates
	// negotiation understands. An empty chunk_hash means SHA256.
	SHA256 ChunkHash = "sha256"
	// BLAKE3 is the unkeyed 256-bit BLAKE3 hash, hex-encoded.
	BLAKE3 ChunkHash = "blake3"
)

// SupportedChunkHashes lists the chunk hashes this implementation can
// produce and verify, in the order it advertises them.
var SupportedChunkHashes = []ChunkHash{SHA256, BLAKE3}

// ParseChunkHash maps a wire value to a ChunkHash; "" means SHA256.
func ParseChunkHash(s string) (ChunkHash, error) {
	switch ChunkHash(s) {
	case "", SHA256:
		return SHA256, nil
	case BLAKE3:
		return BLAKE3, nil
	}
	return "", fmt.Errorf("unsupported chunk hash %q", s)
}

// New returns a fresh hasher for h.
func (h ChunkHash) New() hash.Hash {
	if h == BLAKE3 {
		return blake3.New()
	}
	return sha256.New()
}

// Sum returns the hex chunk ID of data under h.
func (h ChunkHash) Sum(data []byte) string {
	if h == BLAKE3 {
		sum := blake3.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	return HashChunk(data)
}

// Wire returns the value to put in a chunk_hash field: empty (omitted) for
// SHA256, so default messages stay byte-identical to the original protocol.
func (h ChunkHash) Wire() string {
	if h == SHA256 {
		return ""
	}
	return string(h)
}
