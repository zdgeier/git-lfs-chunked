package chunking

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// Manifest describes how an object's content is assembled from chunks. It is
// the on-disk cache format and the core of the "chunked" transfer protocol's
// JSON messages (which add per-chunk actions on top).
type Manifest struct {
	// Oid is the SHA-256 of the whole object, i.e. the LFS object ID.
	Oid string `json:"oid"`
	// Size is the whole object's size in bytes.
	Size int64 `json:"size"`
	// Algorithm records how the chunks were produced. It is informational:
	// a reader only needs Chunks to reassemble the object.
	Algorithm *Params `json:"algorithm,omitempty"`
	// Chunks lists the chunks in stream order; they are contiguous and
	// cover the whole object.
	Chunks []ManifestChunk `json:"chunks"`
}

// ManifestChunk locates one chunk within an object.
type ManifestChunk struct {
	// Oid is the SHA-256 of the chunk's bytes, hex-encoded.
	Oid string `json:"oid"`
	// Size is the chunk's length in bytes.
	Size int64 `json:"size"`
	// Offset is the chunk's position within the object.
	Offset int64 `json:"offset"`
}

// HashChunk returns the chunk ID for data: the hex SHA-256, the same
// function LFS uses for object IDs.
func HashChunk(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Build reads r to EOF, splits it with c, and returns a manifest whose Oid
// and Size are computed from the bytes read. Callers who already know the
// object ID should compare it with the result to catch on-disk corruption.
func Build(r io.Reader, c Chunker) (*Manifest, error) {
	m := &Manifest{Chunks: []ManifestChunk{}}
	if pc, ok := c.(interface{ Params() Params }); ok {
		p := pc.Params()
		m.Algorithm = &p
	} else {
		m.Algorithm = &Params{Algorithm: c.Name()}
	}

	whole := sha256.New()
	err := c.Split(r, func(ch Chunk) error {
		whole.Write(ch.Data)
		m.Chunks = append(m.Chunks, ManifestChunk{
			Oid:    HashChunk(ch.Data),
			Size:   int64(len(ch.Data)),
			Offset: ch.Offset,
		})
		m.Size += int64(len(ch.Data))
		return nil
	})
	if err != nil {
		return nil, err
	}
	m.Oid = hex.EncodeToString(whole.Sum(nil))
	return m, nil
}

// Validate checks that the manifest is internally consistent and describes
// the object with the given oid and size: chunks must be contiguous from
// offset zero, cover exactly Size bytes, and carry well-formed IDs.
func (m *Manifest) Validate(oid string, size int64) error {
	if m.Oid != oid {
		return fmt.Errorf("chunk manifest is for object %s, expected %s", m.Oid, oid)
	}
	if m.Size != size {
		return fmt.Errorf("chunk manifest for %s has size %d, expected %d", oid, m.Size, size)
	}
	var next int64
	for i, c := range m.Chunks {
		if len(c.Oid) != sha256.Size*2 {
			return fmt.Errorf("chunk %d of %s has malformed ID %q", i, oid, c.Oid)
		}
		if c.Size <= 0 {
			return fmt.Errorf("chunk %d of %s has invalid size %d", i, oid, c.Size)
		}
		if c.Offset != next {
			return fmt.Errorf("chunk %d of %s starts at %d, expected %d", i, oid, c.Offset, next)
		}
		next += c.Size
	}
	if next != size {
		return fmt.Errorf("chunks of %s cover %d bytes, expected %d", oid, next, size)
	}
	return nil
}
