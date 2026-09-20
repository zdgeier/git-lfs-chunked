package chunking

import (
	"fmt"
	"io"
)

// FixedName is the registered name of the fixed-size chunker.
const FixedName = "fixed"

// Fixed cuts the stream every AvgSize bytes. It has no deduplication
// resilience against insertions (everything after the edit shifts) but is
// cheap and predictable, which suits content that is rewritten wholesale,
// such as already-compressed archives.
type Fixed struct {
	size int
}

// NewFixed validates p and returns a Fixed chunker of p.AvgSize bytes.
func NewFixed(p Params) (*Fixed, error) {
	if p.AvgSize < 1 {
		return nil, fmt.Errorf("fixed: chunk size %d must be positive", p.AvgSize)
	}
	return &Fixed{size: p.AvgSize}, nil
}

// Name implements Chunker.
func (c *Fixed) Name() string { return FixedName }

// Params returns the effective parameters. Min and max equal the size.
func (c *Fixed) Params() Params {
	return Params{Algorithm: FixedName, MinSize: c.size, AvgSize: c.size, MaxSize: c.size}
}

// Split implements Chunker.
func (c *Fixed) Split(r io.Reader, fn func(Chunk) error) error {
	buf := make([]byte, c.size)
	var offset int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if cerr := fn(Chunk{Offset: offset, Data: buf[:n]}); cerr != nil {
				return cerr
			}
			offset += int64(n)
		}
		switch err {
		case nil:
			continue
		case io.EOF, io.ErrUnexpectedEOF:
			return nil
		default:
			return err
		}
	}
}

func init() {
	Register(FixedName, func(p Params) (Chunker, error) { return NewFixed(p) })
}
