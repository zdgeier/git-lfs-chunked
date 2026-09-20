package chunking

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// FastCDCName is the registered name of the FastCDC algorithm.
const FastCDCName = "fastcdc"

// Bounds on the FastCDC parameters. They match the widely used Rust
// reference implementation (the fastcdc crate, v2020 variant) so that chunk
// boundaries are interoperable with other tools built on it.
const (
	fastcdcMinimumMin = 64
	fastcdcMinimumMax = 1_048_576
	fastcdcAverageMin = 256
	fastcdcAverageMax = 4_194_304
	fastcdcMaximumMin = 1024
	fastcdcMaximumMax = 16_777_216
)

// fastcdcMasks holds one mask per desired number of bits, indexed by
// round(log2(avg_size)) ± normalization level. Entries 0-4 are padding. The
// values for 64 B through 128 KB come from the C reference implementation in
// the destor repository; the larger ones come from restic's FastCDC port. The
// FastCDC paper reports a slightly better deduplication ratio when the mask
// bits are spread out rather than contiguous, hence the "magic" values.
var fastcdcMasks = [26]uint64{
	0, 0, 0, 0, 0,
	0x0000000001804110, // unused except for NC 3
	0x0000000001803110, // 64B
	0x0000000018035100, // 128B
	0x0000001800035300, // 256B
	0x0000019000353000, // 512B
	0x0000590003530000, // 1KB
	0x0000d90003530000, // 2KB
	0x0000d90103530000, // 4KB
	0x0000d90303530000, // 8KB
	0x0000d90313530000, // 16KB
	0x0000d90f03530000, // 32KB
	0x0000d90303537000, // 64KB
	0x0000d90703537000, // 128KB
	0x0000d90707537000, // 256KB
	0x0000d91707537000, // 512KB
	0x0000d91747537000, // 1MB
	0x0000d91767537000, // 2MB
	0x0000d93767537000, // 4MB
	0x0000d93777537000, // 8MB
	0x0000d93777577000, // 16MB
	0x0000db3777577000, // unused except for NC 3
}

// gear is the "gear hash" table from the FastCDC paper: 256 pseudo-random
// 64-bit values, one per possible input byte. As in the C reference
// implementation, entry i is the big-endian high 8 bytes of the MD5 digest
// of 64 bytes each equal to i. Generating the table here (rather than
// pasting it) keeps the provenance obvious; the test suite pins the first
// entries against the published values.
//
// gearLS is gear shifted left by one, which the 2020 variant uses to process
// two bytes per loop iteration.
var gear, gearLS = func() (g, ls [256]uint64) {
	var seed [64]byte
	for i := 0; i < 256; i++ {
		for j := range seed {
			seed[j] = byte(i)
		}
		sum := md5.Sum(seed[:])
		g[i] = binary.BigEndian.Uint64(sum[:8])
		ls[i] = g[i] << 1
	}
	return
}()

// FastCDC implements the FastCDC content-defined chunking algorithm
// (Xia et al., 2016; the "2020" refinement with two-bytes-per-step
// rolling and normalized chunking). Boundaries depend only on content, so
// inserting or removing bytes in a file shifts only the chunks around the
// edit; every other chunk keeps its hash and can be deduplicated.
type FastCDC struct {
	params Params
	maskS  uint64
	maskL  uint64
}

// NewFastCDC validates p and returns a FastCDC chunker.
func NewFastCDC(p Params) (*FastCDC, error) {
	if p.MinSize < fastcdcMinimumMin || p.MinSize > fastcdcMinimumMax {
		return nil, fmt.Errorf("fastcdc: minimum chunk size %d must be between %d and %d", p.MinSize, fastcdcMinimumMin, fastcdcMinimumMax)
	}
	if p.AvgSize < fastcdcAverageMin || p.AvgSize > fastcdcAverageMax {
		return nil, fmt.Errorf("fastcdc: average chunk size %d must be between %d and %d", p.AvgSize, fastcdcAverageMin, fastcdcAverageMax)
	}
	if p.MaxSize < fastcdcMaximumMin || p.MaxSize > fastcdcMaximumMax {
		return nil, fmt.Errorf("fastcdc: maximum chunk size %d must be between %d and %d", p.MaxSize, fastcdcMaximumMin, fastcdcMaximumMax)
	}
	if !(p.MinSize <= p.AvgSize && p.AvgSize <= p.MaxSize) {
		return nil, fmt.Errorf("fastcdc: chunk sizes must satisfy minimum <= average <= maximum (got %d, %d, %d)", p.MinSize, p.AvgSize, p.MaxSize)
	}
	if p.Normalization < 0 || p.Normalization > 3 {
		return nil, fmt.Errorf("fastcdc: normalization level %d must be between 0 and 3", p.Normalization)
	}
	p.Algorithm = FastCDCName

	// Rounded log2 so the mask picked is the one whose target size is
	// closest to the requested average, e.g. 3 MB rounds to the 4 MB mask.
	bits := int(math.Round(math.Log2(float64(p.AvgSize))))
	return &FastCDC{
		params: p,
		maskS:  fastcdcMasks[bits+p.Normalization],
		maskL:  fastcdcMasks[bits-p.Normalization],
	}, nil
}

// Name implements Chunker.
func (c *FastCDC) Name() string { return FastCDCName }

// Params returns the validated parameters in use.
func (c *FastCDC) Params() Params { return c.params }

// Cut returns the length of the next chunk at the start of src, which must
// hold at least MaxSize bytes unless the stream has ended. It is exported so
// callers holding whole buffers can drive the algorithm directly.
func (c *FastCDC) Cut(src []byte) int {
	remaining := len(src)
	if remaining <= c.params.MinSize {
		return remaining
	}
	center := c.params.AvgSize
	if remaining > c.params.MaxSize {
		remaining = c.params.MaxSize
	} else if remaining < center {
		center = remaining
	}

	maskSLS, maskLLS := c.maskS<<1, c.maskL<<1
	index := c.params.MinSize / 2
	var hash uint64

	// Below the average size use the stricter mask (fewer boundaries),
	// above it the looser mask (more boundaries): this is the "normalized
	// chunking" that pulls chunk sizes towards the average.
	for ; index < center/2; index++ {
		a := index * 2
		hash = (hash << 2) + gearLS[src[a]]
		if hash&maskSLS == 0 {
			return a
		}
		hash += gear[src[a+1]]
		if hash&c.maskS == 0 {
			return a + 1
		}
	}
	for ; index < remaining/2; index++ {
		a := index * 2
		hash = (hash << 2) + gearLS[src[a]]
		if hash&maskLLS == 0 {
			return a
		}
		hash += gear[src[a+1]]
		if hash&c.maskL == 0 {
			return a + 1
		}
	}
	// No boundary found (e.g. long runs of zeros): emit a maximum-size
	// chunk.
	return remaining
}

// Split implements Chunker. It streams: memory use is bounded by MaxSize
// regardless of the input length.
func (c *FastCDC) Split(r io.Reader, fn func(Chunk) error) error {
	buf := make([]byte, c.params.MaxSize)
	var (
		n      int   // valid bytes in buf
		offset int64 // stream offset of buf[0]
		eof    bool
	)
	for {
		for !eof && n < len(buf) {
			m, err := r.Read(buf[n:])
			n += m
			if err == io.EOF {
				eof = true
			} else if err != nil {
				return err
			}
		}
		if n == 0 {
			return nil
		}
		cut := c.Cut(buf[:n])
		if err := fn(Chunk{Offset: offset, Data: buf[:cut]}); err != nil {
			return err
		}
		offset += int64(cut)
		n = copy(buf, buf[cut:n])
	}
}

func init() {
	Register(FastCDCName, func(p Params) (Chunker, error) { return NewFastCDC(p) })
}
