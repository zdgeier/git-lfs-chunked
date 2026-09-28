package chunking

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mapEnv map[string]string

func (m mapEnv) Get(key string) (string, bool) { v, ok := m[key]; return v, ok }
func (m mapEnv) Int(key string, def int) int   { return def }

func randomBytes(t *testing.T, seed int64, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	r := rand.New(rand.NewSource(seed))
	_, err := r.Read(b)
	require.NoError(t, err)
	return b
}

func collect(t *testing.T, c Chunker, r io.Reader) []Chunk {
	t.Helper()
	var out []Chunk
	require.NoError(t, c.Split(r, func(ch Chunk) error {
		// Data is only valid during the callback; copy it.
		out = append(out, Chunk{Offset: ch.Offset, Data: append([]byte(nil), ch.Data...)})
		return nil
	}))
	return out
}

func assertCovers(t *testing.T, chunks []Chunk, content []byte) {
	t.Helper()
	var joined []byte
	var next int64
	for _, c := range chunks {
		assert.Equal(t, next, c.Offset)
		next += int64(len(c.Data))
		joined = append(joined, c.Data...)
	}
	assert.Equal(t, content, joined)
}

func TestGearTableMatchesReference(t *testing.T) {
	// First entries of the published GEAR table (fastcdc crate, v2020).
	assert.Equal(t, uint64(0x3b5d3c7d207e37dc), gear[0])
	assert.Equal(t, uint64(0x784d68ba91123086), gear[1])
	assert.Equal(t, uint64(0xcd52880f882e7298), gear[2])
	assert.Equal(t, uint64(0xeacf8e4e19fdcca7), gear[3])
	assert.Equal(t, gear[7]<<1, gearLS[7])
}

func TestFastCDCMaskSelection(t *testing.T) {
	c, err := NewFastCDC(Params{MinSize: 64, AvgSize: 256, MaxSize: 1024, Normalization: 1})
	require.NoError(t, err)
	assert.Equal(t, fastcdcMasks[7], c.maskL)
	assert.Equal(t, fastcdcMasks[9], c.maskS)

	c, err = NewFastCDC(Params{MinSize: 8192, AvgSize: 16384, MaxSize: 32768, Normalization: 1})
	require.NoError(t, err)
	assert.Equal(t, fastcdcMasks[13], c.maskL)
	assert.Equal(t, fastcdcMasks[15], c.maskS)

	c, err = NewFastCDC(Params{MinSize: 1048576, AvgSize: 4194304, MaxSize: 16777216, Normalization: 1})
	require.NoError(t, err)
	assert.Equal(t, fastcdcMasks[21], c.maskL)
	assert.Equal(t, fastcdcMasks[23], c.maskS)

	// Level 0 uses the same mask on both sides of the average.
	c, err = NewFastCDC(Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 0})
	require.NoError(t, err)
	assert.Equal(t, c.maskS, c.maskL)
}

func TestFastCDCRejectsBadParams(t *testing.T) {
	for _, p := range []Params{
		{MinSize: 32, AvgSize: 256, MaxSize: 1024},
		{MinSize: 64, AvgSize: 128, MaxSize: 1024},
		{MinSize: 64, AvgSize: 256, MaxSize: 512},
		{MinSize: 512, AvgSize: 256, MaxSize: 1024},
		{MinSize: 64, AvgSize: 2048, MaxSize: 1024},
		{MinSize: 64, AvgSize: 256, MaxSize: 1024, Normalization: 4},
		{MinSize: 64, AvgSize: 256, MaxSize: 1024, Normalization: -1},
	} {
		_, err := NewFastCDC(p)
		assert.Error(t, err, "%+v", p)
	}
}

func TestFastCDCEmptyInput(t *testing.T) {
	c, err := NewFastCDC(DefaultParams())
	require.NoError(t, err)
	assert.Empty(t, collect(t, c, bytes.NewReader(nil)))
}

func TestFastCDCSmallInputIsOneChunk(t *testing.T) {
	c, err := NewFastCDC(Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1})
	require.NoError(t, err)
	content := randomBytes(t, 1, 1000)
	chunks := collect(t, c, bytes.NewReader(content))
	require.Len(t, chunks, 1)
	assertCovers(t, chunks, content)
}

func TestFastCDCRespectsBounds(t *testing.T) {
	p := Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1}
	c, err := NewFastCDC(p)
	require.NoError(t, err)
	content := randomBytes(t, 2, 1<<20)
	chunks := collect(t, c, bytes.NewReader(content))
	assertCovers(t, chunks, content)
	require.Greater(t, len(chunks), 1)
	for i, ch := range chunks {
		assert.LessOrEqual(t, len(ch.Data), p.MaxSize)
		if i < len(chunks)-1 {
			assert.GreaterOrEqual(t, len(ch.Data), p.MinSize)
		}
	}
	// With 1 MiB of random data and a 16 KiB target the count should be
	// in the right ballpark; a wildly different number means the masks or
	// the rolling hash are wrong.
	assert.InDelta(t, 64, len(chunks), 32)
}

func TestFastCDCAllZerosHitsMaxSize(t *testing.T) {
	p := Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1}
	c, err := NewFastCDC(p)
	require.NoError(t, err)
	content := make([]byte, 3*p.MaxSize+100)
	chunks := collect(t, c, bytes.NewReader(content))
	assertCovers(t, chunks, content)
	require.Len(t, chunks, 4)
	for _, ch := range chunks[:3] {
		assert.Equal(t, p.MaxSize, len(ch.Data))
	}
	assert.Equal(t, 100, len(chunks[3].Data))
}

func TestFastCDCIsDeterministicAndStreamIndependent(t *testing.T) {
	p := Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1}
	c, err := NewFastCDC(p)
	require.NoError(t, err)
	content := randomBytes(t, 3, 300*1024)

	a := collect(t, c, bytes.NewReader(content))
	// A reader that returns tiny reads must not change the boundaries.
	b := collect(t, c, iotestOneByteReader{bytes.NewReader(content)})
	require.Equal(t, len(a), len(b))
	for i := range a {
		assert.Equal(t, a[i].Offset, b[i].Offset)
		assert.Equal(t, a[i].Data, b[i].Data)
	}
}

// iotestOneByteReader mimics io/iotest's OneByteReader without the import.
type iotestOneByteReader struct{ r io.Reader }

func (o iotestOneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

func TestFastCDCInsertionOnlyShiftsLocalChunks(t *testing.T) {
	p := Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1}
	c, err := NewFastCDC(p)
	require.NoError(t, err)
	content := randomBytes(t, 4, 1<<20)
	before := collect(t, c, bytes.NewReader(content))

	// Insert 100 bytes in the middle of the file.
	edited := append([]byte{}, content[:len(content)/2]...)
	edited = append(edited, randomBytes(t, 5, 100)...)
	edited = append(edited, content[len(content)/2:]...)
	after := collect(t, c, bytes.NewReader(edited))

	hashes := map[string]bool{}
	for _, ch := range before {
		hashes[HashChunk(ch.Data)] = true
	}
	reused := 0
	for _, ch := range after {
		if hashes[HashChunk(ch.Data)] {
			reused++
		}
	}
	// Content-defined chunking should keep nearly every chunk; only the
	// couple around the edit may change.
	assert.GreaterOrEqual(t, reused, len(after)-3, "reused %d of %d chunks", reused, len(after))
}

// TestFastCDCMatchesRustReference pins boundaries produced by the Rust
// fastcdc crate (v2020, level 1, seed 0) for a deterministic input so that
// chunks stay interoperable with other tools built on that implementation.
// The expected values were produced with:
//
//	let data: Vec<u8> = (0..65536u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
//	FastCDC::new(&data, 1024, 4096, 16384).map(|c| c.length)
func TestFastCDCMatchesRustReference(t *testing.T) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte((uint32(i) * 2654435761) >> 13)
	}
	c, err := NewFastCDC(Params{MinSize: 1024, AvgSize: 4096, MaxSize: 16384, Normalization: 1})
	require.NoError(t, err)
	chunks := collect(t, c, bytes.NewReader(data))
	assertCovers(t, chunks, data)

	var lengths []int
	for _, ch := range chunks {
		lengths = append(lengths, len(ch.Data))
	}
	assert.Equal(t, rustReferenceLengths, lengths)
}

func TestFixedChunker(t *testing.T) {
	c, err := New(Params{Algorithm: "fixed", AvgSize: 1000})
	require.NoError(t, err)
	assert.Equal(t, "fixed", c.Name())

	content := randomBytes(t, 6, 2500)
	chunks := collect(t, c, bytes.NewReader(content))
	assertCovers(t, chunks, content)
	require.Len(t, chunks, 3)
	assert.Equal(t, 1000, len(chunks[0].Data))
	assert.Equal(t, 1000, len(chunks[1].Data))
	assert.Equal(t, 500, len(chunks[2].Data))

	assert.Empty(t, collect(t, c, bytes.NewReader(nil)))

	_, err = New(Params{Algorithm: "fixed", AvgSize: 0})
	assert.Error(t, err)
}

func TestSplitPropagatesCallbackError(t *testing.T) {
	c, err := New(Params{Algorithm: "fixed", AvgSize: 10})
	require.NoError(t, err)
	boom := errors.New("boom")
	calls := 0
	err = c.Split(bytes.NewReader(make([]byte, 100)), func(Chunk) error {
		calls++
		return boom
	})
	assert.Equal(t, boom, err)
	assert.Equal(t, 1, calls)
}

func TestRegistry(t *testing.T) {
	assert.Contains(t, Names(), "fastcdc")
	assert.Contains(t, Names(), "fixed")

	_, err := New(Params{Algorithm: "nope"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown chunking algorithm "nope"`)

	Register("Custom", func(p Params) (Chunker, error) { return NewFixed(Params{AvgSize: 7}) })
	defer func() {
		registryMu.Lock()
		delete(registry, "custom")
		registryMu.Unlock()
	}()
	c, err := New(Params{Algorithm: "CUSTOM"})
	require.NoError(t, err)
	chunks := collect(t, c, strings.NewReader("0123456789"))
	require.Len(t, chunks, 2)
	assert.Equal(t, "0123456", string(chunks[0].Data))
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int{
		"4096": 4096,
		"256k": 256 * 1024,
		"256K": 256 * 1024,
		"1m":   1024 * 1024,
		"2G":   2 * 1024 * 1024 * 1024,
		" 8k ": 8 * 1024,
	} {
		got, err := ParseSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "abc", "1x", "-5", "0", "k"} {
		_, err := ParseSize(in)
		assert.Error(t, err, in)
	}
}

func TestParamsFromEnv(t *testing.T) {
	p, err := ParamsFromEnv(mapEnv{})
	require.NoError(t, err)
	assert.Equal(t, DefaultParams(), p)

	p, err = ParamsFromEnv(mapEnv{
		"lfs.chunking.algorithm":     "Fixed",
		"lfs.chunking.minsize":       "4k",
		"lfs.chunking.avgsize":       "16k",
		"lfs.chunking.maxsize":       "64k",
		"lfs.chunking.normalization": "2",
	})
	require.NoError(t, err)
	assert.Equal(t, Params{Algorithm: "fixed", MinSize: 4096, AvgSize: 16384, MaxSize: 65536, Normalization: 2}, p)

	_, err = ParamsFromEnv(mapEnv{"lfs.chunking.avgsize": "big"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lfs.chunking.avgsize")

	_, err = ParamsFromEnv(mapEnv{"lfs.chunking.normalization": "one"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lfs.chunking.normalization")
}

func TestBuildAndValidateManifest(t *testing.T) {
	c, err := NewFastCDC(Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1})
	require.NoError(t, err)
	content := randomBytes(t, 7, 200*1024)
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])

	m, err := Build(bytes.NewReader(content), c)
	require.NoError(t, err)
	assert.Equal(t, oid, m.Oid)
	assert.Equal(t, int64(len(content)), m.Size)
	require.NotNil(t, m.Algorithm)
	assert.Equal(t, "fastcdc", m.Algorithm.Algorithm)
	assert.Equal(t, 16384, m.Algorithm.AvgSize)
	require.Greater(t, len(m.Chunks), 1)
	require.NoError(t, m.Validate(oid, int64(len(content))))

	for _, ch := range m.Chunks {
		assert.Equal(t, HashChunk(content[ch.Offset:ch.Offset+ch.Size]), ch.Oid)
	}

	assert.Error(t, m.Validate("0000", int64(len(content))))
	assert.Error(t, m.Validate(oid, 1))

	bad := *m
	bad.Chunks = append([]ManifestChunk{}, m.Chunks...)
	bad.Chunks[1].Offset++
	assert.Error(t, bad.Validate(oid, int64(len(content))))

	bad.Chunks = append([]ManifestChunk{}, m.Chunks...)
	bad.Chunks[0].Oid = "short"
	assert.Error(t, bad.Validate(oid, int64(len(content))))

	// The empty object has no chunks and validates against its own hash.
	empty, err := Build(bytes.NewReader(nil), c)
	require.NoError(t, err)
	assert.Empty(t, empty.Chunks)
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", empty.Oid)
	require.NoError(t, empty.Validate(empty.Oid, 0))
}

func TestChunkHashes(t *testing.T) {
	// Reference values from b3sum / sha256sum.
	assert.Equal(t, "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85", BLAKE3.Sum([]byte("abc")))
	assert.Equal(t, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", SHA256.Sum([]byte("abc")))

	for in, want := range map[string]ChunkHash{"": SHA256, "sha256": SHA256, "blake3": BLAKE3} {
		got, err := ParseChunkHash(in)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := ParseChunkHash("md5")
	assert.Error(t, err)
	assert.Equal(t, "", SHA256.Wire(), "sha256 is omitted on the wire")
	assert.Equal(t, "blake3", BLAKE3.Wire())
}

func TestBuildWithBlake3AndRehash(t *testing.T) {
	c, err := NewFastCDC(Params{MinSize: 4096, AvgSize: 16384, MaxSize: 65535, Normalization: 1})
	require.NoError(t, err)
	content := randomBytes(t, 8, 200*1024)

	sha, err := Build(bytes.NewReader(content), c)
	require.NoError(t, err)
	b3, err := BuildWithHash(bytes.NewReader(content), c, BLAKE3)
	require.NoError(t, err)

	assert.Equal(t, sha.Oid, b3.Oid, "the object ID is SHA-256 whatever the chunk hash")
	assert.Equal(t, "blake3", b3.ChunkHash)
	require.Equal(t, len(sha.Chunks), len(b3.Chunks))
	for i, ch := range b3.Chunks {
		assert.Equal(t, sha.Chunks[i].Offset, ch.Offset)
		assert.Equal(t, BLAKE3.Sum(content[ch.Offset:ch.Offset+ch.Size]), ch.Oid)
	}
	require.NoError(t, b3.Validate(b3.Oid, b3.Size))

	re, err := sha.Rehash(bytes.NewReader(content), BLAKE3)
	require.NoError(t, err)
	assert.Equal(t, b3, re)
	back, err := re.Rehash(bytes.NewReader(content), SHA256)
	require.NoError(t, err)
	assert.Equal(t, sha, back)
	assert.Equal(t, "", sha.ChunkHash, "Rehash must not modify its receiver")

	bad := *b3
	bad.ChunkHash = "md5"
	assert.ErrorContains(t, bad.Validate(b3.Oid, b3.Size), "unsupported chunk hash")
}

func BenchmarkFastCDC(b *testing.B) {
	content := make([]byte, 64<<20)
	rand.New(rand.NewSource(1)).Read(content)
	c, _ := NewFastCDC(DefaultParams())
	b.SetBytes(int64(len(content)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Split(bytes.NewReader(content), func(Chunk) error { return nil })
	}
}
