// Package chunking splits object content into content-addressed chunks so
// that transfer adapters can move only the parts of an object which the other
// side does not already have.
//
// The package is deliberately small: a Chunker interface, a registry of named
// algorithms, and the Params that select and tune one. The git-lfs-chunked
// transfer agent is the primary consumer, but nothing here depends on the
// transfer layer, so the same code can run on a server.
package chunking

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Chunk is one contiguous region of the content passed to Chunker.Split.
type Chunk struct {
	// Offset is the byte offset of the chunk within the whole content.
	Offset int64
	// Data holds the chunk's bytes. It is only valid for the duration of
	// the Split callback; implementations may reuse the backing array.
	Data []byte
}

// Length returns the number of bytes in the chunk.
func (c Chunk) Length() int { return len(c.Data) }

// Chunker divides a stream into chunks. Implementations must be
// deterministic: the same bytes with the same Params always produce the same
// chunk boundaries, since the boundaries determine which chunks can be
// deduplicated between the client and the server.
type Chunker interface {
	// Name returns the registered algorithm name, e.g. "fastcdc".
	Name() string
	// Split reads r until EOF and calls fn once per chunk, in order. The
	// first error returned by fn aborts the split and is returned.
	// Zero-length content produces zero chunks.
	Split(r io.Reader, fn func(Chunk) error) error
}

// Factory builds a Chunker from a set of parameters, returning an error if
// the parameters are not acceptable for the algorithm.
type Factory func(p Params) (Chunker, error)

const (
	// DefaultAlgorithm is used when lfs.chunking.algorithm is unset.
	DefaultAlgorithm = "fastcdc"
	// DefaultMinSize is used when lfs.chunking.minsize is unset.
	DefaultMinSize = 256 * 1024
	// DefaultAvgSize is used when lfs.chunking.avgsize is unset.
	DefaultAvgSize = 1024 * 1024
	// DefaultMaxSize is used when lfs.chunking.maxsize is unset.
	DefaultMaxSize = 4 * 1024 * 1024
	// DefaultNormalization is used when lfs.chunking.normalization is
	// unset. Level 1 is the FastCDC paper's recommendation.
	DefaultNormalization = 1

	algorithmKey     = "lfs.chunking.algorithm"
	minSizeKey       = "lfs.chunking.minsize"
	avgSizeKey       = "lfs.chunking.avgsize"
	maxSizeKey       = "lfs.chunking.maxsize"
	normalizationKey = "lfs.chunking.normalization"
)

// Params selects a chunking algorithm and tunes it. The size fields are in
// bytes. Not every algorithm uses every field; see the algorithm docs.
type Params struct {
	// Algorithm is the registered name of the chunker to use.
	Algorithm string `json:"name"`
	// MinSize is the smallest chunk a content-defined algorithm will cut
	// (the final chunk of a stream may be smaller).
	MinSize int `json:"min_size,omitempty"`
	// AvgSize is the target chunk size. For "fixed" it is the exact size.
	AvgSize int `json:"avg_size,omitempty"`
	// MaxSize is the largest chunk a content-defined algorithm will cut.
	MaxSize int `json:"max_size,omitempty"`
	// Normalization is the FastCDC normalized-chunking level (0-3).
	Normalization int `json:"normalization,omitempty"`
}

// DefaultParams returns the parameters used when nothing is configured.
func DefaultParams() Params {
	return Params{
		Algorithm:     DefaultAlgorithm,
		MinSize:       DefaultMinSize,
		AvgSize:       DefaultAvgSize,
		MaxSize:       DefaultMaxSize,
		Normalization: DefaultNormalization,
	}
}

// Env is the subset of config.Environment that ParamsFromEnv needs.
type Env interface {
	Get(key string) (val string, ok bool)
	Int(key string, def int) int
}

// ParamsFromEnv reads the lfs.chunking.* keys from env, falling back to the
// defaults for anything unset. Sizes accept an optional k, m or g suffix
// (case-insensitive, powers of 1024), matching Git's own size syntax.
// Values which cannot be parsed are reported as errors rather than silently
// replaced, since a typo would otherwise change chunk boundaries and defeat
// deduplication.
func ParamsFromEnv(env Env) (Params, error) {
	p := DefaultParams()
	if v, ok := env.Get(algorithmKey); ok && v != "" {
		p.Algorithm = strings.ToLower(v)
	}

	var err error
	for _, s := range []struct {
		key string
		dst *int
	}{
		{minSizeKey, &p.MinSize},
		{avgSizeKey, &p.AvgSize},
		{maxSizeKey, &p.MaxSize},
	} {
		v, ok := env.Get(s.key)
		if !ok || v == "" {
			continue
		}
		if *s.dst, err = ParseSize(v); err != nil {
			return p, fmt.Errorf("invalid value for %s: %s", s.key, err)
		}
	}

	if v, ok := env.Get(normalizationKey); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return p, fmt.Errorf("invalid value for %s: %q is not an integer", normalizationKey, v)
		}
		p.Normalization = n
	}
	return p, nil
}

// ParseSize parses a byte count such as "4096", "256k", "1M" or "2g".
func ParseSize(s string) (int, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mult := 1
	switch s[len(s)-1] {
	case 'k':
		mult = 1024
	case 'm':
		mult = 1024 * 1024
	case 'g':
		mult = 1024 * 1024 * 1024
	}
	if mult != 1 {
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a size", s)
	}
	n *= int64(mult)
	if n <= 0 || n > int64(^uint(0)>>1) {
		return 0, fmt.Errorf("size %d is out of range", n)
	}
	return int(n), nil
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register makes a chunking algorithm available under name. Registering a
// name twice replaces the earlier factory, which lets tests and embedders
// override the built-in algorithms.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[strings.ToLower(name)] = f
}

// Names returns the registered algorithm names, sorted.
func Names() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// New builds the chunker selected by p.Algorithm.
func New(p Params) (Chunker, error) {
	registryMu.RLock()
	f, ok := registry[strings.ToLower(p.Algorithm)]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown chunking algorithm %q (available: %s)",
			p.Algorithm, strings.Join(Names(), ", "))
	}
	return f(p)
}

// String renders the parameters in a form suitable for trace output.
func (p Params) String() string {
	return fmt.Sprintf("%s(min=%d avg=%d max=%d nc=%d)",
		p.Algorithm, p.MinSize, p.AvgSize, p.MaxSize, p.Normalization)
}
