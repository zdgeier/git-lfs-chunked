package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/zdgeier/git-lfs-chunked/chunking"
)

// Store knows where Git LFS keeps objects and where we cache manifests.
type Store interface {
	ObjectPath(oid string) string
	ManifestPath(oid string) string
	TempDir() string
}

// saveManifest caches m so that later downloads can copy unchanged chunks
// from this object instead of fetching them. Failure is not an error: the
// cache is only an optimisation.
func saveManifest(st Store, m *chunking.Manifest) {
	path := st.ManifestPath(m.Oid)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return
	}
	by, err := json.Marshal(m)
	if err != nil {
		return
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), m.Oid+".*.tmp")
	if err != nil {
		return
	}
	_, werr := tmp.Write(by)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Rename(tmp.Name(), path) != nil {
		os.Remove(tmp.Name())
	}
}

// chunkIndex maps chunk IDs to locations inside local LFS objects, built
// from the cached manifests. Keys carry the chunk hash ("blake3:<id>") since
// a cache may hold manifests from servers that chose different hashes.
type chunkIndex struct {
	st   Store
	root string
	once sync.Once
	mu   sync.Mutex
	locs map[string]chunkLoc
}

func indexKey(h chunking.ChunkHash, oid string) string { return string(h) + ":" + oid }

type chunkLoc struct {
	oid    string
	offset int64
	size   int64
}

func newChunkIndex(st Store) *chunkIndex {
	// The manifest directory is the parent of any manifest's xx/yy dirs.
	root := filepath.Dir(filepath.Dir(filepath.Dir(st.ManifestPath(strings.Repeat("0", 64)))))
	return &chunkIndex{st: st, root: root, locs: map[string]chunkLoc{}}
}

func (ix *chunkIndex) load() {
	ix.once.Do(func() {
		if _, err := os.Stat(ix.root); err != nil {
			return
		}
		filepath.Walk(ix.root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".json") {
				return nil
			}
			by, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			var m chunking.Manifest
			if json.Unmarshal(by, &m) != nil || m.Validate(m.Oid, m.Size) != nil {
				return nil
			}
			// Only index objects that are actually on disk.
			if st, err := os.Stat(ix.st.ObjectPath(m.Oid)); err != nil || st.Size() != m.Size {
				return nil
			}
			ix.add(&m)
			return nil
		})
	})
}

func (ix *chunkIndex) add(m *chunking.Manifest) {
	h, err := m.Hash()
	if err != nil {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for _, c := range m.Chunks {
		k := indexKey(h, c.Oid)
		if _, ok := ix.locs[k]; !ok {
			ix.locs[k] = chunkLoc{oid: m.Oid, offset: c.Offset, size: c.Size}
		}
	}
}

// read returns the bytes of chunk oid (an ID under h) if a local object
// contains it and its content still hashes to oid.
func (ix *chunkIndex) read(h chunking.ChunkHash, oid string, size int64) ([]byte, bool) {
	ix.load()
	k := indexKey(h, oid)
	ix.mu.Lock()
	loc, ok := ix.locs[k]
	ix.mu.Unlock()
	if !ok || loc.size != size {
		return nil, false
	}
	f, err := os.Open(ix.st.ObjectPath(loc.oid))
	if err != nil {
		return nil, false
	}
	defer f.Close()
	data := make([]byte, size)
	if _, err := f.ReadAt(data, loc.offset); err != nil {
		return nil, false
	}
	if h.Sum(data) != oid {
		ix.mu.Lock()
		delete(ix.locs, k)
		ix.mu.Unlock()
		return nil, false
	}
	return data, true
}
