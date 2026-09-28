package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zdgeier/git-lfs-chunked/chunking"
)

// testServer is a minimal chunked-protocol server.
type testServer struct {
	*httptest.Server
	mu         sync.Mutex
	chunks     map[string][]byte
	manifests  map[string]*wireManifest
	objects    map[string][]byte
	puts, gets int
	serveWhole bool
	corrupt    string
	// requireAuth rejects requests without this Authorization header.
	requireAuth string
	// hash is the chunk hash the server keys chunks by ("" = sha256).
	// Chunks, manifests and the chunk PUT check all use it.
	hash chunking.ChunkHash
	// answerHash, if set, is sent as chunk_hash in propose responses
	// instead of hash (to simulate misbehaving servers).
	answerHash string
	// flip answers every proposal with the other supported hash.
	flip      bool
	proposals int
	// lastAccept records the LFS-Chunk-Hashes header of the last
	// manifest GET.
	lastAccept string
}

func (s *testServer) chunkHash() chunking.ChunkHash {
	if s.hash == "" {
		return chunking.SHA256
	}
	return s.hash
}

func newTestServer(t *testing.T) *testServer {
	s := &testServer{chunks: map[string][]byte{}, manifests: map[string]*wireManifest{}, objects: map[string][]byte{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.Close)
	return s
}

func (s *testServer) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.requireAuth != "" && r.Header.Get("Authorization") != s.requireAuth {
		http.Error(w, "unauthorized", 401)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	kind, oid := parts[0], parts[1]
	writeJSON := func(v interface{}) {
		w.Header().Set("Content-Type", lfsMediaType)
		json.NewEncoder(w).Encode(v)
	}
	switch kind + " " + r.Method {
	case "objects POST":
		var m wireManifest
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		s.proposals++
		resp := wireManifest{Oid: m.Oid, Size: m.Size, Chunks: []*wireChunk{}}
		if s.answerHash != "" {
			resp.ChunkHash = s.answerHash
			writeJSON(resp)
			return
		}
		if s.flip {
			if m.ChunkHash == "" {
				resp.ChunkHash = "blake3"
			}
			writeJSON(resp)
			return
		}
		// Proposed in another hash: name ours and list nothing.
		if h, _ := chunking.ParseChunkHash(m.ChunkHash); h != s.chunkHash() {
			resp.ChunkHash = s.chunkHash().Wire()
			writeJSON(resp)
			return
		}
		resp.ChunkHash = m.ChunkHash
		for _, c := range m.Chunks {
			if _, ok := s.chunks[c.Oid]; ok {
				continue
			}
			resp.Chunks = append(resp.Chunks, &wireChunk{Oid: c.Oid, Size: c.Size, Offset: c.Offset,
				Actions: map[string]*action{"upload": {Href: s.URL + "/chunks/" + c.Oid}}})
		}
		resp.Actions = map[string]*action{"commit": {Href: s.URL + "/commit/" + oid}}
		writeJSON(resp)
	case "chunks PUT":
		by, _ := io.ReadAll(r.Body)
		if s.chunkHash().Sum(by) != oid {
			http.Error(w, "bad chunk hash", 422)
			return
		}
		s.chunks[oid] = by
		s.puts++
	case "commit POST":
		var m wireManifest
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		var content []byte
		for _, c := range m.Chunks {
			by, ok := s.chunks[c.Oid]
			if !ok {
				http.Error(w, "missing chunk "+c.Oid, 422)
				return
			}
			content = append(content, by...)
		}
		if chunking.HashChunk(content) != oid {
			http.Error(w, "bad object hash", 422)
			return
		}
		s.objects[oid] = content
		s.manifests[oid] = &m
	case "objects GET":
		content, ok := s.objects[oid]
		if !ok {
			http.NotFound(w, r)
			return
		}
		s.lastAccept = r.Header.Get(chunkHashesHeader)
		// A client that cannot verify our chunk IDs gets the whole object.
		if s.serveWhole || (s.chunkHash() != chunking.SHA256 && !strings.Contains(s.lastAccept, string(s.chunkHash()))) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(content)
			return
		}
		m := s.manifests[oid]
		resp := wireManifest{Oid: m.Oid, Size: m.Size, ChunkHash: m.ChunkHash, Chunks: []*wireChunk{}}
		for _, c := range m.Chunks {
			resp.Chunks = append(resp.Chunks, &wireChunk{Oid: c.Oid, Size: c.Size, Offset: c.Offset,
				Actions: map[string]*action{"download": {Href: s.URL + "/chunks/" + c.Oid}}})
		}
		writeJSON(resp)
	case "chunks GET":
		by, ok := s.chunks[oid]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if oid == s.corrupt {
			by = append([]byte{by[0] ^ 0xff}, by[1:]...)
		}
		s.gets++
		w.Write(by)
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, 405)
	}
}

func (s *testServer) counters() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts, s.gets
}

// tempStore is a Store rooted in a temporary directory, mimicking .git/lfs.
type tempStore struct{ root string }

func (s tempStore) ObjectPath(oid string) string {
	return filepath.Join(s.root, "objects", oid[0:2], oid[2:4], oid)
}
func (s tempStore) ManifestPath(oid string) string {
	return filepath.Join(s.root, "chunks", oid[0:2], oid[2:4], oid+".json")
}
func (s tempStore) TempDir() string { return filepath.Join(s.root, "tmp") }

// client drives an Agent the way git-lfs does, over pipes.
type client struct {
	t    *testing.T
	st   tempStore
	in   *io.PipeWriter
	out  *json.Decoder
	done chan error
}

func newClient(t *testing.T, params map[string]string) *client {
	st := tempStore{root: t.TempDir()}
	p, err := chunking.ParamsFromEnv(mapEnv(params))
	require.NoError(t, err)
	a, err := New(st, p)
	require.NoError(t, err)
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &client{t: t, st: st, in: inW, out: json.NewDecoder(outR), done: make(chan error, 1)}
	go func() { c.done <- a.Run(inR, outW); outW.Close() }()
	c.send(request{Event: "init", Operation: "upload", Remote: "origin", Concurrent: true, ConcurrentTransfers: 3})
	var r response
	require.NoError(t, c.out.Decode(&r))
	assert.Nil(t, r.Error)
	t.Cleanup(func() {
		c.send(request{Event: "terminate"})
		require.NoError(t, <-c.done)
	})
	return c
}

type mapEnv map[string]string

func (m mapEnv) Get(k string) (string, bool) { v, ok := m[k]; return v, ok }
func (m mapEnv) Int(k string, def int) int   { return def }

func (c *client) send(v interface{}) {
	by, err := json.Marshal(v)
	require.NoError(c.t, err)
	_, err = c.in.Write(append(by, '\n'))
	require.NoError(c.t, err)
}

// transfer sends one request and collects progress until completion.
func (c *client) transfer(req request) (*response, []response) {
	c.send(req)
	var progress []response
	for {
		var r response
		require.NoError(c.t, c.out.Decode(&r))
		require.Equal(c.t, req.Oid, r.Oid)
		if r.Event == "complete" {
			return &r, progress
		}
		require.Equal(c.t, "progress", r.Event)
		progress = append(progress, r)
	}
}

func (c *client) upload(s *testServer, path string, hdr map[string]string) (string, int64, *response, []response) {
	by, err := os.ReadFile(path)
	require.NoError(c.t, err)
	sum := sha256.Sum256(by)
	oid := hex.EncodeToString(sum[:])
	res, prog := c.transfer(request{Event: "upload", Oid: oid, Size: int64(len(by)), Path: path,
		Action: &action{Href: s.URL + "/objects/" + oid, Header: hdr}})
	return oid, int64(len(by)), res, prog
}

// download fetches oid and, like git-lfs, moves the returned file into
// the object store.
func (c *client) download(s *testServer, oid string, size int64, hdr map[string]string) (*response, []response) {
	res, prog := c.transfer(request{Event: "download", Oid: oid, Size: size,
		Action: &action{Href: s.URL + "/objects/" + oid, Header: hdr}})
	if res.Error == nil {
		dest := c.st.ObjectPath(oid)
		require.NoError(c.t, os.MkdirAll(filepath.Dir(dest), 0755))
		require.NoError(c.t, os.Rename(res.Path, dest))
	}
	return res, prog
}

func randomFile(t *testing.T, name string, seed int64, n int) string {
	by := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(by)
	p := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(p, by, 0644))
	return p
}

var small = map[string]string{
	"lfs.chunking.minsize": "4k",
	"lfs.chunking.avgsize": "16k",
	"lfs.chunking.maxsize": "64k",
}

func TestRoundTripAndDeduplication(t *testing.T) {
	s := newTestServer(t)
	up := newClient(t, small)

	v1 := randomFile(t, "v1.bin", 1, 512*1024)
	oid1, size1, res, prog := up.upload(s, v1, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	puts1, _ := s.counters()
	assert.GreaterOrEqual(t, puts1, 15)
	assert.Equal(t, size1, prog[len(prog)-1].BytesSoFar, "progress must end at the object size")
	var total int64
	for _, p := range prog {
		total += p.BytesSinceLast
	}
	assert.Equal(t, size1, total)
	_, err := os.Stat(up.st.ManifestPath(oid1))
	require.NoError(t, err, "manifest is cached")

	// Insert bytes near the front: only the touched chunk(s) move.
	v1by, _ := os.ReadFile(v1)
	v2by := append(append(append([]byte{}, v1by[:1000]...), []byte("CHANGED")...), v1by[1000:]...)
	v2 := filepath.Join(t.TempDir(), "v2.bin")
	require.NoError(t, os.WriteFile(v2, v2by, 0644))
	oid2, size2, res, _ := up.upload(s, v2, nil)
	require.Nil(t, res.Error)
	puts2, _ := s.counters()
	assert.LessOrEqual(t, puts2-puts1, 3)
	assert.GreaterOrEqual(t, puts2-puts1, 1)

	// Re-upload: nothing to send, still succeeds.
	_, _, res, prog = up.upload(s, v2, nil)
	require.Nil(t, res.Error)
	puts3, _ := s.counters()
	assert.Equal(t, puts2, puts3)
	assert.Equal(t, size2, prog[len(prog)-1].BytesSoFar)

	// Fresh client downloads v1 (all chunks) then v2 (mostly reused).
	down := newClient(t, nil)
	res, _ = down.download(s, oid1, size1, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	got, _ := os.ReadFile(down.st.ObjectPath(oid1))
	assert.True(t, bytes.Equal(v1by, got))
	_, gets1 := s.counters()
	assert.Equal(t, puts1, gets1)

	res, prog = down.download(s, oid2, size2, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	got, _ = os.ReadFile(down.st.ObjectPath(oid2))
	assert.True(t, bytes.Equal(v2by, got))
	_, gets2 := s.counters()
	assert.LessOrEqual(t, gets2-gets1, 3)
	assert.GreaterOrEqual(t, gets2-gets1, 1)
	assert.Equal(t, size2, prog[len(prog)-1].BytesSoFar)
}

func TestAuthorizationHeaderIsForwardedToChunks(t *testing.T) {
	s := newTestServer(t)
	s.requireAuth = "Bearer secret"
	up := newClient(t, small)
	p := randomFile(t, "a.bin", 2, 100*1024)

	_, _, res, _ := up.upload(s, p, nil)
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, "401")

	oid, size, res, _ := up.upload(s, p, map[string]string{"Authorization": "Bearer secret"})
	require.Nil(t, res.Error, "%+v", res.Error)

	down := newClient(t, nil)
	res, _ = down.download(s, oid, size, map[string]string{"Authorization": "Bearer secret"})
	require.Nil(t, res.Error, "%+v", res.Error)
}

func TestWholeObjectFallback(t *testing.T) {
	s := newTestServer(t)
	up := newClient(t, small)
	p := randomFile(t, "a.bin", 3, 100*1024)
	oid, size, res, _ := up.upload(s, p, nil)
	require.Nil(t, res.Error)

	s.serveWhole = true
	down := newClient(t, nil)
	res, _ = down.download(s, oid, size, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	want, _ := os.ReadFile(p)
	got, _ := os.ReadFile(down.st.ObjectPath(oid))
	assert.True(t, bytes.Equal(want, got))
}

func TestCorruptChunkIsRejected(t *testing.T) {
	s := newTestServer(t)
	up := newClient(t, small)
	p := randomFile(t, "a.bin", 4, 100*1024)
	oid, size, res, _ := up.upload(s, p, nil)
	require.Nil(t, res.Error)
	s.mu.Lock()
	s.corrupt = s.manifests[oid].Chunks[1].Oid
	s.mu.Unlock()

	down := newClient(t, nil)
	res, _ = down.download(s, oid, size, nil)
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, "expected ID")
	entries, _ := os.ReadDir(down.st.TempDir())
	assert.Empty(t, entries, "no temp file left behind")
}

func TestCorruptLocalObjectIsRejected(t *testing.T) {
	s := newTestServer(t)
	up := newClient(t, small)
	p := randomFile(t, "a.bin", 5, 20*1024)
	res, _ := up.transfer(request{Event: "upload", Oid: strings.Repeat("0", 64), Size: 20 * 1024, Path: p,
		Action: &action{Href: s.URL + "/objects/" + strings.Repeat("0", 64)}})
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, "is corrupt")
	puts, _ := s.counters()
	assert.Equal(t, 0, puts)
}

func TestStandaloneModeIsReported(t *testing.T) {
	up := newClient(t, nil)
	res, _ := up.transfer(request{Event: "upload", Oid: strings.Repeat("0", 64), Size: 1, Path: "/nonexistent"})
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, "standalone")
}

func TestFixedAlgorithm(t *testing.T) {
	s := newTestServer(t)
	up := newClient(t, map[string]string{"lfs.chunking.algorithm": "fixed", "lfs.chunking.avgsize": "8k"})
	p := randomFile(t, "a.bin", 6, 100*1024)
	oid, _, res, _ := up.upload(s, p, nil)
	require.Nil(t, res.Error)
	puts, _ := s.counters()
	assert.Equal(t, 13, puts)
	assert.Equal(t, "fixed", s.manifests[oid].Algorithm.Algorithm)
}

func TestBlake3Negotiation(t *testing.T) {
	s := newTestServer(t)
	s.hash = chunking.BLAKE3
	up := newClient(t, small)

	v1 := randomFile(t, "v1.bin", 9, 512*1024)
	oid1, size1, res, _ := up.upload(s, v1, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	assert.Equal(t, 2, s.proposals, "sha256 proposal, then a blake3 one")
	m := s.manifests[oid1]
	assert.Equal(t, "blake3", m.ChunkHash)
	v1by, _ := os.ReadFile(v1)
	for _, c := range m.Chunks {
		assert.Equal(t, chunking.BLAKE3.Sum(v1by[c.Offset:c.Offset+c.Size]), c.Oid)
	}
	puts1, _ := s.counters()

	// The same agent now proposes blake3 straight away, and dedup works.
	v2by := append(append([]byte{}, v1by...), []byte("appended")...)
	v2 := filepath.Join(t.TempDir(), "v2.bin")
	require.NoError(t, os.WriteFile(v2, v2by, 0644))
	oid2, size2, res, _ := up.upload(s, v2, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	assert.Equal(t, 3, s.proposals)
	puts2, _ := s.counters()
	assert.LessOrEqual(t, puts2-puts1, 2)

	// A fresh client verifies blake3 chunks and reuses them locally.
	down := newClient(t, nil)
	res, _ = down.download(s, oid1, size1, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	assert.Equal(t, "sha256, blake3", s.lastAccept)
	_, gets1 := s.counters()
	res, _ = down.download(s, oid2, size2, nil)
	require.Nil(t, res.Error, "%+v", res.Error)
	got, _ := os.ReadFile(down.st.ObjectPath(oid2))
	assert.True(t, bytes.Equal(v2by, got))
	_, gets2 := s.counters()
	assert.LessOrEqual(t, gets2-gets1, 2)

	// A corrupt blake3 chunk is caught.
	s.mu.Lock()
	s.corrupt = m.Chunks[1].Oid
	s.mu.Unlock()
	res, _ = newClient(t, nil).download(s, oid1, size1, nil)
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, "expected ID")
}

func TestUnsupportedChunkHashIsRejected(t *testing.T) {
	s := newTestServer(t)
	s.answerHash = "md5"
	up := newClient(t, small)
	p := randomFile(t, "a.bin", 10, 50*1024)
	_, _, res, _ := up.upload(s, p, nil)
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, `unsupported chunk hash "md5"`)
	assert.Equal(t, 1, s.proposals)
}

func TestChunkHashRenegotiatesOnlyOnce(t *testing.T) {
	s := newTestServer(t)
	s.flip = true
	up := newClient(t, small)
	p := randomFile(t, "a.bin", 11, 50*1024)
	_, _, res, _ := up.upload(s, p, nil)
	require.NotNil(t, res.Error)
	assert.Contains(t, res.Error.Message, "again")
	assert.Equal(t, 2, s.proposals)
	puts, _ := s.counters()
	assert.Equal(t, 0, puts)
}

func TestBadParams(t *testing.T) {
	_, err := New(tempStore{root: t.TempDir()}, chunking.Params{Algorithm: "nope"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown chunking algorithm")
}
