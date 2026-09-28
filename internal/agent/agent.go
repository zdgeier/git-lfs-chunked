// Package agent implements a Git LFS custom transfer agent that speaks the
// chunked transfer protocol: objects are split into content-defined chunks
// and only the chunks the other side lacks are moved.
package agent

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/zdgeier/git-lfs-chunked/chunking"
)

const (
	// Name is the transfer name advertised to servers. It matches the
	// git-lfs fork's built-in adapter so servers cannot tell them apart.
	Name = "chunked"

	lfsMediaType = "application/vnd.git-lfs+json"
	userAgent    = "git-lfs-chunked"

	// maxChunkSize bounds the memory a single chunk download may use.
	maxChunkSize = 256 << 20

	// chunkHashesHeader lists, on a manifest GET, the chunk hashes the
	// client can verify. A server must not answer with a manifest in any
	// other hash; without the header only sha256 is acceptable.
	chunkHashesHeader = "LFS-Chunk-Hashes"
)

// supportedChunkHashes is chunking.SupportedChunkHashes as wire strings.
func supportedChunkHashes() []string {
	out := make([]string, len(chunking.SupportedChunkHashes))
	for i, h := range chunking.SupportedChunkHashes {
		out[i] = string(h)
	}
	return out
}

// Agent runs the custom transfer protocol on a pair of streams.
type Agent struct {
	Store   Store
	Params  chunking.Params
	Chunker chunking.Chunker
	Client  *http.Client
	// Trace, if non-nil, receives diagnostic lines (git-lfs shows the
	// agent's stderr when GIT_TRANSFER_TRACE is set).
	Trace io.Writer

	index *chunkIndex
	outMu sync.Mutex
	out   *json.Encoder

	// hashMu guards chunkHash: the chunk hash the server last asked for.
	// An agent process serves one remote, so after the first renegotiation
	// later proposals use the server's choice directly.
	hashMu    sync.Mutex
	chunkHash chunking.ChunkHash
}

// New builds an agent for the given store and chunking parameters.
func New(st Store, p chunking.Params) (*Agent, error) {
	c, err := chunking.New(p)
	if err != nil {
		return nil, err
	}
	if pc, ok := c.(interface{ Params() chunking.Params }); ok {
		p = pc.Params()
	}
	return &Agent{
		Store:     st,
		Params:    p,
		Chunker:   c,
		Client:    &http.Client{Timeout: 10 * time.Minute},
		index:     newChunkIndex(st),
		chunkHash: chunking.SHA256,
	}, nil
}

func (a *Agent) preferredHash() chunking.ChunkHash {
	a.hashMu.Lock()
	defer a.hashMu.Unlock()
	return a.chunkHash
}

func (a *Agent) setPreferredHash(h chunking.ChunkHash) {
	a.hashMu.Lock()
	defer a.hashMu.Unlock()
	a.chunkHash = h
}

func (a *Agent) trace(format string, args ...interface{}) {
	if a.Trace != nil {
		fmt.Fprintf(a.Trace, "chunked: "+format+"\n", args...)
	}
}

func (a *Agent) send(r *response) error {
	a.outMu.Lock()
	defer a.outMu.Unlock()
	return a.out.Encode(r)
}

func (a *Agent) progress(oid string, soFar, since int64) {
	a.send(&response{Event: "progress", Oid: oid, BytesSoFar: soFar, BytesSinceLast: since})
}

func (a *Agent) complete(oid, path string, err error) error {
	r := &response{Event: "complete", Oid: oid, Path: path}
	if err != nil {
		a.trace("%s failed: %s", oid, err)
		r.Error = &transferError{Code: 1, Message: err.Error()}
	}
	return a.send(r)
}

// Run reads requests from in until "terminate" or EOF, writing responses
// to out.
func (a *Agent) Run(in io.Reader, out io.Writer) error {
	a.out = json.NewEncoder(out)
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			return fmt.Errorf("malformed request %q: %w", line, err)
		}
		switch req.Event {
		case "init":
			a.trace("init: operation=%s remote=%s using %s", req.Operation, req.Remote, a.Params)
			if err := a.send(&response{}); err != nil {
				return err
			}
		case "upload":
			if err := a.complete(req.Oid, "", a.upload(&req)); err != nil {
				return err
			}
		case "download":
			path, err := a.download(&req)
			if err := a.complete(req.Oid, path, err); err != nil {
				return err
			}
		case "terminate":
			return nil
		default:
			a.trace("ignoring unknown event %q", req.Event)
		}
	}
	return scanner.Err()
}

// ---- HTTP helpers ------------------------------------------------------

func (a *Agent) newRequest(method string, act *action, base *action, body io.Reader) (*http.Request, error) {
	if act == nil || act.Href == "" {
		return nil, fmt.Errorf("missing action href")
	}
	req, err := http.NewRequest(method, act.Href, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	// Object-level headers (typically authorization) apply to chunk
	// requests too unless the chunk action overrides them.
	if base != nil {
		for k, v := range base.Header {
			req.Header.Set(k, v)
		}
	}
	for k, v := range act.Header {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (a *Agent) do(req *http.Request) (*http.Response, error) {
	res, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		res.Body.Close()
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", req.Method, strings.SplitN(req.URL.String(), "?", 2)[0], res.StatusCode, strings.TrimSpace(string(msg)))
	}
	return res, nil
}

func (a *Agent) postJSON(act, base *action, v interface{}, out interface{}) error {
	by, err := json.Marshal(v)
	if err != nil {
		return err
	}
	req, err := a.newRequest("POST", act, base, bytes.NewReader(by))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(by))
	req.Header.Set("Content-Type", lfsMediaType)
	req.Header.Set("Accept", lfsMediaType)
	res, err := a.do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if out == nil {
		io.Copy(io.Discard, res.Body)
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

// ---- upload ------------------------------------------------------------

func (a *Agent) upload(req *request) error {
	if req.Action == nil {
		return fmt.Errorf("no upload action for %s (standalone mode is not supported)", req.Oid)
	}
	f, err := os.Open(req.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	m, err := chunking.BuildWithHash(f, a.Chunker, a.preferredHash())
	if err != nil {
		return fmt.Errorf("chunking %s: %w", req.Oid, err)
	}
	if m.Oid != req.Oid || m.Size != req.Size {
		return fmt.Errorf("local object %s is corrupt: content hashes to %s (%d bytes)", req.Oid, m.Oid, m.Size)
	}
	a.trace("%s split into %d chunks", req.Oid, len(m.Chunks))

	// 1. Propose the manifest; learn which chunks the server lacks. The
	// server may instead name a different chunk hash, in which case we
	// re-describe the same chunks with it and propose once more.
	wire, plan, err := a.propose(req, f, m)
	if err != nil {
		return err
	}
	m = wire.manifest()
	saveManifest(a.Store, m)
	a.trace("server is missing %d of %d chunks for %s", len(plan.Chunks), len(m.Chunks), req.Oid)

	// 2. Send the missing chunks.
	have := make(map[string]chunking.ManifestChunk, len(m.Chunks))
	for _, c := range m.Chunks {
		have[c.Oid] = c
	}
	var sent int64
	buf := make([]byte, 0, a.Params.MaxSize)
	for _, pc := range plan.Chunks {
		mc, ok := have[pc.Oid]
		if !ok || (pc.Size != 0 && pc.Size != mc.Size) {
			return fmt.Errorf("server requested chunk %s which is not part of %s", pc.Oid, req.Oid)
		}
		up := pc.Actions["upload"]
		if up == nil {
			return fmt.Errorf("no upload action for chunk %s", pc.Oid)
		}
		if cap(buf) < int(mc.Size) {
			buf = make([]byte, 0, mc.Size)
		}
		buf = buf[:mc.Size]
		if _, err := f.ReadAt(buf, mc.Offset); err != nil {
			return fmt.Errorf("reading chunk %s: %w", mc.Oid, err)
		}
		a.trace("uploading chunk %s (%d bytes) for %s", mc.Oid, mc.Size, req.Oid)
		hreq, err := a.newRequest("PUT", up, req.Action, bytes.NewReader(buf))
		if err != nil {
			return err
		}
		hreq.ContentLength = mc.Size
		if hreq.Header.Get("Content-Type") == "" {
			hreq.Header.Set("Content-Type", "application/octet-stream")
		}
		res, err := a.do(hreq)
		if err != nil {
			return fmt.Errorf("uploading chunk %s: %w", mc.Oid, err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		sent += mc.Size
		a.progress(req.Oid, sent, mc.Size)
	}
	if skipped := req.Size - sent; skipped > 0 {
		a.progress(req.Oid, req.Size, skipped)
	}

	// 3. Commit, if the server wants an explicit step.
	if commit := plan.Actions["commit"]; commit != nil {
		if err := a.postJSON(commit, req.Action, wire, nil); err != nil {
			return fmt.Errorf("committing: %w", err)
		}
		a.trace("committed %s", req.Oid)
	}
	return nil
}

// propose POSTs m to the upload action and returns the manifest the server
// accepted (possibly re-hashed) along with the server's plan.
func (a *Agent) propose(req *request, f io.ReaderAt, m *chunking.Manifest) (*wireManifest, *wireManifest, error) {
	for attempt := 0; ; attempt++ {
		wire := toWire(m)
		wire.ChunkHashes = supportedChunkHashes()
		var plan wireManifest
		if err := a.postJSON(req.Action, nil, wire, &plan); err != nil {
			return nil, nil, fmt.Errorf("proposing manifest: %w", err)
		}
		have, _ := m.Hash()
		want, err := chunking.ParseChunkHash(plan.ChunkHash)
		if err != nil {
			return nil, nil, fmt.Errorf("server chose an %s for %s", err, req.Oid)
		}
		if want == have {
			wire.ChunkHashes = nil
			return wire, &plan, nil
		}
		if attempt > 0 {
			return nil, nil, fmt.Errorf("server rejected chunk hash %s for %s again, asking for %s", have, req.Oid, want)
		}
		a.trace("server wants %s chunk IDs for %s (proposed %s)", want, req.Oid, have)
		if m, err = m.Rehash(f, want); err != nil {
			return nil, nil, err
		}
		a.setPreferredHash(want)
	}
}

// ---- download ----------------------------------------------------------

func (a *Agent) download(req *request) (string, error) {
	if req.Action == nil {
		return "", fmt.Errorf("no download action for %s (standalone mode is not supported)", req.Oid)
	}
	hreq, err := a.newRequest("GET", req.Action, nil, nil)
	if err != nil {
		return "", err
	}
	hreq.Header.Set("Accept", lfsMediaType)
	hreq.Header.Set(chunkHashesHeader, strings.Join(supportedChunkHashes(), ", "))
	res, err := a.do(hreq)
	if err != nil {
		return "", fmt.Errorf("fetching manifest: %w", err)
	}
	defer res.Body.Close()

	if err := os.MkdirAll(a.Store.TempDir(), 0755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(a.Store.TempDir(), req.Oid+".*.chunked")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	fail := func(err error) (string, error) {
		f.Close()
		os.Remove(tmp)
		return "", err
	}

	hasher := sha256.New()
	out := io.MultiWriter(f, hasher)
	var m *chunking.Manifest

	ctype := res.Header.Get("Content-Type")
	if strings.HasPrefix(ctype, lfsMediaType) || strings.HasPrefix(ctype, "application/json") {
		var wire wireManifest
		if err := json.NewDecoder(res.Body).Decode(&wire); err != nil {
			return fail(fmt.Errorf("decoding manifest: %w", err))
		}
		m = wire.manifest()
		if err := m.Validate(req.Oid, req.Size); err != nil {
			return fail(err)
		}
		h, _ := m.Hash() // checked by Validate
		a.trace("%s has %d %s chunks", req.Oid, len(m.Chunks), h)
		var done int64
		for _, c := range wire.Chunks {
			if c.Size > maxChunkSize {
				return fail(fmt.Errorf("chunk %s is too large (%d bytes)", c.Oid, c.Size))
			}
			data, ok := a.index.read(h, c.Oid, c.Size)
			if ok {
				a.trace("reusing local chunk %s (%d bytes) for %s", c.Oid, c.Size, req.Oid)
			} else if data, err = a.getChunk(req, h, c); err != nil {
				return fail(err)
			}
			if _, err := out.Write(data); err != nil {
				return fail(err)
			}
			done += c.Size
			a.progress(req.Oid, done, c.Size)
		}
	} else {
		// Served whole; accept it like a basic download.
		a.trace("%s served as a whole object (%s)", req.Oid, ctype)
		n, err := io.Copy(out, res.Body)
		if err != nil {
			return fail(err)
		}
		a.progress(req.Oid, n, n)
	}

	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != req.Oid {
		return fail(fmt.Errorf("expected OID %s, got %s", req.Oid, actual))
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if m != nil {
		saveManifest(a.Store, m)
		a.index.add(m)
	}
	// git-lfs moves tmp into the object store; the cached manifest then
	// lets later downloads reuse this object's chunks.
	return tmp, nil
}

func (a *Agent) getChunk(req *request, h chunking.ChunkHash, c *wireChunk) ([]byte, error) {
	dl := c.Actions["download"]
	if dl == nil {
		return nil, fmt.Errorf("no download action for chunk %s", c.Oid)
	}
	a.trace("downloading chunk %s (%d bytes) for %s", c.Oid, c.Size, req.Oid)
	hreq, err := a.newRequest("GET", dl, req.Action, nil)
	if err != nil {
		return nil, err
	}
	res, err := a.do(hreq)
	if err != nil {
		return nil, fmt.Errorf("downloading chunk %s: %w", c.Oid, err)
	}
	defer res.Body.Close()
	buf := bytes.NewBuffer(make([]byte, 0, c.Size))
	n, err := io.Copy(buf, io.LimitReader(res.Body, c.Size+1))
	if err != nil {
		return nil, err
	}
	if n != c.Size {
		return nil, fmt.Errorf("chunk %s: expected %d bytes, got %d", c.Oid, c.Size, n)
	}
	if actual := h.Sum(buf.Bytes()); actual != c.Oid {
		return nil, fmt.Errorf("chunk of %s: expected ID %s, got %s", req.Oid, c.Oid, actual)
	}
	return buf.Bytes(), nil
}
