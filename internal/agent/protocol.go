package agent

import "github.com/zdgeier/git-lfs-chunked/chunking"

// Messages of the Git LFS custom transfer protocol
// (https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md),
// line-delimited JSON over stdin/stdout.

// request is any message git-lfs sends us; fields are set per event.
type request struct {
	Event               string  `json:"event"`
	Operation           string  `json:"operation,omitempty"`
	Remote              string  `json:"remote,omitempty"`
	Concurrent          bool    `json:"concurrent,omitempty"`
	ConcurrentTransfers int     `json:"concurrenttransfers,omitempty"`
	Oid                 string  `json:"oid,omitempty"`
	Size                int64   `json:"size,omitempty"`
	Path                string  `json:"path,omitempty"`
	Action              *action `json:"action,omitempty"`
}

// action is the upload/download action from the Batch API response.
type action struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header,omitempty"`
}

type transferError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type response struct {
	Event          string         `json:"event,omitempty"`
	Oid            string         `json:"oid,omitempty"`
	Path           string         `json:"path,omitempty"`
	BytesSoFar     int64          `json:"bytesSoFar,omitempty"`
	BytesSinceLast int64          `json:"bytesSinceLast,omitempty"`
	Error          *transferError `json:"error,omitempty"`
}

// Wire types of the chunked transfer protocol
// (docs/proposals/chunked_transfer_mode.md in the git-lfs fork): a
// chunking.Manifest with per-chunk and per-object actions attached.

type wireManifest struct {
	Oid       string           `json:"oid"`
	Size      int64            `json:"size"`
	Algorithm *chunking.Params `json:"algorithm,omitempty"`
	// ChunkHash names the hash of the chunk IDs in this message; empty
	// means sha256. In a propose response it is the hash the server wants.
	ChunkHash string `json:"chunk_hash,omitempty"`
	// ChunkHashes, sent only in proposals, lists the chunk hashes the
	// client can switch to.
	ChunkHashes []string           `json:"chunk_hashes,omitempty"`
	Chunks      []*wireChunk       `json:"chunks"`
	Actions     map[string]*action `json:"actions,omitempty"`
}

type wireChunk struct {
	Oid     string             `json:"oid"`
	Size    int64              `json:"size"`
	Offset  int64              `json:"offset"`
	Actions map[string]*action `json:"actions,omitempty"`
}

func toWire(m *chunking.Manifest) *wireManifest {
	w := &wireManifest{Oid: m.Oid, Size: m.Size, Algorithm: m.Algorithm, ChunkHash: m.ChunkHash, Chunks: make([]*wireChunk, 0, len(m.Chunks))}
	for _, c := range m.Chunks {
		w.Chunks = append(w.Chunks, &wireChunk{Oid: c.Oid, Size: c.Size, Offset: c.Offset})
	}
	return w
}

func (w *wireManifest) manifest() *chunking.Manifest {
	m := &chunking.Manifest{Oid: w.Oid, Size: w.Size, Algorithm: w.Algorithm, ChunkHash: w.ChunkHash, Chunks: make([]chunking.ManifestChunk, 0, len(w.Chunks))}
	for _, c := range w.Chunks {
		m.Chunks = append(m.Chunks, chunking.ManifestChunk{Oid: c.Oid, Size: c.Size, Offset: c.Offset})
	}
	return m
}
