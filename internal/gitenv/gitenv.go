// Package gitenv discovers the bits of Git and Git LFS configuration the
// transfer agent needs. Git LFS does not forward its configuration to custom
// transfer agents, so the agent asks git itself, in the working directory it
// was started in (git-lfs runs agents inside the repository).
package gitenv

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// Env is the resolved repository environment.
type Env struct {
	// GitDir is the repository's common git directory (shared between
	// worktrees), where Git LFS keeps its storage by default.
	GitDir string
	// LFSStorageDir is the Git LFS storage directory: ".git/lfs" unless
	// lfs.storage overrides it.
	LFSStorageDir string
	// Config holds every git config value, last-wins, keyed by lowercase
	// name.
	Config map[string]string
}

// Discover runs git to resolve the environment for the current directory.
func Discover() (*Env, error) {
	gitDir, err := git("rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil {
		return nil, fmt.Errorf("not inside a git repository: %w", err)
	}
	gitDir = strings.TrimSpace(gitDir)

	e := &Env{GitDir: gitDir, Config: map[string]string{}}
	if out, err := git("config", "--list", "-z"); err == nil {
		for _, kv := range strings.Split(out, "\x00") {
			if kv == "" {
				continue
			}
			k, v, _ := strings.Cut(kv, "\n")
			e.Config[strings.ToLower(k)] = v
		}
	}

	e.LFSStorageDir = filepath.Join(gitDir, "lfs")
	if storage, ok := e.Config["lfs.storage"]; ok && storage != "" {
		if filepath.IsAbs(storage) {
			e.LFSStorageDir = storage
		} else {
			e.LFSStorageDir = filepath.Join(gitDir, storage)
		}
	}
	return e, nil
}

// Get implements chunking.Env.
func (e *Env) Get(key string) (string, bool) {
	v, ok := e.Config[strings.ToLower(key)]
	return v, ok
}

// Int implements chunking.Env.
func (e *Env) Int(key string, def int) int {
	v, ok := e.Get(key)
	if !ok {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}

// ObjectPath returns where Git LFS stores the object with the given ID.
func (e *Env) ObjectPath(oid string) string {
	return filepath.Join(e.LFSStorageDir, "objects", oid[0:2], oid[2:4], oid)
}

// ManifestPath returns where cached chunk manifests live. The layout matches
// the git-lfs "chunked" adapter so the two can share a cache.
func (e *Env) ManifestPath(oid string) string {
	return filepath.Join(e.LFSStorageDir, "chunks", oid[0:2], oid[2:4], oid+".json")
}

// TempDir returns a directory on the same filesystem as the object store,
// so that git-lfs can move downloaded files into place.
func (e *Env) TempDir() string {
	return filepath.Join(e.LFSStorageDir, "tmp")
}

func git(args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// SetConfig writes a git config value, globally or for the current repo.
func SetConfig(global bool, key, value string) error {
	args := []string{"config"}
	if global {
		args = append(args, "--global")
	}
	_, err := git(append(args, key, value)...)
	return err
}

// UnsetConfig removes a git config value if present.
func UnsetConfig(global bool, key string) error {
	args := []string{"config"}
	if global {
		args = append(args, "--global")
	}
	_, err := git(append(args, "--unset", key)...)
	if err != nil && strings.Contains(err.Error(), "exit status 5") {
		return nil // key was not set
	}
	return err
}
