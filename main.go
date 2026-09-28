// git-lfs-chunked is a Git LFS custom transfer agent that moves objects as
// content-defined chunks, so that after a large file is edited only the
// changed chunks are uploaded or downloaded.
//
// It needs no changes to git-lfs: "git-lfs-chunked install" registers it as
// the "chunked" custom transfer, git-lfs advertises that name to servers,
// and servers which implement the chunked transfer protocol pick it.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/zdgeier/git-lfs-chunked/chunking"
	"github.com/zdgeier/git-lfs-chunked/internal/agent"
	"github.com/zdgeier/git-lfs-chunked/internal/gitenv"
)

var version = "dev"

func usage() {
	fmt.Fprintf(os.Stderr, `usage: git-lfs-chunked <command> [options]

commands:
  install [--global] [--algorithm A] [--min-size S] [--avg-size S] [--max-size S] [--normalization N]
          register this program as the "chunked" Git LFS transfer agent
          (and optionally set lfs.chunking.* tuning) in the repository or
          globally
  uninstall [--global]
          remove that configuration
  transfer
          run the custom transfer protocol on stdin/stdout (git-lfs runs this)
  chunk [--hash sha256|blake3] <file>
          print the chunk manifest for a file using the configured algorithm
  version
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "transfer":
		err = runTransfer()
	case "install":
		err = runInstall(os.Args[2:])
	case "uninstall":
		err = runUninstall(os.Args[2:])
	case "chunk":
		err = runChunk(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("git-lfs-chunked %s\n", version)
	case "help", "--help", "-h":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "git-lfs-chunked: %s\n", err)
		os.Exit(1)
	}
}

func loadParams(env *gitenv.Env) (chunking.Params, error) {
	p, err := chunking.ParamsFromEnv(env)
	if err != nil {
		return p, err
	}
	return p, nil
}

func runTransfer() error {
	env, err := gitenv.Discover()
	if err != nil {
		return err
	}
	params, err := loadParams(env)
	if err != nil {
		return err
	}
	a, err := agent.New(env, params)
	if err != nil {
		return err
	}
	if os.Getenv("GIT_TRANSFER_TRACE") != "" || os.Getenv("GIT_TRACE") != "" {
		a.Trace = os.Stderr
	}
	return a.Run(bufio.NewReader(os.Stdin), os.Stdout)
}

func runInstall(args []string) error {
	fs := flag.NewFlagSet("install", flag.ContinueOnError)
	global := fs.Bool("global", false, "write to the global git config instead of the repository's")
	algorithm := fs.String("algorithm", "", "chunking algorithm (fastcdc or fixed)")
	minSize := fs.String("min-size", "", "minimum chunk size, e.g. 256k")
	avgSize := fs.String("avg-size", "", "target chunk size, e.g. 1m")
	maxSize := fs.String("max-size", "", "maximum chunk size, e.g. 4m")
	normalization := fs.String("normalization", "", "FastCDC normalization level 0-3")
	if err := fs.Parse(args); err != nil {
		return err
	}

	exe, err := os.Executable()
	if err != nil {
		return err
	}
	// Prefer a bare name when we are on PATH so the config survives
	// reinstalls and works across machines that share a global config.
	path := exe
	if found, err := exec.LookPath(filepath.Base(exe)); err == nil {
		if abs, err := filepath.Abs(found); err == nil && sameFile(abs, exe) {
			path = filepath.Base(exe)
		}
	}

	settings := [][2]string{
		{"lfs.customtransfer." + agent.Name + ".path", path},
		{"lfs.customtransfer." + agent.Name + ".args", "transfer"},
		{"lfs.customtransfer." + agent.Name + ".concurrent", "true"},
	}
	for k, v := range map[string]string{
		"lfs.chunking.algorithm":     *algorithm,
		"lfs.chunking.minsize":       *minSize,
		"lfs.chunking.avgsize":       *avgSize,
		"lfs.chunking.maxsize":       *maxSize,
		"lfs.chunking.normalization": *normalization,
	} {
		if v != "" {
			settings = append(settings, [2]string{k, v})
		}
	}
	// Validate the tuning before writing anything.
	probe := map[string]string{}
	for _, s := range settings {
		probe[s[0]] = s[1]
	}
	if _, err := chunking.New(mustParams(probe)); err != nil {
		return err
	}

	for _, s := range settings {
		if err := gitenv.SetConfig(*global, s[0], s[1]); err != nil {
			return err
		}
	}
	where := "this repository"
	if *global {
		where = "the global git config"
	}
	fmt.Printf("Registered %q transfer agent (%s) in %s.\n", agent.Name, path, where)
	fmt.Println("Servers that support the chunked transfer will now use it; others are unaffected.")
	return nil
}

type mapEnv map[string]string

func (m mapEnv) Get(k string) (string, bool) { v, ok := m[k]; return v, ok }
func (m mapEnv) Int(k string, def int) int   { return def }

func mustParams(m map[string]string) chunking.Params {
	p, err := chunking.ParamsFromEnv(mapEnv(m))
	if err != nil {
		fmt.Fprintf(os.Stderr, "git-lfs-chunked: %s\n", err)
		os.Exit(1)
	}
	return p
}

func sameFile(a, b string) bool {
	sa, err1 := os.Stat(a)
	sb, err2 := os.Stat(b)
	return err1 == nil && err2 == nil && os.SameFile(sa, sb)
}

func runUninstall(args []string) error {
	fs := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	global := fs.Bool("global", false, "remove from the global git config instead of the repository's")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, k := range []string{"path", "args", "concurrent"} {
		if err := gitenv.UnsetConfig(*global, "lfs.customtransfer."+agent.Name+"."+k); err != nil {
			return err
		}
	}
	fmt.Printf("Removed the %q transfer agent configuration.\n", agent.Name)
	return nil
}

func runChunk(args []string) error {
	fs := flag.NewFlagSet("chunk", flag.ContinueOnError)
	hashName := fs.String("hash", "sha256", "chunk ID hash (sha256 or blake3)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()
	if len(args) != 1 {
		return fmt.Errorf("usage: git-lfs-chunked chunk [--hash sha256|blake3] <file>")
	}
	h, err := chunking.ParseChunkHash(*hashName)
	if err != nil {
		return err
	}
	params := chunking.DefaultParams()
	if env, err := gitenv.Discover(); err == nil {
		if params, err = loadParams(env); err != nil {
			return err
		}
	}
	c, err := chunking.New(params)
	if err != nil {
		return err
	}
	f, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer f.Close()
	m, err := chunking.BuildWithHash(f, c, h)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "%d chunks, %s\n", len(m.Chunks), strings.TrimSpace(params.String()))
	return nil
}
