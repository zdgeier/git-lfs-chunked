# git-lfs-chunked

A [Git LFS custom transfer agent](https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md)
that moves objects as **content-defined chunks**. After you edit a large
file, only the chunks that changed are uploaded — and only the chunks you
don't already have are downloaded.

It works with stock `git-lfs` (no fork, no patches): it registers itself as
the `chunked` transfer, `git-lfs` advertises that name to the server, and
servers that implement the [chunked transfer protocol](https://github.com/zdgeier/git-lfs/blob/fastcdc-chunked-transfers/docs/proposals/chunked_transfer_mode.md)
pick it. Servers that don't are unaffected — `git-lfs` falls back to `basic`.

Motivation: [git-lfs/git-lfs#6338](https://github.com/git-lfs/git-lfs/issues/6338)
— continuously-growing files (logs, CSV exports, training data) currently have
to be re-uploaded in full after every append.

## How much does it save?

For a 512 KiB file with a 7-byte insertion near the front, using 16 KiB
average chunks (the tests' settings): the second push uploads **1 of 25
chunks**, and a clone that already has the first version pulls the second by
fetching **1 chunk and reusing 24** from its local object store. Appends are
the best case: every chunk but the last one is unchanged.

## Install

```bash
go install github.com/zdgeier/git-lfs-chunked@latest
```

Then, in a repository (or with `--global`):

```bash
git-lfs-chunked install
```

That writes three config keys and nothing else:

```
lfs.customtransfer.chunked.path = git-lfs-chunked
lfs.customtransfer.chunked.args = transfer
lfs.customtransfer.chunked.concurrent = true
```

`git-lfs-chunked uninstall` removes them.

## Tuning

The agent reads the same `lfs.chunking.*` keys as the git-lfs fork's built-in
adapter, so the two are interchangeable:

| key | default | meaning |
|---|---|---|
| `lfs.chunking.algorithm` | `fastcdc` | `fastcdc` (content-defined) or `fixed` |
| `lfs.chunking.minsize` | `256k` | smallest chunk FastCDC will cut |
| `lfs.chunking.avgsize` | `1m` | target chunk size (exact size for `fixed`) |
| `lfs.chunking.maxsize` | `4m` | largest chunk FastCDC will cut |
| `lfs.chunking.normalization` | `1` | FastCDC normalized-chunking level, 0–3 |

Sizes accept `k`, `m`, `g` suffixes. `git-lfs-chunked install --avg-size 512k`
etc. sets them for you. Changing parameters only changes which chunks match
earlier uploads; it never affects correctness.

`git-lfs-chunked chunk <file>` prints the manifest a file would produce —
handy for checking how a change affects chunk boundaries.

## How it works

The `chunking` package implements FastCDC (Xia et al., 2016; the 2020
refinement with gear hashing and normalized chunking). Its boundaries are
byte-identical to the Rust [`fastcdc`](https://crates.io/crates/fastcdc)
crate's `v2020` variant, and chunk IDs are hex SHA-256 — the same function
LFS uses for object IDs. It streams, with memory bounded by the maximum
chunk size, at roughly 2.8 GB/s.

For an upload the agent POSTs a manifest (`{oid, size, chunks:[{oid, size,
offset}]}`) to the object's `upload` action, receives back the chunks the
server lacks, PUTs those, and POSTs a commit. For a download it GETs the
manifest, copies any chunk that already exists inside a local LFS object
(manifests are cached under `.git/lfs/chunks/`), fetches the rest, and
verifies every chunk and the whole object. `git-lfs` verifies the object
again before storing it, as it does for every custom transfer.

Authentication is whatever the server puts in the action's `header` (for
example a bearer token); it is forwarded to every chunk request.

## Limitations

* HTTP servers only. Pure SSH (`git-lfs-transfer`) remotes use git-lfs's
  built-in SSH transfer, which custom agents cannot intercept; the fork
  implements the SSH binding in-tree instead.
* Standalone mode (`lfs.standalonetransferagent`) is not supported: the
  agent needs the server's Batch API to learn the object endpoints.

## Development

```bash
make test    # go test -race ./...
make build   # bin/git-lfs-chunked
```

The agent tests drive the JSON protocol over pipes against an in-process
server. For an end-to-end run with real `git-lfs`, the
[fork's](https://github.com/zdgeier/git-lfs/tree/fastcdc-chunked-transfers)
`lfstest-gitserver` implements the server side.

## License

MIT.
