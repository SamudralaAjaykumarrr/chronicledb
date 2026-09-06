# Quickstart

Status: Phase 11. Every command below was actually run against this
repository to produce the output shown (edited only to trim
timestamps/PIDs/temp paths) — see
[`docs/testing-strategy.md`](testing-strategy.md) for the full test
suite this only summarizes, and
[`docs/configuration.md`](configuration.md) for the complete flag
reference.

## 0. Prerequisites

- Go — check the version pinned in [`go.mod`](../go.mod) and
  `.github/workflows/ci.yml` (currently 1.26.x).
- `curl` (for the HTTP control-plane steps below).
- No other tooling: ChronicleDB has zero external Go module
  dependencies (see [`docs/dependencies.md`](dependencies.md)).

```console
$ go version
go version go1.26.5 linux/amd64
```

## 1. Clone

```bash
git clone https://github.com/SamudralaAjaykumarrr/chronicledb.git
cd chronicledb
```

## 2. Build

```console
$ go build ./...
```

Builds every package, including `cmd/chronicledb-node`, with no
output on success.

## 3. Run the test suite

```console
$ go test ./...
ok  	github.com/SamudralaAjaykumarrr/chronicledb/cmd/chronicledb-node	0.4s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/fault	0.1s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/metrics	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/node	16.5s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/oracle	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/raft	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot	0.1s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/sql	1.5s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/storage	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/transport	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/txn	1.5s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/version	0.0s
ok  	github.com/SamudralaAjaykumarrr/chronicledb/internal/wal	0.8s
```

This is the fast suite (~17s on a development machine). It does
**not** include: `-race` (see [`CONTRIBUTING.md`](../CONTRIBUTING.md)),
the `-tags=integration` real-process suite, fuzz targets, or a full
benchmark run — see [`docs/testing-strategy.md`](testing-strategy.md)
and [`docs/benchmarks.md`](benchmarks.md) for those.

## 4. Start a single node

The smallest real thing you can run: one `chronicledb-node` process,
as a one-node Raft "cluster" (still goes through the real commit path
— election, replication trivially satisfied by a quorum of one, WAL
durability — just without other nodes to replicate to).

```bash
go build -o /tmp/chronicledb-node ./cmd/chronicledb-node
mkdir -p /tmp/chronicledb-demo/n1
/tmp/chronicledb-node \
  -id=n1 -listen=127.0.0.1:20001 -http=127.0.0.1:20002 \
  -datadir=/tmp/chronicledb-demo/n1 -cluster=n1 -peers= &
```

Check its version and status:

```console
$ /tmp/chronicledb-node -version
chronicledb-node dev (commit none, built unknown)
```

(A release binary built via `scripts/build-release.sh` or the release
workflow reports a real semantic version/commit/date instead of
`dev`/`none`/`unknown` — see [`docs/versioning.md`](versioning.md).)

```console
$ curl -s http://127.0.0.1:20002/status
{"ID":"n1","Role":2,"Term":1,"Leader":"n1","CommitIndex":1,"AppliedIndex":1,"LastIndex":1,"SnapshotIndex":0}
```

`Role: 2` is `Leader` (see [`docs/observability.md`](observability.md)
for the Role enum and the complete field catalog).

## 5. Observe health and metrics

```console
$ curl -s http://127.0.0.1:20002/health
{"alive":true,"nodeStarted":true,"raftInitialized":true,"storageOpened":true,"role":"Leader","leaderKnown":true,"leader":"n1","note":"quorum availability is not reported: a Follower/Candidate cannot reliably know it, and a Leader only knows it as of its last successful heartbeat round"}

$ curl -s http://127.0.0.1:20002/metrics | head -6
# HELP chronicledb_raft_role current Raft role (0=Follower, 1=Candidate, 2=Leader)
# TYPE chronicledb_raft_role gauge
chronicledb_raft_role 2
# HELP chronicledb_raft_term current Raft term
# TYPE chronicledb_raft_term gauge
chronicledb_raft_term 1
```

See [`docs/observability.md`](observability.md) for the full metric
catalog.

## 6. Propose a write over the control-plane HTTP API

This is not a general client protocol
(`cmd/chronicledb-node/main.go`'s package doc explains why) — it is the
minimal HTTP surface the integration tests drive:

```console
$ curl -s -X POST http://127.0.0.1:20002/propose \
    -d '{"requestId":"qs-1","txnId":1,"startSeq":0,"mutations":[{"key":"hello","value":"chronicledb"}]}'
{"status":"committed","commitSeq":2}
```

Stop the node when done: `kill %1` (or `Ctrl+C` if run in the
foreground). Data under `/tmp/chronicledb-demo` is not deleted for
you.

## 7. A real three-node cluster (scripted)

```bash
./scripts/demo-local-cluster.sh
```

Builds `chronicledb-node`, starts three real OS processes wired
together over real TCP with real disk-backed WALs, waits for a leader
election, proposes one write, and shows the committed state replicated
to a different node — printing every endpoint so you can keep
poking at it with `curl` before pressing Ctrl+C. See
[`docs/configuration.md`](configuration.md) for the equivalent manual,
non-scripted commands.

## 8. Exercise the SQL surface

There is no SQL CLI or wire protocol (see
[`docs/sql.md`](sql.md) §8) — `internal/sql` is a Go library. The
actually-available way to try it:

```console
$ go run ./examples/sql-basics
created table accounts
inserted 2 rows
SELECT WHERE id = 1: columns=[id name balance] rows=[[1 alice 100]]
after UPDATE, balance = 90
explicit transaction committed at CommitSeq=5
final table state:
  [1 alice 90]
  [2 bob 40]
```

See [`examples/sql-basics/main.go`](../examples/sql-basics/main.go) for
the source, and [`examples/basic-transaction`](../examples/basic-transaction)
for the same idea one layer down (raw `internal/txn.Manager`
transactions, no SQL).

## What next

- [`docs/README.md`](README.md) — full documentation map.
- [`docs/configuration.md`](configuration.md) — every
  `chronicledb-node` flag.
- [`docs/testing-strategy.md`](testing-strategy.md) /
  [`docs/adversarial-testing.md`](adversarial-testing.md) — the full
  test suite, including `-race`, chaos, adversarial, and fuzz targets.
- [`docs/benchmarks.md`](benchmarks.md) — measured performance and how
  to reproduce it.
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — how to contribute.
