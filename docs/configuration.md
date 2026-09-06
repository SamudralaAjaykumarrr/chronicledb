# Configuration

Status: Phase 11. `cmd/chronicledb-node` is configured entirely via
command-line flags (`cmd/chronicledb-node/main.go`) — there is no
config file format (YAML/TOML/JSON) today, so this document, not a
parsed config file, is the authoritative reference. Introducing a
config-file parser purely for packaging convenience, with no other
driver, was deliberately not done (see
[`docs/dependencies.md`](dependencies.md) and
[`docs/non-goals.md`](non-goals.md) for the general policy against
building infrastructure ahead of a concrete need).

## Flags

| Flag | Required | Meaning |
|---|---|---|
| `-id` | Yes | This node's ID (`raft.NodeID`) — must be unique within the cluster and must appear in `-cluster`. |
| `-listen` | Yes | This node's Raft transport listen address (`host:port`), e.g. `127.0.0.1:9001`. |
| `-http` | Yes | This node's control-plane HTTP listen address (`host:port`) — see [`docs/observability.md`](observability.md) for the endpoints it serves. |
| `-datadir` | Yes | Directory for this node's durable WAL/snapshot files. One dedicated directory per node — never share a data directory between two nodes or two processes. |
| `-cluster` | Yes | Comma-separated list of every cluster member's ID, **including this node** — e.g. `n1,n2,n3`. Identical across every node in the cluster. |
| `-peers` | Yes (if the cluster has more than one node) | Comma-separated `id=host:port` list for every **other** cluster member's `-listen` address — e.g. `n2=127.0.0.1:9002,n3=127.0.0.1:9003`. |
| `-snapshot-threshold` | No (default: package default, see [`docs/snapshots.md`](snapshots.md)) | Number of durable log entries since the last snapshot before this node creates a new one and compacts its log. |
| `-version` | No | Print version information (`internal/version`) and exit 0, ignoring every other flag. |

There is no flag to disable durability (fsync), authentication, or
TLS — the first because durability is this project's core correctness
property (never configurable off), the second and third because
neither is implemented yet (see [`SECURITY.md`](../SECURITY.md) and
[`docs/non-goals.md`](non-goals.md) §Authentication and TLS).

## Example: a real three-node cluster on one machine

Three separate `-datadir` values and three separate ports, run as
three separate OS processes (see [`docs/quickstart.md`](quickstart.md)
for the fully worked, tested version of this, and
`scripts/demo-local-cluster.sh` for a scripted equivalent):

```bash
./chronicledb-node -id=n1 -listen=127.0.0.1:9001 -http=127.0.0.1:8001 \
  -datadir=/tmp/chronicledb-demo/n1 -cluster=n1,n2,n3 \
  -peers=n2=127.0.0.1:9002,n3=127.0.0.1:9003 &

./chronicledb-node -id=n2 -listen=127.0.0.1:9002 -http=127.0.0.1:8002 \
  -datadir=/tmp/chronicledb-demo/n2 -cluster=n1,n2,n3 \
  -peers=n1=127.0.0.1:9001,n3=127.0.0.1:9003 &

./chronicledb-node -id=n3 -listen=127.0.0.1:9003 -http=127.0.0.1:8003 \
  -datadir=/tmp/chronicledb-demo/n3 -cluster=n1,n2,n3 \
  -peers=n1=127.0.0.1:9001,n2=127.0.0.1:9002 &
```

A single standalone node (no replication — mostly useful for local
experimentation with the SQL frontend, see
[`examples/sql-basics`](../examples/sql-basics)) is not something
`cmd/chronicledb-node` supports directly: `-cluster`/`-peers` always
wire up Raft, even for a "cluster" of one. For a truly standalone
(pre-Raft) engine, use `internal/txn.Manager` or
`internal/sql.NewStandaloneEngine` directly as a library — see
[`examples/basic-transaction`](../examples/basic-transaction).

## Environment variables

`cmd/chronicledb-node` itself reads none. Test suites read a small
number for controlling seed counts at higher-than-CI levels
(`CHRONICLEDB_CHAOS_SEEDS`, `CHRONICLEDB_ADVERSARIAL_SEEDS`) — see
[`docs/testing-strategy.md`](testing-strategy.md) §6.5 and
[`docs/adversarial-testing.md`](adversarial-testing.md).
