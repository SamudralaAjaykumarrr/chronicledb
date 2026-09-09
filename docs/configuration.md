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

### Security Foundation flags (`v0.2.0`, `docs/enterprise-v1-plan.md` §5)

See [`docs/security.md`](security.md) for the full operational guide.
Every flag below defaults to `v0.1.0`'s exact plaintext/unauthenticated
behavior — none of them are required.

| Flag | Required | Meaning |
|---|---|---|
| `-tls-cert` | No | Control-plane HTTP TLS certificate file. Enables client TLS when set (together with `-tls-key`). |
| `-tls-key` | No | Control-plane HTTP TLS private key file. |
| `-tls-ca` | No | CA bundle used to verify client certificates presented to the control-plane HTTP server. Optional unless `-auth-mode=mtls`. |
| `-peer-tls-cert` | No | Peer Raft transport mTLS certificate file. All three `-peer-tls-*` flags must be set together or all left empty — a partial configuration is refused at startup. |
| `-peer-tls-key` | No | Peer Raft transport mTLS private key file. |
| `-peer-tls-ca` | No | CA bundle trusted for peer mTLS. |
| `-auth-mode` | No (default `none`) | Client authentication mode: `none`, `token`, or `mtls`. `none` matches pre-Security-Foundation behavior exactly. |
| `-auth-token-file` | Required if `-auth-mode=token` | Bearer token file, lines of `<token>:<principal>`. |
| `-rbac-mapping-file` | Required if `-auth-mode` is not `none` | JSON file mapping principal name to role (`admin`\|`operator`\|`read-only`). |
| `-audit-log-dir` | No (default `<datadir>/audit`) | Directory for the hash-chained administrative audit log. |
| `-enable-fault-endpoint` | No (default `false`) | Registers the `/fault` fault-injection endpoint. Off by default — the route does not exist at all unless set; never enable in production. |

There is no flag to disable durability (fsync) — durability is this
project's core correctness property, never configurable off.

### Backup / Disaster Recovery / PITR flags (`v0.3.0`, `docs/enterprise-v1-plan.md` §6)

See [`docs/backup.md`](backup.md) for the full format, RPO/RTO model,
and restore runbook. Taking a backup of a *running* node is an
admin/operator-gated HTTP action (`POST /admin/backup?dir=<path>&continuous=<true|false>`,
audited per [`docs/security.md`](security.md)), not a flag — there is
no `-backup-to` flag: this phase's plan itself left the exact CLI shape
open ("or a new `chronicledb-ctl backup` subcommand"), and only the
already-live, already-authenticated node process can safely read a
consistent point of its own in-memory/durable state; see
[`docs/backup.md`](backup.md) §Deviations for the full reasoning.
Restore, by contrast, is a startup-only flag: it only ever targets a
not-yet-opened data directory, run once, before the node ever joins a
cluster.

| Flag | Required | Meaning |
|---|---|---|
| `-restore-from` | No | Before opening `-datadir`, restore it from the backup at this directory. Refuses to run against a non-empty `-datadir` unless `-force-overwrite` is also set. Startup continues normally (opens the now-restored data directory, joins/forms Raft) once restore succeeds. |
| `-restore-until` | No (default: everything the backup includes) | A PITR boundary: the last committed log index to restore up to. Only meaningful together with `-restore-from`. Must name a real committed log index — there is no wall-clock/timestamp form (see [`docs/backup.md`](backup.md) §PITR boundary model for why). |
| `-force-overwrite` | No (default `false`) | Required, together with `-restore-from`, to restore over a `-datadir` that already contains WAL/snapshot state — this destroys that existing state (DESTRUCTIVE RESTORE ISOLATION). Produces an audit record; a failure to write that record aborts startup. |

### Compatibility / Rolling Upgrades flag (`v0.4.0`, `docs/enterprise-v1-plan.md` §7)

See [`docs/upgrades.md`](upgrades.md) for the full runbook. Precheck and
finalize themselves are admin-gated, audited HTTP actions (`GET
/admin/upgrade/precheck`, `POST /admin/upgrade/finalize`), not flags —
`-upgrade-precheck` below is only a standalone CLI convenience for the
read-only dry run.

| Flag | Required | Meaning |
|---|---|---|
| `-upgrade-precheck` | No | Dry-run only: query an already-running node's HTTP control-plane address (`host:port`) for `/admin/upgrade/precheck` and print the result, then exit. Never opens `-datadir` and never calls `node.Open`. Plain HTTP only in this release — see [`docs/upgrades.md`](upgrades.md) §7. |

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
