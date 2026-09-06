# Examples

Runnable programs exercising ChronicleDB's actual, currently-available
APIs — no fake or aspirational surfaces. Each is `go run`-able directly
from a clone with no setup beyond the Go toolchain (see
[`docs/quickstart.md`](../docs/quickstart.md)).

- [`basic-transaction`](basic-transaction) — the real transaction
  engine directly (`internal/txn.Manager`, `internal/mvcc`,
  `internal/wal`), standalone (no Raft): atomic multi-key commit,
  Snapshot Isolation reads, and `RequestID` idempotency.
- [`sql-basics`](sql-basics) — the constrained SQL frontend
  (`internal/sql`, [`docs/sql.md`](../docs/sql.md)) against a real
  standalone engine: `CREATE TABLE`/`INSERT`/`SELECT`/`UPDATE`, and an
  explicit `BEGIN`/`COMMIT` transaction.

For a real multi-node replicated cluster driven over HTTP (no Go code
required), see `scripts/demo-local-cluster.sh` and
[`docs/quickstart.md`](../docs/quickstart.md) instead — there is no
Go client library for the replicated/`internal/node` path in these
examples, since none is offered as a public API yet
(`docs/versioning.md`).
