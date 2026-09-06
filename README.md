# ChronicleDB

ChronicleDB is a from-scratch, open-source distributed transactional
database project. It is being built to demonstrate real engineering in
durable storage, write-ahead logging and crash recovery, multi-version
concurrency control under Snapshot Isolation, transactions with
deterministic conflict detection, request idempotency, a deterministic
replicated state machine, Raft consensus, and deterministic
distributed-systems testing — not to be a CRUD app, a toy key-value
store, or a wrapper around an existing finished database or consensus
library. See [`docs/vision.md`](docs/vision.md) for the full statement
of intent and explicit non-goals.

Current maturity: **TRANSACTIONAL ENGINE**

Phase 1 — durable append-only segment storage (`internal/storage`) and
a checksummed, replayable write-ahead log with crash recovery
(`internal/wal`) — is implemented and tested against the
`docs/scenario-corpus.md` §Local Durability scenarios (LD-1 through
LD-6), including real subprocess kill-based crash tests and direct
on-disk corruption injection. See [`docs/wal.md`](docs/wal.md) and
[`docs/storage.md`](docs/storage.md) for the implemented design.

Phase 2 — MVCC + transactions + Snapshot Isolation (`internal/mvcc`,
`internal/txn`) — is also implemented and tested against the
`docs/scenario-corpus.md` §Transactions scenarios (TX-1 through TX-8):
MVCC visibility, own-write shadowing, first-committer-wins write-write
conflicts, atomic multi-key commit, abort safety, tombstone visibility,
stable snapshots, and a demonstrated Snapshot-Isolation write-skew
example (ChronicleDB does **not** claim SERIALIZABLE isolation — see
[`docs/mvcc.md`](docs/mvcc.md) §1.1). Transaction commits are durably
integrated with Phase 1's WAL and proven to survive restart, including
a real subprocess kill-based crash test. See
[`docs/mvcc.md`](docs/mvcc.md) and
[`docs/transactions.md`](docs/transactions.md) for the implemented
design.

Phase 3 — the deterministic state-machine boundary (`internal/fsm`)
and `RequestID` idempotency — is also implemented and tested against
`docs/scenario-corpus.md` §Idempotency (ID-1 through ID-7; the
snapshot+compaction leg of ID-4 remains Phase 6 scope). `internal/fsm`
is now the sole `Apply(index, command) -> outcome` boundary for
transaction commits, proven deterministic by feeding an identical
ordered command history into two independently constructed `FSM`
instances and asserting byte-identical outcomes. The `RequestID`
outcome table durably survives restart for both `COMMITTED` and
`ABORTED` (conflicted) outcomes, detects a mismatched-payload reuse of
the same `RequestID`, and is queryable via `Manager.GetRequestOutcome`
without resubmitting a mutation payload. See
[`docs/transactions.md`](docs/transactions.md) §6-7/§10 and
[`docs/recovery.md`](docs/recovery.md) §5-6 for the implemented
design.

Per [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model, `TRANSACTIONAL
ENGINE` is Phases 2-3 together; both are now complete and evidenced.

No replication, Raft, or SQL implementation exists yet. See
[`docs/roadmap.md`](docs/roadmap.md) §Maturity Model for the
evidence-based gates that govern every future maturity claim, and
[`docs/architecture.md`](docs/architecture.md) for the system design
itself.

## Documentation

Start at [`docs/README.md`](docs/README.md) for the full reading order.
Key documents:

- [`docs/vision.md`](docs/vision.md) — what ChronicleDB is and is not.
- [`docs/architecture.md`](docs/architecture.md) — system boundaries
  and binding terminology.
- [`docs/invariants.md`](docs/invariants.md) — the correctness
  invariant catalog.
- [`docs/roadmap.md`](docs/roadmap.md) — phase sequence and maturity
  model.
- [`docs/adr/`](docs/adr/) — Architecture Decision Records.
