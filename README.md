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

Current maturity: **REPLICATED PROTOTYPE**

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

Phase 4 — a deterministic Raft consensus core (`internal/raft`) and a
deterministic distributed-systems test harness (`internal/fault`) — is
also implemented: real, unmodified `internal/raft.Core` instances
(follower/candidate/leader roles, term/vote safety, log replication and
divergent-suffix repair, the current-term commit rule) are proven
against election safety, vote safety across restart, quorum safety
under partition, stale-leader step-down, and determinism, all via
`internal/fault`'s in-memory transport/clock/storage — see
[`docs/raft.md`](docs/raft.md) §9 and
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) §Raft/Replication
for exactly what is (and is not) covered. Phase 4 deliberately stopped
short of real transport/disk — per [`docs/roadmap.md`](docs/roadmap.md),
that was Phase 5.

Phase 5 — real replicated storage, quorum commits, and leader failover
(`internal/node`, `internal/transport`) — is now also implemented: the
unchanged Phase 4 `internal/raft.Core` is wired to a real
`internal/wal`-backed `raft.Storage` adapter and a real TCP transport,
in a genuine three-node deployment. `internal/wal` gained the crash-safe
suffix-truncation capability Phase 4 identified as missing, so
divergent-suffix repair is now durable for real. The full client
mutation path (leader-only acceptance → Raft proposal → quorum
commit → deterministic `internal/fsm.Apply` → `RequestID` outcome →
response) is implemented and tested, including the central acceptance
scenario — a client's `RequestID` retry against a newly elected leader,
after the original leader crashes, resolves to the identical outcome
without double-applying anything — the full network-partition contract
(`docs/replication.md` §5), and ADR-0010's `ReadIndex` protocol for safe
strong reads. This is proven at two levels: real disk/real TCP within a
single test process (`internal/node/node_test.go`), and genuine
separate OS processes with real persistent data directories and a real
`SIGKILL` (`cmd/chronicledb-node`, wired into CI as
`go test -tags=integration ./cmd/chronicledb-node/...`). See
[`docs/raft.md`](docs/raft.md) §10 and
[`docs/scenario-corpus.md`](docs/scenario-corpus.md) §Raft/Replication
for exactly which of RF-1 through RF-15 were re-proven against real
disk/network/processes versus remain correctly simulator-only by
deliberate scope decision (RF-2, RF-7, RF-8, RF-14, RF-15).

Per [`docs/roadmap.md`](docs/roadmap.md) §Maturity Model,
`REPLICATED PROTOTYPE` is Phases 4-5 together; both are now complete
and evidenced.

No SQL implementation exists yet. See
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
