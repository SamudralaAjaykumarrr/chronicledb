# ChronicleDB Architecture

Status: Architecture Foundation (Phase 0). No implementation described
here exists yet unless explicitly marked otherwise.

This document is the authoritative map of ChronicleDB's system
boundaries and terminology. Every other document in `docs/` uses the
vocabulary defined here without redefining it. If a term in another
document appears to conflict with this document, this document wins
and the other document has a bug.

## 1. System shape (V1)

- **One logical shard.** The entire keyspace is owned by a single Raft
  group. There is no partitioning, routing, or cross-shard
  coordination in V1.
- **A static three-node Raft cluster.** Membership is fixed at cluster
  creation. Dynamic reconfiguration (adding/removing voters) is
  deferred — see [`docs/non-goals.md`](non-goals.md) and
  [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md).
- **A single leader per term** accepts all writes and, in V1, all
  strong reads. Followers replicate the log and stand ready to become
  leader.

```
                +-------------------+
   client ----> |  leader (node A)  |
                +---------+---------+
                          |  AppendEntries (replicate)
                +---------+---------+
                |                   |
        +-------v------+   +-------v------+
        | follower (B) |   | follower (C) |
        +--------------+   +--------------+
```

Before Raft exists (Phases 1-3 of the roadmap), ChronicleDB is a
**standalone single-node engine**. The standalone engine is not a
disposable prototype: its durable ordered history is designed from the
start to become the state that Raft replicates, so the transition to a
replicated system in Phase 4-5 is additive rather than a rewrite (see
[ADR-0007](adr/0007-deterministic-replicated-state-machine-boundary.md)).

## 2. The four histories, and why there are not three sources of truth

A distributed transactional database has an obvious failure mode:
accidentally growing three unrelated "logs" that can drift out of sync
with each other (a Raft log, a WAL, and a transaction log). ChronicleDB
defines exactly one logical history and one physical persistence
mechanism for it. There is no separate, independently-authoritative
transaction recovery log.

| Concept | What it is | Owned by |
|---|---|---|
| **Logical history (Raft log)** | The ordered sequence of *commands* (e.g. `CommitTxn`) that the cluster has agreed on, indexed by `(term, index)`. This is the single source of truth for "what happened, in what order." | `internal/raft` (logical), described in [`docs/raft.md`](raft.md) |
| **Physical persistence (durable log / WAL)** | The on-disk byte-level mechanism used to make log entries and Raft metadata (`currentTerm`, `votedFor`) survive a crash. It has no opinion about transactions or SQL; it persists framed, checksummed records in order. | `internal/wal` on top of `internal/storage`, described in [`docs/wal.md`](wal.md) and [`docs/storage.md`](storage.md) |
| **State-machine materialization** | The deterministic result of applying committed log entries, in order, to an in-memory (and eventually snapshotted) structure: MVCC version chains, tombstones, and the RequestID outcome table. This is *derived* state, not an independent history. | `internal/fsm` + `internal/mvcc`, described in [`docs/mvcc.md`](mvcc.md) and [`docs/transactions.md`](transactions.md) |
| **Snapshots** | A periodic, coordinated checkpoint of state-machine materialization plus the Raft boundary (`lastIncludedIndex`/`lastIncludedTerm`) it reflects, used to bound log growth and speed up recovery/follower catch-up. | `internal/snapshot`, described in [`docs/snapshots.md`](snapshots.md) |

A transaction commit is **not** written to some independent
"transaction log." A transaction commit is encoded as a single
deterministic command (conceptually `CommitTxn(RequestID, TxnID,
StartSeq, Mutations...)`) that becomes one entry in the Raft log. Once
that entry is committed and applied, the transaction's outcome is a
fact derivable from the committed log — there is nothing else to keep
in sync. See [ADR-0003](adr/0003-wal-raft-log-responsibility-model.md).

Before Raft exists, the standalone engine's durable log plays the role
of "the ordered committed history" directly: each durably appended,
checksummed record *is* a committed command, applied immediately to
the in-memory state machine. This is the same Apply function Raft will
later drive, so the standalone engine is architecturally a Raft group
of one, not a different design.

## 3. Terminology (binding for all documents)

- **`TxnID`** — identifies one in-progress, client-visible transaction
  session. Ephemeral: it lives only in the leader's in-memory session
  state until the transaction commits or aborts. It is never durable
  on its own.
- **`RequestID`** — an opaque, client-supplied idempotency key attached
  to a mutating request (in V1, specifically to a commit request). The
  state machine durably records the terminal outcome of every
  `RequestID` it has completed. See [`docs/transactions.md`](transactions.md)
  and [ADR-0006](adr/0006-requestid-idempotency-and-uncertain-outcomes.md).
- **`StartSeq`** — the logical sequence number a transaction captures
  at `BEGIN`. It defines exactly which committed versions are visible
  to that transaction. Before Raft: derived from the local durable
  log's monotonic index. After Raft: derived from a quorum-confirmed
  committed/applied point (see [`docs/replication.md`](replication.md)
  §Read Consistency). `StartSeq` values may have gaps; they are never
  wall-clock timestamps.
- **`CommitSeq`** — the logical sequence number assigned to a version
  of a key when the transaction that wrote it commits. Before Raft:
  the local durable log index at which the commit command was
  durably appended. After Raft: the committed Raft log index carrying
  the `CommitTxn` command that produced this version. `CommitSeq` is
  monotonically increasing and gap-tolerant; it is never wall-clock
  time.
- **Version / version chain** — for a key `K`, the ordered list of
  `(CommitSeq, value | tombstone)` entries produced by successive
  committed writers. See [`docs/mvcc.md`](mvcc.md).
- **"Snapshot" — two distinct meanings.** ChronicleDB uses "snapshot"
  for two different things, and every document must disambiguate
  which one it means on first use per section:
  - **MVCC snapshot / snapshot view**: the consistent, `StartSeq`-based
    read view a transaction has of the database (see
    [`docs/mvcc.md`](mvcc.md)).
  - **State-machine snapshot / DB snapshot**: the periodic, durable,
    on-disk checkpoint of applied state used for compaction and
    recovery (see [`docs/snapshots.md`](snapshots.md)).

## 4. The durability/consistency pipeline and its vocabulary

Six words recur throughout this repository and must never be treated
as synonyms:

| Term | Meaning |
|---|---|
| **Appended** | Bytes for a record have been written to the durable log's in-process buffer or file handle. Not yet guaranteed to survive a crash. |
| **Persisted** | The record has crossed the explicit fsync (or equivalent durability) boundary defined in [`docs/wal.md`](wal.md). Survives a crash of the process and a subsequent OS-level restart of that single node, barring media failure. |
| **Replicated** | A majority of the Raft group's nodes (in V1: at least 2 of 3) have the entry **persisted** in their local durable log. |
| **Committed** | The Raft commit rule is satisfied for the entry: it is replicated to a majority **and** the leader has verified commitment per the current-term commit rule (see [`docs/raft.md`](raft.md)). Committed is a property the leader (and eventually the cluster) *knows*; it does not by itself mean any node's state reflects the entry yet. |
| **Applied** | The deterministic state machine (`internal/fsm`) has run `Apply` for this entry, in log order, on a given node. Applied is per-node and lags committed. |
| **Acknowledged** | The client has received a response. In V1, a client is acknowledged success only after the entry is committed **and** applied **and** its deterministic transaction outcome is known (see [`docs/replication.md`](replication.md) §Durability Contract). |

Full failure-by-failure treatment of this pipeline lives in
[`docs/failure-model.md`](failure-model.md); the binding commit
sequence for a replicated write is defined in
[`docs/replication.md`](replication.md).

## 5. Component map and dependency direction

Planned future Go packages (none implemented in this phase except
where noted):

```
cmd/chronicledb        entry point / process wiring only

internal/protocol       wire schemas: client request/response,
                         inter-node RPC messages (pure data + encode/decode)

internal/storage         durable append-only segment file primitives:
                          segment files, append, fsync, checksummed
                          reads, directory layout

internal/wal              record framing (type, length, checksum,
                          index) + logical WAL API (Append, Read,
                          Truncate, Replay), built on internal/storage.
                          Used to persist Raft log entries and Raft
                          hard state (currentTerm, votedFor).

internal/mvcc             version chains, tombstones, visibility rule.
                          No I/O, no networking, no clock.

internal/txn               transaction session state (TxnID,
                          StartSeq, local write set), conflict
                          detection, built on internal/mvcc.

internal/fsm               deterministic Apply(command) -> result,
                          RequestID outcome table, owns internal/mvcc
                          and internal/txn state. No I/O.

internal/raft              Raft core as a pure, deterministic
                          state-machine component: Step(input) ->
                          (new state, outbound messages, persistence
                          requests, timer actions, newly committed
                          entries). Depends only on small interfaces
                          it defines itself (persistent store,
                          transport, clock) — never on concrete
                          internal/transport or internal/wal packages.

internal/transport         production network implementation of the
                          transport interface internal/raft defines.

internal/snapshot          state-machine snapshot creation,
                          validation, atomic install; depends on
                          internal/fsm (to serialize/restore state)
                          and internal/storage (atomic file writes).

internal/node               process-level wiring: owns a raft core, a
                          concrete transport, a concrete wal-backed
                          persistent store, an fsm, a snapshot
                          manager, and the client-facing server loop
                          that ties committed Raft entries to fsm.Apply.

internal/fault              deterministic test-only simulation:
                          in-memory transport, logical clock,
                          controlled disk, fault injection. Test-only;
                          never imported by production code paths.
```

Dependency rules (enforced by review, and mechanically once packages
exist):

- `internal/raft` **must not** import `internal/transport`,
  `internal/wal`, or `internal/fsm`. It depends only on interfaces it
  owns; production and test code provide implementations.
- `internal/fsm` **must not** import `internal/raft`, `internal/transport`,
  or anything network- or clock-aware. It is a pure function of
  ordered commands to state.
- `internal/mvcc` **must not** import anything beyond the standard
  library. It has no knowledge of networking, SQL, or Raft.
- `internal/wal` **must not** know about SQL syntax, transactions, or
  Raft semantics — it frames and persists opaque byte records.
- The future SQL layer **must not** bypass `internal/txn`/`internal/fsm`
  to touch `internal/mvcc` or `internal/storage` directly (see
  [`docs/non-goals.md`](non-goals.md) and
  [ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md)).
- No package listed above may cyclically depend on another; the
  arrows above are one-directional.

## 6. Request path (target design, once Phase 5 lands)

```
client
  -> internal/protocol (decode request, extract RequestID)
  -> internal/node (leader check, ReadIndex for BEGIN, session lookup)
  -> internal/txn (accumulate local write set on BEGIN..COMMIT)
  -> on COMMIT: internal/node builds CommitTxn command
  -> internal/raft (Propose -> replicate -> commit)
  -> internal/wal (persist locally, via internal/storage)
  -> internal/raft commit rule satisfied
  -> internal/fsm.Apply(CommitTxn) -> internal/mvcc mutation +
     internal/txn conflict check + RequestID outcome recorded
  -> internal/node returns deterministic outcome to client
  -> internal/protocol (encode response)
```

Standalone mode (Phases 1-3, no Raft) is the same pipeline with the
`internal/raft` stage replaced by "durable append is itself the commit
point" — see [`docs/replication.md`](replication.md) for the precise
before/after-Raft comparison.

## 7. Questions this document, together with the rest of `docs/`, must
   answer

The full list of required questions is enumerated and cross-referenced
in [`docs/README.md`](README.md). This document supplies the shared
vocabulary; the specific rules live in the per-topic documents it
links to above.
