# Raft Architecture

Status: Architecture Foundation. No Raft implementation exists yet.

ChronicleDB's architecture is designed to implement and expose real
Raft mechanics as first-class, inspectable architecture, rather than
hiding consensus behavior behind an opaque, finished, embedded
consensus library. This document defines the Raft core's planned
responsibilities, persistent state, and its
relationship to the durable log, transport, and clock.

## 1. Raft core as a deterministic component

The Raft core (`internal/raft`) is designed as a pure, deterministic
state-machine-style component, in the spirit of well-known
"pull-based"/"mailbox" Raft designs: explicit inputs produce explicit
outputs, with no hidden I/O.

```
Step(currentState, input) -> (newState, outputs)

input  ::= Timeout(kind)                 // election or heartbeat timer fired
         | RequestVoteRPC(from, args)
         | RequestVoteReply(from, args)
         | AppendEntriesRPC(from, args)
         | AppendEntriesReply(from, args)
         | ClientPropose(command bytes)
         | PersistenceComplete(...)      // ack that a requested persist finished

outputs ::= { messagesToSend: []Message,
              persistenceRequests: []PersistRequest,  // must complete before
                                                       // dependent messages send
              timerResets: []TimerAction,
              newlyCommittedEntries: []LogEntry }
```

- The Raft core never performs network I/O, disk I/O, or clock reads
  itself. It requests them via `outputs` and receives their results via
  `input` (e.g. `PersistenceComplete`). This is what makes it testable
  under a deterministic simulator (see [`docs/testing-strategy.md`](testing-strategy.md))
  and what satisfies "Raft core must not depend on production network
  implementation" (see [`docs/architecture.md`](architecture.md) §5).
- Production behavior (real sockets, real timers, real fsync latency)
  is provided by adapters in `internal/transport`, `internal/wal`, and
  a clock package, each implementing small interfaces `internal/raft`
  itself defines. Test/simulation behavior is provided by
  `internal/fault` adapters implementing the same interfaces. See
  [ADR-0009](adr/0009-transport-clock-randomness-abstraction.md).

## 2. Roles, terms, and elections

- **Roles**: `Follower`, `Candidate`, `Leader`. Every node starts as
  `Follower`.
- **Term**: a monotonically increasing logical epoch number. At most
  one leader can be legitimately elected per term (`RAFT-ELECTION-SAFETY`,
  see [`docs/invariants.md`](invariants.md)).
- **Election timeout**: a follower that receives no valid
  `AppendEntriesRPC` (heartbeat or real entries) from a current leader
  within a randomized timeout window becomes a `Candidate`, increments
  its term, votes for itself, and sends `RequestVoteRPC` to all peers.
  Randomization of the timeout window is required to avoid split-vote
  livelock; in production this randomness comes from a real RNG, in
  tests from a seeded, reproducible RNG supplied by the harness (see
  [`docs/testing-strategy.md`](testing-strategy.md)).
- **`RequestVote` safety rule**: a node grants its vote for a given
  term at most once (`votedFor`, persisted — §5), and only if the
  candidate's log is at least as up-to-date as the voter's own log
  (compared by `(lastLogTerm, lastLogIndex)`, standard Raft
  comparison). This is what makes `LEADER-COMPLETENESS` (see
  [`docs/invariants.md`](invariants.md)) hold.
- **Heartbeats**: a leader periodically sends empty (or coalesced)
  `AppendEntriesRPC`s to prevent followers from timing out and to
  advance `commitIndex` information.
- **Leader step-down / stale leader handling**: any node (including a
  current leader) that observes a strictly higher term in an incoming
  RPC immediately reverts to `Follower`, updates `currentTerm`,
  clears `votedFor` for the new term as appropriate, and persists the
  update (§5) before continuing. A leader that cannot reach a majority
  therefore cannot indefinitely believe it is still leader once a
  higher-term message reaches it — see
  [`docs/replication.md`](replication.md) §Network Partition Contract
  for the end-to-end scenario.

## 3. Log replication

- The leader appends a newly proposed command to its own log (subject
  to persistence — §5) and sends `AppendEntriesRPC` to each follower,
  containing the new entries plus `(prevLogIndex, prevLogTerm)` for
  the **log matching** consistency check.
- **Log matching property**: if two logs contain an entry with the
  same `(index, term)`, the logs are identical in all preceding
  entries. Followers reject an `AppendEntriesRPC` whose
  `(prevLogIndex, prevLogTerm)` does not match their own log at that
  position, forcing the leader to walk `nextIndex` backward and resend
  from an earlier point until a matching prefix is found.
- **`nextIndex[peer]`** — the leader's guess of the next log index to
  send to a given peer. Optimistically initialized to
  `leader's last log index + 1`; decremented on rejection.
- **`matchIndex[peer]`** — the highest log index the leader knows is
  replicated (persisted) on a given peer, from that peer's
  acknowledgements.
- **Divergent suffix repair**: if a follower's log has entries at
  positions the leader's log does not agree with (e.g. left over from
  a previous, now-stale leader), the leader instructs the follower to
  **truncate its log at the point of disagreement** and overwrite with
  the leader's entries from that point forward. A follower never keeps
  a divergent suffix once the current leader's authoritative entries
  for those positions are known.
- **`commitIndex`**: the highest log index the leader (and, once
  communicated via heartbeats, followers) know to be committed (§4).
- **`appliedIndex`**: the highest log index this specific node's state
  machine has applied. `appliedIndex <= commitIndex` always; the gap is
  normal lag, not a bug (see [`docs/architecture.md`](architecture.md)
  §4 committed vs. applied).

## 4. Commit rule (current-term commit rule)

An entry at index `N` is **committed** once:

1. A majority of the cluster's nodes (in V1: at least 2 of 3) have the
   entry **persisted** in their local durable log (§5, §
   [`docs/wal.md`](wal.md)), and
2. The entry's term equals the **leader's current term** (the
   "current-term commit rule": a leader may only advance `commitIndex`
   to cover an entry from a **previous** term by virtue of also having
   replicated a later entry from its *own* current term to a majority
   — it never unilaterally declares an old-term entry committed based
   on replication counts alone, since that can incorrectly commit an
   entry a future leader would be entitled to overwrite). This
   preserves `LEADER-COMPLETENESS`.

Only the leader can directly determine a new `commitIndex` (from
`matchIndex` values); it communicates the new `commitIndex` to
followers via subsequent `AppendEntriesRPC`s (or heartbeats), and each
follower advances its own local `commitIndex` (never past what its own
log actually contains) upon learning of it.

## 5. Persistent state (must survive restart)

| State | Must persist? | Notes |
|---|---|---|
| `currentTerm` | Yes | Before granting a vote, before responding to `AppendEntriesRPC` claiming a term update, and before becoming a candidate — persisted via a `HardState` WAL record (see [`docs/wal.md`](wal.md)). |
| `votedFor` | Yes | Together with `currentTerm` in the same `HardState` record, so the two are always durably consistent as a pair. |
| Log entries | Yes | Via `LogEntry` WAL records (see [`docs/wal.md`](wal.md)). |
| `snapshot.lastIncludedIndex` | Yes | Part of the durable snapshot metadata (see [`docs/snapshots.md`](snapshots.md)); tells recovery which log indices are already covered. |
| `snapshot.lastIncludedTerm` | Yes | Same as above; required to validate log-matching continuity for the first log entry after a snapshot. |
| `commitIndex` | **Not independently persisted** | Reconstructed at restart (§5.1) — never blindly trusted from disk as "already committed" without re-derivation. |
| `appliedIndex` | **Not independently persisted as a separate durable fact beyond what the snapshot/apply process implies** | Reconstructed at restart from the snapshot's `lastIncludedIndex` (state at least that far is known-applied) plus deterministic re-application of the log suffix (§5.1). |

### 5.1 Why `commitIndex`/`appliedIndex` are reconstructed, not trusted from disk

A durable log entry existing on disk does **not** mean it was
committed — it may be an uncommitted (or since-superseded) suffix left
by a past leader that never reached a majority, or that a later leader
legitimately overwrote in a different, not-yet-observed term. Blindly
re-applying "every entry present in the local log" on restart would
violate `APPLIED-PREFIX-SAFETY` (see [`docs/invariants.md`](invariants.md)).

Instead, at restart:

1. `appliedIndex` starts at `snapshot.lastIncludedIndex` (or 0, absent
   a snapshot) — that much state is known-good by construction of the
   snapshot (see [`docs/snapshots.md`](snapshots.md)).
2. The node rejoins as a `Follower` (or must win an election to become
   `Leader`) and only advances `commitIndex` — and therefore only
   applies further entries — as directed by a **current, legitimate
   leader's** `AppendEntriesRPC` commit-index field, or, if this node
   itself becomes leader, only after re-establishing commitment per §4
   for entries in its own current term.
3. A durable log suffix beyond the last snapshot that turns out to be
   uncommitted (e.g. this node was a stale former leader) is subject
   to divergent-suffix repair (§3) once a legitimate current leader is
   contacted — it is never applied speculatively in the meantime.

Full recovery ordering is specified in [`docs/recovery.md`](recovery.md).

## 6. Relationship to the durable log and to the state machine

- The Raft log is a **logical** ordered history of commands (see
  [`docs/architecture.md`](architecture.md) §2). Its physical
  persistence is entirely delegated to `internal/wal` — Raft does not
  own or invent its own separate on-disk format.
- Raft's job ends at producing `newlyCommittedEntries`, in order.
  Turning a committed entry into database state is the job of
  `internal/fsm.Apply` (see [`docs/transactions.md`](transactions.md),
  [`docs/mvcc.md`](mvcc.md)) — the Raft core has no knowledge of
  `TxnID`, `RequestID`, MVCC, or SQL. This separation is what keeps
  `internal/raft` reusable and independently testable.

## 7. Failure handling summary (detailed scenarios in `docs/failure-model.md`)

- **Leader crash before replication**: the proposed entry may exist
  only in the old leader's log (if even persisted there); it was never
  committed, so it is safe for the entry to simply not exist in the
  new leader's history. The client, having received no commit
  confirmation, retries with the same `RequestID` against the new
  leader.
- **Leader crash after quorum, before leader applies/replies**: the
  entry **is** committed (majority persisted it) even though the
  original leader never got to apply it or reply. A newly elected
  leader (which must have all committed entries, by the vote-granting
  rule in §2) will apply it in due course; the client's retry-by-
  `RequestID` observes `COMMITTED`.
- **Follower crash/restart**: rejoins as `Follower`, catches up via
  normal `AppendEntriesRPC` log-matching/backfill, or via snapshot
  install if its log is too far behind (see
  [`docs/snapshots.md`](snapshots.md)).
- **Network partition**: see the full worked scenario in
  [`docs/replication.md`](replication.md) §Network Partition Contract.

## 8. Explicit scope boundaries for V1

- Static, fixed three-node membership. No joint-consensus
  reconfiguration protocol in V1 (see [`docs/non-goals.md`](non-goals.md)).
- No pre-vote optimization, no leader leases, no follower reads in V1
  (see [`docs/replication.md`](replication.md) §Read Consistency for
  why lease reads specifically are deferred).
- No batching/pipelining optimizations are assumed by the correctness
  argument in this document; they may be added later as long as they
  do not change the commit rule in §4.
