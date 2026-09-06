# Raft Architecture

Status: Phase 4 (`internal/raft` core + `internal/fault` deterministic
simulator) is implemented and tested — see §9 for exactly what that
does and does not yet mean. Phase 5 (production wiring to real
transport/disk: `internal/transport`, an `internal/wal`-backed
`raft.Storage` adapter, `internal/node`) is now also implemented — see
§10 for what it adds and its own implementation-time decisions. The
`Core` type itself is completely unchanged by Phase 5: every Phase 5
package is a driver/adapter built against the interfaces this document
already specified, exactly as §1 and ADR-0009 intended.

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

## 9. Phase 4 implementation notes

This section records implementation-time clarifications made while
building `internal/raft` and `internal/fault` against the design above.
None of it changes an accepted ADR; it documents the specific,
previously-unspecified choices §1's `Step(state, input) -> outputs`
sketch left open.

### 9.1 Packages and division of responsibility

- `internal/raft` — the pure core (`Core.Step`), its message/entry/
  hard-state types, and the two interfaces it owns: `Storage`
  (durable-state contract, ADR-0009) and `Rand` (election-timeout
  jitter source, called directly by `Step` — ADR-0009 explicitly scopes
  Randomness this way, unlike Storage/Transport/Clock). `Core.Step`
  itself never calls `Storage`.
- `internal/fault` — the deterministic simulator
  (docs/testing-strategy.md §3): `Transport` (in-memory, explicitly
  controllable message queue: drop/duplicate/delay/partition/isolate/
  reorder), `MemoryStorage` (an in-memory `raft.Storage`, §9.4), a
  seeded `Rand`, `Node` (owns one `Core` + `MemoryStorage` + the
  logical timer countdowns that turn `Core.Output` timer-reset requests
  into actual `InputElectionTimeout`/`InputHeartbeatTimeout` calls —
  this is where ADR-0009's "Clock" interface is realized for Phase 4;
  Phase 4 does not introduce a separate `raft.Clock` Go interface,
  since nothing in the pure core needs to call one directly — ticks are
  driven externally per §9.2), and `Cluster` (wires several `Node`s
  together through one `Transport`, per docs/testing-strategy.md §3.1).

### 9.2 Timer model: countdown ticks, not a `raft.Clock` interface

`Core.Output` tells its driver *whether* to (re)arm a timer and *for
how many logical ticks* (`ElectionTimeoutTicks`/`HeartbeatTimeoutTicks`,
sampled using the injected `Rand`) — it never tracks elapsed ticks
itself. `internal/fault.Node` is the driver that owns per-timer
countdowns and calls `Core.Step` with `InputElectionTimeout`/
`InputHeartbeatTimeout` when a countdown reaches zero, driven by
`Cluster.Tick`/`AdvanceTicks` (docs/testing-strategy.md §3.1's single
global logical clock; there is no `time.Sleep` anywhere in either
package). A Leader's election timer and a Follower/Candidate's
heartbeat timer are never explicitly disarmed — an `InputElectionTimeout`
delivered to a Leader (or `InputHeartbeatTimeout` delivered to anyone
else) is simply ignored by `Step` and produces no reset request, so the
stale timer self-extinguishes on its own without special-case driver
logic.

### 9.3 Persistence-gating: exactly what waits for `InputPersistenceComplete`

Per ADR-0008 ("before they can affect other nodes' state — granting a
vote, acknowledging replication"), exactly three effects are withheld
until their `PersistRequest.Seq` is acknowledged complete:

1. A granted `RequestVoteResponse` (a denial is never gated — losing an
   unpersisted denial on crash is always safe, §9.5).
2. A successful `AppendEntriesResponse` (a follower's acknowledgement
   that it has durably stored the entries in question).
3. A Leader's own `matchIndex[self]` update (and the commit-rule
   re-evaluation that follows from it) for an entry it just proposed —
   the leader's own copy counts toward a majority only once the
   leader's own persistence of it is confirmed, mirroring
   docs/replication.md §1.2 steps 2-3's ordering.

Sending `RequestVoteRequest`/`AppendEntriesRequest` (in either
direction, including a Candidate's own vote-for-self) is **not**
gated: losing an unpersisted send on crash is always self-correcting
(the recipient's own persistence-before-grant rule is what actually
protects `RAFT-ELECTION-SAFETY`; a resent/re-derived request after
restart is harmless). Gating is implemented via a small internal
pending-item queue keyed by `PersistRequest.Seq`, each item additionally
tagged with the term (and, for log-index-tied items, the entry's term)
it was created under, so a pending effect that has since gone stale
(term changed; the entry was truncated by a later divergent-suffix
repair) is silently dropped rather than incorrectly released.

### 9.4 Storage: in-memory for Phase 4, `internal/wal`-backed adapter is Phase 5

`internal/fault.MemoryStorage` is Phase 4's only `raft.Storage`
implementation. `internal/wal` today (Phase 1-3) only ever appends
sequentially (`WAL.AppendLogEntry`) — it has no operation to truncate
an already-durable suffix, which Raft's divergent-suffix repair (§3)
requires. Wiring a real `internal/wal`-backed `raft.Storage` adapter —
including whatever `internal/wal` extension that truncation needs — is
explicitly Phase 5 scope (`internal/node` wiring Phase 4's core to
Phase 1-3's durable log), consistent with docs/roadmap.md's Phase 4/5
boundary ("without yet wiring in production transport/disk"). This is
the one point where Phase 4 deliberately stops short of "the real
durable path" per its own charter, not an oversight:
`ApplyPersistRequest` (the exact truncate-then-append-then-set-hard-
state sequence a `Storage` must apply) is written against the
`raft.Storage` interface generically, so a Phase 5 `internal/wal`-backed
implementation is a drop-in replacement for `MemoryStorage`, not a
redesign of `internal/raft`.

### 9.5 No leader no-op entry on election

This document does not specify a no-op-entry-on-election policy (a
common Raft optimization to quickly extend current-term commitment to
prior-term entries). Per this phase's instructions not to invent one,
`Core.becomeLeader` does not append one; a newly elected leader commits
a previous term's entries only once a legitimate client proposal in its
own term reaches a majority (§4), exactly as this document already
specifies. A future ADR may add one explicitly if a future phase's
failover-latency goals require it.

### 9.6 Log indexing convention

`Core`'s internal log is 1-indexed with a fixed sentinel entry at index
0 (`Term` 0), which is never persisted or exposed via `Core.Entries()`
— it exists purely so `prevLogIndex == 0` consistency checks are
trivially satisfied without a special case in the `AppendEntries`
handler. `raft.Storage` implementations (including `MemoryStorage`) do
not store this sentinel; `NewCore` reconstructs it from a gap-free
`entries` slice starting at index 1 on every construction, including
restart.

## 10. Phase 5 implementation notes

This section records implementation-time clarifications made while
wiring the unchanged Phase 4 `Core` to real disk/network in
`internal/node`. None of it changes an accepted ADR.

### 10.1 Packages and division of responsibility

- `internal/node.WALStorage` is the production `raft.Storage`
  implementation §9.4 anticipated: it persists `HardState` and log
  entries through `internal/wal` (the same durable log the rest of
  ChronicleDB uses — no second, Raft-only physical log,
  `docs/invariants.md` `CONSISTENT-LOG-RESPONSIBILITY`), and keeps an
  in-memory mirror (entries/hard state) so `raft.Storage`'s read
  methods stay cheap without re-reading segment files on every `Step`
  call. `internal/wal` itself gained exactly the capability §9.4 said
  it was missing: a crash-safe `Truncate(fromIndex)` (see
  `docs/wal.md`'s Phase 5 implementation note for the algorithm) plus
  `AppendHardState`/`LatestHardState` for the `HardState` record type
  Phase 1 had already reserved but never used. Both internal/wal
  additions treat their payloads as fully opaque bytes, exactly as
  before (`docs/architecture.md` §5) — `WALStorage` owns the small
  encode/decode step turning a `raft.HardState` or a
  `(raft.Term, data)` pair into the bytes `internal/wal` actually
  stores, and back.
- `internal/transport.Transport` is the production transport §1/ADR-0009
  anticipated: real TCP sockets, one dedicated writer goroutine per
  peer (see §10.2's note on why this specific design point is
  load-bearing, not stylistic), explicit length-prefixed framing with a
  version byte and a bounded maximum message size, `encoding/gob` for
  the message body, and panic-safe decoding. `internal/raft` does not
  import it, per §1/`docs/architecture.md` §5.
- `internal/node.Node` is the process-level driver §1 always
  anticipated a Phase 5 would provide: a single event-loop goroutine
  owns every `Core.Step` call, `WALStorage` mutation, and `fsm.FSM`
  mutation (this is the "controlled event-loop ownership" avoiding
  lock cycles across raft/wal/fsm/transport that Phase 5's brief
  calls for) — client calls (`Propose`, `BeginReadIndex`) hand off to
  this loop via buffered-result channels and never touch `Core`/
  `WALStorage`/`fsm.FSM` directly from another goroutine, except
  `fsm.FSM` itself, which is independently safe for concurrent access
  by design (`docs/transactions.md`) and used directly by `Propose`'s
  idempotency pre-check for that reason.
- `cmd/chronicledb-node` is a real OS-process entry point wrapping one
  `internal/node.Node`, with a minimal local HTTP control plane (not a
  general client wire protocol — `internal/protocol` remains out of
  Phase 5 scope) used specifically to drive real multi-process
  integration tests (`cmd/chronicledb-node/main_test.go`).

### 10.2 Real-transport testing scope: what was and was not re-proven

Phase 4's simulator-only RF-7 (duplicate `AppendEntriesRPC`) and RF-8
(stale `AppendEntriesRPC`) scenarios exercise `Core`-internal protocol
logic that is, by construction (§1, ADR-0009), completely independent
of which `Transport` implementation carries the messages — the
identical, unmodified `Core` code proven against those scenarios in
Phase 4 is the exact code Phase 5's `internal/node` drives. Re-running
those specific message-level edge cases through real sockets would
prove the same fact about `Core` a second time without saying anything
new about the Phase 5 wiring itself, so they were not duplicated;
Phase 5's real-transport tests instead focus on what actually is new in
this phase — genuine disk persistence, genuine process/network
failure, and the end-to-end wiring between `Core`, `WALStorage`,
`internal/transport`, and `internal/fsm`.

RF-2/RF-14 (a follower whose delivery is *delayed but not dropped*)
remain simulator-only for a different, honest reason: `internal/transport`
implements `Block`/`Unblock` (outright drop, in both directions, for a
named peer — sufficient for every partition-shaped scenario this phase
tests) but does not yet model non-zero, still-eventually-delivered
network latency. Adding that is a plausible, small future enhancement,
not attempted in this phase because no RF-\* scenario's *safety*
argument depends on it (delay-tolerance here is a liveness property,
already covered in spirit by the fact that `Block` followed by
`Unblock` proves delivery resumes and catch-up completes — RF-3, RF-13).

### 10.3 A full-cluster restart does not, by itself, re-confirm
    prior-term history — and why that is correct, not a bug

Combining two already-accepted, already-documented decisions produces
a real, easy-to-miss consequence worth stating explicitly:

- §9.5: no no-op entry is appended on election, so a newly elected
  leader can only advance `commitIndex` over a *previous* term's
  entries by getting a *fresh* proposal in its *own* current term to
  reach a majority (§4's current-term commit rule).
- §5.1/ADR-0008: `commitIndex` is never persisted and is always
  reconstructed at restart, starting from 0.

If **every** node in a cluster crashes and restarts at (roughly) the
same time — not merely the leader — no surviving node carries live
knowledge of the pre-crash `commitIndex`, even though a majority may
already durably hold an entry that genuinely was committed before the
crash. The newly elected leader's log physically contains that entry
(restored from disk, per §5.1's `NewCore(cfg, hs, entries)`
reconstruction), but per the current-term commit rule it cannot
*declare* that entry committed — and therefore `internal/fsm.Apply`
never runs for it — until some *new* proposal in the new leader's own
term reaches a majority. Once that happens, the current-term commit
rule's backward scan (§4, `advanceLeaderCommit`) correctly and
automatically re-confirms every earlier entry up to and including the
new one in the same step, so nothing is lost — it is simply not
*applied* until that fresh proposal occurs.

This is exactly what a client's own documented retry-by-`RequestID`
responsibility (`docs/transactions.md` §7) provides for free: a client
that never received a response before the whole-cluster crash retries
with the same `RequestID` against whichever node is elected leader
after everyone restarts, and that retry *is* the fresh proposal that
unlocks re-confirming the pre-crash history — after which
`internal/fsm.Apply`'s own idempotency check (§6) ensures the retried
command resolves to the *original* recorded outcome (from the
re-applied pre-crash entry, applied moments earlier in the same
committed batch) rather than a fresh, second evaluation. See
`internal/node/node_test.go::TestClusterRestartRecoversFSMAndRequestIDOutcomes`,
whose doc comment walks through this exact mechanism, and
`docs/recovery.md`'s Phase 5 implementation note for the recovery-side
framing of the same fact.

This is not a design gap this phase should close: adding a no-op
entry on election specifically to shrink this window remains the
`§9.5`-anticipated future ADR territory ("if a future phase's
failover-latency goals require it"), not a Phase 5 correctness
requirement — the property this document's invariants actually
promise (`LEADER-COMPLETENESS`: a committed entry is never lost across
a leadership change) holds throughout; only the *timing* of when it
becomes applied-and-client-visible again depends on a subsequent
proposal.

### 10.4 `BeginReadIndex`'s freshness proof cannot use `matchIndex` directly

`docs/replication.md` §4 / ADR-0010 requires proving current leadership
via "a fresh round of heartbeats acknowledged by a majority" before
assigning a read's `StartSeq`. The natural-looking implementation —
record `target := Core.LastIndex()`, force a heartbeat, and then wait
until a majority of `Core.MatchIndexOf(peer) >= target` — is **not**
sufficient and was caught by a dedicated regression test during Phase 5
development
(`internal/node/node_test.go::TestBeginReadIndexBlockedAfterIsolationEvenWithStaleReplicatedLog`):
`matchIndex` only ever increases while a node remains leader in a term
and is never reset merely because that node becomes partitioned, so a
leader that fully replicated its log to a majority *before* being
isolated would pass a `matchIndex`-based check vacuously, using
entirely stale, pre-partition information, without any live contact
with a majority at all — precisely the unsafe case ADR-0010 exists to
prevent (an isolated former leader serving a "strong" read). The
degenerate case of an entirely empty log (`target == 0`) makes this
concretely visible immediately (`matchIndex >= 0` is trivially true for
every peer, including full strangers, from the very first instant),
but the flaw is general, not limited to that edge case.

`internal/node.Node` instead tracks `lastAck[peer]`, a per-peer counter
bumped to a fresh, strictly increasing value only when this node, still
`Leader` in the *unchanged* current term, processes a genuine `Success`
`AppendEntriesResponse` from that peer (the exact precondition under
which `Core` itself would honor the reply). A pending `BeginReadIndex`
snapshots every peer's `lastAck` value at issuance and only succeeds
once a majority's value has *strictly advanced past its own snapshot*
— proof that a live round-trip happened no earlier than the read's own
issuance, independent of `matchIndex`'s absolute value. This is
implementation detail local to `internal/node` (`Core` itself is
unmodified), recorded here because the failure mode is subtle enough
that a future re-implementation should not rediscover it the hard way.

## 11. Phase 7 implementation notes

### 11.1 Election-timer liveness bug found by chaos testing (resolved)

`docs/testing-strategy.md` §7.1 has the full account;
`docs/failure-model.md` §2.8 has the accepted-behavior framing this bug
actually violated. Summary for readers of this document specifically:
`Core.handleRequestVoteRequest`'s reject-but-stepped-down path,
`handleRequestVoteResponse`'s higher-term stepdown, and both
`handleAppendEntriesResponse`'s and `handleInstallSnapshotResponse`'s
higher-term stepdowns each correctly stepped a former Leader/Candidate
down to Follower (`Output.SteppedDown`) but did not also set
`Output.ResetElectionTimer` — unlike `handleAppendEntriesRequest` and
`handleInstallSnapshotRequest`, which always do, unconditionally,
regardless of whether the specific request they carry is itself
accepted. A Leader has no election timer running (only its heartbeat
timer), so a driver that only ever arms/rearms the election timer from
`ResetElectionTimer` left such a node with none running, permanently,
once it stepped down through one of the four gapped paths — a genuine
liveness bug an adversarial asymmetric partition (a higher-term
candidate whose log is not up to date, correctly denied forever, while
every other node silently loses its own ability to ever call a fresh
election) could trigger. Fixed: all four paths now set
`ResetElectionTimer`/`ElectionTimeoutTicks` whenever `stepDownTo`
reports the node was previously Leader or Candidate, matching the
request-handler paths' existing unconditional behavior. This closes a
general principle worth stating explicitly for any future handler added
to `Core`: **any transition that leaves a node in Follower role must
either come with an election timer reset in the same `Output`, or rely
on one already known to be live** — a plain Follower staying Follower
already has one; a former Leader or Candidate does not, by
construction, and must always get a fresh one.

### 11.2 Directional (asymmetric) partition support

The deterministic simulator already supported this since Phase 4
(`internal/fault.Transport.IsolateLink(from, to)`, one direction only).
Phase 7 added the same capability to the production transport
(`internal/transport.Transport.BlockSend`/`BlockRecv`, alongside the
existing symmetric `Block`/`Unblock`) so an asymmetric partition —
docs/roadmap.md Phase 7's own required topology — can be exercised
against real sockets too, not only the simulator. `Core` itself
required no change: an asymmetric partition is entirely a transport-
layer/message-delivery concern, exactly the kind of fault
`docs/failure-model.md` §2.6 already documents Raft as designed to
tolerate for liveness without affecting safety.
