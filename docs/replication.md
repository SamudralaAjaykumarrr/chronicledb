# Replication, Durability Contract, and Read Consistency

Status: §1.1 (standalone) has been implemented and tested since Phase
2-3. §1.2 (replicated mode), §4 (`ReadIndex`), and §5 (the network
partition contract) are now implemented and tested as of Phase 5 — see
`internal/node.Node.Propose` for §1.2's exact 8-step sequence,
`internal/node.Node.BeginReadIndex` for §4, and
`internal/node/node_test.go`'s `TestRF11`/`TestRF12`/`TestRF13` for §5's
scenario end-to-end against real disk/network. See
[`docs/raft.md`](raft.md) §10 for implementation-time notes.

This document defines exactly what ChronicleDB means by "durable" and
"committed" in both standalone and replicated modes, how strong reads
are made safe, and the required behavior under network partition.

## 1. Durability contract

### 1.1 Standalone mode (Phases 1-3, no Raft)

A standalone commit is acknowledged to the client **only after**:

1. The `CommitTxn` command has been **appended** to the local durable
   log (see [`docs/wal.md`](wal.md)).
2. `Sync()` has been called and has returned successfully — the
   command is now **persisted**.
3. `internal/fsm.Apply` has run for the command (deterministic
   conflict check + atomic mutation + `RequestID` outcome record —
   see [`docs/transactions.md`](transactions.md)).
4. The deterministic outcome (`COMMITTED`/`ABORTED`) is known.

Only after step 4 does the server return a response. A buffered
(appended-but-not-synced) write is **not** durable and must never be
acknowledged as committed.

### 1.2 Replicated mode (Phase 5+)

A replicated commit is acknowledged to the client only after **all**
of the following, in this dependency order:

1. The **leader** accepted the client's request (validated it is
   current leader, request well-formed).
2. The required **local Raft persistence** happened on the leader
   (the `CommitTxn`-carrying entry is `Sync()`-ed into the leader's own
   durable log — see [`docs/wal.md`](wal.md)).
3. The entry was **replicated** according to Raft safety rules: a
   majority of nodes have it persisted in their own durable logs (see
   [`docs/raft.md`](raft.md) §4).
4. **Quorum commit** is established: the current-term commit rule is
   satisfied (see [`docs/raft.md`](raft.md) §4).
5. The **leader knows** the entry is committed (it has observed
   sufficient `matchIndex` values to conclude step 4).
6. The **leader applied** the committed entry (`internal/fsm.Apply`
   ran on the leader, in log order — see
   [`docs/transactions.md`](transactions.md)).
7. The **deterministic transaction outcome is known**
   (`COMMITTED(CommitSeq)` or `ABORTED(reason)`, produced by step 6).
8. The **response is returned** to the client, carrying the outcome
   from step 7.

Steps 1-8 are strictly ordered; none may be skipped or reordered for a
response the client is told is final. (An optimization that returns a
provisional/unconfirmed response before step 4 is explicitly
disallowed for anything represented to the client as a committed
result — see [`docs/invariants.md`](invariants.md) `DURABILITY`.)

### 1.3 Terms used above

`Appended`, `Persisted`, `Replicated`, `Committed`, `Applied`,
`Acknowledged` are defined once, centrally, in
[`docs/architecture.md`](architecture.md) §4 — this document uses them
exactly as defined there.

## 2. What survives what

| Event | What is guaranteed to survive | What may be lost |
|---|---|---|
| Process crash (standalone or any single Raft node), clean restart of that same node | Every commit that was **persisted** on that node before the crash (§1.1 step 2, or §1.2 step 2/3 as applicable) | Any commit that was only **appended**, not yet `Sync()`-ed, on that node |
| Single node permanently lost (disk destroyed), Raft mode, other nodes healthy | Every **committed** (§1.2 step 4) entry — it exists on a majority by definition | The lost node's own copy; it must be re-provisioned from a snapshot + peer log (see [`docs/recovery.md`](recovery.md)) |
| Minority partition (a node or minority of nodes isolated) | The majority partition continues to accept and commit new writes normally; nothing committed before or during the partition on the majority side is lost | The minority side cannot commit new writes while partitioned (see §4) |
| Majority available | Full read/write availability | N/A |
| **Catastrophic loss of all persistent replicas** (all 3 nodes' disks destroyed simultaneously, or in V1's non-durable test/dev configurations) | **Nothing.** ChronicleDB does not and cannot claim durability survives the loss of every durable copy of the data. This is a physical limit, not a design gap. | Everything — this is explicitly out of any durability claim ChronicleDB makes. Operators are responsible for off-cluster backups if this failure mode matters to them; ChronicleDB V1 does not provide that mechanism itself (see [`docs/non-goals.md`](non-goals.md)). |

## 3. Standalone-to-replicated mapping

Before Raft exists, "durable log index" plays the role that "committed
Raft log index" plays afterward — both are the source of `CommitSeq`
(see [`docs/architecture.md`](architecture.md) §3). This is by design:
the standalone engine is a Raft group of one, not an unrelated
prototype (see [`docs/architecture.md`](architecture.md) §1 and
[ADR-0007](adr/0007-deterministic-replicated-state-machine-boundary.md)).

| Standalone (Phases 1-3) | Replicated (Phase 5+) |
|---|---|
| Local durable log index | Committed Raft log index |
| `Sync()` completion = commit point | Quorum commit (§1.2 step 4) = commit point |
| Single node = single point of failure, by definition | Majority (2 of 3) required for commit; tolerates 1 node loss |

## 4. Read consistency

V1 uses **leader-based strong reads only**. Follower reads with
independent consistency semantics are deferred (see
[`docs/non-goals.md`](non-goals.md)) — a stale, partitioned former
leader must never be allowed to serve a read represented as
"current"/strong.

### 4.1 Establishing a transaction's `StartSeq` safely

At `BEGIN` (or immediately before any strong read in a future
read-only fast path), the leader must:

1. **Prove current authority.** Confirm, via a fresh round of
   heartbeats acknowledged by a majority (a Raft `ReadIndex`-style
   check), that it is still the legitimate leader — i.e. no higher
   term has superseded it. This defeats a stale, partitioned former
   leader from unilaterally serving a "strong" read.
2. **Know the required committed point.** Record the current
   `commitIndex` at the moment of step 1 (the "read index").
3. **Apply through that point.** Wait until this node's own
   `appliedIndex >= readIndex` from step 2 — i.e. this leader has
   actually caught its own state machine up to what it just proved is
   committed.
4. **Assign `StartSeq`.** Set the transaction's `StartSeq` to the
   `CommitSeq` watermark implied by `appliedIndex` at that point (see
   [`docs/architecture.md`](architecture.md) §3 for the
   `CommitSeq`/log-index relationship).

Only after step 4 does the transaction begin taking MVCC reads (see
[`docs/mvcc.md`](mvcc.md) §3), which are now guaranteed to reflect a
quorum-backed, authoritative, non-stale point in the committed
history.

### 4.2 Why not lease reads

Lease-based reads (a leader serving reads without a fresh quorum
check, based on a time-bounded lease from its last confirmed
heartbeat round) are a legitimate, common optimization — but they
depend on bounded clock drift assumptions across nodes that
ChronicleDB has not modeled, tested, or proven in this architecture.
V1 does not use lease reads. If a future phase wants them, it must
first explicitly define and test the clock-skew assumptions they
depend on (tracked in [`docs/roadmap.md`](roadmap.md); see
[ADR-0010](adr/0010-read-consistency.md)).

## 5. Network partition contract

Concrete scenario, referenced by [`docs/failure-model.md`](failure-model.md)
and [`docs/scenario-corpus.md`](scenario-corpus.md):

```
Before partition:           After partition:
   A (leader)                  A (isolated)      B --- C
   |        \                                    (B or C becomes
   B ------- C                                    new leader)
```

Three nodes: `A` = leader, `B`/`C` = followers. Network splits so `A`
is isolated; `B` and `C` remain connected to each other.

Required, exact behavior:

1. `A` can no longer reach a majority (it is alone; 1 of 3). Per
   [`docs/raft.md`](raft.md) §4, `A` **cannot** advance `commitIndex`
   for any new entry proposed after the partition begins — new writes
   submitted to `A` are never acknowledged as committed while it
   remains isolated (`QUORUM-SAFETY`, see
   [`docs/invariants.md`](invariants.md)).
2. `B` and `C` (a majority, 2 of 3) time out waiting for `A`'s
   heartbeats and hold an election. One of them (say `B`) is elected
   leader for a new, higher term, per the normal election rule
   ([`docs/raft.md`](raft.md) §2) — its log is checked to be at least
   as up to date as `C`'s, guaranteeing it carries every entry `A` had
   previously **committed** (`LEADER-COMPLETENESS`).
3. `B`/`C` continue to accept and commit new writes normally; **all
   entries that were committed before the partition remain intact and
   available**, and new commits proceed on the majority side.
4. `A`, still believing itself leader, may continue **appending**
   (not committing — it cannot reach quorum) speculative entries to
   its own local log if it keeps receiving client requests during the
   partition. These entries are **uncommitted** by construction.
5. Once the partition heals and `A` receives a message carrying `B`'s
   higher term (a heartbeat, a vote request, anything), `A` observes
   the higher term and **immediately steps down** to `Follower` per
   [`docs/raft.md`](raft.md) §2 — this is what makes the old leader
   harmless. `A` never needs to be told "you were partitioned"; the
   term comparison alone is sufficient.
6. `A`'s **divergent uncommitted suffix** (from step 4, if any) is
   repaired the normal way (see [`docs/raft.md`](raft.md) §3): `B`
   (now leader) sends `AppendEntriesRPC` with `(prevLogIndex,
   prevLogTerm)` that does not match `A`'s speculative tail; `A`
   truncates that tail and accepts `B`'s authoritative entries from
   that point forward. **No committed entry is ever lost** in this
   repair, because `A`'s speculative tail was never committed in the
   first place — only uncommitted entries are subject to truncation.
7. `A` rejoins and fully catches up (via normal replication or, if too
   far behind, via snapshot install — see
   [`docs/snapshots.md`](snapshots.md)), and becomes a normal follower
   again.

### 5.1 Client-visible behavior during this scenario

| Phase | Client talking to `A` | Client talking to `B`/`C` |
|---|---|---|
| Partition active, no leader yet on majority side | Requests time out or are rejected once `A` fails to reach quorum for a commit (client should retry, same `RequestID`, and discover the new leader) | Brief unavailability until election completes, then normal service resumes |
| Partition active, `B` elected | `A` cannot commit; any write appears hung/timed out from the client's perspective until the client redirects to `B`/`C` | Full read/write service, including strong reads via `B`'s `ReadIndex` (§4) |
| Partition healed | `A` (now follower) redirects/rejects writes; a client still pointed at `A` for reads must not be served a stale "strong" read from `A` post-partition-formation — `A` (as follower) does not serve leader-only strong reads at all in V1 (§4) | Unaffected |

### 5.2 Asymmetric partitions and chaos evidence (Phase 7)

The scenario above is a *symmetric* partition (`A` can neither send to
nor receive from `B`/`C`). An asymmetric partition — `A` can still
*send* to `B` (so `B` might durably persist an entry `A` proposes) but
cannot *receive* `B`'s acknowledgements back — is a harder case in
principle (a leader that cannot observe its own replication progress),
but resolves via the identical mechanism: `A` can never observe a
majority `matchIndex` for anything proposed during the cut (no
acknowledgement reaches it), so `QUORUM-SAFETY` holds by the same
current-term commit rule ([`docs/raft.md`](raft.md) §4), without
needing to know the cut is directional rather than total. This is now
proven, not just argued: seeded, randomized chaos testing combines
asymmetric partitions with elections/proposals at the deterministic
raft-core layer, in-process against real disk/network via
`internal/transport`'s new `BlockSend`/`BlockRecv` hooks, and against
genuine separate OS processes via `cmd/chronicledb-node`'s new
`/fault` control-plane endpoint — see
[`docs/testing-strategy.md`](testing-strategy.md) §6 for the full
account, including a genuine election-timer liveness bug this exact
testing found and fixed (§7.1 there).

## 6. What ChronicleDB explicitly does not guarantee

- Availability during a genuine minority-partition on the minority
  side (by design — this is the CAP-theorem trade-off Raft-based
  systems make; ChronicleDB chooses consistency over availability on
  the minority side).
- Any durability if all persistent replicas are destroyed (§2).
- Cross-region or geo-replication consistency claims (see
  [`docs/non-goals.md`](non-goals.md)) — V1's failure model assumes a
  single-region deployment with bounded, if unreliable, network
  behavior between nodes, not WAN-scale partition/latency assumptions.
