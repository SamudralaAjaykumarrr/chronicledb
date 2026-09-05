# ADR-0010: Read Consistency

Status: Accepted

## Context

A replicated system must decide how strong reads are served without
risking a stale, partitioned former leader answering a read as if it
were current. ChronicleDB needs a mechanism to establish a
transaction's `StartSeq` that is provably backed by quorum-confirmed
committed state.

## Decision

V1 uses **leader-based strong reads only**, established via a
`ReadIndex`-style protocol (see
[`docs/replication.md`](../replication.md) §4):

1. The leader confirms current authority via a fresh round of
   heartbeats acknowledged by a majority.
2. It records the current `commitIndex` as the "read index."
3. It waits until its own `appliedIndex` reaches that read index.
4. It assigns the transaction's `StartSeq` from the resulting
   watermark.

Follower reads with independent consistency semantics, and
lease-based reads (avoiding the heartbeat round-trip via a
time-bounded leadership lease), are both deferred.

## Alternatives Considered

1. **Lease reads** (leader serves reads without a fresh quorum check,
   relying on a time-bounded lease since its last confirmed heartbeat
   round). Rejected for V1: correct only under a bounded
   clock-drift assumption across nodes, which ChronicleDB has not
   modeled, tested, or proven. Adopting lease reads without that proof
   would be exactly the kind of unproven-claim risk
   [`docs/vision.md`](../vision.md) warns against. May be revisited
   once clock-skew bounds are explicitly designed and tested.
2. **Follower reads with bounded staleness, exposed as a distinct,
   explicitly weaker read mode.** Deferred, not rejected outright —
   a legitimate future feature, but it requires its own explicit
   consistency contract (e.g. "read no older than X entries behind
   the leader") that has not yet been designed. Serving it without a
   clear contract risks confusing it with the strong-read guarantee
   this ADR defines. See [`docs/non-goals.md`](../non-goals.md).
3. **No `ReadIndex` check at all — leader always assumes it's still
   leader unless told otherwise.** Rejected: this is precisely the
   unsafe case the network-partition contract
   ([`docs/replication.md`](../replication.md) §5) exists to prevent —
   an isolated former leader could otherwise serve arbitrarily stale
   "strong" reads indefinitely.
4. **Every read goes through a full Raft proposal (as if it were a
   write).** Rejected: unnecessarily expensive — `ReadIndex` achieves
   the same safety (proving current leadership and a safe commit
   watermark) without writing a no-op entry to the log for every read
   [note: some Raft implementations use a no-op-entry variant of
   ReadIndex instead of a pure heartbeat-quorum check; either
   satisfies this ADR's safety requirement, and the specific choice is
   an implementation detail left open, provided it proves current
   leadership via majority acknowledgment before serving the read].

## Consequences

- Every `BEGIN` (or future read-only fast path) pays one round-trip of
  heartbeat confirmation latency before `StartSeq` is assigned — an
  accepted latency cost for correctness, to be measured (not assumed)
  in Phase 9 ([`docs/roadmap.md`](../roadmap.md) §Performance Targets).
- All strong reads are served by the current leader only; followers
  do not serve any read in V1, which concentrates read load on one
  node — an accepted V1 scaling limitation (see
  [`docs/non-goals.md`](../non-goals.md)).

## Correctness Implications

- Directly prevents a stale leader from serving reads represented as
  current, supporting `QUORUM-SAFETY` and the overall network
  partition contract ([`docs/replication.md`](../replication.md) §5).

## Testing and Proof Obligations

- A scenario verifying that a partitioned former leader cannot
  complete the `ReadIndex` majority-heartbeat check and therefore
  cannot serve a strong read during the partition (extension of RF-11
  in [`docs/scenario-corpus.md`](../scenario-corpus.md)).
