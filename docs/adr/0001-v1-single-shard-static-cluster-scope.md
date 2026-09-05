# ADR-0001: V1 Single-Shard, Static-Cluster Scope

Status: Accepted

## Context

ChronicleDB's long-term ambition includes a distributed transactional
database, which typically implies sharding, cross-shard transactions,
and dynamic cluster membership. Building all of that simultaneously
with the transactional/consensus core makes it impossible to isolate
whether a correctness bug lives in the core engine or in the
distribution layer on top of it.

## Decision

ChronicleDB V1 is scoped to:

- **One logical shard** — the entire keyspace is owned by a single
  Raft group.
- **A static three-node Raft cluster** — membership is fixed at
  cluster creation; no joint-consensus reconfiguration protocol.

Sharding, multi-shard distributed transactions, 2PC, cross-shard
timestamp coordination, automatic rebalancing, and dynamic membership
are deferred (see [`docs/non-goals.md`](../non-goals.md)).

## Alternatives Considered

1. **Build sharding from day one.** Rejected: multiplies the surface
   area for correctness bugs before the single-shard core is proven,
   and cross-shard transaction protocols (2PC, distributed timestamp
   oracles) are themselves complex enough to deserve their own proven
   foundation to build on, which doesn't exist yet.
2. **Skip Raft, use a simpler leader-based replication scheme (e.g.
   primary-backup with manual failover).** Rejected: would not
   demonstrate real consensus engineering, one of the project's
   explicit goals (see [`docs/vision.md`](../vision.md)), and would
   still need to solve leader election and log matching eventually —
   deferring that work doesn't remove it, it just delays proving it.
3. **Dynamic membership from day one (e.g. via joint consensus).**
   Rejected: adds a substantial, separately-provable layer of Raft
   complexity on top of the already-substantial base protocol;
   correctly implementing base Raft (election safety, log matching,
   leader completeness) is itself a serious undertaking worth proving
   in isolation first.

## Consequences

- A single-shard cluster cannot scale writes horizontally in V1 — all
  writes go through one Raft group's leader. This is an accepted
  limitation, not an oversight.
- The three-node cluster tolerates exactly one node failure (loses
  quorum at 2 failures). A larger static cluster size is a
  configuration choice deferrable to a later ADR if needed; three is
  chosen as the smallest size that demonstrates real quorum/failover
  behavior.
- Every other architecture document in this repository ([`docs/architecture.md`](../architecture.md)
  onward) is written assuming this scope; expanding scope later
  requires updating those documents, not just adding code.

## Correctness Implications

- Fixing the shard count and cluster membership as static removes an
  entire class of correctness concerns (shard rebalancing races,
  membership-change safety during reconfiguration) from V1's proof
  obligations. This is a proof-obligation reduction, not a
  correctness weakening of what V1 does claim.

## Testing and Proof Obligations

- The scenario corpus ([`docs/scenario-corpus.md`](../scenario-corpus.md))
  is scoped entirely to a single three-node cluster; no multi-shard or
  reconfiguration scenarios are included, consistent with this
  decision.
- Revisiting this ADR (see [`docs/non-goals.md`](../non-goals.md)
  §Sharding) requires the single-shard engine to have reached
  `STRONG DISTRIBUTED V1` maturity first.
