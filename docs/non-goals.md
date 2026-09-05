# Non-Goals

Status: Architecture Foundation. Non-goals are deliberate scope
control, not oversights. Each entry states what is deferred, why, and
what would trigger revisiting it.

## Sharding / multi-shard

- **Deferred**: sharding, multi-shard routing, distributed
  transactions across shards, two-phase commit (2PC), cross-shard
  timestamp coordination, automatic rebalancing, dynamic cluster
  reconfiguration.
- **Why**: ChronicleDB V1 must first prove a correct, well-tested
  single-shard replicated database (one logical shard, static
  three-node Raft cluster — see [`docs/architecture.md`](architecture.md)
  §1). A distributed database that has not proven correctness for one
  shard cannot be trusted to coordinate many. See
  [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md).
- **Revisit when**: the single-shard replicated engine has reached at
  least `STRONG DISTRIBUTED V1` maturity (see
  [`docs/roadmap.md`](roadmap.md) §Maturity Model) with the scenario
  corpus for Phases 1-7 passing.

## SQL surface

- **Deferred**: joins, a distributed query optimizer, broad SQL
  compatibility, PostgreSQL wire-protocol compatibility, arbitrary
  PostgreSQL on-disk compatibility.
- **In scope for a later phase (Phase 8), once the transaction
  machinery is proven**: a small, constrained SQL layer — `CREATE
  TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`,
  `ROLLBACK`, primary keys, equality predicates, a limited type system,
  no joins initially. This SQL layer must compile strictly into the
  real transaction/MVCC machinery:

  ```
  SQL / request layer
    -> transaction manager (internal/txn)
    -> MVCC / state machine (internal/mvcc, internal/fsm)
    -> replicated commit path (internal/raft)
    -> durable storage (internal/wal, internal/storage)
  ```

  SQL must never bypass durability, transactions, MVCC, or
  replication — e.g. no direct-to-storage fast path that skips the
  transaction manager. See [ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md).
- **Why deferred entirely until Phase 8**: correctness of the
  underlying transactional/replicated engine is the hard, interesting
  problem this project exists to solve. Building SQL first would
  either (a) sit on top of an unproven engine, making SQL-level bugs
  indistinguishable from engine-level bugs, or (b) tempt the
  implementation to bypass the engine for convenience, which is
  explicitly disallowed above.
- **Revisit when**: Phases 1-7 are complete and their scenario corpus
  passes (see [`docs/roadmap.md`](roadmap.md)).

## Cross-region / geo-replication

- **Deferred**: cross-region consensus, geo-replication, WAN-scale
  partition/latency modeling, any consistency claim across regions.
- **Why**: V1's failure model ([`docs/failure-model.md`](failure-model.md))
  assumes single-region deployment with bounded (if unreliable)
  intra-cluster network behavior. WAN-scale latency and partition
  characteristics are materially different and would invalidate
  several of V1's simplifying assumptions (e.g. read-index round-trip
  cost, election timeout tuning).
- **Revisit when**: a specific multi-region use case is scoped with
  its own explicit latency/partition model and ADR.

## Kubernetes / cloud infrastructure

- **Deferred**: a Kubernetes operator, managed cloud deployment
  tooling, complex cloud infrastructure automation.
- **Why**: infrastructure packaging is orthogonal to, and should not
  precede, engine correctness. See [`docs/roadmap.md`](roadmap.md)
  Phase 11 for where packaging concerns eventually belong.
- **Revisit when**: the engine reaches `PORTFOLIO READY` or
  `OPEN-SOURCE READY` maturity and deployment ergonomics become the
  limiting factor for adoption/review, not before.

## AI/LLM features

- **Deferred indefinitely, out of scope**: no AI/LLM-powered features
  (natural-language query interfaces, AI-assisted query optimization,
  etc.) are part of ChronicleDB's design.
- **Why**: ChronicleDB's purpose is to demonstrate distributed-systems
  and storage-engine engineering; AI/LLM features would be an
  unrelated surface area and would dilute that purpose.
- **Revisit when**: not planned; would require an explicit, separate
  proposal and rationale unrelated to this architecture.

## Sophisticated storage structures ahead of need

- **Deferred**: B-tree/LSM-tree storage engines, general-purpose
  buffer/page-cache managers, before a concrete, evidenced need exists.
- **Why**: see [`docs/storage.md`](storage.md) §1 — ChronicleDB uses
  the smallest technically real storage design that supports its
  correctness goals; added complexity must be justified by measured
  evidence, not anticipated need.
- **Revisit when**: a specific, measured performance or scale
  limitation of the append-only + in-memory-index design is
  demonstrated (see [`docs/roadmap.md`](roadmap.md) Phase 9,
  Performance Engineering) and a new ADR proposes the specific
  structure and its justification.

## Staff/Principal validation and equivalence claims

- **Deferred/disallowed without evidence**: any claim of
  Staff/Principal-level review approval, or of equivalence to
  CockroachDB, PostgreSQL, Spanner, or any other established system.
- **Why**: such claims require external review evidence this project
  does not yet have, and equivalence claims to mature, differently-
  scoped systems would be misleading given ChronicleDB's intentionally
  narrower V1 scope (single shard, Snapshot Isolation not
  Serializable, no joins, etc.).
- **Revisit when**: an actual external review occurs (see
  [`docs/roadmap.md`](roadmap.md) Phase 12, `EXTERNAL-REVIEW READY`);
  even then, claims are scoped to what the review actually covered.

## Authentication and TLS

- **Deferred**: V1 assumes a trusted network between client and
  cluster and among cluster nodes; see
  [`docs/failure-model.md`](failure-model.md) §6.
- **Why**: correctness of the transactional/consensus core is the
  priority for the phases leading up to a distributed prototype;
  authentication/transport security is an orthogonal, well-understood
  problem best layered on once the core is proven.
- **Revisit when**: before any claim of production-readiness or any
  deployment outside a trusted network (must be resolved no later than
  `PORTFOLIO READY`/`OPEN-SOURCE READY`, see
  [`docs/roadmap.md`](roadmap.md)).

## MVCC version garbage collection (implementation, not the rule)

- **Deferred**: the *implementation* of MVCC version GC. The *rule* is
  already defined in [`docs/mvcc.md`](mvcc.md) §6 (`GCWatermark`) so
  the data model does not need to change shape when GC is implemented.
- **Why**: GC is a space/performance concern, not a correctness
  requirement for the phases leading up to a working replicated
  engine; implementing it prematurely, without the `GCWatermark`
  bookkeeping proven correct, risks reclaiming a version an active
  snapshot still needs.
- **Revisit when**: measured memory growth from unbounded version
  retention becomes a concrete problem (see
  [`docs/roadmap.md`](roadmap.md) Phase 9+).

## `RequestID` outcome garbage collection

- **Deferred**: V1 retains `RequestID` outcomes indefinitely (see
  [`docs/transactions.md`](transactions.md) §6) rather than
  implementing a GC/expiry policy now.
- **Why**: unbounded retention is the safe default; a premature expiry
  policy risks expiring an outcome a legitimate, slow client retry
  still needs, silently reintroducing a duplicate-apply risk.
- **Revisit when**: a safe expiry policy (e.g. client-acknowledged
  receipt, or a generous, explicitly justified fixed TTL) is designed
  and given its own ADR.
