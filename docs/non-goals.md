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
- **Delivered in Phase 8** (`internal/sql`), once the transaction
  machinery was proven: a small, constrained SQL layer — `CREATE
  TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`,
  `ROLLBACK`, primary keys, equality predicates, a limited type system,
  no joins. See [`docs/sql.md`](sql.md) for the grammar, data model,
  and execution semantics as actually built. This SQL layer compiles
  strictly into the real transaction/MVCC machinery:

  ```
  SQL / request layer
    -> transaction manager (internal/txn)
    -> MVCC / state machine (internal/mvcc, internal/fsm)
    -> replicated commit path (internal/raft)
    -> durable storage (internal/wal, internal/storage)
  ```

  SQL never bypasses durability, transactions, MVCC, or
  replication — there is no direct-to-storage fast path that skips the
  transaction manager. See [ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md).
- **Why deferred entirely until Phase 8**: correctness of the
  underlying transactional/replicated engine is the hard, interesting
  problem this project exists to solve. Building SQL first would
  either (a) sit on top of an unproven engine, making SQL-level bugs
  indistinguishable from engine-level bugs, or (b) tempt the
  implementation to bypass the engine for convenience, which is
  explicitly disallowed above.
- **Revisit when**: Phases 1-7 completing and their scenario corpus
  passing is what triggered building the delivered subset above (see
  [`docs/roadmap.md`](roadmap.md)); the features still marked
  **Deferred** at the top of this entry (joins, a distributed query
  optimizer, broad SQL/wire-protocol compatibility) have no fixed
  trigger phase and require a specific, evidenced need plus their own
  ADR, per this document's general policy.

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

- **Resolved** (as of `v0.2.0`, Security Foundation,
  `docs/enterprise-v1-plan.md` §5; not yet tagged/released) — see
  [`docs/security.md`](security.md) for the full operational guide.
  Client TLS, peer mTLS, static-token and mTLS-identity authentication,
  fixed-role RBAC, and hash-chained audit logging are implemented in
  `internal/identity`, `internal/transport`, `internal/authn`,
  `internal/authz`, and `internal/audit`. **Not on by default**: every
  relevant flag defaults to `v0.1.0`'s exact plaintext/unauthenticated
  behavior (secure-by-configuration, not yet secure-by-default —
  flipping the default is a separate, explicitly evidenced `v1.0.0`
  gate requirement, `docs/enterprise-v1-plan.md` §13.2).
- **Still deferred, by explicit V1 scope decision** (see
  `docs/enterprise-v1-plan.md` §5's own Non-goals): OIDC/SSO/LDAP/SAML
  integration; a built-in certificate authority/PKI service
  (operators bring their own CA); per-row/per-column SQL-level
  authorization; HSM-backed private key storage; a built-in external
  secret-manager integration.
- **Why originally deferred through `v0.1.0`**: correctness of the
  transactional/consensus core was the priority for the phases leading
  up to a distributed prototype; authentication/transport security is
  an orthogonal, well-understood problem, layered on once the core was
  proven — exactly the trigger `docs/enterprise-v1-plan.md` names for
  starting this phase.
- **Revisit when** (for the remaining still-deferred items above): a
  specific, evidenced need is scoped with its own ADR, per this
  document's general policy.

## MVCC version garbage collection (implementation, not the rule)

- **Resolved, for replicated mode, as of `v0.6.0`**: the rule defined
  in [`docs/mvcc.md`](mvcc.md) §6 (`GCWatermark`) is now implemented —
  a replicated, leader-proposed, bounded-and-resumable watermark
  advance (`docs/storage-lifecycle.md`, [`ADR-0020`](adr/0020-mvcc-gc-replicated-watermark.md)).
  Ships disabled by default (`-gc-interval=0`) — see
  [`docs/roadmap.md`](roadmap.md)'s `v0.6.0` entry for the named
  `v1.0.0`-gate item this creates (GC-on-by-default).
- **Still deferred**: GC for **standalone** (non-replicated) mode —
  deliberately excluded, since the decision to propose a watermark
  advance is leader-local and the mutation itself only ever happens
  inside the replicated `Apply` path (`docs/mvcc.md` §9); there is no
  equivalent concept without a Raft group. See
  [`docs/sql.md`](sql.md) §8a.
- **Why originally deferred through `v0.5.0`**: GC is a space/
  performance concern, not a correctness requirement for the phases
  leading up to a working replicated engine; implementing it
  prematurely, without the `GCWatermark` bookkeeping proven correct,
  risked reclaiming a version an active snapshot still needed.
- **Revisit when** (standalone mode): a concrete, evidenced need for
  bounded memory in long-running standalone/library use is scoped with
  its own ADR.

## `RequestID` outcome garbage collection

- **Deferred**: V1 retains `RequestID` outcomes indefinitely (see
  [`docs/transactions.md`](transactions.md) §6) rather than
  implementing a GC/expiry policy now. **Named, `v0.6.0`**: unlike MVCC
  version data (now bounded — see the entry above), this table is
  measured, not bounded: `chronicledb_requestid_outcomes`
  (`docs/observability.md` §2.1a) grows with every distinct `RequestID`
  a client ever sends, GC or no GC (`docs/storage-lifecycle.md` §28.2/
  D6). This is a named `v1.0.0`-blocker with its own owner-release
  entry — see [`docs/roadmap.md`](roadmap.md)'s `v0.6.0` row for the
  exact framing (retention policy *or* a formal rescoping of the
  "stable disk usage" claim). This entry alone does not satisfy that
  gate; the roadmap entry, naming an owner release, is what does.
- **Why**: unbounded retention is the safe default; a premature expiry
  policy risks expiring an outcome a legitimate, slow client retry
  still needs, silently reintroducing a duplicate-apply risk.
- **Revisit when**: a safe expiry policy (e.g. client-acknowledged
  receipt, or a generous, explicitly justified fixed TTL) is designed
  and given its own ADR — no later than the `v1.0.0` gate named above.

## Per-credential rate limiting

- **Deferred**: rate limiting keyed to an authenticated principal/
  credential (as opposed to `docs/admission-control.md`'s node-wide
  resource-based admission, which has no notion of *who* is asking).
- **Why**: `v0.6.0`'s admission control protects the node from
  resource exhaustion regardless of source; per-credential fairness is
  a separate, multi-tenancy-adjacent concern this project's V1 scope
  (a single trusted operator per cluster, `docs/security.md` §2's
  threat model) does not yet need.
- **Revisit when**: a concrete multi-tenant or shared-credential
  deployment scenario is scoped, with its own fairness model and ADR.

## Adaptive admission

- **Deferred**: admission ceilings that adjust themselves at runtime
  from observed latency/throughput (e.g. a control-loop-tuned
  concurrency limit), as opposed to `docs/admission-control.md`'s
  fixed, operator-configured ceilings plus the one pressure-driven
  exception (disk/heap tightening, itself a fixed two-threshold state
  machine, not a continuously-adaptive one).
- **Why**: a fixed ceiling is simpler to reason about and to prove
  `BOUNDED ADMITTED WORK` against; an adaptive controller adds a new
  class of failure mode (mistuning, oscillation) this release's own
  gate-6 benchmark-derived defaults do not need to solve yet.
- **Revisit when**: measured production load patterns show fixed
  ceilings are operationally difficult to tune well, with a specific
  proposed control law and its own ADR.

## Tiered / cold storage

- **Deferred**: moving older/cold WAL segments or snapshot data to
  cheaper storage (object storage, a slower disk tier), as opposed to
  `docs/storage-lifecycle.md`'s retention knobs, which only ever
  retain-on-the-same-disk or delete.
- **Why**: see [`docs/storage.md`](storage.md) §1's "smallest
  technically real storage design" principle — a second storage tier
  is exactly the kind of complexity this project defers until a
  measured need exists, and `v0.6.0`'s own disk-pressure admission
  already addresses the acute "about to run out of space" case without
  it.
- **Revisit when**: a specific, measured retention-cost or -capacity
  limitation of single-tier local disk is demonstrated, with its own
  ADR.
