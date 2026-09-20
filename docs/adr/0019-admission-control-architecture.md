# ADR-0019: Admission Control / Resource Protection Architecture

Status: **Proposed** (planning only — `docs/v0.6.0-plan.md` Part A; no
production code implements this yet, and this ADR moves to Accepted
only when that implementation lands)

## Context

Through `v0.5.0`, ChronicleDB has no admission control of any kind.
Measured against the tree at `481ff93`:

- Every `internal/node.Node` request channel is **unbuffered**
  (`proposeCh`, `controlCh`, `readIndexCh`, `backupCh`, `precheckCh`,
  `membershipCh`, `membershipStatusCh`). The de facto request queue is
  therefore the set of *blocked caller goroutines*, one per in-flight
  HTTP request, each retaining its decoded `fsm.CommitTxnCommand` (up
  to the existing 1 MiB body limit) for as long as it waits. That set
  is unbounded.
- `len(Node.waiters)` — the true proposed-but-unapplied set — has no
  cap.
- `cmd/chronicledb-node` constructs `&http.Server{Addr, Handler}` with
  no `ReadTimeout`, `WriteTimeout`, `IdleTimeout`,
  `ReadHeaderTimeout`, or `MaxHeaderBytes`, and no bound on concurrent
  connections.
- `internal/transport.acceptLoop` spawns one goroutine per inbound
  peer connection, with no limit and no read deadline.
- There is no disk-headroom or memory-pressure signal anywhere in the
  tree.
- **A caller that cancels its context does not withdraw the state it
  left behind.** `Propose` returns on `ctx.Done()`
  (`internal/node/node.go:1172`) but its waiter (`:1450`) survives
  until the entry applies, the node steps down, or it shuts down;
  `BeginReadIndex` returns the same way but its `pendingRead`
  (`:1479`) survives until the read resolves or leadership is lost.
  Any caller-side concurrency limit therefore bounds *goroutines*,
  not those sets.
- **`checkPendingReads` runs on the consensus path.** It iterates the
  whole `pendingReads` slice after every `processOutput` (`:1596`),
  so that slice's length is a term in per-inbound-message cost.
- **`FSM.mu` is an exclusive `sync.Mutex`.** `Apply` (`fsm.go:184`),
  `Precheck` (`:111`) and `GetOutcome` (`:133`) all take it in write
  mode. `GetOutcome` backs `/outcome`, the documented recovery path
  from an overload rejection.

`docs/enterprise-v1-plan.md` §9 designates `v0.6.0` as the resolution
point, and adds a requirement the codebase has never had to reason
about: Raft's own internal traffic must never be starved by client
overload.

One structural fact dominates the design. `internal/node` runs a
**single event-loop goroutine** that owns `raft.Core`, `WALStorage`,
`fsm.FSM`, and the snapshot manager. Go's `select` already dispatches
fairly among ready cases, so client channel readiness does not starve
`tr.Recv()` in the scheduling sense. What is genuinely unbounded is
(a) the number of goroutines parked holding payloads, and (b) the
service time the loop spends on work a client caused — including the
two long, self-initiated durable operations (`maybeSnapshot`,
`handleBackup`) that already run to completion on that loop, a
documented `v0.1.0`-era tradeoff.

## Decision

**Four lanes, physically separate, with separate capacities — not
priority levels on one queue.**

- **Lane K (consensus)**: `run()`'s `ticker.C` and `tr.Recv()` cases
  and everything reachable from them. **Structurally ungated**: no
  identifier from `internal/admission` is reachable from `(*Node).run`,
  enforced by an AST-based call-graph test
  (`TestAdmissionNeverReachableFromEventLoop`), not by convention.
  Follower-side replication, snapshot installation, and voting are
  Lane K and are never refused for local capacity reasons.
- **Lane A1 (control plane)**: membership, upgrade, TLS reload. Its
  own small gate (`-max-admin-concurrency`, default 2), never shared
  with client work, so a client flood can never prevent an operator
  from acting on a sick cluster.
- **Lane A2 (maintenance)**: backup and scrub, on a separate
  `maintenanceGate` (`-max-maintenance-concurrency`, default 2) plus
  a single slot per kind. These are the only caller-initiated
  operations whose duration is unbounded in the *input* rather than
  the request — a scrub of 200 GiB at the default
  `-scrub-bytes-per-sec` runs ~50 minutes. Had they shared Lane A1's
  capacity-2 gate, one scrub plus one backup would have returned
  `503 admin_operation_in_progress` to an emergency
  `/admin/membership/remove` for that entire window: the release's
  own new feature blocking the action Lane A exists to protect. The
  split is structural (**Rule CP-3**: neither lane's entry points
  reference the other's gate) and is asserted by the same AST test
  that enforces Lane K's separation.
- **Lane B (client)**: `Propose` and `BeginReadIndex`, each with its
  own gate. `/status`, `/health`, `/metrics`, and `/outcome` are
  deliberately ungated — the overload signal and the documented
  recovery path from a rejection must not themselves be sheddable.

**The gate is a fixed-capacity structure, not a rate limiter.**
`admission.Gate` holds two channels: `slots` (cap `MaxConcurrent`) and
`queue` (cap `MaxQueueDepth`). The total number of goroutines that can
be inside `Acquire` is their sum, by channel capacity. Worst-case
resident client payload is therefore arithmetic:
`(MaxConcurrent + MaxQueueDepth) × 1 MiB`.

**The authoritative bounds are enforced on the event-loop
goroutine** — three of them, not one:
`len(n.waiters) >= maxClientWaiters` inside `handlePropose`, and
`len(n.pendingReads)` and the live-read-lease count inside
`handleReadIndex`. These are the places that cannot be bypassed and
need no lock. They apply to *client* work only; control and
membership proposals and the election no-op never consult them, which
is how Lane A and Lane K priority is expressed: as the **absence** of
a check.

**The gate and the ceiling are not interchangeable, and the ceiling
fires in normal operation.** Because a canceled caller frees its gate
slot while its waiter or pending read survives, the gate bounds
goroutines and resident payload memory while the ceilings bound
admitted work. A client population using deadlines shorter than apply
latency will drive the ceiling continuously — that is a *cancellation*
signal, documented as such, not a bug signal, and the corresponding
counter is expected to be nonzero under those workloads.

**Fail-closed configuration.** `MaxConcurrent <= 0` is a startup error.
There is no flag value meaning "unlimited" and no way to disable
admission.

**`FSM.mu` becomes a `sync.RWMutex`.** Leaving `/outcome` ungated is
only defensible if `GetOutcome` is actually cheap, and today it takes
`FSM.mu` in *write* mode — the same mutex the event loop's `Apply`
takes. Ungated plus exclusive would hand unbounded client concurrency
a lock on the consensus path, which is the precise hazard this ADR
cites elsewhere as a reason to gate. `Apply` and the mutating control
applies keep `Lock()`; `Precheck`, `GetOutcome` and the read-only
lookups take `RLock()`. Go's `RWMutex` blocks new readers once a
writer waits, so the event loop cannot be starved by a reader stream.
This is what makes "ungated" mean "cheap" rather than "unprotected".

**The gate still runs before `Precheck`.** The `RWMutex` change
weakens the original lock-contention argument but does not reverse
the decision, which rests on two independent grounds: `Precheck`
calls `fingerprintOf` (`internal/fsm/fsm.go:19`), hashing the full
mutation set on every call — unbounded client-caused CPU work before
admission — and even a shared-mode reader population forces the
event loop's writer to wait for the current cohort to drain on every
`Apply`. The consequence — under overload, a duplicate `RequestID` can
be shed with `503` instead of returning its recorded outcome — is safe
and is documented in the error contract, with the ungated
`GET /outcome` as the resolution path.

**Pressure is sampled, never probed in the hot path.** One monitor
goroutine samples disk headroom (`syscall.Statfs`, standard library,
`unix` build tag) and heap (`runtime/metrics`, never
`runtime.ReadMemStats`, which can stop the world) on a fixed interval
into an atomic snapshot. Three states — `Normal`, `LowSpace`,
`Critical` — tighten or refuse **client writes only**, with 10%
hysteresis on de-escalation. Consensus traffic is unaffected in every
state.

**`CONTROL-PLANE NON-STARVATION` is scoped to what is actually
proven**: Raft message-processing and heartbeat-dispatch latency are
not functions of client queue depth, concurrency, or rejection rate.
It explicitly does **not** claim the event loop is never blocked.
Snapshot creation, backup export, and snapshot installation still run
on it; `v0.6.0` bounds how often they can be triggered (backup and
scrub are Lane A2 and singly serialized), pre-empts follower election
timers with a forced heartbeat round immediately before each, and
**measures** the residual as
`chronicledb_event_loop_block_seconds`. Moving those operations off
the loop is named as a later-release item, not claimed here.

Three structures in the tree would falsify even that scoped claim if
left alone, so the claim is made true rather than narrowed further:
`FSM.mu`'s exclusivity behind the ungated `/outcome` (fixed above);
`checkPendingReads`' unbounded per-message scan (fixed by the
`pendingReads` ceiling); and `ApplyAdvanceGCWatermark`'s
`O(total keys)` walk (fixed by [ADR-0020](0020-mvcc-gc-replicated-watermark.md)'s
`MaxKeys` bound).

## Alternatives Considered

1. **One shared queue with priority levels for consensus, admin, and
   client work.** Rejected: a priority flag on a shared structure is a
   configuration away from being wrong, and a shared structure's
   saturation is still shared. Separate objects with separate
   capacities cannot be misconfigured into starving each other, and
   the property is checkable by a static call-graph test rather than
   by reasoning about scheduling.
2. **Rate limiting (requests/second, token bucket) instead of
   concurrency limiting.** Rejected: the resource actually at risk is
   memory and event-loop service time, both of which scale with
   *concurrency*, not with arrival rate. A token bucket sized for a
   fast disk over-admits catastrophically on a slow one; a concurrency
   bound is self-calibrating because slots are released only when work
   completes.
3. **Buffering the node's request channels instead of adding gates.**
   Rejected: it moves the unbounded set from "parked goroutines" to
   "channel buffer" without bounding total memory, gives the caller no
   explicit rejection, and would make latency degrade silently — the
   exact behavior `docs/enterprise-v1-plan.md` §9 forbids.
4. **Gating only at the HTTP layer.** Rejected: `internal/node` is
   also used directly as a library (`internal/sql`'s
   `replicatedEngine`, every test harness), so an HTTP-only gate would
   leave the real entry point unprotected and would make
   `BOUNDED ADMITTED WORK` untrue for embedded use.
5. **Exempting duplicate `RequestID` retries from the gate.** Rejected:
   determining whether a request is a duplicate requires hashing the
   full command and consulting the outcome table — the very work the
   gate exists to bound.
5a. **Relying on the caller-side gates alone to bound admitted work.**
   Rejected because it is simply untrue on this codebase: a canceled
   caller frees its gate slot while its waiter and pending read
   survive, so `len(waiters)` and `len(pendingReads)` grow without
   bound under a cancel-heavy or never-committing workload. The
   event-loop ceilings are the authoritative mechanism; an earlier
   draft called them a redundant backstop whose counter "must stay
   zero", which would have been an unsatisfiable release gate.
5b. **Leaving the read path's per-node state uncapped because the
   read gate releases at the door.** Rejected. Releasing the
   `readGate` slot when `BeginReadIndex` returns is right — a
   long-running reader must not hold admission capacity — but it
   means the gate bounds nothing that outlives the call. A leader
   isolated into a minority resolves no reads at all, so a flood of
   timing-out `BeginReadIndex` calls grows `pendingReads` without
   bound while `checkPendingReads` scans it on every inbound Raft
   message. Two event-loop ceilings and an `O(1)` `minLease`
   structure close it.
5c. **One admin lane for both control and maintenance operations.**
   Rejected — see Lane A2 above. This is the one place where the
   release's own new feature (scrub) could have denied service to
   the operator action the lane exists to guarantee.
6. **Moving snapshot/backup off the event loop in this release.**
   Rejected *for `v0.6.0`*: it requires a copy-on-snapshot of FSM state
   and a re-proof of `SNAPSHOT SAFETY`, putting a `v0.5.0` guarantee at
   risk for a liveness gain. Deferred with an explicit owner release
   rather than silently skipped.
7. **Per-credential / per-tenant fair-share admission.** Rejected for
   V1: ChronicleDB has no tenant concept at all, consistent with
   `docs/non-goals.md`. Node-global only.
8. **Adaptive/self-tuning admission (latency gradient, CoDel).**
   Rejected for V1: fixed, operator-configured thresholds are
   explainable and testable; an adaptive controller is neither, and
   `docs/enterprise-v1-plan.md` §9 names it as a non-goal.

## Consequences

- One new leaf package, `internal/admission`, importing only the
  standard library and `internal/metrics`, keeping
  `docs/architecture.md` §5's dependency direction acyclic.
- A new client-facing `503` response with a `Retry-After` header and a
  fixed, documented `reason` vocabulary — additive, MINOR-compatible,
  and distinct from every existing status (`409` not-leader, `200`
  with `ABORTED` for an SI conflict).
- `internal/metrics` gains a `Histogram`, the first in the catalog,
  resolving the deferral `docs/observability.md` §9 recorded at
  Phase 9. `docs/observability.md` §9's "no labels at all" rule is
  amended to the accurate rule — only compile-time-bounded label sets
  (`le`, `cert`, `gate`, `path`, `op`, `reason`) — which the existing
  `chronicledb_cert_expiry_seconds{cert="peer"}` metric already
  required.
- HTTP timeouts and connection caps become the single
  default-behavior change in `v0.6.0`; everything else is opt-in.
- Admission state is per-process, in-memory, never persisted and never
  replicated: a restart resets it, and a new leader inherits no stale
  count.
- `internal/fsm`'s `mu` changes from `Mutex` to `RWMutex`. No format,
  determinism or ordering implication — the event loop remains the
  only writer — but it is a prerequisite for leaving `/outcome`
  ungated.
- Two new flags beyond the original set:
  `-max-maintenance-concurrency` (Lane A2) and
  `-max-live-read-leases` (the lease ceiling).
  `-max-concurrent-reads` now binds both the read gate and the
  `pendingReads` ceiling, exactly as `-max-inflight-proposals` binds
  both the write gate and the `waiters` ceiling.
- `chronicledb_admission_defense_rejections_total` is documented as a
  **client-cancellation** signal, not a bug signal, and carries a
  `ceiling="waiters|pending_reads"` label.

## Correctness Implications

- **`BOUNDED ADMITTED WORK`** (new): the gate's channel capacities
  bound goroutines and resident payload memory; three event-loop
  ceilings (`waiters`, `pendingReads`, live read leases) bound
  admitted work. These are complementary, **not** interchangeable —
  the gates do not bound state that outlives a canceled call.
- **`CONTROL-PLANE NON-STARVATION`** (new, precisely scoped above).
- **`REJECTION SAFETY`** (new): every gate acquisition strictly
  precedes the channel send into the event loop, and therefore
  precedes `Core.Step(InputPropose)`. A rejected request is
  indistinguishable, in every durable and replicated artifact, from a
  request that was never made — so it extends `IDEMPOTENCY` rather
  than qualifying it.
- **`ADMISSION FAILS CLOSED`** (new): the same posture
  `AUDIT COMPLETENESS` already takes.
- **Nothing is weakened.** Admission sits strictly outside
  `fsm.Apply`, records nothing, and never touches `internal/raft`;
  `DURABILITY`, `IDEMPOTENCY`, `REQUEST OUTCOME STABILITY`, and every
  Raft invariant are untouched. Disk pressure never refuses consensus
  work, so `QUORUM SAFETY` and `LEADER COMPLETENESS` are unaffected.

## Testing and Proof Obligations

`docs/v0.6.0-plan.md` §30.1 (AC-1…AC-22) and §30.3. In particular:

- AC-1/AC-2: the exact concurrency boundary, and a 1000-iteration
  `-race` acquire/release storm.
- AC-6 plus the **negative control** AC-7: with lane separation
  disabled by a test-only hook, the harness must *detect* the
  heartbeat starvation the separation prevents. A test that passes
  identically with and without the mechanism does not count
  (`docs/testing-strategy.md` §11).
- **Declared proof-tier substitution.** `internal/fault` imports only
  `internal/raft` — it has no `Node`, no event loop and no admission
  gates — so AC-6 and AC-7 **cannot** run in it, despite
  `docs/enterprise-v1-plan.md` §9 naming that simulator. Both move to
  the `internal/node` `testCluster` tier, with the election-clock
  freeze plus an explicit forced-heartbeat driver replacing the
  logical tick and the comparison made on
  `chronicledb_raft_message_process_seconds` bucket counts rather
  than wall-clock sleeps. Recorded as deviation D10 in
  `docs/v0.6.0-plan.md` §26, and explained in §29.3.
- **AC-19**: `-max-http-connections` worth of concurrent `/outcome`
  and `/status` against a leader under sustained Raft traffic, with a
  negative control that reverts `FSM.mu` to an exclusive mutex and
  must *detect* the p99 regression.
- **AC-20**: a cancel-heavy workload proving `len(n.waiters)` stays
  within its ceiling *and* that the defense counter does rise — the
  calibration that makes its zero value elsewhere meaningful.
- **AC-21**: a minority-partitioned leader flooded with
  never-resolving `BeginReadIndex` calls, proving both read-side
  ceilings hold and that `minLease` cost does not grow with the
  number of open readers.
- **AC-22**: a concurrent backup and scrub (Lane A2 saturated) while
  a membership add/promote/remove is issued, proving Rule CP-3.
- AC-8: a 50%-rejection stress with recycled `RequestID`s, checked by
  `internal/oracle` for zero duplicate effects.
- AC-10/AC-11/AC-12: sustained real-cluster saturation with zero
  elections, plus saturation crossed with leader failover and with an
  in-flight membership change.
- AC-13: real disk pressure on a real small filesystem, proving
  admission tightens *before* any write failure.
- AC-18: the AST call-graph test that makes lane separation a property
  the code cannot silently lose.
