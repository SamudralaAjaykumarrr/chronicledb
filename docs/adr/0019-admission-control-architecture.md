# ADR-0019: Admission Control / Resource Protection Architecture

Status: Accepted

## Context

Through `v0.5.0`, `internal/node`'s event loop admitted every client
`Propose`/`BeginReadIndex` call unconditionally: the only bound on
concurrent client work was whatever the caller's own goroutine pool
happened to be. A client that opened enough connections, or a single
misbehaving client retrying aggressively, could grow `n.waiters`/
`n.pendingReads` without limit, and nothing distinguished "the cluster
is out of capacity" from "you lost a write-write conflict" in the error
a client received. `docs/enterprise-v1-plan.md` §9 ("Admission Control /
Resource Protection") is the designated resolution point, targeted at,
and released as, `v0.6.0`. This ADR records the architecture actually
implemented for that phase; `ADR-0020` and `ADR-0021` record the
companion Storage Lifecycle half of the same release.

## Decision

### The one-sentence model

Every unit of work this node ever performs is assigned to exactly one
of four lanes, and admission is a property of the lane, never of a
global counter:

- **Lane K (consensus)**: `AppendEntries`/`RequestVote`/heartbeat
  processing. **Has no gate at all, structurally** — there is no
  `internal/admission` identifier reachable from `run()`, `step()`, or
  anything either calls (`TestAdmissionNeverReachableFromEventLoop`,
  an AST test, proves this rather than merely documenting it).
- **Lane A1 (control)**: membership changes, upgrade precheck/finalize,
  TLS reload. Bounded, but never queued behind client load.
- **Lane A2 (maintenance)**: backup, scrub. Bounded independently of
  Lane A1, so a stuck backup cannot block a membership change, and vice
  versa (`§3.2a`, Rule CP-3).
- **Lane B (client)**: `Propose` (writes) and `BeginReadIndex` (reads),
  gated separately from each other.

### `internal/admission.Gate` — a bounded-capacity primitive, not a rate limiter

A rate limiter (token bucket, leaky bucket) bounds *arrival rate*; it
says nothing about how many callers are concurrently inside the
protected section. `Gate` instead bounds the literal count: the total
number of goroutines simultaneously inside `Acquire` — running with a
held slot, or still waiting for one — never exceeds
`MaxConcurrent+MaxQueueDepth`, enforced by two Go channels' fixed
capacities, not by counting. This is the direct implementation of
`BOUNDED ADMITTED WORK` (`docs/invariants.md` §27.1): the property
`v0.6.0` needs is "how much work can be in flight at once," which a
concurrency bound answers directly and a rate limiter only answers
indirectly (and incorrectly, if service time varies).

`internal/admission` is a leaf package — it imports only
`internal/metrics` and the standard library — specifically so
`internal/node`'s own dependency graph never runs through it in the
other direction, and so `internal/sql`'s statement gate (`§9.2`) can
depend on it without pulling in Raft/FSM machinery it has no business
needing.

### Structural separation over a priority queue

`docs/enterprise-v1-plan.md` §9 asks for "control-plane/consensus
priority... processed on a priority path never blocked behind a full
client-work queue." A priority queue was rejected: it requires an
explicit, ongoing judgment about relative priority under every future
combination of lanes, and a bug in that judgment (a control message
misclassified, a priority inversion under a specific interleaving) is
silent until an incident surfaces it. Structural separation — Lane K
has no gate, Lane A1/A2 have their own independent gates, Lane B's own
saturation can only ever block Lane B — makes the corresponding safety
property (`CONTROL-PLANE NON-STARVATION`, `§27.2`) a fact about the
dependency graph an AST test can check, not a claim about scheduler
behavior under load that can only be observed, never proven, in
production.

### The gate runs *before* `Precheck`

`Propose`'s admission gate is acquired before `fsm.FSM.Precheck`, which
computes a full mutation-set fingerprint on every call — unbounded
client-caused CPU work. Admitting before doing per-request work is the
general rule this release applies everywhere a gate exists. The
consequence, documented rather than hidden: under overload, a retry of
an already-decided `RequestID` can itself be shed with a `503` rather
than returning its recorded outcome — safe, because a rejection never
records anything (`REJECTION SAFETY`, `§27.3`), and the deliberately
ungated `GET /outcome` remains the correct way to resolve a known
`RequestID` under load.

### `ADMISSION FAILS CLOSED`

There is no "unlimited" admission configuration reachable from either
the direct Go API or the CLI. `admission.NewGate` refuses
`MaxConcurrent <= 0` outright; `cmd/chronicledb-node` additionally
refuses `0` for every admission-control flag whose own default is never
`0` (an operator reaching `0` there did so explicitly, and it is
treated as a startup error rather than "unlimited"). `internal/node.Config`
keeps a separate, ergonomic "0 means package default" convention for
direct Go-API callers — the two conventions are deliberately different
because they answer different questions (§10.3 of the plan discusses
this at length): `Config`'s zero value must keep every pre-`v0.6.0` test
working unmodified; the CLI's own flag defaults are never `0`, so a `0`
there is never ambiguous.

### Resource pressure: sampling, never probing in the hot path

Disk and heap headroom are sampled by a single dedicated goroutine
(`admission.PressureMonitor`) on a fixed interval (`-resource-poll-interval`,
default 5s), storing an immutable snapshot in an `atomic.Pointer` — no
request path and no event-loop code ever makes a `syscall.Statfs` or a
`runtime/metrics.Read` call directly. This is what lets a deterministic
test inject a fake `PressureSource` and drive the hysteresis state
machine (`internal/node/pressure.go`) without ever touching a real
filesystem or sleeping for a real interval
(`SetPressureSourceForTest`, `internal/node/pressure_test.go`).

Three states, with 10% de-escalation hysteresis so a workload hovering
at a threshold does not flap on every poll: **Normal** (full
`MaxConcurrent`), **LowSpace** (`MaxInflightProposals/4`, floored at 1;
reads/admin/consensus unaffected), **Critical** (all client writes
refused, `disk_critical`). Heap pressure can only ever produce
LowSpace, never Critical — `-max-heap-bytes` is an admission threshold,
never an allocator limit (`GOMEMLIMIT` is the real mechanism an
operator should also set). Entering LowSpace or Critical triggers an
immediate GC pass proposal if GC is enabled (`ADR-0020`) — the one
automatic action pressure ever takes, and it deletes only what
`docs/mvcc.md` §6's rule already permits.

### The stable error contract

Every admission rejection is a `*admission.RejectedError` carrying a
fixed, documented `Reason` string (`internal/admission/reason.go`) and
a `Retry-After` hint — never a bare `503` a client has to guess about.
A client that cannot distinguish "the cluster is at capacity" from "you
lost a conflict" retries the wrong thing forever; the `Reason`
vocabulary exists specifically so it never has to guess.

## Alternatives Considered

- **A single global semaphore across all lanes.** Rejected: it
  reintroduces exactly the priority-inversion risk structural
  separation exists to remove — a client-write burst would still be
  able to starve a membership change sharing the same counter.
- **A token-bucket rate limiter instead of a bounded-concurrency
  gate.** Rejected (see "a bounded-capacity primitive, not a rate
  limiter" above): the actual failure mode `v0.6.0` protects against is
  unbounded *concurrent* work (memory held by in-flight proposals), not
  unbounded arrival rate; a rate limiter bounds the wrong quantity.
- **Probing disk/heap synchronously inside the admission check itself.**
  Rejected: a `syscall.Statfs` or a stop-the-world-risking memory stat
  on every `Propose` call would make admission's own overhead scale
  with load, defeating its purpose under exactly the conditions it
  exists to handle.
- **A priority queue with configurable weights.** Rejected: see
  "Structural separation over a priority queue" above — every weight
  scheme this project considered was a claim about behavior under a
  specific load shape, not a structural guarantee.

## Consequences

- A client integrating against `v0.6.0` for the first time may observe
  `503` responses it never saw before, each carrying a `Reason` and a
  `Retry-After` — this is the one client-visible behavior addition this
  release makes on the admission side (the HTTP timeout/connection-cap
  defaults are the other, unrelated one, `§10.4`).
- Every future phase that adds a new kind of client-facing work now has
  an established lane to place it in, rather than needing to invent
  admission control from scratch.
- `CONTROL-PLANE NON-STARVATION`'s own proof is permanently scoped to
  what `§4.3`/`§4.4` of the plan actually claims: Go's own fair
  `select` dispatch, plus the absence of a Lane B gate anywhere
  reachable from the event loop. It does **not** claim the event loop
  itself never blocks for a long-running operation it performs
  synchronously (snapshot creation, backup export) — that residual
  exposure is measured, not hidden, and is the first of the three named
  `v1.0.0`-blocker items (`docs/roadmap.md`).

## Correctness Implications

See `docs/invariants.md`'s new "Admission Control / Storage Lifecycle
invariants (`v0.6.0`)" section for the complete, itemized argument:
`BOUNDED ADMITTED WORK` (§27.1), `CONTROL-PLANE NON-STARVATION` (§27.2),
`REJECTION SAFETY` (§27.3), and `ADMISSION FAILS CLOSED` (§27.4) are
this ADR's four correctness claims, each with its own proof tier and
negative control.

## Testing and Proof Obligations

- Deterministic: `internal/admission`'s own unit/`-race`/fuzz suite
  (`AC-1`, `AC-2`, `AC-4`, `AC-17`).
- Real-disk/real-TCP `internal/node` `testCluster`: `AC-3`, `AC-5`
  through `AC-14`, `AC-18` through `AC-22` — including the two
  structural AST tests (`TestAdmissionNeverReachableFromEventLoop` for
  Rule CP-2/CP-3, and its negative control) and the injected-fake-gate
  negative control for `AC-7`.
- Real-filesystem: `AC-13` (a small dedicated filesystem filled toward
  each threshold in turn, via `internal/testfs`'s unprivileged
  user+mount-namespace harness — never a mocked `DiskUsage`).
- Chaos: `AC-10` (60s sustained saturation, scaled via
  `CHRONICLEDB_AC10_DURATION`), `AC-11` (saturation plus leader
  failover — both an ungraceful `Stop` and an isolate/heal `SteppedDown`
  transition — no gate-token leak, the new leader's own ceiling applies
  immediately), `AC-12` (saturation plus a full
  add/promote/remove membership sequence — the admin lane is never
  blocked).
- §29.3's `internal/fault` limitation (D10, `docs/v0.6.0-plan.md` §29.3):
  `internal/fault` imports only `internal/raft` and has no event loop,
  no admission gates, and no GC — `AC-6`/`AC-7` (lane separation) and
  the GC chaos obligations `ADR-0020` names both run at the
  `internal/node` `testCluster` tier instead, a deliberate, declared
  retiering rather than a silent gap.
