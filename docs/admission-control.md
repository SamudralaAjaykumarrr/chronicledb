# Admission Control / Resource Protection

Status: `v0.6.0` (`docs/enterprise-v1-plan.md` §9). This is the operator
guide; `ADR-0019` records why the architecture is shaped this way.

## 1. The four lanes

Every unit of work a node performs belongs to exactly one lane, and
admission is a property of the lane:

| Lane | Work | Gated? |
|---|---|---|
| **K** (consensus) | `AppendEntries`/`RequestVote`/heartbeat processing | Never — no gate exists anywhere reachable from the event loop, structurally |
| **A1** (control) | Membership add/promote/remove, upgrade precheck/finalize, TLS reload | Yes, own gate (`-max-admin-concurrency`), never queued behind client load |
| **A2** (maintenance) | Backup, scrub | Yes, own gate (`-max-maintenance-concurrency`), independent of A1; each kind (backup, scrub) additionally has its own single-slot lock so two backups can never overlap but a backup and a scrub can |
| **B** (client) | `Propose` (writes), `BeginReadIndex` (reads) | Yes, two independent gates |

A saturated Lane B can never block Lane A1/A2/K; a saturated Lane A2
can never block Lane A1. This is checked structurally
(`TestAdmissionNeverReachableFromEventLoop`, an AST test), not merely
documented.

## 2. Flags and their defaults

Every default below is justified against one of two things: the memory
arithmetic in §3, or "exactly the pre-`v0.6.0` behavior." None is a
number that "looks reasonable" (`docs/v0.6.0-plan.md` §10.3).

| Flag | Default | Meaning |
|---|---|---|
| `-max-inflight-proposals` | `256` | Lane B write concurrency, and the authoritative `len(n.waiters)` event-loop ceiling (§4). `0` is a startup error. |
| `-max-concurrent-reads` | `512` | Lane B `BeginReadIndex` concurrency, and the `len(n.pendingReads)` ceiling. `0` is a startup error. |
| `-max-live-read-leases` | `4096` | Ceiling on simultaneously live read leases (`docs/storage-lifecycle.md` §4). `0` is a startup error. |
| `-max-concurrent-transactions` / `-max-concurrent-sql-statements` | `256` | Either name sets the `internal/sql` statement gate; setting both is a startup error. |
| `-admission-queue-depth` | `256` | Waiting-room capacity behind the write/read gates. `0` = reject immediately, never wait — a legitimate permanent choice, not merely "unset." |
| `-admission-max-wait` | `500ms` | Upper bound on queued wait before `queue_timeout`. `0` = bounded only by the caller's own context. |
| `-max-admin-concurrency` | `2` | Lane A1 concurrency. `0` is a startup error. |
| `-max-maintenance-concurrency` | `2` | Lane A2 concurrency (shared by backup and scrub, each additionally single-slot). `0` is a startup error. |
| `-disk-pressure-threshold` | `""` (off) | Absolute (`2GiB`) or percentage (`10%`) free-space floor below which Lane B writes tighten. |
| `-disk-critical-threshold` | `""` (off) | Free-space floor below which Lane B writes are refused outright. Must be strictly less than `-disk-pressure-threshold`, and requires it to also be set. |
| `-max-heap-bytes` | `0` (off) | Heap ceiling above which Lane B writes tighten exactly as disk `LowSpace` does. An admission threshold, never an allocator limit — set `GOMEMLIMIT` for that. |
| `-resource-poll-interval` | `5s` | Disk/heap pressure sampling cadence. |
| `-max-http-connections` | `1024` | Bounded accept on the control-plane HTTP listener. |
| `-max-peer-connections` | `64` | Bounded accept on the Raft peer listener. `0` = unlimited (`v0.5.0` behavior). |
| `-peer-idle-timeout` | `60s` | Read deadline on an inbound peer connection. `0` = no deadline (`v0.5.0` behavior). |
| `-http-read-header-timeout` | `5s` | `http.Server.ReadHeaderTimeout`. |
| `-http-read-timeout` | `30s` | `http.Server.ReadTimeout`. |
| `-http-write-timeout` | `60s` | `http.Server.WriteTimeout`. |
| `-http-idle-timeout` | `120s` | `http.Server.IdleTimeout`. |

**The one default-behavior change in `v0.6.0`:** the HTTP timeouts and
connection caps above are on by default, because "no timeouts at all"
was never a behavior worth preserving compatibility with. A client
holding an idle connection longer than `-http-idle-timeout`, or
streaming a request body for longer than `-http-read-timeout`, is now
disconnected. Every other flag above defaults to exactly `v0.5.0`
behavior (admission simply never fires unless an operator opts in by
raising a limit past what real traffic needs, or by setting a
disk/heap threshold at all).

## 3. Sizing: the memory arithmetic

With the defaults above and the existing 1 MiB request-body limit, the
worst-case resident client-write payload on a node is:

```
(-max-inflight-proposals + -admission-queue-depth) × 1 MiB
= (256 + 256) × 1 MiB
= 512 MiB
```

Any change to either flag should be checked against this formula before
being deployed. This bound covers request-body memory only — it is not
a total-process memory budget, and it says nothing about heap growth
from MVCC version chains (`docs/storage-lifecycle.md`) or the
`RequestID` outcome table (§7, below).

## 4. The two event-loop ceilings, and what a rising defense-rejection counter means

The admission gate bounds *goroutines and resident request-body
memory*. It does **not** bound `len(n.waiters)`/`len(n.pendingReads)` —
those are bounded by a second, independent check inside the event loop
itself (`handlePropose`/`handleReadIndex`), which is the actual,
authoritative mechanism behind `BOUNDED ADMITTED WORK`.

The two mechanisms are not interchangeable. A client that cancels its
context frees its gate slot (the gate's own `defer release()` fires),
but its `waiters`/`pendingReads` entry survives until the entry applies,
the node steps down, or the node stops. A new caller is then admitted
through the now-free gate slot while the old entry is still occupying
the event-loop ceiling. This is ordinary operation, not a bug: a client
population using request deadlines shorter than real apply latency (or
a leader stuck in a minority partition, where nothing commits at all)
produces exactly this pattern continuously.

`chronicledb_admission_defense_rejections_total{ceiling="waiters"|"pending_reads"}`
rising is therefore **a normal operating signal under client
cancellation, not a bug signal.** It means "clients are abandoning
requests faster than they apply." The tuning response is to raise
`-max-inflight-proposals`/`-max-concurrent-reads`, or to lengthen client
request deadlines — not to investigate for a defect.

## 5. Resource pressure

Disk and heap headroom are sampled on a single background goroutine
every `-resource-poll-interval`; no request path ever makes a syscall
directly. Three states, with 10% de-escalation hysteresis:

| State | Entered when | Lane B writes | Lane B reads | Lane A | Lane K |
|---|---|---|---|---|---|
| Normal | Headroom above both thresholds | Full ceiling | Full | Full | Unaffected |
| LowSpace | Free bytes < `-disk-pressure-threshold`, or heap > `-max-heap-bytes`, or the probe itself errored | `max(1, -max-inflight-proposals / 4)` | Full | Full | Unaffected |
| Critical | Free bytes < `-disk-critical-threshold` | All rejected (`disk_critical`) | Full | Full | Unaffected |

De-escalation requires headroom to clear the threshold by 10%, so a
workload hovering at the boundary does not flap between states on every
poll — each transition is an audited event (`docs/storage-lifecycle.md`
§5), and flapping would be noisy, not merely inefficient. Entering
LowSpace or Critical triggers an immediate GC pass proposal if GC is
enabled (`docs/storage-lifecycle.md`) — the one automatic action
pressure ever takes, and it deletes only what `docs/mvcc.md` §6's rule
already permits. Lane K is never affected: a follower under Critical
still appends and still votes; if its disk is genuinely full, the
append itself fails and the existing `Node.fail` path halts that node
explicitly — a correct, already-proven outcome pressure must never
pre-empt.

`-disk-pressure-threshold`/`-disk-critical-threshold` require Linux or
Darwin (`internal/storage.DiskUsage`'s `unix` build tag; only Linux
amd64 is actually tested, `docs/support-matrix.md`). Setting either on
an unsupported platform refuses startup, naming the flag; leaving both
unset on any platform is `v0.5.0` behavior exactly, and `/status`
reports `diskPressure: "unsupported"` so an operator is never misled
into believing protection is active.

## 6. The `Reason` vocabulary and HTTP contract

Every admission rejection carries a fixed, documented `Reason` and a
`Retry-After` hint — never a bare `503`:

| `Reason` | Meaning | `Retry-After` |
|---|---|---|
| `queue_full` | The gate's waiting room was full | 1s |
| `queue_timeout` | Waited `-admission-max-wait` without a slot | 1s |
| `concurrency_limit` | An event-loop ceiling fired (§4) | 1s |
| `read_lease_limit` | `-max-live-read-leases` was reached | 1s |
| `disk_pressure` | LowSpace; the tightened ceiling was reached | 5s |
| `disk_critical` | Critical; all client writes refused | 30s |
| `memory_pressure` | `-max-heap-bytes` exceeded | 5s |
| `admin_operation_in_progress` | A Lane A single-slot operation is already running | 10s |
| `shutting_down` | The node is stopping | none — do not retry here |

New reasons may be added in a MINOR release; existing ones never change
meaning. A client must treat an unknown reason as retryable if `503`
was returned.

```
HTTP/1.1 503 Service Unavailable
Retry-After: 1
Content-Type: application/json

{"error":"admission: overloaded (queue_full)","reason":"queue_full","retryable":true}
```

A `503` never overlaps a correctness outcome: overload is always `503`;
not-leader is `409` (`leaderHint`); an SI write-write conflict is `200`
(`status: "ABORTED"`); a snapshot older than the GC horizon is `200`
(`status: "ABORTED_STALE"`, `docs/storage-lifecycle.md`); malformed
input is `400`; auth failures are `401`/`403`. A client can therefore
always distinguish "try again, nothing happened" from "this transaction
lost a conflict" from "your snapshot aged out."

**A rejection is always safe to retry.** Every gate acquisition happens
before the request reaches the event loop, and therefore before any log
index is allocated, any byte is appended, or any `RequestID` is
recorded — a rejected request is indistinguishable, in every durable
artifact, from a request that was never made.

## 7. Metrics

| Metric | Type | Notes |
|---|---|---|
| `chronicledb_admission_defense_rejections_total{ceiling="waiters"\|"pending_reads"}` | counter | See §4 — normal under client cancellation |
| `chronicledb_node_waiters` | gauge | `len(n.waiters)` |
| `chronicledb_node_pending_reads` | gauge | `len(n.pendingReads)` |
| `chronicledb_read_leases_active` | gauge | Live read-lease count |
| `chronicledb_disk_probe_failures_total` | counter | A `DiskUsage`/heap sample itself errored |
| `chronicledb_raft_message_process_seconds` | histogram | Lane K's own service-time histogram — the `CONTROL-PLANE NON-STARVATION` proof metric; unaffected by admission state by construction |

Every per-gate stat (`InFlight`, `Queued`, `EffectiveConcurrent`,
`AdmittedTotal`, `RejectedTotal` by reason, wait-time histogram) is
available programmatically via `admission.Gate.Stats()`; see
`docs/observability.md` for what is currently exposed over `/metrics`
versus available only to a direct Go-API caller.

## 8. Tuning runbook

1. **Seeing steady `503`s with `queue_full`/`queue_timeout`?** Real
   sustained overload. Either the workload has genuinely exceeded this
   node's capacity (scale out, or accept shedding as correct behavior),
   or the limits are set too low for the hardware — raise
   `-max-inflight-proposals`/`-admission-queue-depth` after checking the
   memory arithmetic in §3 against actual available RAM.
2. **Seeing a rising `admission_defense_rejections_total` with no
   corresponding `503`s at the gate?** Clients are cancelling faster
   than requests apply (§4) — not a capacity problem the gate can see.
   Lengthen client deadlines, or investigate why apply latency is high
   (snapshot creation on the event loop, disk latency, a saturated
   disk).
3. **Seeing `disk_pressure`/`disk_critical`?** Free disk space. GC is
   already being triggered automatically if enabled; if it is not
   enabled, consider enabling it (`docs/storage-lifecycle.md`) once its
   three rate flags have been derived from a measured
   version-creation rate for this workload.
4. **Seeing `admin_operation_in_progress`?** A backup or scrub (or,
   rarely, a concurrent admin call of the identical kind) is already
   running. Retry after the indicated backoff; this is never a
   capacity signal about client load.

## 9. What this does **not** protect against

Stated plainly, because overstating a safety mechanism is worse than
not having one:

- **Standalone (non-replicated) mode.** `internal/sql`'s statement gate
  applies there, but there is no node-level admission control and no
  GC — version chains grow unbounded in standalone mode exactly as they
  did before `v0.6.0` (`docs/sql.md` §8).
- **The event loop blocking on its own long-running synchronous work.**
  Snapshot creation and backup export both still run synchronously on
  the event-loop goroutine in `v0.6.0` — admission control bounds
  *client-caused* concurrent work, not the cost of this node's own
  self-initiated maintenance operations. This is a named, deferred
  `v1.0.0` item (`docs/roadmap.md`), not a gap this release claims to
  have closed.
- **A hard memory cap.** `-max-heap-bytes` is an admission threshold
  checked on a polling interval, not an allocator limit — Go provides
  no hard heap cap of its own. `GOMEMLIMIT` is the actual mechanism an
  operator should also set if a hard limit is required.
- **Any platform other than Linux/Darwin, for disk-pressure admission
  specifically.** Heap-pressure admission and every non-disk mechanism
  in this document work identically everywhere; see §5.

## 10. Related documents

- [`ADR-0019`](adr/0019-admission-control-architecture.md) — why this
  architecture, and the alternatives considered.
- [`docs/storage-lifecycle.md`](storage-lifecycle.md) — the companion
  Storage Lifecycle half of `v0.6.0` (MVCC GC, retention, disk-full
  handling, scrub), which shares this document's pressure-sampling
  infrastructure.
- [`docs/invariants.md`](invariants.md) — the full correctness argument
  (`BOUNDED ADMITTED WORK`, `CONTROL-PLANE NON-STARVATION`,
  `REJECTION SAFETY`, `ADMISSION FAILS CLOSED`).
- [`docs/configuration.md`](configuration.md) — every flag in this
  document, in the project-wide flag reference.
- [`docs/observability.md`](observability.md) — the full metric
  catalog.
