# Dynamic Membership Implementation Plan (`v0.5.0`)

Status: **planning only**. Nothing in this document has been
implemented. No production code, test, ADR, or other doc has been
changed to produce it, and no maturity claim in
[`docs/roadmap.md`](roadmap.md) or release status in `CHANGELOG.md`
changes as a result of it existing. Written against baseline commit
`8eeb0aa` (`docs: correct stale v0.3.0 release status`) — `main` clean,
CI green, `v0.4.0` (Compatibility / Rolling Upgrades) tagged and
released. This document does not authorize starting `v0.5.0`
implementation; that authorization, when it comes, is a separate, later
decision referencing this document, exactly as
[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §0 states for
itself.

**Revision 2 — architecture-review response.** This document was
revised in response to a full adversarial correctness review of
revision 1 (planning commit `6df4e68`), which returned
`V0.5.0 PLAN REQUIRES REVISION` against three architecture blockers,
eight correctness gaps in the plan, and eight proof/test gaps. Every
one of those findings is resolved in the text below and traced,
finding by finding, in §23. Revision 2 changes **design**, not scope:
the phase still adds exactly the capability §8 of the enterprise plan
describes, with the same non-goals (§20). The three load-bearing
statements the review tested are now stated in their corrected form:

- *"Single-server changes plus one outstanding change preserve quorum
  safety"* — true **only** with the leader-term commit gate of §2.2a
  as an explicit premise. Revision 1 omitted it and was unsafe (a
  concrete two-leader/two-commits-at-one-index trace is reproduced in
  §2.3 and turned into scenario **DM-12**); §2.3 now proves the
  branch-confinement invariant that premise buys (revision 2 stated
  that invariant as a two-element window, which revision 3 corrects —
  §23/F4).
- *"The configuration becomes effective as soon as `EntryConfig` is
  appended"* — correct, and load-bearing, for every **live** quorum
  decision (§2.2). It is **not** correct at a snapshot or compaction
  boundary, which needs the configuration effective **at a specific
  index** (§6.3's `ConfigAt`).
- *"A single current-membership field in `Core` is enough, rather than
  index-aware configuration state"* — **false**, and revision 1's §7
  was wrong because of it. `Core` keeps `activeConfig` for live
  decisions **and** the index-aware `ConfigAt` derivation for boundary
  decisions; both are defined by one algorithm (§6.3).

**Revision 3 — boundary-closure response.** A second adversarial
closure review, of revision 2 (planning commit `d41ec09`), again
returned `V0.5.0 PLAN REQUIRES REVISION`: three correctness gaps
(**F1**–**F3**), one proof/test gap (**F4**), and five non-blocking
items (**F5**–**F9**). All are resolved below and traced, finding by
finding, in §23's revision-3 table. Revision 3 changes **no consensus
mechanism**: §2.1's single-server protocol, §2.2's append-time-effective
rule, §2.2a's three gates, §2.6's four shapes and §6.3's single
reconstruction algorithm are unchanged in substance. What changes is
that three *boundaries* revision 2 did not carry membership semantics
across are now carried across explicitly, and one theorem is restated
in the form that is actually true:

- **The WAL round trip** (F2, §6.1a). Revision 2 gated `Entry.Type`'s
  durable header on the writing node's cluster generation — a value
  every follower updates *after* it persists — so the **first**
  `EntryConfig` after finalization was written type-less on every
  follower, and that node's next restart reconstructed a stale
  configuration. The gate is now the entry's own type.
- **The restore path** (F1, §7.6). Revision 2 cleared the staged
  snapshot's configuration but left the source cluster's `EntryConfig`
  entries in the restored WAL suffix, where `ConfigAt`'s step-1 scan
  finds them *before* it ever consults the snapshot. Restore now voids
  those entries in the same pass.
- **The read path** (F3, §4.2a). Revision 2 excluded a self-removing
  leader from its own commit quorum (§4.2) but never from its own
  ReadIndex quorum, where `internal/node.checkPendingReads` counts
  `self` unconditionally. One rule now covers both quorums.
- **The window theorem** (F4, §2.3). "At most two live configurations,
  and adjacent" is false as literally stated — a partitioned node keeps
  a superseded configuration live indefinitely, in revision 2's *own*
  repaired DM-12 schedule. The mechanically checkable invariant is
  branch confinement (W1/W2); the safety conclusion follows from it
  plus the new Lemma 3, not from a two-element window. The
  configuration-lineage oracle is corrected to match, and calibrated by
  DM-22.

Revision 3 additionally deletes every statement the closure review
found to describe the current codebase inaccurately — most importantly
the claim that `Core` owns election-timer tick state, which
`internal/node` actually owns (§2.7 Rule 2, F6) — replaces the
"`snapshotConfig` is empty" sentinel with an explicit durable
`HasConfiguration` bit (F7), pins `termAt`'s snapshot-boundary
behavior that P1 depends on (F8), defines the `Core` membership
accessors and their complete caller list (§6.3a, F9), removes §9's
second configuration derivation (F5), and promotes four items from
"implementation-time choice" to plan-level decisions because each can
affect test determinism, compatibility or security (§22).

**Revision 4 — final-review response.** A third adversarial review, of
revision 3 (planning commit `8b4a7a7`), confirmed **F1**–**F9** closed
and the consensus architecture intact, and returned
`V0.5.0 PLAN REQUIRES REVISION` against four further findings — one
correctness gap (**G1**), three proof/test gaps (**G2**–**G4**) — plus
six non-blocking items (**G5**–**G10**). All are resolved below and
traced in §23's revision-4 table. Revision 4 changes **no consensus
mechanism whatsoever**: §2.1's single-server protocol, §2.2's
append-time-effective rule, §2.2a's three gates, §2.6's four shapes,
§4.2a's one-rule-for-both-quorums, §6.3's single reconstruction
algorithm and §7.6's two-part restore transform are all unchanged in
substance. What changes is that three *claims* revision 3 made about
those mechanisms were stronger than the mechanisms themselves, and one
rule contradicted itself:

- **The restore replication rule** (G1, §7.6). Revision 3 wrote that a
  voided `EntryConfig` "arriving from a live leader" is `Node.fail`,
  one sentence before stating that a restored directory legitimately
  replicates its voided entries onward. Both cannot hold, the
  fail-closed reading halts every node that joins a restored cluster
  before it snapshots (DM-21 step 6's own schedule), and no receiver
  can tell the two cases apart in the first place. A voided entry is
  now **accepted unconditionally on the replication path**;
  fail-closed lives only on the emit side, where a discriminator
  exists.
- **The read-path outcome claim** (G2, §4.2a, §15, §16). "Either
  resolves or fails cleanly with `ErrLeadershipLost`, never hangs" is
  two of three cases. When no majority of `C_new` is reachable, the
  `EntryConfig` cannot commit *either*, so there is no step-down and no
  clean error — the read blocks to the caller's deadline, exactly as an
  ordinary read on a minority-partitioned leader does. DM-17's
  self-removal sub-case asserted both halves of a schedule that cannot
  produce both; it is now three phases, one per row of §4.2a's new
  case table.
- **The replacement safety proof** (G3, §2.3). Lemma 3 reduced every
  non-adjacent pair to "two children of one common ancestor" by a step
  that is false when two live configurations hang off *different*
  committed ancestors — a reachable state. The conclusion holds; the
  proof now has the case analysis to reach it, including the
  `isLogUpToDate`-rejects-**by-index** branch revision 3 had no case
  for at all.
- **The pinned promotion default** (G4, §3.3). `PromotionMaxLagEntries
  = 0` was pinned *for* determinism on the claim that it makes the gate
  "a pure function of observable state". It does not — `lag == 0` is
  observed by one request and re-evaluated on the event loop
  afterwards, while §16's own background writer moves `LastIndex()`.
  The default stays `0` (it is the conservative value), and the
  determinism is recovered by pinning the other half: `425` is a
  records-nothing, pre-proposal refusal and is explicitly retryable.

Revision 4 additionally corrects the `cfg.Peers`/`cfg.majority()`
call-site enumeration, which was missing `handleRequestVoteResponse`'s
election-win tally and counted seven where there are eight (G5); pins
that §7.6's suffix transform must not be applied inside the
`Export`-shared `copyWALSuffix` helper (G6); states
`Config.validate()`'s empty-`Bootstrap` carve-out that §3.1 depends on
(G7); extends §7.2's `InstallSnapshot` cross-check to the
`HasConfiguration` flag as well as the configuration value (G8);
documents `heardFromLeader`'s composition with the existing
`PauseTicksForTest` election-clock freeze (G9); and states §17's
invariant count once, correctly, in both places that carried a
different wrong number (G10).

**Revision 5 — proof-detail finalization.** A fourth review, of
revision 4 (planning commit `1900617`), confirmed **G1**–**G10** closed
and returned one proof gap (**H1**) plus three cosmetic/test-wording
items (**H2**–**H4**). Revision 5 changes **no mechanism and no
conclusion**: it corrects one intermediate bound inside §2.3's Lemma 3
Case 1, which was stated as `<= term(L)` where the reachable bound is
`< term(L')` (a leader may legitimately be elected on an *uncommitted*
configuration branch and extend it, which makes a node's `activeConfig`
and its **anchor** different things — revision 4's justification
conflated them); replaces §16's promote-step "decreasing-or-equal
`lag`" assertion, which required a monotonicity nothing provides
against a `LastIndex()` the background writer advances, with a bound
the test sets from its own inputs; states explicitly that DM-17
sub-case 3's two phases observe **one** read — the caller's context
error in Phase A, then that same still-registered read's
`ErrLeadershipLost` on its buffered `resultCh` in Phase B, with no
second `BeginReadIndex`; and fixes §2.2's call-site count, which mixed
per-row and per-function conventions in one sentence. Lemma 3's
conclusion, the theorem that rests on it, and every one of §17's
invariants are unchanged.

Revision 1's baseline remains commit `8eeb0aa`; revision 2 is written
against `6df4e68` (`docs: define v0.5.0 dynamic membership plan`),
revision 3 against `d41ec09` (`docs: harden v0.5.0 membership plan
after architecture review`), and revision 4 against `8b4a7a7`
(`docs: close v0.5.0 membership plan boundary gaps`) — still
`main`-clean, still planning-only, still not an authorization to begin
implementation.

This document is the detailed, implementation-ready design for
[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §8 ("Dynamic
Membership," target release `v0.5.0`) — it resolves every open design
question that section leaves at plan-level granularity, reconciled
against the actual current implementation of `internal/raft`,
`internal/node`, `internal/fsm`, `internal/snapshot`, `internal/wal`,
`internal/version`, and `internal/authz` as of the baseline commit
above. Where this document's mechanism differs in detail from §8's own
prose (most notably: where membership control commands live in the
package boundary, and the exact append-time-effective semantics), this
document's design is authoritative for implementation — §8 is the
proposal-level scope agreement, this is the engineering resolution of
it. Every invariant, format, and test named below is a plan to add
when `v0.5.0` is actually built, mirroring exactly how
[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §5–§7 named
invariants "(new, added to `docs/invariants.md` when implemented)" —
**this document does not itself edit `docs/invariants.md`,
`docs/adr/`, `docs/membership.md`, `docs/backup.md`,
`docs/snapshots.md`, or `docs/raft.md`'s substantive content**; those
are implementation-time deliverables this plan specifies precisely
enough to write without further architectural judgment calls. §19's
doc-update list is the complete, authoritative enumeration of them.

## 0. Scope reaffirmation

Per [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md) and
[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §0.2, this phase
changes *cluster membership* (which nodes host the one Raft group) —
never the number of *shards* (still exactly one, always). Nothing
below reintroduces sharding, cross-shard transactions, or multi-group
Raft. See §20 for the complete non-goals list.

Depends on: `v0.2.0` Security Foundation (mTLS node identity, RBAC,
audit — §13), `v0.3.0` Backup/DR (**modified**, not merely reused —
see §7.6), `v0.4.0` Compatibility/Rolling Upgrades (the
generation-gating mechanism this phase's own new wire/log-entry/
snapshot formats ride on — §8). All three are released.

**Non-weakening claim, stated precisely** (revision 1 overstated this,
§23/B3): no *guarantee* of `v0.2.0`–`v0.4.0` is weakened, but two of
their *mechanisms* change and are re-proved here rather than assumed:

- `internal/snapshot.Decode`'s strict format-version **equality** check
  becomes a bounded supported **range** check (§7.1). The invariant
  (`NO SILENT FORMAT MISINTERPRETATION`: an unrecognized version is
  never guessed at) is preserved; the mechanism is not identical, and
  `docs/snapshots.md` §5 and `ADR-0017` must say so.
- `internal/backup`'s restore path gains an explicit, **two-part**
  membership re-bootstrap step (§7.6) — one part on the staged
  snapshot, one on the staged WAL suffix — without which this phase
  would silently break `docs/backup.md`'s already-documented "restore
  onto a new peer set" operation. `BACKUP INTEGRITY`,
  `BACKUP CONSISTENCY`, and `DESTRUCTIVE RESTORE ISOLATION` are
  unchanged in both statement and mechanism. Restored application state
  remains exact, with exactly one deliberate, narrowly scoped exception
  stated in §7.6: membership-change `RequestID` outcomes belonging to
  the *source* cluster's own membership operations are not carried into
  the restored cluster, because those operations name nodes that are
  not members of it. Every other `RequestID` outcome, every MVCC
  version and the cluster generation are byte-identical.

---

## 1. Membership model

### 1.1 Exact membership state representation

Membership is Raft-core-level state, not application (FSM) state — see
§2.4 for why this placement is load-bearing, not a style choice.

```go
// internal/raft/config.go (new type, alongside the existing Config)

// NodeID already exists (internal/raft/types.go); Member pairs it with
// the dial address other nodes need to replicate to it. Core never
// dials Address itself (docs/raft.md §1: no network I/O in Core) — it
// carries Address opaquely, exactly as it already carries
// Message.SnapshotData opaquely (see MsgInstallSnapshotRequest's doc
// comment) — only internal/node's transport layer ever reads it.
type Member struct {
    ID      NodeID
    Address string // "host:port", operator-supplied at add time
}

// Configuration is the complete, self-contained description of who
// participates in this Raft group and how, at one point in the log.
// It is Core's own type — never passed through internal/fsm.
type Configuration struct {
    Voters   []Member // majority-counted; may vote, may be voted for, may become Leader
    Learners []Member // replicated to; never counted; never votes; never elected
}

func (c Configuration) majority() int  { return len(c.Voters)/2 + 1 }
func (c Configuration) isVoter(id NodeID) bool   { /* linear scan; cluster sizes are small (§20) */ }
func (c Configuration) isLearner(id NodeID) bool { /* same */ }
func (c Configuration) isMember(id NodeID) bool  { return c.isVoter(id) || c.isLearner(id) }
```

`Configuration` fully replaces `Config.Peers` as the thing majority/
replication decisions are computed from. `Config.Peers []NodeID` is
**removed**; `Config` keeps exactly the truly static, per-process
fields (`ID`, timeouts, `Rand`) plus one new field:

```go
type Config struct {
    ID NodeID
    // Bootstrap is the starting Configuration, consulted ONLY when a
    // Core is constructed with no prior log entries and no snapshot at
    // all (a brand-new, never-before-run cluster, OR one produced by
    // -restore-from, whose staged snapshot deliberately carries no
    // Configuration — see §1.8 and §7.6). Every
    // later restart derives the active Configuration from durable
    // state instead (§6), never from this field again.
    Bootstrap Configuration
    ElectionTimeoutTicks, ElectionTimeoutJitterTicks, HeartbeatTimeoutTicks int
    Rand Rand
    // PromotionCatchUpTicks/... (leader-only tuning) live in
    // internal/node, not here — see §3.3.
}
```

`Config.validate()` changes from "`Peers` must include `ID`" to a
**conditional** check, and the condition is load-bearing rather than
incidental (§23/G7):

```
if len(Bootstrap.Voters) > 0 || len(Bootstrap.Learners) > 0:
    Bootstrap.Voters must include Config.ID
// an entirely empty Bootstrap is VALID and is the required state for
// §3.1's never-joined node
```

Today's check is unconditional (`Config.Peers` must contain
`Config.ID`, `internal/raft/config.go`), so translating it literally to
`Bootstrap` would make `NewCore` reject the brand-new learner process
of §3.1 — which is constructed with a deliberately **empty**
`Bootstrap` and acquires its configuration only by replication — and
would take the "join an existing cluster" path with it. The check is
therefore a *bootstrap-seed well-formedness* check ("if you are seeding
a cluster, you must be in it"), not a membership check: a node joining
an already-running cluster as a learner never uses `Bootstrap` at all
(§3.1), and a restored directory's `Bootstrap` is the operator's
`-cluster`/`-peers` flag set, which does include this node (§7.6). A
direct unit test pins both branches — empty `Bootstrap` accepted,
non-empty `Bootstrap` without `ID` rejected.

### 1.2 Voter semantics

Exactly today's existing Raft semantics (`docs/raft.md` §2–§4),
computed against `Configuration.Voters` instead of `Config.Peers`:
votes requested from and counted from Voters only; `matchIndex`
counted toward the commit rule for Voters only; only a Voter may become
Candidate/Leader.

### 1.3 Learners exist in `v0.5.0`

Yes — required by [`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md)
§8's own architecture ("a new node joins as a learner") and by this
plan's own safety requirement (§3's closing line: "a newly added node
must not endanger quorum before it has sufficient state"). A Learner:

- Receives `AppendEntriesRPC`/`MsgInstallSnapshotRequest` exactly like
  a Voter (same `nextIndex`/`matchIndex` tracking, same log-matching
  rule).
- Is never sent `RequestVoteRPC` by a candidate, never grants a vote,
  and never becomes Candidate itself — all three enforced by §2.7's
  Rule 1, whose third clause makes a non-Voter's own election timeout
  a no-op. That last point is a **real code change**:
  `handleElectionTimeout` campaigns unconditionally today unless
  already Leader (§21 slice 1).
- Never counted in `majority()`; its absence, crash, or slowness never
  delays or blocks commit for the Voter set (`LEARNER NON-INTERFERENCE`,
  §17).
- Its `matchIndex` **is** tracked (needed for promotion eligibility and
  for `/admin/membership/status`'s lag reporting, §9, §14).

### 1.4 Node identity rules

Unchanged from `v0.2.0`: `NodeID` is an opaque, equality-compared
string (`internal/raft/types.go`'s existing doc comment), bound to a
node process's TLS certificate subject at startup
(`internal/node.Open`'s existing identity-binding check, unchanged).
Dynamic membership adds no new identity *mechanism* — it adds a second
place identity matters: a `Member.ID` inside a replicated
`Configuration` must match the actual TLS identity the corresponding
physical process presents on connection (§13).

### 1.5 Duplicate / reused node IDs

- **Duplicate within one `Configuration`**: structurally impossible by
  construction — every mutation is validated as one of exactly four
  transition shapes (§2.3), each of which is checked against the
  *current* `Configuration` for the target `ID`'s absence (add) or
  presence (promote/remove) before being accepted, on every replica
  (§2.4's defense-in-depth re-check).
- **Reuse of a previously-removed `ID` for a genuinely different
  physical node**: **permitted by the mechanism, not specially
  detected**. Once an `ID` is removed from `Configuration`, nothing
  distinguishes "this ID was never used" from "this ID was used and
  removed" — Raft, like this system's existing `WAL`/`FSM`, retains no
  historical membership ledger beyond the current active
  `Configuration` plus whatever is still in the retained log/snapshot.
  This is a deliberate scope decision (inventing a permanent
  removed-ID tombstone/ledger is exactly the kind of "policy this
  project does not build in v1" the enterprise plan's non-goals
  repeatedly reject — see §20) with two real defenses instead of a
  ledger:
  1. **mTLS identity binding** (§13, unchanged from `v0.2.0`): reusing
     an `ID` for a different physical node requires the operator to
     also issue/rotate a certificate presenting that same identity —
     an explicit, auditable operator action, not something that
     happens by accident.
  2. **§2.7's two rules**: a stray old process that still believes it
     holds a removed `ID` and keeps campaigning under it has its
     `RequestVoteRequest`s dropped by every current member whose own
     `Configuration` no longer contains that `ID` (Rule 1), and
     ignored by every member currently hearing from a legitimate
     leader (Rule 2) — see §4.5 for the complete "stale removed node"
     treatment, and note the property claimed there is liveness, not
     safety.
  - Documented operator guidance (`docs/membership.md`, written at
    implementation time): do not reuse a removed node's `ID` for a new
    physical node without first decommissioning (wiping the old data
    directory and revoking its certificate) the original.

### 1.6 Address / endpoint representation and mutation

Address lives inside `Member.Address` (§1.1), replicated as part of
`Configuration` — not a separately-gossiped or separately-configured
value. **In-place address mutation for an existing member is out of
scope for `v0.5.0`** (§20): re-IP'ing a node requires removing it and
re-adding it (as a fresh learner) under the same or a different `ID`
with its new address. This is a deliberate simplification, not an
oversight — a fourth transition shape ("same `ID`, different address")
would need its own catch-up/quorum reasoning for no clear operational
benefit over remove+re-add at this project's target cluster sizes
(§20).

### 1.7 Persistent vs. volatile membership state

| State | Persistent? | Where |
|---|---|---|
| The currently active `Configuration` | Yes, derived | Not stored as its own record. It is **always** the output of the single reconstruction algorithm `ConfigAt` (§6.3) applied to this node's own durable log + snapshot boundary + bootstrap seed — there is exactly one such algorithm and exactly one priority order, used identically by append, recovery, truncation repair, snapshot creation, compaction, `InstallSnapshot`, and leader initialization (§6.3's call-site table). This mirrors exactly how `commitIndex`/`appliedIndex` are **not** independently persisted but are always reconstructed (`docs/raft.md` §5.1) — membership gets the identical treatment, for the identical reason (never trust a cached derived value; always recompute from the log/snapshot that is the actual source of truth). |
| The configuration effective at an *older* index (snapshot/compaction boundaries, and the committed-configuration view `/admin/membership/status` reports) | Yes, derived | `ConfigAt(index)` (§6.3) — **never** `activeConfig`, which is append-time-effective and may reflect an entry above that index that has not committed and may still be truncated (§23/C1). The same function called with `commitIndex` is the *only* source of `committedConfigIndex` (§9); revision 2's separate "previous configuration's index" derivation is deleted (§23/F5). |
| `matchIndex`/`nextIndex` per member (including learners) | No (volatile, leader-only) | Exactly like today's existing `matchIndex`/`nextIndex` — reconstructed by the leader from live `AppendEntriesResponse` traffic after every election, never persisted. |
| The membership-change `RequestID` → outcome idempotency table | Yes | `internal/fsm.FSM` (§2.4, §10) — snapshotted like every other outcome table (`REQUEST OUTCOME STABILITY`). |
| `-peers`/`-cluster`/`PeerAddrs` CLI flags | Advisory bootstrap-only | See §1.8. |

### 1.8 Relationship between Raft membership and node process configuration

Today, `-peers`/`-cluster` (`cmd/chronicledb-node/main.go`) and
`internal/node.Config.PeerAddrs` are the **sole, permanent** source of
truth for cluster membership — re-read and re-trusted on every
process start. From `v0.5.0` onward:

- They remain the **only** source of truth for a truly fresh data
  directory (`wal.FirstIndex() == 1` and no snapshot present) — this
  is what seeds `Config.Bootstrap` for the very first `NewCore` call a
  freshly-`chronicledb-node -datadir=<empty>` invocation ever makes.
  This is exactly today's existing three-node startup path, unchanged.
- On every subsequent restart of a data directory that has ever
  processed a committed `EntryConfig` entry or restored a snapshot
  carrying one, the flags are **advisory only**: `internal/node.Open`
  computes the real active `Configuration` from durable state (§6) and
  uses that, unconditionally. If the flag-supplied peer list disagrees
  with the durably-derived one, `Open` logs a loud, single-line
  diagnostic (peer list drift is almost always a stale deployment
  script, worth a human noticing) but **never refuses to start** over
  it — refusing availability over a cosmetic mismatch would be a worse
  trade-off than the mismatch itself, consistent with this project's
  existing "diagnostic state is not a correctness dependency" posture
  (`docs/observability.md`) applied here to a flag that is now
  diagnostic-only post-bootstrap.
- **A data directory produced by `-restore-from` is the one deliberate
  exception to "durable wins."** Restore re-bootstraps `Configuration`
  from the operator's `-cluster`/`-peers` flags and never resurrects
  the source cluster's membership — see §7.6, which is a hard
  requirement, not a preference, because `docs/backup.md` already
  documents restoring into a different peer set / onto replacement
  hardware as a supported operation.
- `-listen` and `-http` (this node's *own* listen addresses) are
  unaffected — they are process-local configuration, never part of
  replicated `Configuration`, and remain required on every start.
- `internal/transport.Transport`'s dial-address table is no longer
  fixed at construction from `PeerAddrs` alone: `internal/node.Node`
  updates it live whenever the active `Configuration` changes (new
  learner added → new dial target; member removed → dial target
  retained but no longer used, per §4.4's drain semantics — the
  `Transport` itself does not need a "remove peer" operation for
  `v0.5.0`; an address the current `Configuration` no longer names is
  simply never dialed again by anything that consults `Configuration`
  first, which every send path does).

---

## 2. Membership change protocol

### 2.1 Decision

**Constrained, single-server-at-a-time membership changes, effective
immediately on log append (not on commit), serialized so at most one
change is outstanding at a time — not joint consensus.**

This matches [`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §8's
stated direction but this section gives the *exact* mechanism and the
safety proof that direction's prose does not itself spell out — in
particular, the append-time-effective timing (§2.2) and the FSM/Core
placement decision (§2.4), both load-bearing details the plan-level
document leaves implicit.

**Why not joint consensus**: joint consensus (Raft paper §6's more
general mechanism) is strictly more complex to implement and prove
correct — it requires a two-phase `C_old,new` intermediate
configuration under which *both* the old and new quorums must agree,
specifically so that *arbitrary* (not just single-server)
reconfigurations are safe, including concurrent/overlapping ones. This
project's target cluster sizes (three to seven voters, §20) and its
non-goal of concurrent reconfiguration (§11) make that generality
unnecessary: the single-server-change restriction, serialized, is
*exactly* as safe (§2.3's proof) for every mutation this system
actually needs to support (add one learner, promote one learner,
remove one member), at a fraction of the implementation and proof
surface. This is not a simplicity-only choice — see the proof below;
it would remain the correct choice even measured purely on "which
mechanism is easier to get right," which is itself a legitimate
engineering criterion this project's own vision doc treats as
first-class (`docs/vision.md`'s "smallest technically real design"
principle), applied here with an actual safety proof behind it, not
merely asserted.

### 2.2 Exact mechanism: append-time-effective single-server changes

A membership change is proposed, replicated, and committed as an
ordinary Raft log entry — but of a new, Core-native kind,
`EntryConfig`, distinct from the existing opaque `EntryNormal` kind
that carries FSM command bytes:

```go
// internal/raft/types.go
type EntryType uint8
const (
    EntryNormal EntryType = 0 // existing opaque FSM-command entries; the
                              // zero value, so every existing Entry{...}
                              // literal in every existing test/caller
                              // continues to mean exactly what it means today
    EntryConfig EntryType = 1
)

type Entry struct {
    Index Index
    Term  Term
    Type  EntryType // NEW field
    Data  []byte    // for EntryConfig: a raft-native encoded Configuration (§2.5); Core decodes this itself, never via internal/fsm
}
```

**The critical timing rule**: the moment an `EntryConfig` entry is
appended to a node's own in-memory log — whether because that node is
the Leader proposing it (`handlePropose`-equivalent), or because it is
a Follower accepting it via `AppendEntriesRPC`
(`handleAppendEntriesRequest`) — that node's `activeConfig` switches to
the entry's embedded `Configuration` **immediately, before the entry
commits**. This is the standard Raft single-server-change mechanism
(Ongaro thesis §4.1; Raft paper §6's extended discussion), not an
invention of this plan, and it is what makes the safety proof in §2.3
work: from the instant of append onward, *every* quorum-relevant
decision on that node (does a candidacy have enough votes; does an
entry have enough `matchIndex` replicas to commit; who gets sent
`AppendEntriesRPC`/`RequestVoteRPC` at all) uses the new
`Configuration`, including for deciding whether the `EntryConfig` entry
*itself* is committed.

Concretely, `Core` gains:

```go
type Core struct {
    // ... existing fields unchanged ...

    // activeConfig governs every LIVE decision this node makes right
    // now: who is sent AppendEntriesRPC/RequestVoteRPC, whose
    // matchIndex counts toward majority(), whether this node may
    // campaign at all. It is append-time-effective (this section) and
    // is, by construction, always exactly ConfigAt(lastIndex()) — a
    // maintained cache of that one function (§6.3), never an
    // independent source of truth.
    activeConfig      Configuration
    // activeConfigIndex is the log index of the EntryConfig entry that
    // produced activeConfig, or 0 when activeConfig came from the
    // snapshot boundary or from Config.Bootstrap (§6.3).
    activeConfigIndex Index
    // snapshotConfig is the configuration effective AT snapshotIndex —
    // i.e. ConfigAt(snapshotIndex) as computed at the moment that
    // boundary was established (Compact, NewCoreFromSnapshot, or
    // handleInstallSnapshotRequest). It is NEVER assigned from
    // activeConfig: doing so was revision 1's §7 defect (§23/C1),
    // because activeConfig can reflect an uncommitted EntryConfig entry
    // at an index above the boundary.
    snapshotConfig    Configuration
    // pendingConfIndex is the leader-only serialization floor, set to
    // lastIndex() at becomeLeader (§2.2a premise P2). No EntryConfig
    // may be appended by this leader while commitIndex < pendingConfIndex.
    pendingConfIndex  Index
    // snapshotHasConfig records whether the snapshot boundary carries a
    // configuration AT ALL — an explicit durable fact (§7.1's
    // Meta.HasConfiguration), never inferred from snapshotConfig being
    // empty, which is a legitimate value and a different statement
    // (§23/F7). False for a FormatVersion 1 snapshot and, deliberately,
    // for a snapshot staged by a restore (§7.6).
    snapshotHasConfig bool
    // heardFromLeader is §2.7 Rule 2's leader-contact state: set when
    // this node accepts an AppendEntries/InstallSnapshot from the
    // leader of its current term, cleared on InputElectionTimeout and
    // on any step-down to a new term. It is a boolean derived from
    // inputs Core ALREADY receives — Core owns no tick counter and no
    // clock; internal/node owns the election clock (§2.7, §23/F6).
    heardFromLeader   bool
}
```

Every existing call site that iterated `c.cfg.Peers` or called
`c.cfg.majority()` is changed to `c.activeConfig.Voters` /
`c.activeConfig.majority()`. The complete enumeration against the
current tree is the table below: **eight rows**, seven in
`internal/raft/core.go` and one covering the two mirrored copies in
`internal/node`. **One counting convention, used by the table and the
prose alike (§23/H4)**: a row is one *quorum-or-fan-out computation*,
not one function and not one `for` statement — `handleElectionTimeout`
contributes two rows (its peer fan-out and its single-node instant-win
check), `becomeLeader`'s two peer loops are one row (a single
`nextIndex`/`matchIndex` initialization over one member set), and
`internal/node`'s `Node.majority()` and `checkPendingReads` are one row
because they are two halves of the same read-quorum computation.
Counted by *function* the same set is six in `internal/raft/core.go`
plus two in `internal/node`; revision 4 mixed the two conventions in
one sentence. The table is authoritative:

| # | Call site | What it computes |
|---|---|---|
| 1 | `handleElectionTimeout` — peer fan-out | who is sent `RequestVoteRPC` |
| 2 | `handleElectionTimeout` — the single-node instant-win check | `len(votesReceived) >= majority()` |
| 3 | **`handleRequestVoteResponse`** — the election-win threshold | `len(votesReceived) >= majority()` |
| 4 | `handleHeartbeatTimeout` — peer fan-out | who is sent `AppendEntriesRPC` |
| 5 | `handlePropose` — peer fan-out | who is sent the new entry |
| 6 | `becomeLeader` — `nextIndex`/`matchIndex` initialization | over which member set |
| 7 | `advanceLeaderCommit` — the majority loop | the commit quorum |
| 8 | `internal/node.Node.majority()` and `checkPendingReads` | the read quorum (§4.2a) |

**Row 3 was missing from revisions 1–3, which all said "seven"
(§23/G5).** `handleRequestVoteResponse`'s tally is a distinct function
from `handleElectionTimeout`'s, and it is the *election* quorum — the
one place this plan can least afford an un-migrated `cfg.majority()`.
In practice a missed row cannot ship, because slice 1 **removes**
`Config.Peers` and `Config.majority()` outright, so every unmigrated
site is a compile error rather than a silent stale-quorum bug; that is
the structural backstop, and the enumeration above is the checklist.
Treat this table, not the prose, as authoritative, and re-derive it
against the tree at implementation time rather than trusting the count.

Learner fan-out (`AppendEntriesRPC` sent to `activeConfig.Learners`
too, never `RequestVoteRPC`) is a small addition alongside rows 4 and
5, and explicitly **not** alongside rows 1, 2, 3 or 7 — a learner is
never sent a vote request, never counted in a tally, and never counted
toward commit.

`becomeLeader` additionally initializes `nextIndex`/`matchIndex` for
**every** member of `activeConfig` (voters *and* learners) rather than
for `cfg.Peers`, and `nextIndex` for a member that first appears
mid-term (a learner just added by an `EntryConfig` this leader appended)
is initialized to `lastIndex()+1`, **not** left to Go's zero value:
a zero `nextIndex` is clamped to `1` by the existing
`appendEntriesMessage`, which would then copy this leader's entire
retained log tail into a single message. The existing backoff/
`ConflictIndex` path (unchanged) corrects `nextIndex` downward from
`lastIndex()+1` in the ordinary way, and the existing
`next <= snapshotIndex` branch routes a far-behind learner to
`MsgInstallSnapshotRequest` exactly as it already does for any
far-behind follower.

**Reverting on divergent-suffix repair.** When `handleAppendEntriesRequest`
truncates `c.log` because of a log-matching conflict (existing
mechanism, unchanged), `Core` recomputes `activeConfig`,
`activeConfigIndex` by calling `ConfigAt(c.lastIndex())` on the
repaired log — **the same function, with the same fixed priority
order, that recovery, snapshot creation, compaction, and leader
initialization all call** (§6.3). Revision 1 specified a separate,
subtly different backward-scan here whose "no entry found" fallback
stopped at `snapshotConfig` and therefore disagreed with §6.2's
recovery fallback on a node that has never snapshotted (§23/C2); there
is now exactly one algorithm and no second description of it anywhere
in this document.

**Reverting on crash.** No special handling needed: `activeConfig` is
pure in-memory `Step` state, exactly like `c.log` itself, and is always
recomputed by the identical `ConfigAt` call on every restart (§6.3) —
a crash before an `EntryConfig` entry is durably persisted simply
means it never happened, identical to any other uncommitted,
unpersisted log content (`docs/failure-model.md` §2.1).

### 2.2a The two proposal gates (`LEADER-TERM CONFIGURATION GATE`)

Append-time-effective single-server changes are safe **only** under
premises revision 1 left implicit. Both are now explicit, both live in
`Core`, and both are named premises of §2.3's proof.

**Premise P1 — leader-term commit gate.** `Core.ProposeConfigChange`
refuses unless

```
c.termAt(c.commitIndex) == c.currentTerm
```

i.e. this leader has already **committed an entry of its own current
term**. Rationale, stated as the property it actually buys: under
Raft's current-term commit rule (`docs/raft.md` §4,
`advanceLeaderCommit`, unchanged), committing any current-term entry
commits this leader's entire log prefix. So at the instant P1 holds,
`activeConfig == ConfigAt(lastIndex())` is a **committed**
configuration — not a speculative one that a future leader might
truncate away. That is precisely what makes every leader's
`activeConfig` a node on one *committed* configuration chain rather
than on a private, divergent branch, which is the hypothesis §2.3's
branch-confinement invariant needs and which revision 1 asserted
without establishing.

**How P1 converges — `proposeElectionNoOp` (existing, `v0.1.0`+).**
ChronicleDB already appends a synthetic current-term entry on every
election: `internal/node.processOutput` calls
`Node.proposeElectionNoOp` on `Output.BecameLeader`, submitting an
empty-mutation `CommitTxnCommand` whose `RequestID` is derived from
this node's ID and new term (`internal/node/node.go`; added in Phase 8
as the `BeginReadIndex` liveness fix — see that function's doc
comment, which already explains the current-term commit rule this gate
depends on). That entry commits under the *old* configuration's
majority in the ordinary way, after which `termAt(commitIndex) ==
currentTerm` holds and stays true for the rest of the term. P1 is
therefore satisfied within one normal replication round of every
election on a healthy cluster, and **is not satisfied** exactly when
the leader cannot reach a majority — which is exactly when it must
not be starting a reconfiguration anyway. `proposeElectionNoOp` is
thus promoted from "a liveness fix for ReadIndex" to "a documented
safety dependency of dynamic membership," and §17's new invariant
names it as such so it can never be removed as dead weight.

**`termAt` at the snapshot boundary — existing behavior, stated
because P1 depends on it (§23/F8).** `Core.termAt`
(`internal/raft/core.go`, **unchanged by this phase**) returns the term
of the sentinel entry `{Index: snapshotIndex, Term: snapshotTerm}` when
`i == snapshotIndex`, and `0` when `i < snapshotIndex`. And
`commitIndex >= snapshotIndex` always holds: `NewCoreFromSnapshot`
seeds `commitIndex = snapshotIndex`, and `Compact` refuses any
`uptoIndex > appliedIndex`, which is itself bounded by `commitIndex`.
Therefore `termAt(commitIndex)` is **always defined and never an
error** — at a freshly compacted boundary it is the snapshot term,
which *is* the term of the entry at `commitIndex`. P1 must be
implemented against exactly that behavior and must **not** be
"corrected" into an error return or a panic for `i <= snapshotIndex`:
the `0` return below the boundary is unreachable for this call, and if
a future change made it reachable, P1 would fail closed, which is the
conservative direction. A direct unit test pins
`termAt(snapshotIndex) == snapshotTerm` immediately after `Compact`
(§18).

**Premise P2 — inherited-suffix floor (`pendingConfIndex`).**
`becomeLeader` sets

```go
c.pendingConfIndex = c.lastIndex()   // BEFORE any current-term entry is appended
```

and `ProposeConfigChange` refuses while `c.commitIndex <
c.pendingConfIndex`. This is the conservative etcd-style floor: a new
leader cannot distinguish a committed `EntryConfig` in its inherited
log tail from an uncommitted one, so it treats the whole inherited tail
as possibly-pending until it is committed. P2 is strictly weaker than
P1 (P1 implies `commitIndex == lastIndex()`-at-election once the
election no-op commits, hence P2) and is kept anyway as defense in
depth: it is independently checkable, it is what makes the refusal
comprehensible in a log line ("inherited log tail not yet committed"),
and it keeps the rule correct if a future phase ever changes what
`proposeElectionNoOp` proposes.

**Premise P3 — local serialization (revision 1's rule, retained).**
`ProposeConfigChange` refuses while `c.activeConfigIndex >
c.commitIndex` — a change this leader itself appended has not yet
committed. Retained because it is the only one of the three that
covers a *second* proposal inside a single, stable leadership term.

All three are `Core`-level, all three are checked in
`ProposeConfigChange` before any entry is created, and **none of them
is ever re-checked by a follower** — they are leader-only proposal
preconditions about this leader's own knowledge, structurally distinct
from §2.6's four-shape structural validation, which *every* replica
re-verifies. A follower has no way to evaluate P1/P2/P3 for a remote
leader and must not try.

### 2.3 Proof: quorum intersection, the configuration window, and split-brain prevention

Revision 1 proved the pairwise arithmetic (Lemma 1 below) correctly and
then asserted, without proof, that it extends "inductively across the
whole serialized sequence." That asserted step is the entire safety
argument, and it is **false without P1**. This section proves it with
P1/P2/P3 as explicit premises, and reproduces the counterexample that
defeats the revision-1 form.

#### Definitions

- The **configuration chain** is the sequence `C_0, C_1, C_2, …` where
  `C_0` is the bootstrap (or restored) configuration and each `C_{k+1}`
  is obtained from `C_k` by exactly one of §2.6's four transition
  shapes. Adjacent chain members differ by at most one voter.
- A configuration is **live at instant `t`** if some node's
  `activeConfig` equals it at `t`, or some in-flight message was
  generated under it.
- A live configuration is **decisive** at instant `t` if some node
  could still, from `t` onward, win an election or advance
  `commitIndex` while holding it. A live configuration that is not
  decisive is **superseded**: it still sits in some node's
  `activeConfig` and may sit there indefinitely, but no majority of it
  will ever again be assembled.
- The **branch-confinement invariant**, in two mechanically checkable
  parts. This **replaces revision 2's "window invariant (W)"**, which
  claimed at most two live configurations, adjacent — a claim that is
  false as literally stated, in revision 2's own repaired DM-12
  schedule, and that would make any oracle asserting it raise false
  alarms (§23/F4; the worked counterexample is below).
  - **(W1) Committed-chain linearity**: the set of *committed*
    configurations is totally ordered by the index of the `EntryConfig`
    entry that established each, forms exactly one chain (no two
    committed configurations are siblings), and every adjacent pair on
    that chain differs by at most one voter.
  - **(W2) Uncommitted-branch confinement**: every live-but-uncommitted
    configuration is a single-shape (§2.6) child of the newest
    committed configuration present in the log of the node that holds
    it.

  Both parts are statements about durable logs, so both are directly
  checkable by a harness oracle that reads those logs itself — which
  is exactly what DM-10's lineage oracle must do (§15).

#### Lemma 1 (pairwise intersection) — unchanged from revision 1

For any two `Configuration` values `C_old` and `C_new` differing by the
addition or removal of exactly one voter, every majority of
`C_old.Voters` intersects every majority of `C_new.Voters`.

*Proof.* Let `C_old` have `n` voters, majority `⌊n/2⌋+1`. Take the add
case (`n+1` voters, majority `⌊(n+1)/2⌋+1`); removal is symmetric with
`n-1` in place of `n+1`. A `C_old` majority excludes at most
`⌊(n-1)/2⌋` of the `n` original voters. A `C_new` majority of `n+1`
members excludes exactly `(n+1) - (⌊(n+1)/2⌋+1) = ⌈(n-1)/2⌉` members
out of `n+1` total — of which at most one exclusion can be the newly
added voter, so at least `⌈(n-1)/2⌉ - 1` of the exclusions fall among
the original `n`. Summing the two exclusion counts against the original
`n`: `⌊(n-1)/2⌋ + (⌈(n-1)/2⌉ - 1) < n` for every `n ≥ 1`, so the two
exclusion sets cannot jointly cover all `n` original voters and the two
majorities must share at least one original voter. ∎

Lemma 1 places **no parity requirement on `n`** and holds for every
`n ≥ 1`; §12's worked 3→4, 4→3, 3→2, 2→3 table is Lemma 1 instantiated,
and the add-then-remove / remove-then-add sequences are Lemma 1 applied
twice along the chain — *provided* the two configurations compared are
adjacent, which is what W1/W2 plus Lemma 3 deliver, and which is the
whole point.

#### Lemma 2 (a proposing leader's `activeConfig` is committed)

If P1 holds on leader `L` at the instant `L` appends an `EntryConfig`
entry, then `L`'s `activeConfig` immediately before that append is a
**committed** configuration.

*Proof.* P1 says `termAt(commitIndex) == currentTerm`. By Raft's
current-term commit rule (unchanged; `advanceLeaderCommit` only ever
advances `commitIndex` to an index whose entry is in `currentTerm`, and
`commitIndex` is a prefix bound), every entry at index `≤ commitIndex`
is committed, and by `LEADER COMPLETENESS` is present in every future
leader's log. P2 gives `commitIndex ≥ pendingConfIndex =
lastIndex()`-at-election, and P3 gives `activeConfigIndex ≤
commitIndex`. Hence the `EntryConfig` entry that produced `L`'s
`activeConfig` (or the snapshot/bootstrap boundary, if
`activeConfigIndex == 0`) is itself committed. ∎

#### Lemma 3 (superseded-branch exclusion) — new in revision 3, completed in revision 4, Case 1's bound corrected in revision 5

Two live configurations that are **not** adjacent are never both
decisive: at least one is superseded, and a superseded configuration
can never again assemble a majority.

**What revision 3 got wrong, and why it needed a case it did not have
(§23/G3).** Revision 3 proved this from a single reduction: "if they
were children of *different* committed elements … non-adjacency forces
them to be two distinct children of **one** common committed ancestor."
That reduction is **false**, and the counterexample lives inside this
document's own rules. Take a committed chain
`C_1 = {a,b,c} → C_2 = {a,b,c,d} → C_3 = {a,b,c,d,e}`, a node
partitioned since before `C_2` committed still holding the uncommitted
child `C = {a,b,c,x}` of `C_1` (W2-legal **relative to that node's own
log**, which is how W2 is stated), and `C' = {a,b,c,d,e,f}`, a child of
`C_3`, live on the majority side. `C` and `C'` are five voters apart,
are non-adjacent, and are children of *different* committed elements —
so revision 3's proof covered nothing at all here, and the state is
reachable (it is DM-22's shape with two further committed
transitions). The conclusion still holds; it needs the case analysis
below, which turns on `isLogUpToDate` rejecting **by index** as well as
by term.

*Proof.* Define a live configuration's **anchor** as the newest
*committed* configuration present in the log of the node holding it. By
W2 every live configuration is its holder's anchor or a single-shape
child of that anchor, and by W1 all anchors lie on one committed
chain. Let `C` and `C'` be live and non-adjacent, with anchors `C_a`
and `C_b`; order them so `C_a` is at or before `C_b` on that chain.

**Case 1 — the anchors coincide (`C_a = C_b`).** Neither `C` nor `C'`
equals `C_a` (they are non-adjacent, and each is `C_a` or adjacent to
it), so they are two **distinct children** of one common committed
ancestor, produced by two different leaders `L` and `L'`: P3 forbids
one leader appending a second `EntryConfig` above an uncommitted one,
and `RAFT ELECTION SAFETY` (unchanged) forbids two leaders sharing a
term. Say `term(L) < term(L')`.

By P1, `L'` appended its entry only after committing an entry of its
own term `term(L')`. That commit required a majority of `L'`'s
`activeConfig` at that instant, which by W2 was `C_a`. So **a majority
of `C_a` durably holds an entry of term `term(L')`.**

Now take any majority `M` of `C`. `C` and `C_a` are adjacent (W2), so
by Lemma 1 `M` intersects every majority of `C_a` — in particular the
one holding that term-`term(L')` entry. So **`M` contains a node whose
`lastLogTerm >= term(L')`.**

**The candidate-side bound is `< term(L')`, not `<= term(L)`, and
revision 4 stated the wrong one (§23/H1).** Revision 4 wrote that a
candidate holding `C` "carries a log whose last entry is from a term
`<= term(L)`", justified by "no leader of a later term extended that
branch (a later leader extending it would have had `C` as *its* own
anchor, which makes `C` committed)". That justification conflates a
node's **anchor** (its newest *committed* configuration) with its
**`activeConfig`** (which is `ConfigAt(lastIndex())` and may rest on an
uncommitted entry — §2.2, §6.3). A leader elected on `C`'s branch holds
`C` as its `activeConfig` while its anchor stays `C_a` and `C` stays
uncommitted, so that branch can legitimately be extended with entries
of a term **above** `term(L)`. The state is reachable under this
document's own gates, because P1/P2/P3 gate `EntryConfig` appends only
— never elections, never `EntryNormal` appends:

| # | Event |
|---|---|
| 1 | `C_a = {a,b,c,d}` is committed; `a` leads term 1 and appends idx `k` `Remove(d)` → `C = {a,b,c}`, persisted locally, replicated to nobody. `a` crashes. |
| 2 | `a` restarts as a **Follower** with idx `k` durable, so its `activeConfig` is `C` (§6.3 step 1) and it is a Voter in it. Its election timer fires; it campaigns at term 2 needing 2 of `{a,b,c}`; `b`'s log ends at `k-1`, so `b` grants (`isLogUpToDate`). `a` wins and appends a term-2 no-op at `k+1`. **`C`'s branch now carries term 2 > `term(L) = 1`.** `a` is then partitioned. |
| 3 | `c` campaigns at term 3 with `activeConfig = C_a` (it never saw idx `k`), wins with `{b,c,d}`, commits its election no-op under `C_a` (P1 ✓, P2 ✓, P3 ✓), then appends `C' = Remove(a) = {b,c,d}`. |

`C` and `C'` are live, non-adjacent, and share the anchor `C_a`, so this
is Case 1 — and `a`'s last log term is `2`, not `term(L) = 1`. W2 holds
at every step (`C` is a single-shape child of the newest committed
configuration in `a`'s log), so nothing earlier in this section excludes
the schedule. The lemma's **conclusion** is unaffected; only this step's
bound is.

**The corrected step.** *Every candidate holding `C` has
`lastLogTerm < term(L')`.* Every entry above idx `k` in such a
candidate's log was appended by a leader that itself held `C` at that
moment: the entry sits above `C`'s establishing entry with no
configuration-establishing entry in between (otherwise the candidate's
`activeConfig` would not be `C`, §6.3 step 1), and by log matching that
leader held the same prefix. So the candidate's `lastLogTerm` is
`term(L'')` for some leader `L''` that held `C`, with `L'' = L` when the
branch was never extended, and it suffices to show `term(L'') <
term(L')` for every such `L''`. Suppose not; `term(L'') = term(L')` is
impossible (`RAFT ELECTION SAFETY`: two leaders never share a term), so
suppose `term(L'') > term(L')`. Two orderings exhaust the schedule, and
both are contradictory:

- *`L''` was elected before `L'` committed its own term entry.* `L''`
  was elected while holding `C`, so a majority of `C` voted for it and
  advanced `currentTerm` to `term(L'')`. `C` and `C_a` are adjacent, so
  by Lemma 1 **every** majority of `C_a` contains one of those nodes.
  P1 requires `L'` to commit an entry of term `term(L')` under `C_a`
  before appending `C'`, which requires a majority of `C_a` to accept an
  `AppendEntries` of term `term(L') < term(L'')` — and every such
  majority contains a node already at `term(L'')`, which rejects it
  (existing term rule, unchanged). `L'` can therefore never satisfy P1,
  so `C'` is never appended: contradiction.
- *`L'` committed its own term entry first.* Then, by the paragraph
  above, every majority of `C` already contained a node with
  `lastLogTerm >= term(L')`, while `L''`'s own log at candidacy ended on
  `C`'s branch at a term below `term(L')`. `isLogUpToDate` denies it
  every such vote, so `L''` could not have been elected at all:
  contradiction.

Hence `term(L'') < term(L')` in every reachable schedule, and every
candidate holding `C` presents `lastLogTerm < term(L')`.

The existing `isLogUpToDate` rule compares last-log **term before
index** (`internal/raft/core.go`, unchanged), so every node in `M`
rejects every such candidate **regardless of how long its log is**. `C`
therefore has no assemblable majority: it is superseded. In the worked
schedule above that is exactly what happens — `{b,c,d}` holds term 3,
every 2-subset of `{a,b,c}` meets it, and `a`'s term-4 candidacy
carrying a term-2 tail is denied by term.

**Case 2 — the anchors differ (`C_a` strictly precedes `C_b`).** `C_b`
is an anchor and is therefore committed, and it is a strict descendant
of `C_a`, so `C_a`'s immediate successor `C_{a+1}` exists on the chain,
is committed, and was established by an entry at index `k`, term `t`.
Two sub-cases exhaust `C`:

- **2a — `C` is a single-shape child of `C_a`.** Then `C ≠ C_{a+1}`: a
  child of `C_a` equal to `C_{a+1}` would be committed, hence its own
  holder's anchor, contradicting that the anchor is `C_a`. So `C` and
  `C_{a+1}` are two distinct children of `C_a` and **Case 1 applies
  verbatim with `C' := C_{a+1}`**, once we know `term(L) < t`. We do:
  if `term(L) > t` then `L`, having committed an entry of its own term
  (P1) and `commitIndex` being a prefix bound, held the committed entry
  at `(k, t)` in its own log, so every log carrying `C`'s entry carries
  `(k, t)` below it and the newest configuration-establishing entry
  below `C`'s would be `C_{a+1}`'s — making the anchor `C_{a+1}`, not
  `C_a`, a contradiction; and `term(L) = t` is impossible because two
  leaders never share a term. Hence `C` is superseded.
- **2b — `C = C_a` itself** (a *committed* configuration still live on
  a node that has not yet seen `C_{a+1}`). **This is the sub-case
  revision 3 had no argument for, and the term comparison alone does
  not settle it**: a node holding `C_a` may perfectly well hold an
  entry of term `t` — `C_{a+1}`'s proposer's own election no-op, for
  instance — so it is not term-dominated. It is *index*-dominated.
  Every node holding `C_a` necessarily **lacks** the entry at `(k, t)`:
  holding it would make `C_{a+1}`'s entry the newest
  configuration-establishing entry in that node's log and therefore its
  `activeConfig` (§6.3 step 1), so it would hold `C_{a+1}`, not `C_a`.
  `C_{a+1}` is committed, so a majority `M` of `C_{a+1}` durably holds
  `(k, t)` (§2.2: the entry's own configuration governs its own
  commit). `C_a` and `C_{a+1}` are adjacent (W1), so by Lemma 1 every
  majority of `C_a` meets `M` and therefore contains a node with
  `lastLogTerm >= t` whose `lastLogIndex >= k` whenever its term is
  exactly `t`. A candidate holding `C_a` has `lastLogTerm <= t` — an
  entry of a term above `t` could only have been appended by a leader
  of that later term, which by this same argument could not have been
  elected without `(k, t)` — and, when its term is exactly `t`, its
  `lastLogIndex < k`, because it lacks `(k, t)` and logs are gap-free.
  `isLogUpToDate` therefore denies the vote in both branches: **by term
  when the candidate's term is lower, and by index when the terms are
  equal.** `C_a` is superseded.

In every case at least one of the two non-adjacent live configurations
is superseded, which is the statement. ∎

**Note on what this does *not* say.** A committed configuration is not
superseded merely by being old; it is superseded exactly once its
successor on the committed chain exists (Case 2b's hypothesis). While
`C_a` is still the newest committed configuration, `C_a` and any
single-shape child of it are **adjacent**, Lemma 1 applies directly,
and Lemma 3 is not invoked at all. This is why DM-22's three
simultaneously-live configurations are safe without any of them being
superseded on *arrival* — `C1` becomes superseded at the precise
instant `b`'s term-2 no-op commits, by Case 1, and not before.

#### Theorem (split-brain is impossible)

*Proof.* **W1** holds by induction on committed `EntryConfig` entries: a
leader appends one only under P1+P2+P3, so by Lemma 2 its
`activeConfig` is a *committed* configuration, and §2.6 makes the new
entry a single-shape child of it; `LEADER COMPLETENESS` then places
that committed parent in every future leader's log, so no two committed
configurations can be siblings and every adjacent pair on the chain
differs by at most one voter. **W2** holds by construction:
`activeConfig` is always `ConfigAt(lastIndex())` over the holder's own
log (§6.3), and P1/P2/P3 forbid appending a second `EntryConfig` above
an uncommitted one.

Given W1 and W2, take any two configurations live at one instant. If
they are adjacent, Lemma 1 gives intersecting majorities directly, so
two disjoint quorums cannot both act. If they are not adjacent, Lemma 3
says at most one of them is decisive, so again two disjoint quorums
cannot both act. In both cases two conflicting leaders cannot be
elected in one term and two different entries cannot be committed at
one index, and the existing `RAFT ELECTION SAFETY` (vote-once-per-term,
persisted before granting — unchanged) and `QUORUM SAFETY`
(current-term commit rule, unchanged, now evaluated against
`activeConfig.majority()`) arguments carry through verbatim. ∎

#### Why the two-element window is *not* the invariant (revision 2's overstatement)

Revision 2 asserted the stronger claim that at most two configurations
are live at any instant and that they are adjacent, and derived safety
from it. That claim is false, and the counterexample is revision 2's
**own** repaired DM-12 schedule — the branch it describes, correctly,
as safe. Continuing from the trace above, in the branch where `d` *is*
reachable:

| # | Event |
|---|---|
| 1 | `a` appends idx10 `Promote(e)` → `C1 = {a,b,c,d,e}`; delivered to `e` only; uncommitted. `C1` is live on `{a,e}`. |
| 2 | `{a,e}` partitioned from `{b,c,d}`. |
| 3 | `b` wins term 2 and **commits its election no-op under `C0`** (3 of `{a,b,c,d}`). P1 now holds on `b`, legitimately. |
| 4 | `b` appends idx11 `Remove(a)` → `C2 = {b,c,d}`, live on `b`. `c` and `d` still hold `C0`; `a` and `e` still hold `C1`. |

Three configurations are live at once — `C0`, `C1`, `C2` — and
`C1`/`C2` differ by **two** voters with admissible disjoint majorities
(`{a,d,e} ∩ {b,c} = ∅`). Nothing in the design prevents this state, and
nothing should: it is safe, but it is safe by **Lemma 3**, not by any
window. `C1` became superseded the instant `b`'s term-2 no-op
committed, because every majority of `C1` intersects every majority of
`C0` (Lemma 1, they are adjacent) and that intersection now holds a
term-2 entry — so `a`'s term-3 candidacy, carrying only a term-1 tail,
is rejected by every `C1` majority it could possibly ask, however long
`a`'s log is, because term dominates index in `isLogUpToDate`.

**This distinction is load-bearing for the test plan, not only for the
prose.** An oracle asserting "at most two live configurations, and
adjacent" raises a false safety alarm on exactly this schedule, and an
implementing session trusting it would either weaken the oracle
arbitrarily or "fix" a non-bug. DM-10's configuration-lineage oracle
therefore asserts **W1 and W2** — both true here — and never a
two-element live-set window; **DM-22** exists specifically to drive the
cluster into this three-configuration state and assert that the oracle
stays quiet while `committedOracle` stays clean, and that the same
oracle *does* fire on an injected W1 violation (§15).

#### Why branch confinement fails without P1 — the concrete counterexample (scenario DM-12)

Revision 1 enforced serialization only as P3, which is derived from the
proposing leader's **own log**. A newly elected leader whose log does
not contain a predecessor's uncommitted `EntryConfig` sees
`activeConfigIndex ≤ commitIndex` and proposes freely — producing two
changes from a common ancestor, i.e. two live configurations differing
by **two** voters, for which Lemma 1 does not apply. Concretely, with
voters `C0 = {a,b,c,d}` (majority 3) and an already-committed learner
`e`, all logs committed through index 9 in term 1:

| # | Event |
|---|---|
| 1 | Leader `a` appends **idx10 = Promote(e)** → `C1 = {a,b,c,d,e}`, majority 3. Effective on `a` at append. Revision 1's P3 passed (`activeConfigIndex ≤ 9 = commitIndex`). |
| 2 | Replicated to `e` only: count over `C1.Voters` is `{a,e}` = 2 < 3 → **uncommitted**. |
| 3 | `a`+`e` partitioned away from `b,c,d`. |
| 4 | `b,c,d` elect `b` in term 2 under `C0` (3 of 4). `b`'s log lacks idx10, so **`b`'s P3 passes: `b` sees no change in flight.** |
| 5 | `b` appends idx10 = its election no-op (term 2). *Before that no-op commits*, `remove(a)` is accepted: `b` appends **idx11 = Remove(a)** → `C2 = {b,c,d}`, majority 2. |
| 6 | `d` is unreachable. `advanceLeaderCommit` counts over `C2.Voters`: `b=11, c=11` → 2 ≥ 2, `termAt(11) == currentTerm` → **commits idx10 and idx11 on `{b,c}` alone.** The operator is told `remove(a)` is `StatusCommitted`. |
| 7 | The partition shifts; `a` reaches `d` and `e`, and campaigns at term 3 under `C1` (majority 3). `e` grants (logs equal). `d` grants (`a`'s `(10, term 1)` ≥ `d`'s `(9, term 1)`). Votes `{a,d,e}` = 3 → **`a` is leader of term 3.** |
| 8 | `a` appends a term-3 entry, replicates to `d` and `e`; `d` backs up and accepts `a`'s idx10. matchIndex over `C1` = 3 ≥ 3 → **`a` commits index 10 and 11 with its own content.** |

Index 10 is now committed as *both* `no-op(term 2)` (by `{b,c}`) and
`Promote(e)(term 1)` (by `{a,d,e}`); index 11 likewise. `STATE MACHINE
SAFETY` and `LEADER COMPLETENESS` are violated and the operator's
acknowledged `remove a` is silently undone. The cause is exactly a
violation of **W1**: `C1 = {a,b,c,d,e}` and `C2 = {b,c,d}` both
*commit*, as two **siblings** of one common ancestor `C0`, so the
committed configurations no longer form a chain. Being two voters
apart they have **disjoint** majorities — `{a,d,e} ∩ {b,c} = ∅` — and
Lemma 3 cannot rescue it, because neither branch was ever superseded:
no leader committed a current-term entry under `C0` before the second
branch was created, which is precisely the thing P1 now forces. Every
individual transition was a valid §2.6 shape and passed revision 1's
serialization check.

**P1 closes both branches of this trace.** At step 5, `b` must first
commit its term-2 no-op under `C0` (3 of `{a,b,c,d}`); with `d`
unreachable it cannot, so `remove(a)` is refused with
`ErrConfigChangeNotReady` (§2.6a) and nothing enters the log. In the
branch where `d` *is* reachable and the no-op does commit, `d`'s
`lastLogTerm` becomes 2, so at step 7 `d` **rejects** `a`'s term-3
`RequestVote` and `a` gathers only `{a,e}` = 2 < 3 — no conflicting
leader. Note that P2 alone does **not** close this trace (`b`'s
`lastIndex` at election is 9 and everything ≤ 9 is committed, so the
`pendingConfIndex` floor is already satisfied); P1 is the premise that
does the work, and P2 is defense in depth around it.

#### What the mechanism already defeated (traces attempted, and the invariant that stops each)

Recorded so the implementing session knows which hazards are *already*
closed and must stay closed, rather than re-deriving them:

| Attempted violation | Prevented by |
|---|---|
| 3→4, 4→3, 3→2, 2→3 as a single transition losing a committed entry | Lemma 1 plus the existing `isLogUpToDate` rule: old-configuration survivors are always too few to elect, or a survivor holds the entry and rejects every shorter-log candidate |
| A removed node campaigning forever and eventually winning | It can never gather a majority of any live configuration (Lemma 1 + W); §2.7 additionally removes its ability to *disturb* the cluster |
| A learner restarting and self-promoting from stale local state | `activeConfig` is purely `ConfigAt`-derived from durable log/snapshot (§6.3); no code path lets a node write itself into `Voters` |
| A learner holding an **uncommitted** promote entry campaigning and winning | Safe: it is a voter in `C_new`, `C_new` is adjacent to `C_old`, Lemma 1 applies. Surprising but correct — covered explicitly by scenario **DM-15** so it is never mistaken for a bug |
| Promoting a badly-lagging learner as a *safety* problem | Not a safety problem: a short-log voter grants votes liberally, but a behind candidate still needs a majority that includes up-to-date voters. This is why §3.3's `PromotionMaxLagEntries` is correctly an **operational** threshold, not a safety rule |
| Two leaders extending the **same** committed configuration into two different children (a chain *fork*, not a window violation) — the shape revision 2's window claim mis-described | P1 forces the second leader to commit a current-term entry under the common parent first; Lemma 3 then makes the first child superseded, because every majority of it intersects a majority that now holds the newer term and `isLogUpToDate` compares term before index. Reached deliberately, and asserted safe, by **DM-22** |
| A leader committing before its own fsync via `handlePropose`'s optimistic `matchIndex[self]` | `internal/node.processOutput`'s single-goroutine ordering: `ApplyPersistRequest` blocks and recurses into `InputPersistenceComplete` before any peer acknowledgement can be read from a channel. **Not a defect — but it is load-bearing and previously undocumented**, so §17's `LEARNER NON-INTERFERENCE` proof obligations now include an explicit assertion of it, and §15's harness scenarios must preserve the same ordering |


### 2.4 Placement: why `Core`, not `internal/fsm` (rejected alternative)

**Considered and rejected**: encode `AddLearner`/`PromoteToVoter`/
`RemoveServer` as `internal/fsm.ControlCommandMarker` commands, exactly
mirroring `SetClusterVersionCommand`'s existing pattern
(`internal/fsm/clusterversion.go`) — i.e., applied only at
`internal/node.applyCommitted`/`applyControlEntry` time, after commit.

**Rejected because**: `internal/fsm.Apply` (and `applyControlEntry`)
only ever runs *after* an entry commits. But §2.2's append-time-effective
mechanism — the specific property the §2.3 proof depends on — requires
the *new* configuration to govern majority computation **before** the
entry commits, including for deciding the entry's own commit. Deferring
the configuration's effect until FSM-apply time would mean every entry
proposed *after* the config-change entry but *before its own commit*
would still be evaluated under the *old*, stale quorum size — silently
reintroducing exactly the "two simultaneously-valid quorum
definitions" hazard joint consensus exists to solve, for a duration no
smaller than one round-trip. This is the precise, mechanical reason
membership must be `Core`-native rather than FSM-native, distinguishing
it from `SetClusterVersionCommand` (which correctly *is* FSM-native,
because the cluster generation has no effect on quorum computation at
all — it is a pure application-level agreement, unlike membership).

**The idempotency/outcome table, however, does live in `internal/fsm`**
— this is not a contradiction. `Core` owns the *quorum-relevant*
identity of `Configuration`; `internal/fsm` owns the
*RequestID → Outcome* durable idempotency record for membership
change requests, exactly the same role it already plays for every
other command kind, updated via a new, explicitly non-`Apply` method
(`FSM.RecordMembershipOutcome`, §2.6) called deterministically by
`internal/node.applyCommitted` at the same committed index on every
replica — identical determinism guarantee as routing through `Apply`
would give, without requiring `Core`/`internal/raft` to depend upward
on `internal/fsm` (which would invert `docs/architecture.md` §5's
dependency direction: `raft` is lower-level than `fsm`, never the
reverse).

### 2.5 `EntryConfig` payload encoding (raft-native, compatibility-gated)

```
EntryConfig.Data layout (internal/raft, never touches internal/fsm):
  byte[0]      = 0xF0                          // deliberately mirrors fsm.ControlCommandMarker's value — see below for why
  byte[1]      = membershipChangeKind: 1=AddLearner 2=PromoteToVoter 3=RemoveServer
  requestIDLen(4B) requestID(requestIDLen bytes)
  targetIDLen(4B)  targetID(targetIDLen bytes)
  targetAddrLen(4B) targetAddr(targetAddrLen bytes)   // empty for PromoteToVoter/RemoveServer
  fullConfigLen(4B) fullConfig(fullConfigLen bytes)   // the complete resulting Configuration (§1.1), fully self-contained
```

`internal/raft` defines its own local constant equal to `0xF0` — **it
does not import `internal/fsm`** (that would invert the dependency
direction, §2.4). This is a deliberate, documented mirroring, guarded
at `init()` time in *both* packages (mirroring `internal/fsm`'s
existing `ControlCommandMarker`/`commitTxnCommandVersion` non-collision
guard) against ever accidentally diverging.

**Why this specific byte, and why it delivers `NO SILENT FORMAT
MISINTERPRETATION` for free on an old binary**: `Entry.Type` is a
*brand-new* struct field. `internal/transport` gob-encodes `Message`
(which carries `[]Entry`) end-to-end — `encoding/gob` silently drops a
field the decoder's struct does not have (exactly the mechanism
`ADR-0017` already relies on for `Message.SenderGeneration`). A
pre-`v0.5.0` binary's `Entry` struct has no `Type` field at all, so it
decodes every `EntryConfig` entry exactly as if it were an ordinary
`EntryNormal` entry — its `Core` (unmodified) treats it as an opaque
command, replicates and commits it exactly like any other entry (the
*safety* of doing so is a separate question, closed by §8's
generation-gate, which prevents this from happening during ordinary
operation at all), and once committed, its `internal/node.applyCommitted`
(unmodified) dispatches on `fsm.IsControlCommand(payload)` — which
returns true, because `payload[0] == 0xF0` — into
`fsm.DecodeSetClusterVersion` (the only control-command decoder that
binary knows), which reads `byte[1]` as `kind` and finds `1`, `2`, or
`3` — none of which equal `controlKindSetClusterVersion` (`1`)...

**Correction / precise statement**: `membershipChangeKind` value `1`
(`AddLearner`) *does* numerically collide with
`controlKindSetClusterVersion`'s value (`1`) in the scheme above as
first drafted — a genuine encoding hazard this proof-obligation process
is specifically designed to catch before implementation, not after.
**Resolution**: `internal/raft`'s local mirror of the control-kind byte
range starts at `controlKindMembershipChangeBase = 16` (an arbitrary,
generously-spaced offset chosen so `internal/fsm` can add several more
of its *own* new control-kinds in future phases before ever
approaching this range, and vice versa) — i.e. `AddLearner=16`,
`PromoteToVoter=17`, `RemoveServer=18`, and `Voided=19` (§7.6's
restore transform: an `EntryConfig` entry that establishes no
configuration, defined here so the whole reserved range stays in one
place. It is **produced** only by `internal/backup`'s offline restore
transform and by no live code path — but once it exists in a restored
directory's log it is **replicated and accepted exactly like any other
entry**, including by nodes that join that cluster later; producing one
and handling one are different questions, and revision 3 conflated them
(§7.6, §23/G1). Implementation must add a
cross-package non-collision test (`TestControlKindRangesNeverCollide`,
one of §18's proof obligations) asserting
`internal/fsm`'s and `internal/raft`'s reserved control-kind byte
ranges are disjoint, at both `init()` time (defense in depth, mirroring
the existing `ControlCommandMarker` guard) and as an explicit test —
this is exactly the kind of easy-to-miss integer collision this
document's own instruction to "reconcile the plan with the actual
existing architecture" exists to surface before, not after,
implementation.

With that correction, an old binary hitting a `RemoveServer` (`18`)
entry decodes `kind=18`, which its `DecodeSetClusterVersion` (which
only recognizes `kind==1`) rejects as `ErrUnknownControlCommand` →
`applyControlEntry` → `Node.fail` → that node's own event loop halts
and its process exits — `NO SILENT FORMAT MISINTERPRETATION`, proven
by the identical mechanism `ADR-0017` already proved for
`SetClusterVersionCommand`, with zero code change required on the old
binary. This is **defense in depth**, not the primary safety
mechanism — the primary mechanism is that membership operations are
refused at the admin-API layer before ever being proposed unless the
cluster's agreed generation already permits them (§8).

On a **new** (`v0.5.0`+) binary, `internal/node.applyCommitted` checks
`entry.Type` *first*, before ever consulting `fsm.IsControlCommand`:
an `EntryConfig` entry is never handed to `internal/fsm.Apply` or
`applyControlEntry` for its *quorum* effect (that already happened at
append time, inside `Core`, per §2.2) — the only work left at commit
time is (a) recording the deterministic outcome via
`FSM.RecordMembershipOutcome` (§2.6, §10) and (b) updating
`internal/transport`'s dial-address table (§1.8) and resolving any
local admin-API waiter (§9).

### 2.6 The four legal transition shapes, validated deterministically

`Core.ProposeConfigChange` (leader-only entry point, analogous to
`handlePropose`) and every replica's acceptance path both validate a
proposed/received `Configuration` transition against exactly one of:

1. **AddLearner**: `C_new.Voters == C_old.Voters` and `C_new.Learners
   == C_old.Learners + {one new Member}` whose `ID` is not already
   present anywhere in `C_old`.
2. **PromoteToVoter**: `C_new.Voters == C_old.Voters + {one Member
   moved from C_old.Learners}`, `C_new.Learners == C_old.Learners -
   {that Member}` — voter count changes by exactly one, learner count
   changes by exactly one, no `Member`'s `ID`/`Address` pair changes.
3. **RemoveServer (voter)**: `C_new.Voters == C_old.Voters - {one
   Member}`, `C_new.Learners == C_old.Learners`, and
   `len(C_new.Voters) >= 1` (`MINIMUM VOTER INVARIANT`, §17 — removing
   the last voter is refused deterministically, never attempted).
4. **RemoveServer (learner)**: `C_new.Learners == C_old.Learners - {one
   Member}`, `C_new.Voters == C_old.Voters` (no quorum effect at all;
   included here for completeness since `/admin/membership/remove`
   accepts either a voter or a learner target).

Any proposed `Configuration` that is not exactly one of these four
shapes relative to the replica's own current `activeConfig` is
rejected: at propose time by the leader (returned synchronously,
before ever entering the log — no wasted entry, no ambiguous outcome);
and, as **defense in depth**, at accept time by every follower too (a
follower that ever receives a structurally-invalid `EntryConfig` entry
refuses the append and fails closed — `Node.fail`-equivalent — since
this can only mean a leader-side bug or a corrupted/adversarial
message, not a legitimate state a correct leader would ever produce;
this is the same fail-closed-on-should-never-happen posture
`RECOVERY NON-INVENTION` already takes elsewhere).

**The one carve-out, and it is not an exception to the shape rule
(§23/G1).** A `Voided` `EntryConfig` (kind `19`, §2.5, §7.6) carries
**no `Configuration` at all** — `fullConfigLen == 0` — so there is no
`C_new` to compare against `C_old` and the four-shape check simply has
no input to run on. It is therefore neither accepted nor rejected *by
this check*: the check is not reached. A replica appends a voided entry
on the ordinary replication path exactly as it appends an
`EntryNormal` entry, and this is **required**, not tolerated — a node
joining a restored cluster receives voided entries from a live leader
as a matter of course (§7.6, §7.2, DM-21 step 6), and no receiver can
distinguish that legitimate case from a hypothetical illegitimate one.
The structural guarantee lives on the emit side instead, where it can:
`ProposeConfigChange` admits only the four shapes and can never
construct a voided entry, pinned by a debug-build assertion (§7.6).

`FSM.RecordMembershipOutcome(requestID fsm.RequestID, outcome
fsm.Outcome)` is a new, small, `Apply`-adjacent method (same locking
discipline as `ApplySetClusterVersion`, same idempotency-first check —
a duplicate `RequestID` is a no-op returning the previously recorded
outcome) called by `internal/node.applyCommitted` for every committed
`EntryConfig` entry, deterministically, on every replica, at the exact
committed index — giving `REQUEST OUTCOME STABILITY` for membership
requests the identical guarantee every other command kind already has,
without requiring `Core` itself to own idempotency semantics (an
application-boundary concept it should not own — see §2.4).

### 2.6a `Core.ProposeConfigChange` preconditions, in order, with exact errors

The complete, ordered precondition list. Every check is synchronous and
returns **before any entry is created** — a refused membership change
never occupies a log index, never has an ambiguous outcome, and never
records anything in the idempotency table (§10), so the operator's
`RequestID` remains entirely unused and safe to reuse verbatim.

| # | Check | Error returned | HTTP | Retry semantics |
|---|---|---|---|---|
| 1 | This node is Leader | `raft.ErrNotLeader` (existing `ProposalRejected`/`LeaderHint` shape) | `409` + `leaderHint` | Retry against the hinted leader, same `RequestID`. |
| 2 | **P2**: `commitIndex >= pendingConfIndex` (§2.2a) | `ErrConfigChangeNotReady` (reason: `inherited-suffix-uncommitted`) | `503` + `Retry-After: 1` | **Automatically retryable, same `RequestID`.** Converges as soon as this leader's inherited tail commits. |
| 3 | **P1**: `termAt(commitIndex) == currentTerm` (§2.2a) | `ErrConfigChangeNotReady` (reason: `no-current-term-commit`) | `503` + `Retry-After: 1` | **Automatically retryable, same `RequestID`.** Converges within one replication round of `proposeElectionNoOp` committing (§2.2a); does *not* converge while this leader cannot reach a majority, which is the correct outcome. |
| 4 | **P3**: `activeConfigIndex <= commitIndex` (§2.3, §11) | `ErrConfigChangeInProgress` | `409` | **Operator-visible, not auto-retried**: a different change is genuinely outstanding. Poll `/admin/membership/status` until `inProgress` clears (§9). |
| 5 | The transition is exactly one of §2.6's four shapes relative to `activeConfig` | `ErrInvalidTransition` (naming which shape check failed) | `400` | Not retryable; the request is wrong. |
| 6 | `MINIMUM VOTER INVARIANT`: `len(C_new.Voters) >= 1` | `ErrLastVoterRemoval` | `400` | Not retryable. Pure Raft safety — never operator-overridable (§12.2). |

Checks 1–6 are `Core`'s. Two further checks live **above** `Core`, in
`internal/node`, and are deliberately not `Core`'s business because
they are not quorum-safety conditions:

| # | Check | Error | HTTP | Notes |
|---|---|---|---|---|
| 7 | Cluster generation `>= 2` (§8.2) | `ErrMembershipNotPermitted` | `412` | Fail-closed compatibility gate; see §8.2 for exactly when it starts passing. |
| 8 | Sub-three-voter operator confirmation (§12.2) | `ErrConfirmationRequired` | `409` | Operator **policy**, deliberately separate from quorum mathematics; see §12.2 for the exact field and its idempotency interaction. |

Checks 2 and 3 (`ErrConfigChangeNotReady`) are the only ones of *this*
table the admin API itself may retry on the operator's behalf, and it
does so for a bounded window only (the request context's deadline,
mirroring `FinalizeUpgrade`'s existing await discipline) before
returning `503` to the caller. Because nothing was proposed, a
client-side retry with the same `RequestID` is always safe and is the
documented operator action.

**One check outside this table is retryable on exactly the same
terms**: §3.3's `ErrLearnerNotCaughtUp` / `425`, which is a
time-of-check/time-of-use refusal against a moving `LastIndex()` rather
than a statement that the request is wrong (§23/G4). It proposes
nothing, records nothing, and leaves the `RequestID` untouched, so it
satisfies the identical precondition that makes 2 and 3 safely
retryable. The complete set of admin-layer-retryable refusals is
therefore exactly three: `ErrConfigChangeNotReady`
(`inherited-suffix-uncommitted`), `ErrConfigChangeNotReady`
(`no-current-term-commit`), and `ErrLearnerNotCaughtUp`. Every other
refusal in this table is terminal for that submission.

### 2.7 Stale- and non-member message handling (replaces revision 1's over-broad rule)

Revision 1 specified a single blanket rule — drop *every* inbound
`Message` (requests **and** responses, `AppendEntries`,
`InstallSnapshot`, `RequestVote` alike) whose `From` is absent from the
receiver's own `activeConfig`. The architecture review demonstrated
that this is both **broader than the problem it solves** and
**actively harmful**, so it is replaced here by two narrower rules.

#### What actually needed solving

`v0.5.0` introduces, for the first time, a node that can be
legitimately removed while still running and still network-reachable.
Absent any new mechanism, such a node retrying elections with an
ever-increasing term would force every real member to step down on
each higher-term message (existing `stepDownTo` behavior, unconditional
today), causing indefinite, needless re-elections. That is a genuine
liveness degradation this phase introduces, and it must be closed.
Nothing beyond it needed closing.

#### Why revision 1's rule was wrong

Dropping `AppendEntriesRPC`/`MsgInstallSnapshotRequest` on a membership
basis breaks **legitimate transition traffic**, because a receiver's
membership view is *expected* to be temporarily ahead of or behind the
sender's during any valid transition. The concrete trace (scenario
**DM-14**): leader `a` appends `Remove(a)` (self-removal, §4.2) and
replicates it to follower `b`. `b` adopts `C_new ∌ a` at append time —
correctly, per §2.2 — and from that instant drops **every** subsequent
message from `a`, including:

- the `AppendEntriesRPC` carrying the `LeaderCommit` that would tell
  `b` the entry committed;
- the `AppendEntriesRPC` that would **repair `b`'s divergent suffix**
  if the change is abandoned and must be truncated away;
- heartbeats, so `b`'s election timer fires and `b` campaigns with an
  ever-increasing term — reproducing, from a *legitimate current
  member*, exactly the disruption the rule was introduced to prevent.

A leader is not required to be a member of the configuration whose
commit decisions it brokers (§4.2), so "the sender is not in my
configuration" is simply not evidence that the sender is illegitimate.

#### The replacement: two narrow rules

**Rule 1 — `MEMBERSHIP-SCOPED VOTE ACCEPTANCE` (election traffic only).**
Applied in `handleRequestVoteRequest`, before any term/log state is
touched:

- If `msg.From` is not a **Voter** in the receiver's own
  `activeConfig`, the `RequestVoteRequest` is dropped outright — no
  term bump, no vote decision, no reply. (A Learner is likewise never
  a legitimate candidate, so this one condition covers both "removed
  node" and "learner campaigning.")
- If the **receiving** node's own `ID` is not a Voter in its own
  `activeConfig`, it denies (never grants) the vote — a Learner, or a
  node that has observed its own removal, is not entitled to
  participate in leader election at all. This rule is **retained
  verbatim from revision 1**; it was correct and remains correct.
- Symmetrically, a node whose own `ID` is not a Voter in its own
  `activeConfig` never starts an election: `handleElectionTimeout`
  returns an empty `Output` for it. This is the concrete implementation
  of §1.3's "a Learner's own election timeout is a no-op," which
  revision 1 stated as prose without naming the guard;
  `handleElectionTimeout` today campaigns unconditionally unless
  already Leader, so this is a real code change with its own unit test
  (§18).

**Rule 2 — leader-contact suppression (Raft §4.2.3, applied to
`RequestVote` only).** A node that has accepted an
`AppendEntriesRPC`/`MsgInstallSnapshotRequest` from the leader of its
current term, and whose election timer has not expired since, ignores
any `RequestVoteRequest` — **including one carrying a higher term** —
without bumping its own term, granting anything, or replying. This is
the standard Raft treatment of disruptive servers, and Rule 2 is what
makes a removed node harmless even to a member whose own
`activeConfig` still contains it (the case Rule 1 cannot cover,
because such a receiver has no way to know the sender was removed).

**Ownership, stated against the actual code (§23/F6).** Revision 2
claimed "`Core` already tracks an election-timer deadline as tick state
driven by `Output.ResetElectionTimer`/`ElectionTimeoutTicks`."
**It does not, and there is no such state to extend.** The election
clock lives entirely in `internal/node`: `Node.electionArmed` and
`Node.electionTicksLeft` (`internal/node/node.go`), decremented once
per tick on the event loop, re-armed from
`Output.ResetElectionTimer`/`Output.ElectionTimeoutTicks` in
`processOutput`, and pausable through the existing
`electionTicksPaused` test hook. `Core` receives **no ticks at all** —
only the discrete `InputElectionTimeout` event the driver raises when
that counter reaches zero (`Core.Step`'s input kinds, unchanged). A
`ticksSinceLeaderContact` counter "incremented on
`InputElectionTimeout`-adjacent ticks" is therefore not a small
addition to something existing; it is unimplementable without either a
new tick input into `Core` or a second clock duplicating the driver's.
Revision 3 adds neither.

**Resolution — split the rule along the ownership line that already
exists.**

- **`internal/node` keeps owning the clock.** No change to
  `electionArmed`/`electionTicksLeft`/`electionTicksPaused`, and no new
  `Input` kind. The driver's existing behavior *is* the timeout
  threshold.
- **`Core` keeps owning the vote decision**, and gains exactly one new
  boolean, `heardFromLeader` (§2.2), derived from events `Core`
  already receives:
  - **set** at the end of `handleAppendEntriesRequest` /
    `handleInstallSnapshotRequest` whenever the message is accepted
    from the leader of this node's current term — the same point that
    already sets `out.ResetElectionTimer`;
  - **cleared** at the top of `handleElectionTimeout`;
  - **cleared** on any step-down to a higher term.
- **`handleRequestVoteRequest` consults it** as its first check after
  Rule 1: if `heardFromLeader` is set, return an empty `Output` — no
  term bump, no vote decision, no reply.

`heardFromLeader == true` means "a leader message has arrived since
this node's election timer last expired," which is at least one
minimum election timeout of leader contact and is therefore a
conservative, correct implementation of Raft §4.2.3's threshold. It
needs no tick counter, no jitter arithmetic and no second clock, and it
cannot drift from the driver's timer because the same two events drive
both. The alternative placement — filtering `RequestVote` in
`internal/node` before `Core.Step` — is **rejected**: it would put a
Raft vote-eligibility decision outside `Core`, where
`internal/fault`'s deterministic harness (which drives `Core` directly)
could not exercise it, and §15's DM-9/DM-14 both require it to be
exercised there.

**One interaction an implementer must not get wrong.** Rule 1's third
clause makes `handleElectionTimeout` return an empty `Output` for a
non-Voter, which means no `ResetElectionTimer`, which means the driver
leaves `electionArmed == false` until the next leader message re-arms
it (`internal/node.processOutput`, existing behavior). That is correct
and deliberate for a Learner or a `selfRemoved()` node — it must never
campaign — but it means `InputElectionTimeout` stops arriving for such
a node, so its `heardFromLeader` is then cleared only by the step-down
path. This can never affect a decision, because Rule 1's second clause
already makes a non-Voter deny every vote unconditionally, ahead of
any `heardFromLeader` test. The interaction is asserted by a direct
unit test (§18) so it is pinned rather than rediscovered as a
"bug".

**A second interaction, with an existing test hook (§23/G9).**
`heardFromLeader` is cleared by exactly two events, and one of them —
`InputElectionTimeout` — is produced by the driver's election clock,
which this project already has a test-only freeze for:
`Node.PauseTicksForTest`/`electionTicksPaused`
(`internal/node/node.go`), the mechanism a pre-`v0.1.0` CI flake was
fixed with, deliberately freezing **only** the election side of the
clock and never heartbeats. While that freeze is engaged,
`electionTicksLeft` stops decrementing, `InputElectionTimeout` never
arrives, and a node's `heardFromLeader` therefore stays `true`
indefinitely once any leader message has been accepted — so that node
ignores **every** `RequestVoteRequest`, including higher-term ones, for
as long as the freeze lasts. That is the correct and intended
composition of the two mechanisms (a frozen election clock means "this
node's leader contact never goes stale"), but it is a real behavioural
change for any existing or new test that freezes ticks on one node and
expects another node's election to succeed. Implementation must:
- state this in `Core.heardFromLeader`'s doc comment and in
  `PauseTicksForTest`'s, on both sides of the coupling, so neither can
  be changed in ignorance of the other;
- add a direct unit test asserting the composition explicitly (freeze
  ticks, deliver a leader `AppendEntries`, then deliver a higher-term
  `RequestVote` and assert it is ignored with no term bump), so the
  behaviour is pinned rather than discovered as a hang;
- audit the existing tests that call `PauseTicksForTest` when slice 1
  lands, and where one needs a vote to be granted under a frozen clock,
  resume ticks (or step `InputElectionTimeout` explicitly) rather than
  weakening Rule 2.
This is scoped as an implementation-time obligation, not a design
change: no production path freezes ticks, so `heardFromLeader` can
never go stale on a Voter in a running cluster (a Voter's
`handleElectionTimeout` always re-arms the timer, so the flag is
cleared at least once per election timeout), and a Leader always
reaches leadership *through* `handleElectionTimeout`, which clears the
flag on the way.

**Explicitly NOT filtered, at any layer**: `AppendEntriesRequest`,
`MsgInstallSnapshotRequest`, and every `*Response` message. These are
processed exactly as they are today, by term and log state alone. A
receiver whose membership view is temporarily behind or ahead accepts
leader replication normally and converges by ordinary log repair —
which is the *only* mechanism that can fix a divergent membership view
in the first place, so filtering it can never be right.

#### Why dropping the blanket rule costs nothing in safety

Safety was never provided by revision 1's rule; it comes from Lemma 1 +
W (§2.3). A removed or stale node cannot gather a majority of any live
configuration, cannot commit anything, and cannot win an election —
with or without message filtering. Rules 1 and 2 are **liveness**
mechanisms, and §17 classifies them as such. §13.3's layered-defense
argument is unchanged in substance: membership (Rule 1) and
leader-contact recency (Rule 2) both bound a removed node's
disruption at the Raft layer, and certificate revocation remains the
operator-driven transport-layer defense (`v0.2.0`, unchanged).


## 3. Add node (complete lifecycle)

1. **Admin request**: operator calls `POST /admin/membership/add`
   with `{requestId, nodeId, address}` against the current leader
   (§9). The new physical process is already running, already has a
   valid mTLS identity certificate for `nodeId` (§13), and is listening
   on `address`, but is *not yet* in any `Configuration` — it currently
   sits idle, its own `Core` never constructed with a `Configuration`
   that includes it (it has no cluster to be a `Core` of yet; see
   §3.1's "does the new process even run `Core.Step` before joining"
   resolution below).
2. **Validation** (leader-side, before proposing — §2.6's four-shapes
   check plus): `nodeId` not already a Voter or Learner;
   `SERIALIZED MEMBERSHIP CHANGE` (`activeConfigIndex <= commitIndex`,
   §2.3); cluster generation `>= 2` (§8's gate); RBAC `admin` (§13);
   idempotency pre-check against `FSM`'s membership outcome table
   (§10) — a duplicate `requestId` short-circuits to the recorded
   outcome without re-proposing.
3. **Propose + commit**: leader calls `Core.ProposeConfigChange`
   (AddLearner shape); the resulting `EntryConfig` entry replicates and
   commits through the ordinary Raft log/quorum mechanism (§2.2), using
   the **old** (pre-add) `Configuration`'s majority to commit itself —
   the new learner is not yet a voter, so it contributes nothing to
   nor detracts from this commit decision (`LEARNER NON-INTERFERENCE`).
4. **The new process joins the replication stream**: the instant the
   entry *appends* (before it even commits) on the leader, the leader's
   `activeConfig.Learners` includes the new member, so ordinary
   `AppendEntriesRPC` fan-out (§2.2's extended loops) begins targeting
   it immediately — `internal/transport` dials `address` for the first
   time at this point (§1.8). The new process, on its side, starts as
   a bare `internal/node.Node` with **no** `Config.Bootstrap` at all
   (its own data directory is empty) — it simply begins accepting
   inbound Raft messages from whichever peer(s) it is configured to
   listen from (its own process is started with the *existing*
   cluster's peer addresses so it knows who to expect traffic from/how
   to identify the leader, but forms no opinion about `Configuration`
   itself until it receives one via replication — see §3.1).
5. **Catch-up via replication or snapshot**: standard, entirely
   pre-existing mechanism (`docs/raft.md` §3, §7), completely reused,
   unmodified: if the new learner's required log range still exists in
   the leader's retained log, ordinary `AppendEntriesRPC` backfill
   catches it up; if the leader has already compacted past that point
   (a learner added to a cluster that has been running a while),
   `MsgInstallSnapshotRequest`/`Response` (existing mechanism, §7 below
   for the one extension: the snapshot itself now also carries
   `Configuration`) brings it to the boundary in one shot, followed by
   ordinary replication for anything after.
6. **Eligibility to vote**: **never automatic**. Reaching a caught-up
   state makes the learner *eligible* for promotion (visible via
   `/admin/membership/status`'s lag field, §9, §14) — it does not
   itself trigger anything. This matches
   [`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §8's explicit
   non-goal of automatic membership rebalancing (§20): promotion is
   always a distinct, later, explicit `/admin/membership/promote` call.
7. **Configuration commit** (of the AddLearner entry itself): as in
   step 3 — uses the old configuration's majority, unaffected by the
   new learner's catch-up state, because a learner never counts toward
   anything.
8. **Restart/recovery behavior**: if the new process crashes and
   restarts *before* the AddLearner entry ever committed, nothing is
   lost — it restarts with an empty data directory exactly as before
   and simply re-joins the replication stream once reconnected; the
   in-flight AddLearner proposal, if it hadn't committed yet, either
   still commits (learner absence during catch-up never blocks commit)
   or, if the leader itself also crashed with it uncommitted, is as if
   it never happened (§5's full boundary-by-boundary account). If the
   new process crashes and restarts *after* the AddLearner entry
   committed, it restarts as an ordinary node whose own durable log/
   snapshot (whatever it had received before crashing) already reflects
   its own learner membership (§6) — recovery is completely ordinary,
   no membership-specific special case at all.
9. **Client-visible success point**: `/admin/membership/add`'s HTTP
   response returns once the AddLearner entry's outcome resolves
   (`StatusCommitted`, mirroring `/admin/upgrade/finalize`'s existing
   response-timing convention) — **not** once the learner has caught
   up. Catch-up progress is separately observable via
   `/admin/membership/status` (§9); conflating "the operator's add
   request was durably accepted" with "the new node is fully caught
   up" would be a poor API (the former is a fast, bounded Raft commit;
   the latter can take arbitrarily long for a large state machine)
   — this directly satisfies this document's own instruction that
   "a newly added node must not endanger quorum before it has
   sufficient state," because the promotion gate (§3.3 below), not the
   add response, is what actually protects quorum.

### 3.1 Does a not-yet-added learner process run a `Core` at all?

Yes, from the moment its own process starts — an important, easy-to-miss
point worth resolving precisely. `internal/node.Node` always owns
exactly one `Core`; there is no "pre-Raft" mode. A brand-new node
process intended to join an existing cluster as a learner is started
with an **empty** `Config.Bootstrap`: `Open` detects "empty data
directory, and the operator has *not* passed the existing
`-cluster`/`-peers` bootstrap flags meant only for forming a
*brand-new* cluster" and constructs a `Core` whose `activeConfig` is
the zero-value `Configuration{}` — no voters, no learners, including
not itself.

Such a `Core` is inert with respect to elections (§2.7 Rule 1's third
clause: a node that is not a Voter in its own `activeConfig` never
campaigns) but is fully able to *receive* `AppendEntriesRPC`/
`MsgInstallSnapshotRequest` from the real leader once the `AddLearner`
entry reaches it.

**Under revision 2 this needs no special-case acceptance rule at all.**
Revision 1 required one, because its blanket §2.7 filter would have
made a brand-new node reject the very first `AppendEntriesRPC` that was
about to deliver its own membership to it. Revision 2's §2.7 never
filters `AppendEntriesRPC`/`MsgInstallSnapshotRequest` on a membership
basis, so the ordinary path just works, and the "relaxation" is
**deleted rather than corrected**.

This matters beyond simplicity: revision 1's relaxation was conditioned
on "a node whose own `activeConfig` does not yet include itself," which
is *also* precisely the state of a node that has **observed its own
removal** — so a removed node would have fallen into the
accept-from-anyone case, directly contradicting §4.4/§4.5 (§23/C4).
Wherever this document still needs to distinguish the two states, it
does so by the only condition that actually separates them:

- **Never joined**: `activeConfig` is the zero value —
  `len(Voters) == 0 && len(Learners) == 0`. This node has never
  observed any configuration from any source.
- **Removed**: `activeConfig` is non-empty and does not contain this
  node's own `ID`. This node has observed a real configuration that
  excludes it.

`Core` exposes these as two distinct predicates (`neverJoined()`,
`selfRemoved()`), and no rule in this document is ever written against
"does not include itself" alone.

The instant a never-joined node processes an `EntryConfig` entry that
includes itself, it adopts a real `activeConfig` via the ordinary
`ConfigAt` path (§6.3) and behaves as an ordinary member from then on.
The trust boundary for that first contact is, as it already is today
for `-peers`/`-cluster` at first bootstrap, mTLS at the transport
layer (§13) — not a Raft-level membership check, which by definition
cannot exist before the node has any membership.


### 3.2 Snapshot catch-up when required

Fully covered by §7: the snapshot format gains `Meta.Configuration`
behind `FormatVersion 2` (§7.1's resolved Option B), and the
`MsgInstallSnapshotRequest` path adopts the installed snapshot's own
`Meta.Configuration` as authoritative (§7.2). No new catch-up
*mechanism* — this phase extends an existing one. The routing into
that path is likewise unchanged, and reaches a newly added learner
correctly because §2.2 initializes its `nextIndex` to
`lastIndex()+1` rather than to Go's zero value (§7.3).

### 3.3 Promotion threshold / eligibility

Leader-only, non-replicated precondition, checked synchronously by
`/admin/membership/promote`'s handler (`internal/node.PromoteToVoter`)
**before** ever calling `Core.ProposeConfigChange`:

```go
// internal/node — leader-only config, not part of raft.Config (§2.4:
// this must never be a deterministic, replica-checked condition)
PromotionMaxLagEntries uint64 // default 0: learner's matchIndex must
                               // equal the leader's own last log index
                               // at the moment of the promote call
```

**The default is pinned at `0` by this plan, and is not an
implementation-time choice (§23/F-NB1).** The value stays operator
tunable at runtime, but the shipped default must be `0` — promote only
a learner whose `matchIndex` equals the leader's `LastIndex()` — because
a non-zero default makes §16's real-process promote step
non-deterministic: the promote would succeed while the new voter is
still behind, and "zero failed commits attributable to the membership
change itself" would then depend on how fast that voter catches up
rather than on an assertion.

**`0` alone does not make the gate deterministic, and revision 3's
claim that it does was wrong (§23/G4).** Revision 3 justified the pin
by asserting that with `0` "the gate is a pure function of observable
state (poll `/admin/membership/status` until `lag == 0`, then
promote)." It is not a pure function of anything the caller can hold
still: `lag == 0` is observed by one HTTP request and re-evaluated by
`PromoteToVoter` on the event loop afterwards, and §16 runs a
background `/propose` writer **across the entire test**. Any entry
appended in between advances `LastIndex()` while the learner's
`matchIndex` has not yet caught up, so the promote returns `425`
`ErrLearnerNotCaughtUp` — a plain time-of-check/time-of-use race, and
one that gets *tighter*, not looser, the stricter the threshold is.
Under sustained write load the window in which `matchIndex ==
LastIndex()` holds is only the gap between one write committing and
the next being appended, so a single-shot promote is unreliable by
construction. This is also an operational statement, not only a test
one: with the shipped default, promotion on a continuously written
cluster needs retry or a quiet moment.

**Both halves of the decision are therefore pinned, not just the
number:**

- **The default stays `0`.** It is the conservative, availability-
  protecting value (§12's 3→4 row depends on the new voter acking
  immediately), and loosening it to paper over a TOCTOU would trade a
  real availability guarantee for a test convenience.
- **`425` is explicitly a retryable refusal**, and every caller that
  must succeed retries it. It joins §2.6a's checks 2–3 as the third
  refusal the admin layer may retry on the operator's behalf, under
  the identical discipline: it is a **pre-proposal** refusal (§3.3
  proposes nothing), so nothing is recorded, the `RequestID` is
  untouched, and a retry with the same `RequestID` is always safe
  (§10). The handler retries within the request context's deadline and
  then surfaces `425` to the caller; §9's status-code table carries
  this, and `docs/membership.md`'s runbook states it for operators
  alongside the `503` polling instruction.
- **§16's promote step retries on `425` under the existing bounded-
  polling discipline** (`docs/testing-strategy.md` §4 — bounded
  polling, never a fixed sleep), rather than asserting a single call
  succeeds. The assertion that matters is unchanged and is now
  actually well-founded: the promote eventually succeeds, and when it
  does the new voter's `matchIndex` **was** equal to `LastIndex()` at
  the instant `Core` evaluated it, which is what makes "zero failed
  commits attributable to the membership change itself" an assertion
  about state rather than about timing.

The determinism the pin buys is thus located where it actually holds —
in the *condition `Core` evaluates*, which is exact and observable —
rather than in the caller's ability to hit it first try.

**This threshold is operational, not a safety requirement, and the
distinction is worth stating precisely** because it is easy to assume
otherwise. Promoting a badly-lagging learner is **not** a safety
violation: a short-log voter grants votes liberally, but a behind
candidate still needs a majority of `C_new`, and every majority of
`C_new` intersects every majority of `C_old` (Lemma 1), so
`LEADER COMPLETENESS` is unaffected. The safety requirement for a
promotion is exhausted by "it is a single-server transition of shape 2,
proposed under §2.2a's gates" — which `Core` enforces deterministically
on every replica. `PromotionMaxLagEntries` protects **availability**:
a newly promoted voter that cannot immediately acknowledge raises the
quorum size without raising the number of nodes able to satisfy it,
which can stall commits until it catches up. That is a real and
important operational hazard, and it is exactly why the gate is
leader-only, non-replicated, and operator-tunable — a safety rule could
be none of those three things.

If the learner's current `matchIndex` (tracked by `Core`, read via a
new `Core.MatchIndexOf` call — the accessor already exists,
unmodified) is not within `PromotionMaxLagEntries` of `Core.LastIndex()`,
`PromoteToVoter` returns a distinct, synchronous error (`ErrLearnerNotCaughtUp`,
carrying the current lag) **without ever proposing anything** — no
wasted log entry, no ambiguous outcome, no idempotency-table row, and
the operator's `RequestID` left freshly usable (§10).

A **second** leader-only promotion precondition is added by §8.2a, and
belongs here alongside the first: the target learner's last-known
`SenderGeneration` (existing `Node.peerGenerations` mirror, `v0.4.0`,
unchanged) must be `>=` the cluster's committed generation. A learner
that was legitimately not required to block generation finalization
(§8.2a) must not be able to become a voter while still running a binary
that does not understand the cluster's agreed formats. Unknown counts
as too old. Refusal is `ErrPeerGenerationTooOld` / `412`, again
proposing nothing. This check is deliberately **not** re-validated by followers
at accept time (§2.6's four-shapes check does not include it) — it is
a leader-only **availability** judgment call using non-replicated
`matchIndex` data, exactly the kind of leader-only gate §2.6
distinguishes from the deterministic structural checks every replica
re-verifies.

---

## 4. Remove node (complete lifecycle)

### 4.1 Follower removal (ordinary case)

Operator calls `/admin/membership/remove` against the leader with the
target follower's (voter or learner) `nodeId`. Leader validates (§2.6
shape 3 or 4, `MINIMUM VOTER INVARIANT` for a voter target), proposes,
commits using the **new** (post-removal) configuration's majority
(§2.2/§2.3 — the removed member's own `matchIndex`/vote no longer
counts from the instant of append). Once committed, the removed
member's `ID` no longer appears in anyone's `activeConfig`; ordinary
replication to it simply stops (its dial-address entry is dropped from
`internal/transport`'s active target set, §1.8).

### 4.2 Leader / self removal

The leader may be asked to remove **itself**. Revision 1's account of
this case contained a direct contradiction with §2.2 that would have
produced an unsafe implementation if followed; the corrected account
follows.

**The quorum boundary, stated exactly.** The instant the self-removing
`EntryConfig` entry is **appended**, `activeConfig` becomes `C_new`,
which does **not** contain the leader. From that instant, every
majority calculation on that leader — `advanceLeaderCommit`'s loop
included, and including the commit decision for *this very entry* —
iterates `activeConfig.Voters` and therefore **does not count the
leader's own `matchIndex`**. There is no "one last time" exemption.

Revision 1's §4.2 asserted the opposite ("the leader's own vote/
matchIndex still counts toward this specific commit decision"). That is
wrong and would have been a straight safety bug: in a 3→2 self-removal
(`C_new = {b,c}`, majority 2), counting the leader would let the entry
"commit" on `{leader, b}` — a count of 2 when only **one** of
`C_new`'s two voters actually holds the entry. A subsequent election
among `{b,c}` could then be won by `c`, which never saw the entry, and
a committed entry would be lost. The corrected rule is simply §2.2
applied without exception: **a self-removing leader must collect
acknowledgements from a genuine majority of `C_new`, none of which can
be itself.**

This is not a contradiction of Raft: nothing requires a leader to be a
member of the configuration whose commit decisions it is currently
brokering, only that a majority of *that configuration's* voters
persist the entry. A leader proposing its own removal is, from the
moment of proposing, already "a non-member acting as leader of a
configuration that does not include it," which is a well-defined and
supported state.

**Concrete consequence for availability, stated honestly.** Because the
leader no longer counts itself, self-removal from an `n`-voter cluster
requires `⌊(n-1)/2⌋+1` acknowledgements from the *other* `n-1` voters —
for 3→2 that is **both** remaining voters, and for 2→1 it is the single
remaining voter. If those acknowledgements do not arrive, the entry
simply does not commit and the leader does not step down; the operator
sees the request time out and may retry with the same `RequestID`
(§10). This is strictly safer than revision 1's rule and strictly less
available; the availability cost is the correct trade and is surfaced
to the operator by `/admin/membership/status`'s `inProgress` field and
by §12.2's confirmation requirement for any result below three voters.

**Step-down.** Once the entry **commits** (not merely appends — waiting
for commit specifically avoids needlessly giving up leadership for a
change that might never actually succeed), the leader immediately steps
down to `Follower`: a new `Output.SteppedDown`-style transition, forced
the instant `advanceLeaderCommit` observes a newly committed
`EntryConfig` entry that excludes this node's own `ID` from `Voters`.
The remaining voters then elect a new leader among themselves. From
that moment the node is `selfRemoved()` (§3.1) and behaves per §4.4/§4.6.

**Interaction with §2.7, stated explicitly.** Under revision 1's
blanket filter, followers stopped accepting the self-removing leader's
messages the instant they adopted `C_new`, which made this entire
sequence unreachable (the leader could never learn of the commit and
could never push `LeaderCommit`) — see §2.7's DM-14 trace. Under
revision 2, `AppendEntriesRPC` is never filtered on a membership
basis, so the self-removing leader continues to function normally as
leader right up to the commit-then-step-down boundary above. **This is
the specific reason §2.7 had to be narrowed**, not merely a reason it
was nice to narrow it.

The `/admin/membership/remove` HTTP call itself, having been issued
against the (soon to no longer be) leader, still returns the committed
outcome normally — self-removal is not a special case from the
*caller's* point of view, only from the *node's own* post-commit
behavior.


### 4.2a The exclusion rule applies to **every** quorum this node computes — including ReadIndex

Revision 2 stated the self-exclusion boundary for
`advanceLeaderCommit` and stopped there. That is incomplete:
`internal/node` computes a **second** quorum, on a second code path,
and it is the path this project has already had one real bug in
(Phase 8's `BeginReadIndex` hang).

`internal/node.checkPendingReads` — the ReadIndex path every SQL
statement enters via `Session.Begin` → `BeginReadIndex`
(`internal/sql/engine.go`) — today begins by counting this node
unconditionally:

```go
acked := 1 // self
for _, p := range n.cfg.Peers { /* ... */ if n.ackSeq[p] > pr.requiredSeq { acked++ } }
if acked < n.majority() { /* stays pending */ }
```

Translated mechanically to the new membership state — iterate
`activeConfig.Voters`, compare against `activeConfig.majority()` — that
`acked := 1` line silently reintroduces **exactly** the defect §4.2
removed from the commit path. In a 3→2 self-removal (`C_new = {b,c}`,
majority 2), a self-removing leader that still counts itself reaches
`acked = self + b = 2` while only **one** genuine `C_new` voter has
confirmed its leadership, and the read resolves against a quorum basis
that does not exist.

**The rule, stated once, for both quorums:**

> A node contributes to a quorum count — its `matchIndex` toward
> commit, or its own implicit self-ack toward ReadIndex — **iff it is
> a Voter in its own current `activeConfig`.** There is no
> self-exemption on either path, and the two paths must not state the
> rule differently.

Concretely:

```go
cfg := n.core.ActiveConfig()          // §6.3a, read ONCE per pass
acked := 0
if cfg.isVoter(n.cfg.ID) {
    acked = 1                          // self, only while self is still a voter
}
for _, p := range cfg.Voters {
    if p.ID != n.cfg.ID && n.ackSeq[p.ID] > pr.requiredSeq { acked++ }
}
if acked < cfg.majority() { /* stays pending */ }
```

`cfg` is read **once per `checkPendingReads` call**, not once per
pending read, so every read resolved in one pass is evaluated against
one configuration rather than against a set that could differ between
loop iterations.

**What happens to a read that can no longer be satisfied.** Nothing new
is needed, but the outcome must be stated in all three of its branches
rather than two — revision 3 collapsed them and overstated the result
(§23/G2). A pending read stays pending while `acked < majority`, and
is failed with `ErrLeadershipLost` by the existing
`Role() != Leader || CurrentTerm() != pr.term` check the instant the
self-removing leader steps down at commit (§4.2). The complete case
analysis for a read in flight across a self-removal, with `C_new`'s
majority written `m`:

| Acknowledgements from `C_new` voters (self never counted) | The `EntryConfig` entry | The read |
|---|---|---|
| `>= m`, and `appliedIndex >= pr.target` | commits (the same `matchIndex` majority, §4.2) | **Resolves** against a genuine `C_new` majority. |
| `>= m`, but `appliedIndex < pr.target` when the pass runs | commits | Stays pending for that pass; the step-down that follows commit then fails it with **`ErrLeadershipLost`**. |
| `< m` | **does not commit** — `advanceLeaderCommit` counts the same voters against the same `m` | Stays pending, and the leader does **not** step down, so no `ErrLeadershipLost` is produced. The read **blocks until the caller's context deadline**. |

The third row is the one revision 3 wrote out of existence by asserting
the read "never hangs". It does block, and the plan should say so: a
self-removing leader that cannot reach a majority of `C_new` can
neither commit its own removal nor confirm a read, which is exactly the
availability cost §4.2 already states honestly for the commit path,
inherited unchanged by the read path. It is *not* a hang in the Phase-8
`BeginReadIndex` sense (a stall with no possible resolution on a
**healthy** cluster); it is the ordinary, correct behaviour of a
linearizable read on a leader that cannot assemble the quorum it needs,
identical to a read issued on a minority-partitioned leader today, and
it terminates at the caller's deadline.

What is guaranteed unconditionally, and is the property that matters:
**a read never resolves against a quorum basis that is not a genuine
majority of the configuration in force when it resolves**, and the
leader's own self-ack is never part of that basis once the leader is
not a Voter. Blocking is the conservative direction; resolving would
be the bug.

**Removal and promotion of *other* voters need no additional rule**
beyond recomputing against the current `activeConfig` on every pass: a
removed voter's stale `ackSeq` stops counting because the loop no
longer iterates it, and a newly promoted voter with `ackSeq == 0`
simply does not count until it acks, which is the conservative
direction. Those two directions plus this section's self-removal case
are DM-17's three required sub-cases (§15), and §16's background SQL
reader is what exercises all three against real processes.

### 4.3 Node unavailable during removal

No special case. The removal entry's own commit is evaluated against
`C_new` (§2.2: the new configuration governs even its own commit), so
an unavailable — crashed or partitioned — *voter being removed* does
not need to acknowledge its own removal at all: its absence never
blocks the removal from committing, because `C_new` already excludes
it from the majority calculation. This is the property that makes
"remove the node that died" work at all, and it is the same rule that,
applied to a leader removing itself, excludes that leader from its own
commit count (§4.2). An unavailable
*learner* being removed is even simpler: learners never affect any
majority calculation regardless.

### 4.4 Removal followed by restart

The removed process, if it later restarts (having never learned of its
own removal — e.g. it was already offline when the entry committed),
restarts with whatever it had durably persisted before going offline.
`ConfigAt(lastIndex())` (§6.3) over that durable state may still show
it as a member, since it never received the removal entry. It resumes
as a `Follower` and attempts to reconnect. The instant it reconnects to
any current, legitimate member and receives real Raft traffic, one of
two things happens depending on how far behind it is:

- If the leader's retained log still spans back far enough, ordinary
  `AppendEntriesRPC` delivers the removal entry and the node adopts the
  new configuration excluding itself (§2.2). **This works
  unconditionally under revision 2's §2.7**, which never filters
  `AppendEntriesRPC` on a membership basis — under revision 1's
  blanket filter this repair path was reachable only by accident, and
  in the symmetric case (§2.7's DM-14 trace) not reachable at all.
- If the leader has already compacted past the required range, a
  `MsgInstallSnapshotRequest` (carrying the snapshot's own
  `Meta.Configuration`, §7.2) achieves the identical outcome in one
  step.

From the instant it adopts a configuration excluding itself, the node
is `selfRemoved()` (§3.1) and:

- it never campaigns again (§2.7 Rule 1's third clause);
- it never grants a vote (§2.7 Rule 1's second clause);
- any client still pointing at it receives `ErrNodeRemoved` (§4.6)
  rather than silently timing out forever.

**If it never reconnects at all** (fully, permanently partitioned): it
may continue believing itself a Voter and periodically call elections
forever. This is harmless to cluster **safety** by construction (§2.3:
a permanently isolated node can never gather a majority of any live
configuration) and is bounded in its effect on the rest of the
cluster's **liveness** by §2.7's two rules acting together:

- Every current member whose own `activeConfig` no longer contains the
  removed node drops its `RequestVoteRequest`s outright (Rule 1) — no
  term bump, no disruption.
- Every current member whose `activeConfig` *still* contains it — a
  member that itself has not yet caught up on the removal — ignores
  those `RequestVoteRequest`s anyway for as long as it is hearing from
  a current leader (Rule 2, the standard Raft §4.2.3 treatment). This
  is the case revision 1's rule could not cover at all, because such a
  receiver has no way to know the sender was removed.

Together these bound the disruption without ever filtering leader
replication, which is what makes the repair paths above reachable.

### 4.5 Stale removed node sending old Raft traffic (full treatment)

Covered completely by §2.7's two rules, as itemized in §4.4's final
paragraphs. The property being claimed is precise and narrower than
revision 1's:

- **Safety**: a removed node's traffic can never cause an incorrect
  outcome, with or without any filtering, by §2.3's Lemma 1 + W. This
  was always true and was never what §2.7 provided.
- **Liveness**: a removed node's indefinitely-retried elections cannot
  force repeated re-elections among current members, because every
  current member either drops its votes on a membership basis (Rule 1)
  or ignores them on a leader-contact basis (Rule 2).
- **Non-interference with repair**: a removed, stale, or
  behind-on-membership node is *always* reachable by a legitimate
  leader's `AppendEntriesRPC`/`MsgInstallSnapshotRequest`, because
  those are never filtered. This is a property revision 1 lacked and
  §2.7's DM-14 trace demonstrates the cost of lacking it.


### 4.6 Client-visible behavior against a removed node

A client (or a stray internal caller) that continues to send
`/propose`/`/outcome`/SQL requests to a process whose own node has
observed its own removal receives a new, distinct error —
`ErrNodeRemoved` — rather than the existing `NotLeaderError` (which
implies "ask someone else who might currently be leader," a category
error for a node that is not even a cluster member anymore and never
will be again without an explicit operator re-add). This is a small,
explicit addition to `internal/node.Node`'s existing propose-rejection
paths.

### 4.7 Quorum changes during removal

Fully covered by §2.2/§2.3: the quorum size changes the instant the
removal entry appends (not when it commits, and not when the physical
process actually stops), and the proof in §2.3 is exactly what
guarantees this transition is always safe regardless of what else is
happening concurrently (elections, partitions, crashes — §5 covers
every boundary explicitly).

---

## 5. Failure during reconfiguration — every boundary

Each boundary below states the precise, unambiguous post-recovery
state — the explicit requirement of this document's item 5 ("there
must be no ambiguous configuration state after recovery").

| Boundary | What survives | Resulting configuration state |
|---|---|---|
| **Before configuration proposal** (admin call arrives, leader crashes before ever calling `Core.ProposeConfigChange`) | Nothing was attempted. | Unchanged — as if the admin call never happened. Operator retries with the same `RequestID` (§10) against whichever node becomes the new leader; idempotency table has no record of it (nothing was ever proposed), so the retry proceeds as a fresh attempt. |
| **After proposal, before commit** (leader appended the `EntryConfig` entry to its own log, possibly replicated to some but not a majority-of-`C_new`, then crashed) | The entry may or may not survive in the new leader's log (ordinary divergent-suffix repair reasoning applies identically to `EntryConfig` entries — no special case; the repaired node recomputes its configuration by the ordinary `ConfigAt(lastIndex())` call of §6.3). | If a new leader's log does **not** contain the entry (it never reached a majority-of-`C_new`): as if the proposal never happened — every node's `activeConfig` becomes whatever `ConfigAt(lastIndex())` returns over its own repaired log, snapshot boundary, and bootstrap seed, in that fixed order (§6.3). If a new leader's log **does** contain it (it *did* reach a majority-of-`C_new`, satisfying `LEADER COMPLETENESS`): the new leader necessarily adopts it as part of normal log reconstruction (§6) and the entry proceeds to commit normally, exactly as if the original leader had not crashed — **this is not ambiguous**: `LEADER COMPLETENESS`'s existing vote-granting log-comparison rule (unchanged mechanism) guarantees only a candidate whose log already contains every entry a majority-of-`C_new` might have persisted can win an election, so there is no possible new-leader outcome that "loses" an entry that genuinely reached a majority. |
| **After commit, before apply** (entry committed — majority-of-`C_new` persisted it — but the committing node crashed before `internal/node.applyCommitted` ran `FSM.RecordMembershipOutcome`/updated its dial table) | The entry, being committed, is guaranteed present in every future legitimate leader's log (`LEADER COMPLETENESS`). | Identical to the existing, already-proven `docs/failure-model.md` §2.3 case ("leader crash after quorum, before reply") extended to this command kind: on restart/re-election, `applyCommitted` runs normally over the durable committed log, including this entry, producing the identical deterministic outcome it always would have — no ambiguity, no re-derivation needed beyond the existing, already-tested recovery path. |
| **Leader crash mid-change** (any point during propose/replicate/commit) | Covered by the two rows above, exhaustively — "mid-change" decomposes into exactly "before commit" or "after commit," nothing else. | See above. |
| **Candidate election during change** | An election can occur at any point regardless of an in-flight config change; §2.3's proof is precisely what makes this safe — a candidate's own log either does or does not contain the (possibly uncommitted) `EntryConfig` entry, and `LEADER COMPLETENESS`'s existing log-comparison rule decides eligibility exactly as it always does, config entries included with no special case. | Whichever candidate wins reconstructs its own `activeConfig` from its own (possibly config-entry-including, possibly not) log via the same §2.2 mechanism every node always uses — never ambiguous, always a pure function of that node's own now-current log. |
| **Network partition during change** | The proposal can only commit by reaching a majority of `C_new` (§2.2) — an isolated minority (whether it holds the old or new quorum's minority) can never independently commit it, by `QUORUM SAFETY`, unchanged mechanism now evaluated against `activeConfig.majority()`. | If the majority side includes enough of `C_new`'s voters, the change commits there normally and the healed minority catches up afterward via ordinary replication/snapshot (§2.2, §7) with **no** special reconciliation step — `activeConfig`'s revert-on-truncate rule (§2.2) already handles a minority node's stale, divergent, or simply-behind log correctly, uniformly with every other kind of entry. |
| **Leader elected with an uncommitted `EntryConfig` inherited in its log tail** (the boundary revision 1 did not have) | The inherited entry, committed or not, is present in this leader's log by `LEADER COMPLETENESS`; whether it *committed* is not locally knowable. | The new leader refuses every membership change until **P1 and P2 both hold** (§2.2a) — i.e. until it has committed an entry of its own current term, which also commits (or has already committed) the entire inherited tail. `ProposeConfigChange` returns `ErrConfigChangeNotReady`, nothing enters the log, and the operator's `RequestID` is untouched and safely reusable (§2.6a). Once `proposeElectionNoOp` commits, the state is unambiguous in both directions: the inherited entry is now known-committed and is the new chain head, or it was truncated away by this leader's own election and never existed. Reproduced as **DM-12**. |
| **Snapshot taken while a membership change is appended but not committed** | The snapshot covers only applied history; the *uncommitted* entry is above `appliedIndex` and is not in it. | `Meta.Configuration` is `ConfigAt(appliedIndex)` (§6.3, §7.1) — the configuration effective **at the boundary**, which by `ConfigAt`'s prefix-stability invariant is unaffected by any entry above it. Revision 1 wrote `activeConfig` here and would have durably captured a configuration that may never commit, making both the live revert (§2.2) and the post-restart reconstruction (§6.2) return the abandoned configuration (§23/C1). Reproduced as **DM-13**. |
| **Snapshot during/involving a membership transition** | Fully covered by §7. A snapshot taken while an `EntryConfig` entry is committed-but-still-in-the-retained-log captures `ConfigAt(appliedIndex)` — **never `activeConfig`**, which is the §23/C1 correction and must be stated the same way in every row of this table (revision 2 left this cell phrased in terms of `activeConfig`, a second derivation contradicting §6.3; §23/F5's consistency sweep removed it). There is no special "is a change in flight" case: `SERIALIZED MEMBERSHIP CHANGE` guarantees at most one is ever outstanding, and an *uncommitted* `EntryConfig` entry is never included in a snapshot at all (snapshots only ever cover committed, applied history, unchanged existing rule). | The snapshot's `Meta.Configuration` is exactly `ConfigAt(LastIncludedIndex)`, with `Meta.HasConfiguration = true` — always unambiguous by construction. |

---

## 6. Durability / recovery

### 6.1 Where membership state is persisted

- **Raft log**: every `EntryConfig` entry, via the *existing*
  `WALStorage.Append`/`internal/wal.AppendLogEntry` path — no new
  `internal/wal` record type and no change inside `internal/wal` at
  all. The change is confined to `internal/node/storage.go`'s
  `encodeEntryPayload`/`decodeEntryPayload`, the opaque `Term + Data`
  blob `WALStorage` hands to `internal/wal` entirely outside
  `internal/wal`'s own awareness (`docs/architecture.md` §5's existing
  "WAL must not know Raft semantics" boundary, unchanged). The exact
  encoding is §6.1a.
- **WAL metadata**: no new field needed.
  `Metadata.ClusterGeneration` (existing, `v0.4.0`) already gates
  whether membership commands may be proposed *and* whether
  generation-2 entry payloads may be written (§6.1a, §8).
- **Snapshots**: `Meta.Configuration`, guarded by the explicit durable
  `Meta.HasConfiguration` bit (§7.1) — the fallback source of truth
  once the log no longer retains the relevant `EntryConfig` entry.
  Always written as `ConfigAt(LastIncludedIndex)`, never as
  `activeConfig` (§6.3, §7.1); presence is always the flag, never
  "the value is non-empty" (§23/F7).
- **FSM/state machine**: the membership `RequestID → Outcome`
  idempotency table (§2.6, §10), via `FSM.EncodeState`/`DecodeState`
  exactly like every other outcome table, so it is included in every
  snapshot automatically.

### 6.1a `Entry.Type`'s durable encoding (self-describing, generation-gated)

Revision 1 proposed "a trailing byte appended only when
`Type != EntryNormal`." That is **not decodable** and is withdrawn
(§23/C6): today's payload is `term(8B) || data` and `decodeEntryPayload`
returns `data = b[8:]`, the entire remainder, so a trailing type byte
is indistinguishable from the last byte of an ordinary command. Since
`MEMBERSHIP RECOVERY DETERMINISM` depends on `Entry.Type` surviving the
WAL round-trip (§6.3 scans recovered entries *by type*), the encoding
must be self-describing.

**Decision: a leading, sentinel-prefixed type header, written for
every entry whose `Type != EntryNormal`, and for no other entry.**

```
encodeEntryPayload layout

  untyped form (byte-identical to today; written iff Type == EntryNormal):
      term(8B) || data

  typed form (written iff Type != EntryNormal):
      term(8B) || 0xFF || entryType(1B) || data
```

**Why the gate is the entry's own type, and not the cluster generation
(§23/F2).** Revision 2 gated the header on "the node's durable
`ClusterGeneration >= 2`". That condition is evaluated at a point where
it is *systematically stale on followers*: a node raises its durable
generation only when it **applies** the finalize entry
(`internal/node.adoptClusterGeneration`, called from
`applyControlEntry`), but it **persists** log entries earlier in the
very same `processOutput` pass — `ApplyPersistRequest` runs, and
recurses into `InputPersistenceComplete`, *before* `applyCommitted` is
called at all. The resulting trace is not an edge case; it is the
ordinary path for the **first** membership change after finalization:

| # | Event |
|---|---|
| 1 | Leader commits the generation-2 finalize at index `k`, applies it, its own durable generation becomes `2`. |
| 2 | Leader proposes the first `EntryConfig` at `k+1`; `handlePropose` sends `AppendEntries` carrying entry `k+1` with `LeaderCommit = k`. |
| 3 | Follower `F` handles that single message: it **persists `k+1` first** — its durable generation is still `1`, so revision 2's rule writes the untyped form and silently discards `Entry.Type` — and only then applies `k`, raising its generation to `2`. |

`F`'s copy of that `EntryConfig` entry is now durably type-less while
its in-memory copy is correct, so nothing is observably wrong until `F`
restarts. Then `decodeEntryPayload` returns `EntryNormal`; `ConfigAt`
step 1 matches on `Type == EntryConfig` and never finds it, so `F`
reconstructs a **stale configuration** — two nodes computing different
configurations from legitimately-produced durable state, which is
precisely what `MEMBERSHIP RECOVERY DETERMINISM` forbids — and the
entry's `Data[0] == 0xF0` then routes it to `fsm.IsControlCommand` →
`applyControlEntry` → `ErrUnknownControlCommand` → `Node.fail`. A
catching-up learner receiving the finalize and the `EntryConfig` in one
`AppendEntries` batch widens the same window arbitrarily.

Gating on `Type != EntryNormal` closes it completely and costs nothing:

- **Rollback safety is unchanged, and becomes structural.** The old
  gate existed for `ROLLBACK BOUNDARY HONESTY`: a pre-finalization
  `v0.5.0` node must write payloads byte-identical to `v0.4.0`'s. It
  still does — §8.2 guarantees no `EntryConfig` entry can exist
  anywhere before generation 2, so a pre-finalization node has no typed
  entry to write and emits the untyped form for every entry,
  byte-for-byte as today. The property is now guaranteed by "there is
  nothing to write" rather than by a runtime check that can read a
  stale value.
- **The decoder is unchanged and remains self-describing**, so one WAL
  may freely interleave untyped and typed payloads — which is exactly
  what every real WAL contains across the finalize boundary.
- **No write decision depends on a value updated on a different
  schedule from the write it gates.** That was the actual defect class;
  it is now absent rather than narrowed.

Regression-tested by **DM-20**, including a negative control that
re-enables the generation gate behind a test-only hook and asserts the
post-restart configuration goes wrong (§15, §19 gate 3).

`0xFF` is the **entry-payload type sentinel**, a value in a namespace
that does not exist today at that offset. Decoding is unambiguous in
both directions and needs no external context:

```
decodeEntryPayload(b):
  require len(b) >= 8
  term = b[0:8]
  if len(b) >= 10 && b[8] == 0xFF:
      typ  = b[9]
      data = b[10:]
      if typ is not a known EntryType: return ErrUnknownEntryType   // fail closed
  else:
      typ  = EntryNormal
      data = b[8:]
```

**Why `0xFF` is safe as the discriminator.** `b[8]` is the first byte
of `Entry.Data`, which in every format that exists today is one of
exactly two things: `fsm.ControlCommandMarker` (`0xF0`), or
`commitTxnCommandVersion` (a small sequentially-incrementing integer,
currently `2`). `0xFF` collides with neither, and — exactly like
`ControlCommandMarker`'s own existing guard — an `init()` assertion in
`internal/node` panics at process start if `entryPayloadTypeSentinel`
ever equals `fsm.ControlCommandMarker` or any value
`commitTxnCommandVersion` has reached, with a matching explicit test
(`TestEntryPayloadSentinelNeverCollides`, §18). This is the same
structural non-collision discipline the codebase already uses at the
`Data[0]` layer, applied one layer up at the payload framing layer.

**Compatibility with existing WAL files.** An untyped payload is
**byte-identical to today's encoding**, so:

- every WAL file written by `v0.1.0`–`v0.4.0` decodes under the new
  decoder to exactly the `(term, EntryNormal, data)` it always did —
  the `b[8] == 0xFF` branch is simply never taken;
- a `v0.5.0` node running pre-finalization writes byte-identical
  payloads to what `v0.4.0` writes, because every entry it can possibly
  hold is `EntryNormal` (§8.2) — `ROLLBACK BOUNDARY HONESTY` preserved
  structurally, in the same additive-and-conditional spirit
  `docs/wal.md` §14 and `ADR-0017` established for `Metadata`'s own new
  fields, but with no runtime generation read anywhere in the encode
  path;
- a `v0.4.0` binary can never encounter a typed payload during
  correct operation (§8.2), and if it does through operator error, it
  reads `data = 0xFF || entryType || …`, whose first byte is neither
  `ControlCommandMarker` nor a recognized `commitTxnCommandVersion`, so
  its unmodified `DecodeCommitTxn` returns
  `ErrUnsupportedCommandVersion` → `Node.fail` — `NO SILENT FORMAT
  MISINTERPRETATION`, fail-closed, zero code change on the old binary.
  Note this is a **second, independent** fail-closed path from §2.5's
  (which relies on `Data[0] == 0xF0` reaching `DecodeSetClusterVersion`);
  both must be asserted by tests, because §2.5's path is the one that
  fires for an entry received over the **wire** (where gob drops
  `Entry.Type` and the payload framing never applies) and §6.1a's is
  the one that fires for an entry read from **disk**.

**Malformed / unknown type behavior.** An unknown `entryType` byte, or
a payload truncated mid-header (`len(b) == 9`), is
`ErrUnknownEntryType`/`ErrMalformedEntryPayload` — a decode error, not
a guess. `WALStorage.load` already fails the open on a
`decodeEntryPayload` error (`internal/node/storage.go`), which is
`RECOVERY NON-INVENTION` behaving exactly as it already does. Never
repaired, never skipped, never defaulted to `EntryNormal`.

**Fuzz targets** (§18): `FuzzDecodeEntryPayload` (arbitrary bytes into
`decodeEntryPayload`: never panics, never returns a type it did not
read, never returns `data` aliasing outside the input) and
`FuzzDecodeEntryConfig` (arbitrary bytes into the §2.5 `Configuration`
payload decoder: never panics, never yields a structurally-invalid
`Configuration` without an error).

### 6.2 Recovery ordering

Extends `docs/recovery.md`'s existing ordering (unchanged for every
step already there) with exactly one new step, inserted at the point
`Core` is reconstructed:

1. *(existing)* Open WAL, validate/replay to recover `HardState` and
   log entries — now including each entry's `Type` via §6.1a.
2. *(existing)* Load and validate the most recent snapshot, if any.
3. **(new)** Seed the reconstruction inputs: `snapshotConfig :=`
   the loaded snapshot's `Meta.Configuration` (§7.1), or the zero
   `Configuration{}` if no snapshot exists; `bootstrapConfig :=`
   `Config.Bootstrap` (§1.1, §1.8, and §7.6 for the restore case).
4. **(new)** `activeConfig, activeConfigIndex := ConfigAt(lastIndex())`
   — the single algorithm of §6.3, called here with exactly the same
   semantics it has everywhere else. **There is no recovery-specific
   configuration algorithm.**
5. *(existing, unchanged)* `commitIndex`/`appliedIndex` reconstruction
   via legitimate leader contact or this node's own election
   (`docs/raft.md` §5.1) — membership gets no special treatment; a
   config entry that turns out to have been uncommitted is subject to
   exactly the same divergent-suffix repair as any other entry, with
   `activeConfig` recomputed by the same §6.3 call the moment that
   repair happens.

### 6.3 `ConfigAt` — the single configuration-reconstruction algorithm

Revision 1 described configuration reconstruction **three times**, in
three sections, with subtly different fallbacks (§1.7's prose, §2.2's
backward scan, §6.2's recovery list) — and they disagreed on a node
that has never snapshotted (§23/C2). Revision 2 defines it **once**,
here, as one function with one priority order. Every other section
references this one.

```go
// ConfigAt returns the Configuration effective at log index i, and the
// index of the EntryConfig entry that established it (0 when it came
// from the snapshot boundary or the bootstrap seed).
//
// Preconditions: i >= c.snapshotIndex and i <= c.lastIndex().
//
// Pure: reads only c.log, c.snapshotIndex, c.snapshotConfig, and
// c.bootstrapConfig. Performs no I/O, mutates nothing, and is
// deterministic — identical durable state always yields an identical
// result, on every node, in every process lifetime. This is the
// entirety of MEMBERSHIP RECOVERY DETERMINISM's mechanism.
func (c *Core) ConfigAt(i Index) (Configuration, Index)
```

**Algorithm — one fixed priority order, evaluated in this order and no
other:**

1. Scan `c.log` backward from position `pos(i)` down to (but **not**
   below) `pos(snapshotIndex)`, for the newest entry with
   `Type == EntryConfig` **whose decoded kind establishes a
   configuration** — `AddLearner`, `PromoteToVoter` or `RemoveServer`,
   but **not** `Voided` (kind `19`, §2.5, written only by §7.6's
   offline restore transform). If found at index `k`, return
   `(decode(entry.Data).fullConfig, k)`. A `Voided` entry is skipped by
   this scan exactly as an `EntryNormal` entry is: it occupies an index
   and a term and carries nothing else.
2. Otherwise, if `c.snapshotHasConfig` is true, return
   `(c.snapshotConfig, 0)` — "the snapshot boundary's own
   configuration; no later change survives at or below `i`." The test
   is the explicit durable flag `Meta.HasConfiguration` (§7.1),
   **never** "`snapshotConfig` is non-empty" (§23/F7): an empty
   configuration is a legitimate value, "there is no configuration
   here" is a different fact, and a restored data directory asserts the
   latter deliberately (§7.6). Conflating them would make restore's
   correctness depend on a value coincidentally being empty.
3. Otherwise return `(c.bootstrapConfig, 0)` — the fresh-cluster seed
   (`Config.Bootstrap`, §1.1). On a node that has never snapshotted,
   this is the step revision 1's §2.2 omitted, which is exactly why
   its truncation path and its recovery path disagreed.
4. If `bootstrapConfig` is *also* empty, return the zero
   `Configuration{}` — the legitimate "never joined" state of §3.1,
   distinguished from every other state by
   `len(Voters) == 0 && len(Learners) == 0`.

`bootstrapConfig` is a new `Core` field holding `Config.Bootstrap`
verbatim, so step 3 is available at every call site rather than only at
construction. The backward scan is bounded by `c.log`'s size, which is
already bounded by "since last snapshot" — the same assumption every
other `c.log` operation already makes.

**Invariants `ConfigAt` must satisfy** (each an explicit test
obligation, §18):

- **Determinism**: `ConfigAt(i)` depends only on durable state; two
  `Core`s constructed from byte-identical WAL + snapshot always agree
  for every `i`.
- **Monotone provenance**: if `ConfigAt(i)` returns index `k > 0`, then
  `k <= i` and `c.log[pos(k)].Type == EntryConfig`.
- **Prefix stability**: truncating the log at any index `> i` never
  changes `ConfigAt(i)`. This is the property that makes
  revert-on-truncate correct *and* makes snapshot capture correct, and
  it is the single property revision 1's `snapshotConfig = activeConfig`
  assignment violated.
- **Boundary agreement**: whenever `snapshotHasConfig` is true,
  `ConfigAt(snapshotIndex)` equals `snapshotConfig`; whenever it is
  false — a `FormatVersion 1` snapshot (§7.1) or a restored directory
  (§7.6) — `ConfigAt(snapshotIndex)` equals `bootstrapConfig`,
  deterministically, by steps 2→3. Stating both halves is what makes
  the invariant checkable without reference to whether a value happens
  to be empty.

**Every call site, and what it passes** — this table is the complete
enumeration; no other code may compute a configuration:

| Call site | Call | Purpose |
|---|---|---|
| `handlePropose` / `ProposeConfigChange` (append) | `ConfigAt(lastIndex())` | Append-time-effective activation (§2.2). In practice the new config is known directly from the entry being appended; the call is the *definition*, the direct assignment is the permitted optimization, and a debug-build assertion checks they agree. |
| `handleAppendEntriesRequest` (accept path) | `ConfigAt(lastIndex())` | Same, on the follower side. |
| `handleAppendEntriesRequest` (truncation/conflict repair) | `ConfigAt(lastIndex())` on the repaired log | Revert-on-truncate (§2.2). Replaces revision 1's separate backward scan. |
| `NewCore` / `NewCoreFromSnapshot` (startup recovery) | `ConfigAt(lastIndex())` | §6.2 step 4. |
| `becomeLeader` (leader initialization) | `ConfigAt(lastIndex())` | Initializes `nextIndex`/`matchIndex` over the correct member set (§2.2); `pendingConfIndex = lastIndex()` is set in the same place (§2.2a P2). |
| `internal/node.maybeSnapshot` (snapshot creation) | `ConfigAt(appliedIndex)` | **The correction of §23/C1.** Never `activeConfig`. |
| `Core.Compact(uptoIndex)` | `snapshotConfig = ConfigAt(uptoIndex)` | §7.4. Never `activeConfig`. |
| `handleInstallSnapshotRequest` | *not a call* — adopts the installed snapshot's own `Meta.Configuration`/`Meta.HasConfiguration` as `snapshotConfig`/`snapshotHasConfig`, and sets `activeConfig` to what step 2/3 would yield from them, with `activeConfigIndex = 0` | The whole log is discarded, so there is nothing to scan; §7.2. |
| `internal/node.refreshStatusLocked` (status publication) | `ConfigAt(commitIndex)`, **second** return value | The sole source of `/admin/membership/status`'s `committedConfigIndex` (§9). Revision 2 defined that field as "`activeConfigIndex` when `activeConfigIndex <= commitIndex`, and the previous configuration's index otherwise" — a second, independent configuration derivation whose second clause had no defined mechanism at all. Deleted (§23/F5). |
| `internal/backup.buildStaging` (restore transform, §7.6) | *not a `Core` call* — an offline rewrite of the staged WAL that voids every `EntryConfig` payload, so a restored directory reaches step 2 with `snapshotHasConfig == false` and then step 3 | Restore runs entirely before any `Core` exists (`internal/backup` never imports `internal/raft`; it matches on the payload framing of §6.1a and §2.5). Listed here because it is the only other code in the tree permitted to *reason about* `EntryConfig` entries, and §6.3's exhaustiveness claim would otherwise be false. |

### 6.3a The `Core` membership accessors, and every caller (`§23/F9`)

`internal/node` cannot read `Core`'s fields, and revision 2 named only
`Core.MatchIndexOf` while leaving every configuration read implicit —
which is how a second derivation crept into §9 in the first place.
`Core` exposes exactly these, and no other way to obtain a
configuration exists:

```go
func (c *Core) ActiveConfig() Configuration              // deep copy of activeConfig
func (c *Core) ActiveConfigIndex() Index                 // activeConfigIndex
func (c *Core) ConfigAt(i Index) (Configuration, Index)  // §6.3, the one algorithm
func (c *Core) MatchIndexOf(id NodeID) Index             // existing accessor, unmodified
```

**`ActiveConfig` returns a deep copy**, never slices aliasing `Core`'s
own `Voters`/`Learners` backing arrays. This is not defensive
boilerplate: `internal/node.refreshStatusLocked` publishes its result
into `Node.status`, which `Node.Status()` serves **from another
goroutine** under `statusMu` (`internal/node/node.go`'s existing
pattern), so an aliased slice would be read off the event loop while
`Core` mutates it on the next append — a data race `-race` would catch
only intermittently. `ConfigAt` returns a copy for the same reason.

**Goroutine rule**: all four are callable **only from the event-loop
goroutine**, exactly like every existing `Core` method. Anything
serving HTTP reads the already-published `Node.status` snapshot, or
routes a request through the event loop — the discipline `/status`
already follows, extended to `/admin/membership/status` unchanged.

**Complete caller list.** Anything not listed here does not get a
configuration; adding a caller means adding a row here and, if it
derives rather than reads, a row in §6.3's call-site table too:

| Caller | Accessor | Purpose |
|---|---|---|
| `Node.majority()` | `ActiveConfig()` | replaces `len(n.cfg.Peers)/2 + 1` |
| `Node.checkPendingReads` | `ActiveConfig()`, once per pass | §4.2a's read quorum, including the self-is-a-voter test |
| `Node.applyCommitted` (`EntryConfig` branch) | `ActiveConfig()` | dial-table update (§1.8), waiter resolution (§9) |
| `Node.maybeSnapshot` | `ConfigAt(appliedIndex)` | the snapshot's `Meta.Configuration`/`HasConfiguration` (§7.1) |
| `Node.refreshStatusLocked` | `ActiveConfig()`, `ActiveConfigIndex()`, `ConfigAt(commitIndex)` | `/status` and `/admin/membership/status` fields (§9, §14) |
| `Node.AddLearner` / `PromoteToVoter` / `RemoveServer` | `ActiveConfig()` | §12.2's `resulting` voter count; §2.6a checks 7–8 |
| `Node.PromoteToVoter` | `MatchIndexOf` + `ActiveConfig()` | §3.3's lag gate; §8.2a's peer-generation gate |
| `Node.computePrecheck` | `ActiveConfig()` | §8.2a: voters **and** learners in status, voters only in `Ready` |
| `Node`'s metrics refresh | `ActiveConfig()`, `ActiveConfigIndex()` | §14's gauges |
| `internal/transport` dial-table update | **none** | `Node` hands it the member list; `transport` never calls `Core` (`docs/architecture.md` §5's dependency direction) |

### 6.4 Corruption / fail-closed behavior

No new corruption class: an `EntryConfig` entry's payload is subject to
the *same* WAL-level checksum/framing validation every entry already
gets (`internal/wal` never inspects `Data`'s content, §6.1) — a
corrupted `EntryConfig` payload is caught by the existing checksum
mechanism before `internal/node` ever tries to decode it as a
`Configuration` (`RECOVERY NON-INVENTION`, unchanged). Layered above
that, and reached only by a genuine bug rather than by corruption:

- an unrecognized `Entry.Type` discriminator → `ErrUnknownEntryType`
  from `decodeEntryPayload`, failing the WAL open (§6.1a);
- a checksum-valid but structurally-invalid `Configuration` payload
  (not one of §2.6's four shapes relative to its predecessor, or a
  duplicate `ID`, or a zero-voter set) → refuse to adopt it, `Node.fail`,
  never guess or silently repair, identical to §2.6's live-path
  defense-in-depth check. **A `Voided` entry is not such a payload and
  must not be caught by this rule** (§23/G1): it declares
  `fullConfigLen == 0` and carries no `Configuration`, which is a
  well-formed statement of "this entry establishes nothing", not a
  zero-voter configuration. The check is on a `Configuration` that is
  present and wrong; a voided entry presents none.


## 7. Snapshots / compaction

### 7.1 Membership encoded in snapshots — format decision (resolved, Option B)

Revision 1 left a latent contradiction here that would have broken the
`v0.4.0` → `v0.5.0` upgrade outright (§23/B2). This section resolves
the format question completely; **no part of it is left to the
implementing session.**

#### The two candidate architectures, and why B wins

- **Option A — keep `FormatVersion 1`, encode `Configuration`
  additively inside the existing length-prefixed `fsmState` region**,
  exactly as `v0.4.0` encoded `ClusterGeneration`
  (`docs/snapshots.md` §10). **Rejected.** It is genuinely
  backward-decodable, but it would place Raft-level membership inside
  `internal/fsm`'s serialized state — precisely the layering §2.4
  argues membership must not have. `internal/snapshot` would then carry
  a `Configuration` it cannot see, and `internal/fsm` would own a value
  that has no `Apply` semantics and no FSM meaning. That is a worse
  architecture bought with a cheaper format change.
- **Option B — bump `snapshot.FormatVersion` `1` → `2`, add
  `Configuration` to the snapshot's own outer frame, and implement a
  genuinely version-aware decoder with an explicit v1 path.**
  **Chosen.** The configuration belongs in the outer frame alongside
  `LastIncludedIndex`/`LastIncludedTerm`, which is where the consensus
  boundary already lives, and a version bump is the honest signal for
  new outer-frame content — `ADR-0017` deliberately did *not* bump for
  `v0.4.0` precisely because that phase added no outer-frame content;
  this one does.

#### The thing revision 1 got wrong, stated plainly

`internal/snapshot.Decode` today performs a **strict equality** check:

```go
if version != FormatVersion {
    return Snapshot{}, fmt.Errorf("%w: snapshot format version %d, expected %d", ErrUnsupportedVersion, version, FormatVersion)
}
```

Bumping the constant to `2` **without changing this line** would make a
`v0.5.0` binary unable to read *any* snapshot it did not itself write
at generation ≥ 2. Concretely: a node upgraded from `v0.4.0` fails to
start against its own existing on-disk snapshot, and
`internal/backup/restore.go`'s `snapshot.Decode` call rejects every
backup ever taken by `v0.3.0`/`v0.4.0`. Revision 1 asserted the
opposite ("requiring no new logic, only the constant bump plus the new
field's encode/decode"; "an old `FormatVersion 1` file remains fully
readable forever"). **Option B is only viable together with the
decoder change below, and the two must land in the same slice.**

#### Exact persisted representation

```go
// internal/snapshot/snapshot.go
const FormatVersion    uint8 = 2  // version this build WRITES
const MinReadVersion   uint8 = 1  // oldest version this build READS

type Meta struct {
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    // HasConfiguration reports whether this snapshot carries a
    // configuration AT ALL. It is a durable, explicitly encoded bit in
    // v2 — never inferred from Configuration being empty, which is a
    // legitimate value and a different statement (§23/F7). Always
    // false when decoded from a v1 file, and deliberately false in a
    // snapshot staged by internal/backup's restore path (§7.6).
    HasConfiguration  bool
    // Configuration is meaningful iff HasConfiguration is true;
    // otherwise it is the zero value and must not be read.
    Configuration     Configuration
}

// Configuration mirrored as a snapshot-package-local type: this package
// has no dependency on internal/raft today (docs/snapshots.md §1) and
// keeps none. internal/node converts at its own boundary, exactly as it
// already does for LastIncludedIndex/Term's raft.Index/raft.Term <->
// uint64 conversion.
type Member struct { ID string; Address string }
type Configuration struct { Voters []Member; Learners []Member }
```

```
v1 frame (unchanged, still written by v0.1.0-v0.4.0 and by a
pre-finalization v0.5.0 node):
  magic(4B) version(1B)=1 lastIncludedIndex(8B) lastIncludedTerm(8B)
    fsmStateLen(8B) fsmState(...) crc32(4B)

v2 frame (written by a v0.5.0 node only once ClusterGeneration >= 2):
  magic(4B) version(1B)=2 lastIncludedIndex(8B) lastIncludedTerm(8B)
    fsmStateLen(8B) fsmState(...)
    hasConfig(1B)                                  <-- NEW: exactly 0x00 or 0x01
    configLen(8B) config(configLen bytes)          <-- NEW outer-frame section
    crc32(4B)

  hasConfig/configLen are ALWAYS both present in a v2 frame, and are
  cross-checked on decode:
    hasConfig == 0x01  =>  configLen > 0, config decodes per the layout below
    hasConfig == 0x00  =>  configLen MUST be 0 and no config bytes follow
    any other hasConfig byte, or a hasConfig/configLen disagreement,
      => ErrCorrupt, fail closed, nothing decoded
  ErrCorrupt is internal/snapshot's EXISTING error for exactly this
  class (declared-length/bytes-present disagreements, trailing bytes,
  checksum mismatch — internal/snapshot/errors.go). This phase adds no
  new snapshot error value; a new one would be a second name for a
  condition the package already reports.
  Always writing both fields (rather than omitting the length when the
  flag is clear) keeps the frame's shape fixed, makes the decoder
  branch-free up to the cross-check, and turns a corrupted flag into a
  detected error instead of a shifted read.

config section layout (length-prefixed throughout; no trailing
ambiguity, decodable without any external context):
  voterCount(4B)
    [ idLen(4B) id(idLen) addrLen(4B) addr(addrLen) ] x voterCount
  learnerCount(4B)
    [ idLen(4B) id(idLen) addrLen(4B) addr(addrLen) ] x learnerCount
```

The `config` section is placed **after** `fsmState` and **before** the
checksum, so the CRC covers it exactly as it covers every other byte,
and so a v1 decoder's byte offsets for everything it knows about are
unchanged.

#### Reader behavior, both directions, fail-closed

```go
version := data[off]
switch {
case version > FormatVersion:
    return Snapshot{}, fmt.Errorf("%w: snapshot format version %d, this build writes/reads at most %d", ErrUnsupportedVersion, version, FormatVersion)
case version < MinReadVersion:
    return Snapshot{}, fmt.Errorf("%w: snapshot format version %d, this build reads at least %d", ErrUnsupportedVersion, version, MinReadVersion)
}
// then: decode the common v1 prefix; decode the config section only when version >= 2
```

| Reader | Input | Behavior |
|---|---|---|
| `v0.5.0` | v1 file | **Decodes fully**, with `Meta.HasConfiguration = false` and `Meta.Configuration` the zero value. `ConfigAt` step 2 is skipped on the **flag**, falling through to the bootstrap seed (§6.3 step 3). This is exactly right for an upgraded node: its pre-finalization snapshot predates dynamic membership, so its configuration genuinely *is* the bootstrap one. |
| `v0.5.0` | v2 file, `hasConfig == 0x01` | Decodes fully, including `Configuration`; `ConfigAt` step 2 returns it. |
| `v0.5.0` | v2 file, `hasConfig == 0x00` | Decodes fully; `ConfigAt` step 2 is skipped and step 3 adopts the bootstrap seed. This is the **restored-directory** case (§7.6) and the only way a v2 frame is written without a configuration. |
| `v0.5.0` | v2 file with an invalid `hasConfig` byte, or `hasConfig == 0x00` with `configLen != 0` | `ErrCorrupt`, **fail closed**, before any configuration is read. |
| `v0.5.0` | v3+ file | `ErrUnsupportedVersion`, **fail closed**, before decoding anything further. A future version is never guessed at. |
| `v0.4.0` and earlier | v1 file | Unchanged. |
| `v0.4.0` and earlier | v2 file | Its existing strict-equality check refuses outright with `ErrUnsupportedVersion` — `NO SILENT FORMAT MISINTERPRETATION`, zero code change on the old binary. This is the behavior revision 1 correctly described; it is the *other* direction revision 1 got wrong. |

#### Generation gating (`ROLLBACK BOUNDARY HONESTY`)

A node never **writes** a `FormatVersion 2` snapshot until the cluster
has finalized to generation ≥ 2 (mirroring how `v0.4.0`'s WAL/FSM
generation fields are never written until finalize, and matching
§6.1a's identical gate for entry payloads). `snapshot.Encode` therefore
takes the write-version as an explicit parameter supplied by
`internal/node` from its durable `clusterGeneration`, rather than
reading the package constant directly — a small signature change that
makes the gate impossible to forget. Consequence: a pre-finalize
`v0.5.0` snapshot is **byte-identical** to `v0.4.0`'s output, so
rollback to a `v0.4.0` binary before finalization remains safe by the
same argument `v0.4.0` established, unchanged.

#### The relaxation of strict version equality is a real change, and is recorded as one

`docs/snapshots.md` §5 point 1 ("its format version must be
recognized") is satisfied by a bounded `[MinReadVersion,
FormatVersion]` range just as it was by equality, but the *mechanism*
changes and the invariant text must say so. This is an explicit
implementation-time deliverable (§19): `docs/snapshots.md` §5 gains the
range rule and the exhaustive reader-behavior table above, and
`ADR-0017`'s reasoning about *not* bumping the version gains a short
"superseded for `v0.5.0`, and why" note rather than being silently
contradicted. The new ADR
(`docs/adr/0018-dynamic-membership-architecture.md`) records the
Option A / Option B decision and this rejection rationale.

#### Required tests (all in §18)

- Round-trip: v2 encode → decode preserves `Configuration` exactly,
  including empty voter/learner lists and multi-byte-UTF-8 IDs.
- Round-trip with `HasConfiguration = false`: decode yields
  `HasConfiguration == false` and a zero `Configuration`, and is
  **distinguishable** from a snapshot carrying a genuinely empty
  `Configuration` with `HasConfiguration == true` — the single
  assertion that pins §23/F7's flag-not-sentinel decision.
- A v2 frame with `hasConfig = 0x02`, and one with `hasConfig = 0x00`
  but `configLen = 17`, are each refused with `ErrCorrupt`.
- **A real `v0.4.0`-produced snapshot file, checked into
  `internal/snapshot/testdata/`, decodes correctly under `v0.5.0`** and
  yields `Meta.HasConfiguration == false`. Generated once by the
  `v0.4.0` binary via the existing `mixed_version_test.go` git-worktree
  technique, then committed as a fixture so the compatibility boundary
  is pinned by a genuine artifact rather than by a re-implementation of
  the old encoder.
- A v2 file fed to a `v0.4.0` decoder (exercised in the mixed-binary
  real-process variant, §16) is refused with `ErrUnsupportedVersion`.
- A synthetic `version = 3` file is refused, fail-closed.
- `internal/backup` restore of a real `v0.4.0`-produced backup
  succeeds under `v0.5.0` (§7.6, §18).
- Pre-finalization `v0.5.0` `Encode` output is byte-identical to
  `v0.4.0` `Encode` output for the same `Meta`/FSM.

### 7.2 Installing a snapshot with different membership

`MsgInstallSnapshotRequest` gains **two** explicit fields,
`Configuration` and `HasConfiguration` (mirroring how
`LastIncludedIndex`/`LastIncludedTerm` are already explicit `Message`
fields `Core` reads/writes directly, unlike the opaque `SnapshotData`).
They are a **pair**, exactly as they are in the durable frame (§7.1),
and `Core` populates both from the sending node's
`snapshotHasConfig`/`snapshotConfig` — i.e. `ConfigAt(snapshotIndex)`,
never `activeConfig`. §8.1's generation-2 definition already names both
fields; revision 3's §7.2 described only the first.

**Exactly one value is authoritative on the receiving side: the
installed snapshot's own `Meta.HasConfiguration`/`Meta.Configuration`
pair.** `internal/node`'s existing `handleInstallSnapshot` sequence is
unchanged in ordering (the driver validates and durably installs the
bytes *before* ever calling `Core.Step` for this message, per the
existing documented contract), and it gains one check, **over both
fields**: if `msg.HasConfiguration != snap.Meta.HasConfiguration`, or
if `msg.Configuration` does not equal `snap.Meta.Configuration`, the
install is **refused** and `Node.fail` is called. Checking only the
configuration would leave the flag unguarded, and the flag is the field
that decides whether the configuration is read at all (§6.3 step 2,
§23/F7) — a `true`/`false` disagreement with both configurations empty
would pass a value-only check silently (§23/G8). Revision 1 introduced
both values without saying which wins (§23/P4); two sources of truth
for one value is exactly the ambiguity this document exists to remove.
The message fields exist only so `Core` can reason about the transfer
without the driver handing it snapshot bytes it must not parse
(`docs/raft.md` §1); they are never the values adopted, so this check
is a should-never-happen detector rather than a correctness
dependency — which is precisely why it must cover the whole pair
rather than half of it.

On successful, durable installation,
`Core.handleInstallSnapshotRequest` adopts the snapshot's
`Meta.HasConfiguration`/`Meta.Configuration` pair into
`snapshotHasConfig`/`snapshotConfig`, sets `activeConfigIndex = 0`, and
sets `activeConfig` to whatever §6.3's steps 2→3 yield from that pair —
unconditionally, exactly mirroring the existing "always discard the
whole log" simplification this method already documents for the log
itself. Whatever `activeConfig` this node had before is entirely
superseded: no merge, no reconciliation. No backward scan is performed
or needed, because the log it would scan has just been discarded
(§6.3's call-site table records this as the one non-call).

**The `HasConfiguration == false` case is legitimate and must not be
treated as an error.** It arises exactly once in practice: a cluster
that was restored from backup (§7.6, whose staged snapshot deliberately
carries no configuration) and has not yet crossed its own snapshot
threshold, which then has to serve a snapshot to a newly added learner.
The receiver adopts `ConfigAt(snapshotIndex)` = its own
`bootstrapConfig` — for a joining learner, the zero `Configuration`,
i.e. `neverJoined()` (§3.1). That state is immediately and
deterministically superseded: the `AddLearner` entry that named this
learner is necessarily **above** the sender's snapshot boundary (if it
were at or below it, the sender's snapshot would post-date the restore
and would therefore carry a configuration), so ordinary replication
resuming at `snapshotIndex + 1` delivers it and the learner adopts a
real configuration by the ordinary §2.2 path. Refusing the install
instead would break "add a learner to a freshly restored cluster,"
which is a legitimate and likely first operation after a disaster
recovery. Asserted by a direct unit test and by DM-21's follow-on step
(§15, §18).

### 7.3 Catch-up of newly added nodes

No new mechanism beyond §3.1/§3.2 and this section — a learner far
enough behind to need a snapshot receives one exactly like any existing
far-behind follower always has (`docs/snapshots.md` §7, completely
reused), now additionally carrying `Configuration`. The
`nextIndex <= snapshotIndex` routing that selects `InstallSnapshot`
over `AppendEntries` is unchanged, and §2.2's `nextIndex =
lastIndex()+1` initialization for a newly appearing member is what
makes a learner take the ordinary backoff path into it rather than a
single unbounded log-tail message.

### 7.4 Compaction across configuration changes

`Core.Compact(uptoIndex)` (existing method) additionally sets:

```go
c.snapshotConfig, _ = c.ConfigAt(uptoIndex)   // NOT activeConfig
c.snapshotHasConfig = true                    // a locally created boundary always has one
if c.activeConfigIndex <= uptoIndex {
    c.activeConfigIndex = 0                   // the config now lives only at the boundary
}
```

`snapshotHasConfig` is set to `true` here unconditionally and
correctly: `ConfigAt` is total (§6.3's step 4 is a value, not a
failure), so a locally created boundary always *has* a configuration,
even if that configuration is the zero one for a node that has never
joined anything. `false` is reachable only from the two places that
genuinely mean "this boundary carries no configuration": decoding a
`FormatVersion 1` snapshot (§7.1) and installing a restore-staged one
(§7.6).

Using `ConfigAt(uptoIndex)` rather than `activeConfig` is the second
half of §23/C1's correction, and it is what makes `ConfigAt`'s
**boundary agreement** invariant (§6.3) true by construction:
`ConfigAt(snapshotIndex) == snapshotConfig` holds immediately after
every `Compact`, because that is literally the assignment.

This can never lose information: whoever calls `Compact` is, by
existing contract (`docs/snapshots.md` §3: `uptoIndex <= appliedIndex`),
only ever compacting already-applied, already-durable history, and the
caller (`internal/node.maybeSnapshot`) is required to have already
captured the **same** `ConfigAt(appliedIndex)` value into the new
snapshot's `Meta.Configuration` (§7.1) *before* calling `Compact` — the
same existing ordering discipline `LOG COMPACTION SAFETY` already
requires between "snapshot durable" and "truncate" for every other kind
of state. A debug-build assertion checks the two values are equal at
the call site.

#### Entries before, at, and after an `EntryConfig`, stated exhaustively

The architecture review asked for this explicitly; it is the clearest
way to see why `ConfigAt` is necessary and why `activeConfig` is
sufficient everywhere else.

| Position of index `j` relative to an `EntryConfig` at index `k` | Which configuration governs its **commit** decision | Which configuration is captured if a snapshot boundary lands at `j` |
|---|---|---|
| `j < k` (before) | The leader's **current** `activeConfig` at the moment `advanceLeaderCommit` runs — which may already be `C_new`, because commit decisions are always made under the live configuration. This is safe by Lemma 1: `C_new`'s majority intersects `C_old`'s, so an entry held by a `C_new` majority is held by at least one member of every `C_old` majority, and no future leader elected under either can miss it. | `ConfigAt(j) = C_old`. This is the case revision 1 got wrong: `activeConfig` may already be `C_new`, and `C_new` may never commit. |
| `j == k` (the config entry itself) | `C_new` — the entry's own configuration governs its own commit (§2.2). Combined with the current-term rule, this is what makes the removed voter unable to block its own removal (§4.3) and what makes a self-removing leader unable to count itself (§4.2). | `ConfigAt(k) = C_new`, correctly — but note a snapshot boundary can only land at `k` once `k <= appliedIndex`, i.e. once the entry is committed and applied, so this case never captures a speculative configuration. |
| `j > k` (after) | `C_new`, unambiguously. | `ConfigAt(j) = C_new` (absent a later `EntryConfig`), and again only reachable once `j <= appliedIndex`. |

The asymmetry is the whole point: **commit decisions are always made
under the live configuration (correct, and required by §2.2), while
boundary captures must be made under the configuration effective at
that index (correct, and required by §6.3).** One global field cannot
serve both, which is why statement C is false.

### 7.5 Restarted nodes whose local configuration is stale

Fully resolved by §6.3's single reconstruction algorithm — a "stale
local configuration" cannot exist as an ambiguous state after restart,
by construction: the node always adopts exactly `ConfigAt(lastIndex())`
over its own durable log/snapshot/bootstrap seed, deterministically,
with the identical priority order used by every other call site. If
that happens to be genuinely behind the *cluster's* current
configuration (this node missed changes while it was down), that is not
staleness in the node's own recovery — it is ordinary catch-up,
resolved the moment it reconnects (§4.4), identical in kind to any node
that missed ordinary committed writes while offline, and reachable
because §2.7 never filters the leader replication that performs it.

### 7.6 Backup and restore: state restoration and membership bootstrap are separate concerns

Revision 1 claimed `v0.3.0` Backup/DR was "unaffected, reused as-is"
and that "none of their guarantees is weakened." Both claims were
**false** once `Meta.Configuration` exists (§23/B3), and this section
replaces them.

#### The conflict

`docs/backup.md` already documents, as a supported and tested
operation, restoring a backup into a **differently-configured**
cluster:

> `clusterId` … **Diagnostic only** — restoring a backup into a
> differently-configured cluster (a different peer set, e.g. onto
> replacement hardware) is a legitimate, supported use of this feature,
> so this is never validated against a restore target.

> …restore that same backup into … (same `-cluster`/`-peers` as before,
> **or a new peer set** … this is fully supported).

Meanwhile §1.8 makes durable configuration override `-cluster`/`-peers`
unconditionally. A restored data directory carries a snapshot whose
`Meta.Configuration` names the **source** cluster's node IDs and
addresses. Composed naively, restoring onto replacement hardware would
silently ignore the operator's new peer set and try to form a cluster
with the dead original members.

#### The rule, in two parts — because a restored directory has two membership carriers

**`-restore-from` re-bootstraps `Configuration` from the operator's
`-cluster`/`-peers` flags, and never restores the source cluster's
runtime membership.** Delivering that requires acting on **both**
durable carriers of membership in a restored data directory, not one.

Revision 2 acted on the snapshot only, and therefore did not achieve
its own rule (§23/F1). `internal/backup.buildStaging` also copies the
source's **WAL suffix** — every entry in
`(m.LastIncludedIndex, until]`, verbatim, through `copyWALSuffix` — and
`ConfigAt` scans the log *before* it consults the snapshot boundary
(§6.3 step 1 precedes step 2). Any membership change the source
committed after its last snapshot is therefore present in the restored
log, and step 1 finds it first. That is the common case, not a corner
one: snapshots are threshold-driven, so a cluster that reconfigures and
is backed up shortly afterwards has its `EntryConfig` entries in the
suffix — including in §16's own first restore step, which backs up a
*post-membership-change* cluster. The concrete failure is total and
silent: every restored node computes `activeConfig` = the source's
members, finds its own `ID` absent from a **non-empty** configuration,
is therefore `selfRemoved()` rather than `neverJoined()` (§3.1), never
campaigns, never grants a vote, and answers clients `ErrNodeRemoved`
(§4.6) — a permanently leaderless cluster, in the one operation that
exists for when the original hardware is gone.

**Part 1 — the staged snapshot carries no configuration.**
`buildStaging` re-encodes the staged snapshot with
`Meta.HasConfiguration = false` and `Meta.Configuration` zeroed
(§7.1's explicit durable flag, not an empty-value sentinel — §23/F7),
re-encoding a v2 source frame rather than copying its bytes through. A
v1 source snapshot already decodes with `HasConfiguration == false` and
needs no transform.

**Part 2 — the staged WAL suffix carries no configuration either.**
While copying each source entry, `buildStaging` inspects its payload
framing (§6.1a) and, for any entry whose type byte is `EntryConfig`,
rewrites the payload into the **voided** form before appending it.
**The transform belongs to the restore path only**: the helper that
does the copying (`copyWALSuffix`) is shared with `Export`, and voiding
there would strip membership out of the backup itself — so the
transform is passed in rather than baked into the shared helper, and a
test pins that `Export`'s output still carries its `EntryConfig`
entries intact (§21 slice 3b, §23/G6). The voided form is:

```
voided EntryConfig payload (written only here, defined in internal/raft §2.5)
  term(8B) || 0xFF || entryType(1B)=EntryConfig ||
    0xF0                      // unchanged control marker
    19                        // membershipChangeKindVoided (§2.5's reserved range)
    requestIDLen(4B)  = 0
    targetIDLen(4B)   = 0
    targetAddrLen(4B) = 0
    fullConfigLen(4B) = 0
```

The entry keeps its **index and its term**, so the restored log's
index/term shape — and with it every log-matching relationship, the
`LastIncludedIndex` boundary, and `copyWALSuffix`'s existing
`idx != rec.Index` consistency assertion — is exactly what it was. What
it loses is the only thing restore must not carry: the embedded source
`Configuration`.

A voided entry is defined once and treated identically everywhere:

- `ConfigAt`'s step-1 scan **skips** it (§6.3 step 1), so a restored
  directory reaches step 2 with `snapshotHasConfig == false` and then
  step 3 — the operator's bootstrap flags, exactly what §7.6 promises.
- §2.6's four-shape validation does not apply to it: it proposes no
  transition, so there is no `(C_old, C_new)` pair to validate. A
  replica **accepts a voided entry on the ordinary replication path
  unconditionally**, exactly as it accepts an `EntryNormal` entry: it
  occupies an index and a term and establishes nothing.
- **A voided entry is never `Node.fail`-worthy on the accept path, and
  revision 3 was wrong to say otherwise (§23/G1).** Revision 3 wrote
  that "a voided entry arriving from a live leader is a
  should-never-happen treated as one — `Node.fail`", one sentence
  before stating that a restored directory legitimately *replicates*
  its voided entries onward. Both cannot hold, and the fail-closed
  reading breaks the flow §7.2 exists to protect: a learner added to a
  restored cluster before that cluster has taken its own snapshot
  receives the restored boundary by `InstallSnapshot` and then
  entries `snapshotIndex+1 …` by ordinary `AppendEntries` — **voided
  entries, from a live leader** — and would halt on its first join
  (DM-21 step 6's exact schedule). The rule was also not implementable:
  a receiver has no signal that separates "replicated out of a restored
  directory's durable log" from "invented by a live leader", because
  both arrive as an `AppendEntries` from the current leader and no node
  knows another node's restore boundary. Fail-closed therefore lives
  **only** where a discriminator genuinely exists, which is the emit
  side: `Core.ProposeConfigChange` can never construct a voided entry
  (§2.6a's check 5 admits exactly the four shapes, and `Voided` is not
  one of them), and a debug-build assertion in `ProposeConfigChange`
  pins that. The legitimate sources of a voided entry are a restored
  directory's own durable log and the ordinary replication of that log
  to every other member of the restored cluster — including members
  that join later.
- `internal/node.applyCommitted` applies it as a **no-op**: no
  `RecordMembershipOutcome`, no dial-table change, no waiter
  resolution, no generation check beyond §8.2's. It is a committed
  index with no effect — precisely what a restored cluster needs it to
  be.

**Why rewriting in place, and not the alternatives.** *Dropping* the
entries would renumber every later index, breaking both
`copyWALSuffix`'s index assertion and the snapshot-boundary
relationship. *Truncating* the restore at the last pre-membership index
would silently discard committed user data — unacceptable in the one
operation whose purpose is not losing data. *Folding the suffix into a
fresh snapshot* would require `internal/backup` to apply entries it
cannot know are committed (the manifest records `WALUntilIndex`, a WAL
extent, not a commit boundary), changing restore semantics for every
non-membership restore too. Rewriting in place changes exactly the
bytes that carry source membership and nothing else.

**Determinism across the restored cluster.** `docs/backup.md` §5 already
specifies that **every** node of the new cluster restores the
*identical* backup ("Because every node restores the identical backup,
all `N` copies start with byte-identical committed history"). The
voiding transform is a pure function of the backup bytes, so all `N`
staged WALs remain byte-identical to each other and log matching across
the restored cluster is unaffected. A node that joins *later* as a
learner receives the already-voided entries by ordinary replication
from the restored leader, so there is no path by which a source
`Configuration` reaches any node of the new cluster.

**The one scoped exception to "restored application state is exact."**
Voiding an `EntryConfig` discards the source's membership
`RequestID → Outcome` record for any membership change not yet captured
in the source's last snapshot. This is deliberate and correctly scoped:
those `RequestID`s name operations against a cluster that no longer
exists, targeting nodes that are not members of the restored one, and
§7.6's entire purpose is that they must not resolve there. Membership
outcome rows already captured *inside* the restored snapshot's FSM
state are untouched and restore byte-for-byte like any other FSM state;
they are harmless, because an outcome row carries no configuration —
the worst a stale source `RequestID` can do is return its recorded
outcome instead of proposing, which is the correct idempotent answer to
"did this already happen." **Every non-membership `RequestID` outcome,
every MVCC version, and the cluster generation remain exact and
byte-identical**, so `BACKUP INTEGRITY`, `BACKUP CONSISTENCY` and
`REQUEST OUTCOME STABILITY` are unchanged for every command kind that
has any meaning in the restored cluster. §16's restore assertions are
scoped to exactly this statement, and DM-21 is its deterministic
regression test, with a negative control that disables part 2 and
asserts the restore goes wrong.

#### Why this separation is correct rather than merely convenient

A backup captures **application state and the consensus boundary that
state corresponds to** — MVCC versions, outcome tables, the cluster
generation, `LastIncludedIndex`/`LastIncludedTerm`. Cluster
**identity** — which physical processes, at which addresses, holding
which certificates, constitute this Raft group — is a property of the
*deployment*, not of the data. These are independent by nature: the
same backup is legitimately restored onto the original hardware, onto
replacement hardware, into a smaller staging cluster, or onto a single
machine for forensic inspection, and the data is identical in all four
while the membership is different in all four. Conflating them would
make a backup un-restorable precisely when it is most needed (the
original machines are gone), which is the scenario DR exists for.
`docs/backup.md`'s `clusterId`-is-diagnostic-only decision already
encodes this separation for cluster *identity*; §7.6 extends the same
decision to cluster *membership*, which is the same kind of fact.

#### Cases covered

| Case | Behavior |
|---|---|
| Restore into a brand-new cluster with the same node IDs and addresses | `Configuration` comes from the flags, which happen to match the source. Indistinguishable from the source membership, by coincidence rather than by resurrection. |
| Restore with **different NodeIDs** (replacement hardware, new identities) | `Configuration` comes from the flags. The source's IDs appear nowhere. mTLS identity binding (§13) applies to the new IDs as it would for any fresh cluster. |
| Restore with **same NodeIDs, different addresses** (re-IP'd hosts) | `Configuration` comes from the flags, carrying the new addresses. This is additionally the sanctioned way to perform the in-place address mutation §1.6 declares out of scope for the live path. |
| Source cluster had learners, or a membership change in flight at backup time | Irrelevant: no source membership is carried, by either part of the rule. A restored cluster starts with exactly the voters the operator names and no learners. An in-flight (uncommitted) source `EntryConfig` in the suffix is voided like any other. |
| Source committed a membership change **after** its last snapshot | The entry is in the copied WAL suffix and is **voided** there (part 2). Without part 2 this is the case that resurrects source membership outright — the §23/F1 defect. |
| Restored **application state** | **Exact, and unchanged by this phase**, with the single scoped exception stated above (source *membership* `RequestID` outcomes not already inside the snapshot). Every MVCC version, every non-membership `RequestID → Outcome` record, and the cluster generation are restored byte-for-byte as `v0.3.0` already guarantees. `BACKUP INTEGRITY`/`BACKUP CONSISTENCY` are untouched. |
| Restoring a `v0.3.0`/`v0.4.0` backup under `v0.5.0` | Works, and needs neither part of the transform: the v1 snapshot decodes under §7.1's range check with `HasConfiguration == false` (part 1 is already satisfied), and a pre-`v0.5.0` WAL contains no `EntryConfig` entries at all (part 2 has nothing to rewrite). Pinned by the real-artifact fixture test in §7.1 and the restore test in §18. |

#### Consequent corrections elsewhere in this document

- §0's dependency list no longer claims `v0.3.0` is untouched; it names
  `internal/backup` as a package this phase modifies.
- §21 gains an `internal/backup` slice, covering **both** parts of the
  rule — the staged snapshot *and* the staged WAL suffix.
- §6.3's call-site table names `buildStaging` explicitly, because it is
  the only code outside `internal/raft`/`internal/node` permitted to
  reason about `EntryConfig` entries at all.
- §18 gains restore-compatibility rows.
- §19's doc-update list gains `docs/backup.md` (a new subsection
  stating the membership-bootstrap separation and the runbook step for
  restoring onto replacement hardware) and `docs/recovery.md`.


## 8. Compatibility / rolling upgrades integration

### 8.1 New generation boundary: `MaxSupportedGeneration` `1` → `2`

`internal/version.MaxSupportedGeneration` bumps to `2` as part of this
phase's own implementation. Generation `2` is defined as: "this binary
understands `Entry.Type` and its typed entry-payload framing (§6.1a),
`EntryConfig` entries including the `Voided` kind (§2.5, §7.6),
`Message.Configuration`/`Message.HasConfiguration` on
`MsgInstallSnapshotRequest` (§7.2), and `snapshot.FormatVersion 2`
including its `HasConfiguration` bit (§7.1)." This is a genuinely new, real correctness
gate — exactly the kind `MaxSupportedGeneration`'s own doc comment
already anticipates ("expected to move in lockstep with, at most, one
MINOR version at a time").

### 8.2 Membership changes are forbidden until finalization — and the gate is enforced on **both** sides

**Yes, unconditionally.** Revision 1 gated only the proposing side;
revision 2 gates both, because a leader-side-only check makes every
replica's correctness depend on a remote node's code being right
(§23/P3).

#### Leader side (proposal gate)

Every admin-API entry point (`AddLearner`/`PromoteToVoter`/
`RemoveServer`, §9) checks `n.clusterGeneration >= 2` — the existing,
durable, Raft-replicated value `v0.4.0` already introduced, read via
the existing `FSM.ClusterGeneration()`/`Node`-cached mirror, zero new
plumbing — **before ever calling `Core.ProposeConfigChange`**,
returning `ErrMembershipNotPermitted` / `412` otherwise, with an
actionable message ("cluster not yet finalized to generation 2; run
`/admin/upgrade/precheck` and `/admin/upgrade/finalize` first"). This
is §2.6a check 7.

#### Follower side (acceptance gate, new in revision 2)

`internal/node.applyCommitted`'s `entry.Type` dispatch refuses an
`EntryConfig` entry when this node's own **durable** cluster generation
is `< 2`: `Node.fail`, halting this node, exactly as
`applyControlEntry` already does for a `SetClusterVersionCommand`
whose `TargetGeneration` exceeds `version.MaxSupportedGeneration`. The
reasoning is identical to that existing check's: a node applying a
command it does not have the agreed capability to interpret must refuse
rather than guess, even though a correct leader makes this unreachable.

Note the deliberate asymmetry between *appending* and *applying*.
`Core` accepts and appends an `EntryConfig` on the ordinary log-matching
path without consulting the generation — `Core` has no access to FSM
state and must not acquire any (§2.4's dependency direction) — but the
entry can never be **applied**, and therefore can never resolve an
operator's request or update the dial table, on a node below
generation 2. Since the generation is itself Raft-committed and
monotonically forward-only, and since a `v0.5.0` leader only proposes
once *its own* committed generation is ≥ 2, any node that commits an
`EntryConfig` has by definition already committed the generation-2
finalize that precedes it in the same log. The follower gate is
therefore unreachable in correct operation and exists as defense in
depth — which is exactly the posture `ADR-0017` documents for its own
equivalents.

#### Exactly when membership changes become legal

Stated as a single unambiguous rule, because "after finalization" is
ambiguous about *whose* view:

> A membership change may be proposed by leader `L` at time `t` iff the
> `SetClusterVersionCommand` raising the cluster generation to `2` is
> **committed and applied in `L`'s own FSM** at `t` — i.e.
> `L.clusterGeneration >= 2`, the same durable value `wal.Metadata`
> carries.

Consequences, each worth naming:

- A `200` from `/admin/upgrade/finalize` is the operator-visible
  signal, but the authoritative condition is the committed generation,
  not the HTTP response.
- A follower that has committed but not yet applied the finalize entry
  cannot be the proposer, because it is not the leader; when it becomes
  leader it will have applied it (`applyCommitted` runs on the same
  event loop before any new proposal is accepted).
- After a leader failover mid-finalize, the new leader either has the
  finalize entry committed (membership changes legal) or does not
  (refused with `412`, retryable with the same `RequestID`). There is
  no third state.
- `MINIMUM VOTER INVARIANT`, §2.6's four-shape check, and §2.2a's P1/
  P2/P3 are all evaluated **after** this gate, so a pre-finalization
  request is rejected with `412` and never with a confusing
  membership-specific error.

### 8.2a Learners, `UpgradePrecheck`, and generation finalization (resolved)

Revision 1 left this undecided (§23/P2). It cannot stay undecided,
because generation `2` is not the last generation this project will
ever have: a future `v0.6.0` finalize will run in a cluster that
**does** contain learners, and `computePrecheck`/`Status.Ready` iterate
`n.cfg.Peers` today.

**Decision: learners appear in precheck status, and cannot block
finalization.**

- `PrecheckResult.Peers` is computed over `activeConfig`'s **voters and
  learners alike**, and each entry gains a `Role` field
  (`"voter"`/`"learner"`) so an operator can see a lagging learner's
  binary generation. Diagnostics must be complete.
- `PrecheckResult.Ready` — the value `FinalizeUpgrade` actually gates
  on — is computed over **voters only**. A learner that is unknown,
  unreachable, or running an older binary is reported but does not
  block.
- Rationale, and why this is the safe direction rather than the
  convenient one: `MIXED-VERSION QUORUM SAFETY` is a statement about
  the set of nodes whose agreement can commit anything. Learners never
  vote and never count toward any majority (`LEARNER
  NON-INTERFERENCE`), so a learner cannot participate in committing a
  command it does not understand. Letting one block finalization would
  give a non-voting node veto power over a cluster-wide operation —
  strictly worse, and precisely the "a learner's absence never blocks
  progress" property this phase otherwise guarantees everywhere.
- The residual risk — a learner left behind on an old binary, later
  promoted to voter — is closed at the **promotion** boundary instead,
  which is the right place: `/admin/membership/promote` refuses
  (`412`, `ErrPeerGenerationTooOld`) if the target learner's
  last-known `SenderGeneration` (existing `Node.peerGenerations`
  mirror, `v0.4.0`, unchanged) is below the cluster's committed
  generation, or is unknown. A node that cannot be verified to
  understand the cluster's agreed formats never becomes a voter. This
  is a leader-only, non-replicated precondition of exactly the same
  kind as §3.3's catch-up gate, and is listed alongside it there.

### 8.3 Old (`v0.4.0`) binary encountering dynamic-membership state

- **An `EntryConfig` entry on the wire**: §2.5's account — decoded as
  an ordinary opaque entry (its `Type` field silently dropped by gob),
  then fails closed at the existing
  `applyControlEntry`/`ErrUnknownControlCommand` path once committed,
  because `Data[0] == 0xF0` routes it to `DecodeSetClusterVersion`
  which rejects control-kind `16`/`17`/`18`.
- **An `EntryConfig` entry read from its own WAL**: §6.1a's account —
  a typed payload's `Data` begins with the `0xFF` sentinel,
  which is neither `ControlCommandMarker` nor a recognized
  `commitTxnCommandVersion`, so its unmodified `DecodeCommitTxn`
  returns `ErrUnsupportedCommandVersion` → `Node.fail`. **This is a
  second, independent fail-closed path** and both are separately
  asserted by tests (§18), because they fire in different situations:
  §2.5's on the wire (where gob framing applies and payload framing
  does not), §6.1a's on disk (where the reverse is true).
- **A `FormatVersion 2` snapshot**: §7.1's reader table — the old
  binary's existing strict `FormatVersion` equality check refuses it
  outright, `ErrUnsupportedVersion`, before decoding anything further.
- **A `v0.5.0`-produced backup**: same mechanism, since
  `internal/backup/restore.go` decodes via `snapshot.Decode`.
- **Because of §8.2, none of these should ever actually happen during a
  correctly-operated rolling upgrade** (an old binary is never still
  part of the cluster once membership changes are permitted at all) —
  all four are genuine defense in depth, exercised only by operator
  error (e.g. redeploying an old binary onto a node after the rest of
  the cluster has already moved past generation 2), exactly the posture
  `ADR-0017` already documents for its own equivalent cases.


### 8.4 Downgrade behavior

Unaffected, and not newly weakened: `ROLLBACK BOUNDARY HONESTY`
(unchanged mechanism) already means rollback is only ever safe
*before* the relevant `finalize` — since §8.2 forbids membership
changes before finalization to generation `2` in the first place, there
is no scenario where a downgrade needs to reason about "undoing" a
membership change that happened pre-finalize, because none can have
happened yet. A downgrade attempted *after* finalizing to generation
`2` (with or without any actual membership change having occurred yet)
is refused by the exact same, already-proven mechanism `v0.4.0`
established (`ApplySetClusterVersion`'s monotonic-forward-only rule;
an old binary's own fail-closed decode of anything generation-2-specific
it encounters) — this phase adds no new downgrade path and removes
none.

### 8.5 Mixed-version cluster restrictions

During a `v0.4.0`/`v0.5.0` mixed rolling upgrade (before finalizing to
generation `2`): the cluster continues operating in generation `1`'s
exact existing semantics — no `EntryConfig` entry is ever proposed
(§8.2), so `MIXED-VERSION QUORUM SAFETY` (unchanged, `v0.4.0`'s own
invariant, `docs/invariants.md`) continues to hold by the identical
argument `ADR-0017` already proved: every command a leader proposes
before finalize is byte-identical to a format every peer, old or new
binary alike, already understands.

---

## 9. Admin API / operator workflow

Four endpoints, mirroring `/admin/upgrade/precheck`/`finalize`'s
existing request/response/error-handling conventions
(`cmd/chronicledb-node/upgrade.go`) exactly, in a new
`cmd/chronicledb-node/membership.go`:

```
POST /admin/membership/add
  { "requestId": "<opaque>", "nodeId": "n4", "address": "host:port" }
  -> 200 { "status": "committed"|"aborted", "error": "", "leaderHint": "", "warning": "" }
  -> 409 (NotLeaderError, leaderHint set) | 409 (ErrConfigChangeInProgress)
  -> 412 (cluster generation < 2)
  -> 503 + Retry-After (ErrConfigChangeNotReady; §2.6a checks 2-3)
  RBAC: admin only. Audited.

POST /admin/membership/promote
  { "requestId": "<opaque>", "nodeId": "n4", "confirmVoterCount": 0 }
  -> 200 { "status": "committed"|"aborted", ... }
  -> 425 { "error": "learner not caught up", "lag": <entries> }  (§3.3, never proposed; RETRYABLE with the same requestId)
  -> 412 { "error": "peer generation older than cluster generation" } (§8.2a)
  -> 409 | 412 | 503 as above
  RBAC: admin only. Audited.

POST /admin/membership/remove
  { "requestId": "<opaque>", "nodeId": "n4", "confirmVoterCount": 0 }
  -> 200 { "status": "committed"|"aborted", "warning": "..." }
  -> 409 { "error": "confirmVoterCount required", "resultingVoterCount": 2 }  (§12.2)
  -> 409 | 412 | 503 as above
  RBAC: admin only. Audited.

GET /admin/membership/status
  -> 200 {
       "configIndex": 42,
       "committedConfigIndex": 42,
       "voters":   [{"id":"n1","address":"...","matchIndex":41}, ...],
       "learners": [{"id":"n4","address":"...","matchIndex":30,"lag":11,"generation":2}],
       "inProgress": false,
       "changesReady": true,
       "notReadyReason": ""
     }
  RBAC: admin, operator, and read-only (pure observability — mirrors
  /status's existing openness, unlike the three mutating endpoints
  above, which follow /admin/upgrade/*'s stricter admin-only precedent).
```

`changesReady`/`notReadyReason` expose §2.6a checks 2 and 3 (`P1`/`P2`)
so an operator polling before a change can see *why* the cluster is
momentarily not accepting one ("waiting for current-term commit after
election") instead of discovering it as a `503`.

`configIndex` is `Core.ActiveConfigIndex()`. **`committedConfigIndex`
is the second return value of `ConfigAt(commitIndex)` and nothing
else** (§6.3's call-site table, §6.3a's caller table). Revision 2
defined it as "`activeConfigIndex` when `activeConfigIndex <=
commitIndex` and the previous configuration's index otherwise" — a
second, independent configuration derivation, in direct violation of
§6.3's one-algorithm rule, and one whose "previous configuration's
index" clause named no mechanism at all. It is deleted (§23/F5). The
pair still makes the appended-vs-committed distinction directly
observable, which is the single most useful thing to have during an
incident; it now does so through the one algorithm.

**The status codes above are part of this plan, not an
implementation-time choice (§23/F-NB2).** They are asserted by §16's
real-process suite, published in `docs/membership.md`'s runbook, and
mirrored into §13.4's audit reasons, so silently changing one changes a
test, a runbook and an audit record together. The mapping is fixed:
`400` malformed request or illegal transition; `409` not-leader,
change-in-progress, or confirmation-required; `412` capability not
permitted; `425` learner not caught up; `503` transiently not ready.
**`425` and `503` are the retryable pair** — both are pre-proposal
refusals that record nothing and leave the `RequestID` verbatim
reusable, and both are retried by the handler within the request
context's deadline before being surfaced (§2.6a, §3.3); `400`, `409`
and `412` are terminal for that submission. `docs/membership.md`'s
runbook states this split explicitly, because treating `425` as a
failure is exactly the mistake the `503` polling instruction already
warns against (§11, §19 gate 8).
Because two genuinely distinct conditions share `412` (cluster
generation `< 2`, §8.2; target peer's generation too old, §8.2a),
**every error response body carries a machine-readable `reason` field
drawn verbatim from §13.4's fixed reason vocabulary** — a client must
never have to distinguish conditions by status code alone, and the
`reason` in the body and the `reason` in the audit record must be the
identical string for the identical refusal.

`authz.go` gains four new `Endpoint*` constants and decision-table rows
(`EndpointMembershipAdd`/`Promote`/`Remove`: `{admin: true, operator:
false, read-only: false}`; `EndpointMembershipStatus`: `{admin: true,
operator: true, read-only: true}`), added to `AllEndpoints` — the RBAC
decision-table test (`authz_test.go`, existing pattern) automatically
covers all four once added, no new test *shape* needed.

Every mutating endpoint requires and validates `requestId` (§10);
`nodeId` is validated as non-empty and, for `add`, `address` as a
syntactically valid `host:port` before ever reaching `Core` — an
operator typo produces an immediate `400`, never a proposed-then-
rejected log entry.

No broader management platform is introduced — exactly these four
endpoints.


## 10. Idempotency

All four mutating operations carry a client/operator-supplied
`RequestID` (`fsm.RequestID`, reused type), checked against
`FSM`'s new membership outcome table **before** ever proposing
anything (mirroring `Node.Propose`'s existing idempotency pre-check
pattern against `FSM`, and `ProposeControl`'s for `SetClusterVersionCommand`):

- **Lost response**: operator retries with the same `RequestID`; the
  leader (whoever it now is) finds the recorded `Outcome` in `FSM`
  (already committed and applied — or, if the original proposal never
  even reached the log, simply proceeds as a fresh attempt, §5's
  "before proposal" row) and returns it directly, without re-proposing.
- **Duplicate add / duplicate remove**: identical mechanism — a second
  `add` with the same `RequestID` and the same `{nodeId, address}`
  returns the original recorded outcome.
- **Conflicting operations with the same `RequestID`** (same
  `RequestID` reused for a *different* `nodeId`/`address`/operation
  kind): detected via a fingerprint comparison — the outcome table
  stores not just `Outcome` but a hash of the original request's
  `{kind, nodeId, address}` tuple, exactly mirroring
  `CommitTxnCommand`'s existing fingerprint-mismatch detection
  (`docs/transactions.md` §6) — a mismatch returns a distinct
  `ErrRequestIDConflict` rather than silently returning the unrelated
  original outcome. This is a deliberately *stronger* guarantee than
  `SetClusterVersionCommand`'s existing simpler policy (which
  skips fingerprinting, judged acceptable there because that command
  has no legitimate reason to be resubmitted with different fields at
  all) — membership commands, being genuinely different requests an
  operator or script could plausibly conflate, warrant the extra check.
- **Retry after leader failover**: the new leader's `FSM` state (via
  normal log replay or snapshot restore, §6) contains the identical
  outcome table entry — the retry resolves identically regardless of
  which node answers it.
- **Retry after restart**: the outcome table is snapshotted (§6.1) —
  identical guarantee to every other command kind's
  `REQUEST OUTCOME STABILITY`.
- **A request refused before proposal** (any of §2.6a's eight checks —
  not leader, `ErrConfigChangeNotReady`, `ErrConfigChangeInProgress`,
  `ErrInvalidTransition`, `ErrLastVoterRemoval`,
  `ErrMembershipNotPermitted`, `ErrConfirmationRequired`, or §3.3's
  `ErrLearnerNotCaughtUp`): **nothing is recorded.** No log entry, no
  outcome-table row, no partial state. The `RequestID` is untouched and
  the operator may resubmit it verbatim once the condition clears. This
  is the single most important idempotency property of the whole
  design, because §2.6a's two retryable checks (P1/P2) are *expected*
  to fire transiently after every leader election.
- **An operation whose config entry exists but has not committed yet**:
  the outcome table has no row (rows are written only by
  `applyCommitted`, §2.6), so a retry with the same `RequestID`
  re-enters §2.6a — where check 4 (P3, `activeConfigIndex >
  commitIndex`) refuses it with `409 ErrConfigChangeInProgress` rather
  than proposing a duplicate. The operator polls
  `/admin/membership/status` until `inProgress` clears, then either
  sees the outcome recorded (it committed) or retries afresh (it was
  truncated away). There is no window in which the same change is
  appended twice.
- **A `Voided` `EntryConfig` entry records nothing** (§7.6): it is
  applied as a no-op, writes no outcome row, and resolves no waiter.
  A restored cluster therefore starts with no membership outcome rows
  beyond those already inside the restored snapshot's FSM state, and
  the operator's `RequestID` space for the *new* cluster is entirely
  unused.
- **`confirmVoterCount` is excluded from the idempotency fingerprint**
  (§12.2): it is an authorization gesture about one submission, not
  part of the operation's identity. Fingerprints cover
  `{kind, nodeId, address}` only.

---

## 11. Concurrent reconfiguration

**Decision: rejected/serialized, not permitted.** Serialization is
enforced structurally by `Core.ProposeConfigChange`'s three premises
(§2.2a, §2.6a checks 2–4), **not** by the single check revision 1
specified:

- **P3** (`activeConfigIndex <= commitIndex`) covers a second proposal
  inside one stable leadership term. This is revision 1's rule,
  retained.
- **P2** (`commitIndex >= pendingConfIndex`) covers a change inherited,
  possibly uncommitted, from a previous term's leader.
- **P1** (`termAt(commitIndex) == currentTerm`) covers the case neither
  of the other two can see: a change that exists on *some other node*
  and is invisible in this leader's own log. It is the premise §2.3's
  branch-confinement invariant actually needs (specifically W1: without
  P1, two *committed* configurations can be siblings), and its absence
  in revision 1 was the phase's single most serious defect (§2.3's
  DM-12 trace).

Concurrent changes are not merely undesirable, they are **proven
unsafe** (§2.3), so serialization is not a preference but a
requirement — and, per §2.3, revision 1's version of it was not
sufficient to deliver what it claimed.

Operator-visible behavior:

- A second change while one is genuinely in flight on this leader:
  immediate `409 ErrConfigChangeInProgress`. No ambiguity about whether
  the request "queued" — it did not; retry once
  `/admin/membership/status`'s `inProgress` clears.
- A change requested in the window after an election but before the
  current-term commit lands: `503 ErrConfigChangeNotReady` with
  `Retry-After`, and `/admin/membership/status`'s `changesReady: false`
  / `notReadyReason` naming which premise is outstanding (§9). This is
  a **new, expected, transient** state revision 1 did not have, and it
  is the price of P1. It resolves within one replication round of
  `proposeElectionNoOp` committing (§2.2a) on any cluster that has a
  functioning majority — and on one that does not, refusing a
  reconfiguration is the correct behavior anyway.


## 12. Quorum / availability semantics

`majority(n) = ⌊n/2⌋ + 1`. Concrete worked examples (voter counts
only — learners never affect any of these). Note that **every**
transition's own commit is evaluated against `C_new`'s majority
(§2.2), including the removal of the very node being removed and
including a leader's removal of itself (§4.2); revision 1's table
contained two mid-cell corrections on this point, now stated once,
correctly, up front.

| Transition | Old majority | New majority | Availability during the transition |
|---|---|---|---|
| **3 → 4** (promote a caught-up learner) | 2 of 3 | 3 of 4 | Commits under `C_new`'s majority (3 of 4). Momentarily *more* demanding than 2 of 3, but never blocking: the new voter is by definition already caught up (§3.3) and acks immediately. |
| **4 → 3** (remove) | 3 of 4 | 2 of 3 | Commits under `C_new`'s majority (2 of the 3 **remaining** voters) — strictly easier than the old 3 of 4, so removal can never be blocked by the node being removed being unavailable (§4.3). |
| **3 → 2** (remove) | 2 of 3 | 2 of 2 | Resulting cluster has **zero** fault tolerance. Permitted by the mechanism (Lemma 1 holds for every `n`), but gated by §12.2's explicit operator confirmation and flagged by the `warning` field. |
| **2 → 3** (promote) | 2 of 2 | 2 of 3 | Strictly improves fault tolerance; the add's own commit needs 2 of 3, satisfiable as soon as the new voter acks, without requiring both original members simultaneously — a genuine availability improvement path out of a degenerate 2-voter cluster. |
| **Self-removal, 3 → 2** | 2 of 3 | 2 of 2, **excluding the leader** | The self-removing leader does not count itself (§4.2), so it needs acks from **both** remaining voters. Strictly less available than revision 1's (unsafe) rule, and correct. |
| **Loss of a node during a transition** | — | — | Fully covered by §5's boundary table: the only thing that matters is whether the in-flight `EntryConfig` entry reached a majority of `C_new` before the loss — if yes, it is durably committed and unaffected (`LEADER COMPLETENESS`); if no, it is exactly as if it never happened. |
| **Leadership change during a transition** | — | — | The new leader refuses all further membership changes until §2.2a's P1/P2 hold (§5's new row, **DM-12**). |

### 12.1 Even-sized voter sets: permitted, discouraged, warned

**Decision**: even-sized voter counts are **permitted** by the
mechanism (Lemma 1 places no parity requirement on `n` — it holds for
every `n ≥ 1`) but are **operationally discouraged**, because an
even-sized voter set never provides more fault tolerance than the next
smaller odd size while requiring a strictly larger quorum
(`majority(4) = 3` tolerates exactly one failure, identical to
`majority(3) = 2`, at the cost of one more required ack per commit) —
standard Raft/Paxos operational guidance, not a ChronicleDB-specific
consideration. **Mechanism**: `/admin/membership/add` and
`/admin/membership/remove`'s response includes a non-blocking
`"warning"` string field whenever the resulting voter count is even,
computed deterministically from `C_new.Voters`, entirely diagnostic —
never gates the operation, consistent with `docs/observability.md`'s
"diagnostic state is not a correctness dependency" rule applied to an
operational-quality signal rather than a safety one.

### 12.2 Minimum cluster size, and the sub-three-voter confirmation

Revision 1 got the layering here backwards (§23/C8): it warned
(non-blocking) about **even** voter counts and said nothing at all
about dropping to **one** voter, which is by far the more dangerous
outcome. Revision 2 separates the two concerns explicitly, because they
are different kinds of rule and must not be conflated.

#### Raft safety rule — `Core`, absolute, never overridable

`MINIMUM VOTER INVARIANT` (§2.6 shape 3, §2.6a check 6, §17): a
`RemoveServer` reducing `len(Voters)` to `0` is deterministically
refused by `Core`, on the leader at propose time and on every replica
at accept time. A zero-voter `Configuration` has an undefined
`majority()` and is a structurally broken state that must never be
reachable, not even transiently. This is quorum mathematics; there is
no flag, no confirmation, and no operator override. `ErrLastVoterRemoval`,
`400`.

#### Operator policy rule — `internal/node`/API, explicit, confirmable

Any operation whose **resulting voter count is fewer than 3** requires
explicit operator confirmation. This is a deployment-safety guard, not
a safety property, and it therefore lives entirely above `Core`:
`Core` neither knows nor cares about it, no replica re-validates it,
and it appears nowhere in §2.3's proof.

**Mechanism — `confirmVoterCount`.** `/admin/membership/remove` (and
`/admin/membership/promote`, for symmetry, though a promotion can only
ever *raise* the count) accepts an integer field `confirmVoterCount`.
Validation is deterministic and is evaluated in `internal/node` before
`Core.ProposeConfigChange` (§2.6a check 8):

```
resulting := len(C_new.Voters)             // computed from activeConfig + the requested shape
if resulting >= 3:
    confirmVoterCount is ignored entirely (may be absent or any value)
else if confirmVoterCount != resulting:
    refuse: ErrConfirmationRequired, 409,
            body { "error": ..., "resultingVoterCount": resulting }
else:
    proceed
```

Requiring the operator to state the *resulting count* rather than pass
a boolean `force: true` is deliberate: it cannot be satisfied by a
script that blindly sets a flag, it fails safe if the cluster changed
size since the operator last looked (the stated number no longer
matches, so the request is refused rather than silently doing something
different), and the refusal response tells them the correct number so
the retry is unambiguous.

**Deterministic validation.** `resulting` is a pure function of the
node's own `activeConfig` and the requested transition shape — the same
computation §2.6 already performs — so the check is deterministic on
whichever node serves the request, and a `409` from one leader means
the same thing as a `409` from any other.

**Idempotency interaction (important).** A refusal for missing or
mismatched confirmation happens **before anything is proposed**, so:

- nothing is written to `FSM`'s membership outcome table;
- the operator's `RequestID` is completely unused and may be resubmitted
  verbatim with the correct `confirmVoterCount`;
- the resubmission is a **fresh attempt**, not a duplicate, and is
  fingerprinted (§10) on `{kind, nodeId, address}` only —
  `confirmVoterCount` is deliberately **excluded** from the
  idempotency fingerprint, because it is an authorization gesture about
  *this* submission, not part of the operation's identity. Including it
  would make the corrected retry look like a conflicting reuse and
  return `ErrRequestIDConflict`, which would be actively unhelpful.

**Audit.** A refused confirmation still produces an audit record
(`membership.remove`, outcome `rejected`, reason
`confirmation-required`, including `resultingVoterCount`) — §13.4.
An operator being stopped by this guard is exactly the kind of event
an auditor wants to see, and `AUDIT COMPLETENESS` already requires one
record per call regardless of outcome.

**What remains permitted.** `3 → 2` and `2 → 1` are both technically
supported and both reachable with confirmation. `docs/membership.md`
states plainly that a 2-voter cluster has zero fault tolerance
(`majority(2) = 2`) and a 1-voter cluster has no fault tolerance and no
remaining purpose for Raft — and that a 1-voter cluster's loss of that
one node's disk is unrecoverable except from backup (§7.6). The
mechanism does not forbid what the operator explicitly and
specifically asks for; it just makes sure they asked for it.


## 13. Security

### 13.1 Admin authorization

Fully covered by §9/§13's RBAC table: all three mutating endpoints
`admin`-only; status readable by `admin`/`operator`/`read-only`. Every
call is audited (§13.4) via the existing `security.wrap` middleware
chain (`v0.2.0`, unchanged) — no new audit mechanism, just four new
audited actions, exactly like `v0.4.0`'s upgrade endpoints added two.

### 13.2 Peer TLS identity vs. configured membership

**Yes — a mismatch must fail closed.** When a newly added learner's
process connects (either it dials the existing cluster, or the
existing cluster dials it once the `AddLearner` entry names its
address, depending on `internal/transport`'s existing dial-direction
convention), the *existing* `v0.2.0` mTLS mechanism
(`RequireAndVerifyClientCert` plus `internal/identity`'s
certificate-subject-to-`NodeID` binding, unchanged) already refuses
any connection whose presented certificate identity does not match the
`NodeID` the connection claims to be. This phase adds exactly one new
check on top of that existing one: `internal/transport`, when
accepting or establishing a connection for a peer named in the current
`Configuration` (a `Member.ID`), verifies the connection's already-mTLS-verified
identity equals that `Member.ID` — a certificate that is valid and
trusted (passes the existing mTLS check) but presents a *different*
identity than the `Configuration` names for that address is rejected,
exactly as **fail closed**, not merely logged. This directly answers
this document's item-13 requirement: "adding a node whose certificate
identity does not match configuration must fail" — yes, both directions
(a node claiming to be `nodeId` without the matching certificate, and
a certificate presenting an identity the current `Configuration`
does not expect at that address).

### 13.3 Removed-node credentials and stale traffic

Two independent, layered defenses, neither of which is newly invented
by this phase — both already exist and simply continue to apply:

1. **§2.7's two Raft-level rules**, which under revision 2 bound a
   removed node's *disruption* rather than blanket-dropping its
   traffic: `MEMBERSHIP-SCOPED VOTE ACCEPTANCE` drops its
   `RequestVoteRequest`s at every member whose own `activeConfig` no
   longer contains it, and leader-contact suppression (Raft §4.2.3)
   makes every member that is hearing from a current leader ignore
   those requests regardless. Neither depends on the removed node's
   certificate still being valid — membership and leader-contact
   recency, not certificate validity, are the gates at this layer.
   Note the property being claimed is **liveness**, not safety: safety
   against a removed node never depended on message filtering at all
   (§2.3, §4.5), and revision 1 overstated this layer's role.
2. **Certificate revocation** (`v0.2.0`, unchanged, operational):
   ChronicleDB does not build a certificate revocation *mechanism* of
   its own (no OCSP/CRL checking exists today, and this phase does not
   add one — see §20) — an operator who wants to fully deny even
   *transport-level* connectivity from a removed node's process must
   separately revoke or let expire its certificate via their own CA
   tooling, documented as an operational step in `docs/membership.md`'s
   decommission runbook (mirroring how certificate rotation is already
   an operator-driven, `v0.2.0`-documented process, not an automatic
   one). §2.7 is what makes this *optional for safety* (the cluster is
   already safe against a removed node's Raft traffic without it) but
   still *recommended for defense in depth and to reduce useless
   background connection attempts*. Revision 2 additionally notes that
   §2.7 deliberately does **not** filter `AppendEntriesRPC`/
   `MsgInstallSnapshotRequest` on a membership basis, so a removed node
   that reconnects is always reachable by a legitimate leader and can
   always be told about its own removal (§4.4) — filtering that traffic
   would have made the removed node *harder* to decommission cleanly,
   not easier.

### 13.4 Audit completeness

Every one of the three mutating endpoints produces exactly one audit
record per call, following the identical existing invariant/mechanism
(`AUDIT COMPLETENESS`, `v0.2.0`, unchanged) — no new audit-log format,
just three new audited action kinds.

"Per call" means **per call, regardless of outcome** — this is already
what `AUDIT COMPLETENESS` requires, and it is worth stating explicitly
here because revision 2 adds several pre-proposal refusal paths that a
careless implementation might treat as "nothing happened, nothing to
log." Each of the following produces exactly one record, carrying the
outcome and a machine-readable reason:

| Outcome | Reason values |
|---|---|
| `committed` / `aborted` | the resolved `fsm.Outcome` |
| `rejected` | `not-leader`, `not-ready-inherited-suffix` (§2.6a-2), `not-ready-no-current-term-commit` (§2.6a-3), `change-in-progress` (§2.6a-4), `invalid-transition` (§2.6a-5), `last-voter-removal` (§2.6a-6), `generation-too-low` (§2.6a-7), `confirmation-required` (§12.2, with `resultingVoterCount`), `learner-not-caught-up` (§3.3, with `lag`), `peer-generation-too-old` (§8.2a) |

A `rejected` record for `confirmation-required` in particular is one an
auditor specifically wants: it is the record of an operator being
stopped from shrinking the cluster below three voters without saying so
explicitly.

---

## 14. Observability

New metrics (`internal/metrics`, following the existing
Counter/Gauge conventions), deliberately avoiding per-node-ID labels
(high cardinality — this document's own explicit instruction):

```
membership_voters_count        gauge   current len(activeConfig.Voters)
membership_learners_count      gauge   current len(activeConfig.Learners)
membership_config_index        gauge   activeConfigIndex
membership_change_in_progress  gauge   0 or 1 (activeConfigIndex > commitIndex)
membership_changes_total       counter labeled only by {kind=add|promote|remove, outcome=committed|aborted|rejected} — never by nodeId
membership_learner_max_lag     gauge   max over all current learners of (leader's LastIndex - matchIndex); 0 if no learners
membership_changes_ready       gauge   0 or 1 — whether §2.2a's P1+P2 currently hold on this leader (§2.6a checks 2-3); always 0 on a non-leader
membership_change_refused_total counter labeled only by {reason=...} using §13.4's reason vocabulary — never by nodeId
```

`membership_changes_ready` is deliberately a metric and not only a
`/status` field: the transient not-ready window after every election
(§11) is new in revision 2, and an operator watching a dashboard during
a rolling restart should be able to see it open and close rather than
discover it as an unexplained `503`. A sustained `0` on a node that
believes it is leader is a genuine alert condition — it means that
leader cannot commit in its own term.

Per-node detail (which specific learner is lagging, by how much, its
address) belongs on `/admin/membership/status` (§9), **not** on a
metrics label — exactly the same precedent `PeerGenerationInfo`
already established for `v0.4.0`'s per-peer generation detail
(`/status`, not `/metrics`).

`Status` (existing `internal/node.Node.Status()` struct) gains
`VoterCount`/`LearnerCount`/`ConfigIndex`/`CommittedConfigIndex`/
`ChangesReady` fields (§9), mirroring how it
already gained `ClusterGeneration` in `v0.4.0`. All of them — and the
per-member lists `/admin/membership/status` serves — are populated in
`refreshStatusLocked` **on the event loop**, from `Core.ActiveConfig()`
(a deep copy), `Core.ActiveConfigIndex()` and
`Core.ConfigAt(commitIndex)` (§6.3a). `Node.Status()` continues to
serve the published snapshot under `statusMu` from whatever goroutine
asks, exactly as it does today; nothing reads `Core` off the event
loop, and no HTTP handler derives a configuration of its own.

---

## 15. Deterministic fault harness scenarios

Extends `internal/fault` (the Phase 4/7 deterministic simulator,
`docs/testing-strategy.md` §3, §6) with a `Cluster.ProposeConfigChange`
method mirroring the existing `Cluster.Propose`/`Compact` wrapper
pattern. The harness must preserve `internal/node.processOutput`'s
persist-before-consume ordering (§2.3's final table row), since several
scenarios below depend on it.

New scenarios (named here; implemented as
`internal/fault/membership_test.go` and additions to
`internal/fault/chaos_test.go`'s combined schedule, at implementation
time). **DM-1 through DM-11 are revision 1's list, unchanged. DM-12
through DM-19 are new in revision 2, and each one exists because the
architecture review produced a concrete trace the revision-1 design did
not survive.**

- **DM-1 Leader failure during add, before commit** — leader crashes
  after appending an `AddLearner` entry but before a majority acks;
  assert the entry is either fully committed (by the new leader) or
  entirely absent, never partially adopted by some nodes.
- **DM-2 Leader failure during remove, after commit, before self-observation**
  — the specific self-removal case (§4.2): leader commits its own
  removal then crashes before stepping down on its own; assert the
  remaining voters correctly elect among themselves regardless. Assert
  additionally that the commit required acknowledgements from a
  majority of `C_new` **not counting the leader** (§4.2's corrected
  boundary): in a 3→2 self-removal, inject the loss of one remaining
  voter and assert the entry does **not** commit.
- **DM-3 Partition isolating the learner during catch-up** — assert no
  effect on voter-side commit progress (`LEARNER NON-INTERFERENCE`);
  assert the learner catches up fully once healed.
- **DM-4 Partition isolating a voter being removed, before the removal commits**
  — assert the removal still commits via the remaining majority-of-`C_new`
  (§4.3); assert the isolated node, once healed, correctly observes and
  adopts its own removal (§4.4).
- **DM-5 Election during an in-flight (uncommitted) config change** —
  randomized elections interleaved with a pending `AddLearner`/
  `RemoveServer`; assert `LEADER COMPLETENESS` extended to config
  entries (§5's "election during change" row) and assert
  `SERIALIZED MEMBERSHIP CHANGE` never observably violated (at most one
  outstanding change at any simulated instant, across all nodes' views).
- **DM-6 Repeated `RequestID` retry across a leader failover mid-change**
  — checked against the outcome table (§10), including the revision-2
  requirement that a pre-proposal refusal records **nothing**, so the
  same `RequestID` remains freshly usable.
- **DM-7 Crash/restart at every boundary in §5's table** — one subtest
  per row, each asserting the exact resulting state that row specifies,
  not merely "no crash." §5 gained two rows in revision 2, so this
  scenario gains two subtests (see DM-12, DM-13).
- **DM-8 Snapshot install carrying a different `Configuration` than the
  receiver's own stale one** — assert unconditional adoption (§7.2), no
  merge attempted; assert additionally that a `msg.Configuration` that
  disagrees with the installed snapshot's `Meta.Configuration` causes
  `Node.fail` rather than either value being silently preferred, **and
  the same for a `msg.HasConfiguration` that disagrees with
  `Meta.HasConfiguration` while both configurations are empty** — the
  flag-only disagreement a value-only check would miss (§23/G8).
- **DM-9 Stale removed node retrying elections indefinitely post-removal**
  — assert zero term bumps and zero disruption to the live cluster,
  **via both** of §2.7's rules separately: once against members whose
  `activeConfig` already excludes the removed node (Rule 1), and once
  against a member whose `activeConfig` still includes it but which is
  hearing from a current leader (Rule 2). Uses the existing
  `committedOracle`-style reference-model pattern extended to also
  track "current legitimate `Configuration`" as a second invariant
  checked after every scheduled action.
- **DM-10 Combined randomized schedule**: membership changes
  (add/promote/remove, each individually valid per §2.6) interleaved
  with the existing Phase 7 fault classes (elections, partitions,
  crashes, message drop/duplicate/delay, compaction) at the same
  seed-count discipline `docs/testing-strategy.md` §6.5 already
  establishes — checked after every action against `QUORUM CONTINUITY`,
  `SERIALIZED MEMBERSHIP CHANGE`, `CONFIGURATION BRANCH CONFINEMENT`
  (§2.3's **W1 and W2**, never a two-element live-set window — a window
  oracle raises false alarms on the safe three-configuration state
  DM-22 reaches, §23/F4), and the existing `committedOracle`, extended
  with a configuration-lineage oracle.
  **Oracle-independence requirement** (the same discipline §19 gate 3
  already imposes on DM-12): the lineage oracle reconstructs each
  node's configuration *itself*, from that node's own durable log and
  snapshot bytes, using its own straightforward implementation — it
  must never ask `Core` for the answer it is checking. An oracle that
  calls `Core.ActiveConfig()` cannot detect the bug classes it exists
  to detect (a lost or wrong `Entry.Type` on disk, §23/F2; a wrong
  `ConfigAt` fallback, §23/C2), because it would be asking the
  suspected component to grade itself.
- **DM-11 Disk-fault injection during an `EntryConfig` append** — reuses
  `MemoryStorage.FailNextAppends` (existing Phase 7 mechanism) targeted
  specifically at a config-change entry; assert the affected node never
  falsely reports success and the cluster continues correctly
  afterward.

### New in revision 2

- **DM-12 The T1 counterexample: a new leader must not stack a second
  transition onto an inherited, uncommitted one.** This is the direct
  regression test for §23/B1 and it is the single most important
  scenario in the list. Exact schedule, deterministic, no randomization:
  1. Build voters `{a,b,c,d}` plus a committed learner `e`; commit
     through index 9 in term 1 with `a` as leader.
  2. `a` proposes `Promote(e)`; deliver the resulting `AppendEntries`
     to `e` **only**; assert index 10 is **uncommitted** and that
     `a.activeConfig` is nonetheless already `{a,b,c,d,e}` (append-time-
     effective, §2.2).
  3. Partition `{a,e}` from `{b,c,d}`. Force an election; assert `b`
     wins in term 2 with `activeConfig == {a,b,c,d}` and
     `activeConfigIndex <= commitIndex` (i.e. **revision 1's P3 check
     would have passed**, which the test asserts explicitly so the
     scenario documents the defect it prevents).
  4. Before `b`'s election no-op commits, call
     `Cluster.ProposeConfigChange(b, Remove(a))`. **Assert it is
     refused with `ErrConfigChangeNotReady` / reason
     `no-current-term-commit`, and assert `b`'s log length is
     unchanged** — nothing entered the log.
  5. Assert the operator's `RequestID` has no outcome-table row and is
     resubmittable (§10).
  6. Heal `d`; let `b`'s no-op commit; assert `changesReady` becomes
     true and the same `Remove(a)` request now succeeds.
  7. Finally, re-run the *whole* schedule with the P1 gate disabled via
     a test-only build hook, and assert the harness's
     `committedOracle` **detects** the two-values-at-index-10
     divergence. A regression test that cannot fail when the mechanism
     is removed is not a regression test; this step is what proves DM-12
     is actually exercising the gate.
- **DM-13 The T2 counterexample: a snapshot must capture the
  configuration at its own boundary.** Regression test for §23/C1:
  1. Drive a leader to `appliedIndex == commitIndex == N` with the
     snapshot threshold about to trip.
  2. Propose an `EntryConfig` at `N+1`; do **not** deliver it to
     anyone.
  3. Trigger `maybeSnapshot`. Assert the written
     `Meta.Configuration == ConfigAt(N)` — the configuration **before**
     the pending change — and explicitly assert it is **not**
     `activeConfig`.
  4. Assert `Compact(N)` set `snapshotConfig == ConfigAt(N)` and that
     `ConfigAt(snapshotIndex) == snapshotConfig` (§6.3's boundary-
     agreement invariant).
  5. Truncate `N+1` away via a conflicting leader; assert
     `activeConfig` reverts to `ConfigAt(N)` — i.e. the revert is
     **not** a no-op, which is precisely what revision 1's
     `snapshotConfig = activeConfig` would have made it.
  6. Restart the node from its durable state alone; assert the
     recovered `activeConfig` is byte-identical to step 5's.
- **DM-14 Self-removing leader remains able to lead until commit.**
  Regression test for §23/C3 and the §2.7 narrowing: leader `a`
  proposes `Remove(a)`, follower `b` adopts `C_new ∌ a` at append time;
  assert `b` still accepts `a`'s subsequent `AppendEntriesRPC`
  (`LeaderCommit` advance, and a divergent-suffix repair if the change
  is abandoned), assert `b` does **not** campaign while hearing from
  `a` (Rule 2), and assert `a` steps down exactly at commit (§4.2).
  Run the abandoned-change variant too: kill the change before commit
  and assert `b` is repaired by `a` rather than stranded.
- **DM-15 A learner holding an uncommitted promote entry campaigns.**
  Asserts the outcome §2.3's table calls "surprising but correct": the
  learner is a Voter in its own `activeConfig`, may campaign, and may
  win with a majority of `C_new`; assert no safety-oracle violation and
  assert the resulting configuration lineage is still a valid chain.
  Exists so this behavior is never later "fixed" as a bug.
- **DM-16 `ConfigAt` determinism and prefix stability (property test).**
  Generate random logs containing `EntryConfig` entries at random
  indices, with and without a snapshot boundary, with and without a
  bootstrap seed; assert all four §6.3 invariants (determinism,
  monotone provenance, prefix stability, boundary agreement) for every
  `i` in range. Includes the never-snapshotted case that revision 1's
  two divergent algorithms disagreed on (§23/C2): assert that
  truncating every `EntryConfig` away yields `Config.Bootstrap`, not
  the zero `Configuration{}`.
- **DM-17 ReadIndex across a configuration change.** Regression test for
  §23/P1, §23/F3, and for the Phase 8 `BeginReadIndex` hang class:
  issue a `BeginReadIndex` whose acknowledgement quorum is still
  outstanding, then commit a membership change that alters the voter
  set; assert the read never resolves against a quorum basis that is
  not a majority of the configuration in force when it resolved, and
  that every terminal outcome it does reach is one of §4.2a's three
  table rows. **Three required sub-cases, all of §4.2a:**
  1. *Promote* — a new voter with `ackSeq == 0` enters the set; the
     read must wait rather than count it.
  2. *Remove* — a voter whose ack was being counted leaves the set; the
     read must stop counting it.
  3. *Self-removal* — the **leader itself** leaves the set. Revision 3
     specified this sub-case as a single schedule asserting both that
     the read does not resolve with one acking voter **and** that it
     then fails with `ErrLeadershipLost` at the step-down following
     commit. Those two assertions are not simultaneously satisfiable
     (§23/G2): with `C_new = {b,c}`, one acking voter is `1 < 2` for
     `checkPendingReads` *and* `1 < 2` for `advanceLeaderCommit`, so
     the `EntryConfig` never commits, no step-down occurs, and no
     `ErrLeadershipLost` is ever produced. The sub-case is therefore
     **two phases against one single read**, in this order — and the
     split between the caller-side and the node-side view of that one
     read is what makes both phases simultaneously satisfiable
     (§23/H3):
     - **Phase A (the F3 assertion).** 3→2 self-removal; deliver the
       `EntryConfig` to exactly one remaining voter, which acks. Assert
       `acked == 1 < majority(C_new) == 2` and that the read does
       **not** resolve — revision 2's mechanical `acked := 1` would
       have resolved it against a one-of-two "majority". Assert in the
       same breath that the entry has **not** committed and that the
       leader has **not** stepped down, so the test pins that the read
       is blocked rather than quietly failed, and that this is the
       third row of §4.2a's table (blocks to the caller's deadline, not
       an `ErrLeadershipLost`). Assert the deadline fires and **the
       caller observes a context error from `BeginReadIndex`**
       (`ctx.Err()`), never a result.
     - **Between the phases: the read is still alive on the node.**
       `BeginReadIndex`'s caller-side deadline (its second `select`,
       `internal/node/node.go`) returns `ctx.Err()` to the caller and
       does **nothing** to `n.pendingReads`: the `pendingRead` entry
       stays registered until `checkPendingReads` resolves or fails it,
       and its `resultCh` is buffered (capacity 1), so a later
       resolution neither blocks the event loop nor is lost. Phase A
       ends the *caller's* wait, not the read — which is why the two
       phases assert different outcomes without contradicting each
       other.
     - **Phase B (the clean-failure assertion), against that same
       still-pending read.** Heal the second remaining voter so both
       ack. Hold the read at `appliedIndex < pr.target` (the second row
       of §4.2a's table — arrange it by keeping `pr.target` above the
       applied watermark, e.g. by choosing the read's target at the
       `EntryConfig`'s own index). Assert the `EntryConfig` now commits
       against a genuine `C_new` majority **not counting the leader**,
       that the leader steps down at that commit, and that the
       **original** pending read fails cleanly with `ErrLeadershipLost`
       on the next `checkPendingReads` pass — observed by receiving
       that error **from that read's own buffered `resultCh`**, which
       the harness retains from Phase A.
     - **The test must not issue a second `BeginReadIndex` for Phase
       B.** A fresh read would be registered *after* the configuration
       change and would exercise a different schedule: §4.2a's row 2
       requires a read whose `term`/`requiredSeq` were captured
       **before** the self-removal, which is exactly the read Phase A
       left pending. Re-reading would silently convert this sub-case
       into a duplicate of the positive phase and lose row 2's
       coverage entirely.
     - A third, positive phase asserts §4.2a's first row on a separate
       read: both voters ack and `appliedIndex >= pr.target`, so the
       read **resolves**, and the harness checks the resolving basis
       was a majority of `C_new` with the leader excluded.
  Repeat all three sub-cases with a leader failover interleaved.
- **DM-18 Membership change refused before generation-2 finalization,
  on both sides.** Assert the leader-side `412` (§8.2) and, separately,
  that a synthetic `EntryConfig` delivered to a node whose durable
  generation is `< 2` causes `Node.fail` at apply time rather than
  silent acceptance (§8.2's follower gate).
- **DM-19 Sub-three-voter confirmation.** Assert `3 → 2` without
  `confirmVoterCount` is refused with `409` and
  `resultingVoterCount: 2`, nothing proposed, `RequestID` unused;
  assert the same request with `confirmVoterCount: 2` succeeds; assert
  a stale `confirmVoterCount` (cluster changed size in between) is
  refused; assert `RemoveServer` to zero voters is refused by `Core`
  itself regardless of any confirmation value (§12.2's two-layer
  split).

### New in revision 3

- **DM-20 `Entry.Type` survives the WAL round trip across the
  finalization boundary.** Direct regression for §23/F2, and the reason
  §6.1a's gate is the entry's type rather than the cluster generation.
  Deterministic, no randomization:
  1. Bring a three-node cluster to the point where the generation-2
     finalize is committed on the leader and applied there, but a
     chosen follower `F` has **not** yet applied it (its durable
     generation is still `1`).
  2. Propose the first `EntryConfig`; deliver to `F` the single
     `AppendEntries` that carries entry `k+1` **and** `LeaderCommit = k`
     together — the ordinary message shape, which forces `F` to persist
     the new entry before it applies the finalize.
  3. Assert at the **byte** level that `F`'s durable payload for that
     entry begins with `0xFF` followed by `EntryConfig` — asserted
     against the WAL record, never against the in-memory `Entry`, which
     is correct either way and would mask the defect.
  4. Restart `F` from its durable state alone; assert no `Node.fail`,
     and assert its recovered `activeConfig` is byte-identical to its
     pre-restart one.
  5. **Negative control**: re-run the identical schedule with the
     encoder gated on durable cluster generation (revision 2's rule)
     behind a test-only build hook, and assert step 3 **fails** and
     step 4 produces a stale configuration. A regression test that
     cannot fail when the defect is reintroduced does not count
     (§19 gate 3's discipline, applied here).
- **DM-21 A restore carries no source membership, from either
  carrier.** Direct regression for §23/F1:
  1. Build a source cluster, finalize to generation 2, and commit an
     `AddLearner` **and** a `PromoteToVoter` *after* its last snapshot
     boundary, so both `EntryConfig` entries live in the WAL suffix a
     backup will copy.
  2. Take a real backup; restore into a directory whose bootstrap flags
     name a **different** peer set (different `NodeID`s and addresses).
  3. Assert, on the staged directory, before any `Core` exists:
     (a) the staged snapshot decodes with `HasConfiguration == false`;
     (b) every `EntryConfig` entry in the staged WAL decodes to kind
     `Voided` with `fullConfigLen == 0`;
     (c) each staged entry's index **and term** equal the source's,
     entry for entry.
  4. Open the restored directory and assert `ConfigAt(lastIndex())`
     returns exactly the bootstrap configuration with index `0`, and
     that no source `NodeID` or address appears anywhere in the
     restored node's decoded state.
  5. Assert every **non-membership** `RequestID` outcome and every MVCC
     version is byte-identical to the source (§7.6's scoped exception
     asserted as an exception, not glossed over).
  6. Add a learner to the restored cluster *before* it has taken its
     own snapshot, forcing an `InstallSnapshot` whose
     `HasConfiguration` is `false`. The replication that follows
     necessarily carries the restored suffix's **voided** entries from
     a live leader, so assert explicitly, in this order: (a) the
     learner **appends** every voided entry it receives and does
     **not** `Node.fail` (§7.6's accept rule, the §23/G1 correction —
     this assertion is the whole reason step 6 exists, and revision
     3's fail-closed wording would have made it fail); (b) the
     learner's `ConfigAt(lastIndex())` remains the zero
     `Configuration` — `neverJoined()`, never `selfRemoved()` — while
     only voided entries have arrived; (c) the learner then adopts a
     real configuration from the replicated `AddLearner` entry that
     follows (§7.2) rather than failing or stalling.
  7. **Negative control**: disable part 2 of the transform (leave the
     suffix verbatim) and assert step 4 fails — specifically, that the
     restored node reports `selfRemoved()` and never elects a leader,
     which is the exact production failure §23/F1 describes.
- **DM-22 Three live configurations is a safe state, and the oracle
  knows it.** Calibration test for §23/F4; it guards the *oracle*, not
  the implementation.
  1. Run §2.3's worked schedule to completion: `C1` live on the
     partitioned `{a,e}`, `C0` still live on two voters, `C2` live on
     the new leader `b` after its term-2 no-op commits.
  2. Assert the configuration-lineage oracle reports **no** violation
     (W1 and W2 both hold), and `committedOracle` reports no
     divergence — i.e. the oracle does not fire on a safe state that a
     two-element-window oracle would have flagged.
  3. Assert `a`'s term-3 candidacy under `C1` gathers strictly fewer
     than `C1.majority()` votes, and that the run terminates with a
     single configuration chain — Lemma 3 observed, not assumed.
  4. Assert the converse, so the oracle is not merely permissive: a
     synthetic schedule that genuinely violates **W1** (two *committed*
     sibling configurations, reachable only via DM-12's test-only
     P1-disable hook) **is** reported by the same oracle.


## 16. Real-process proof plan

Extends `cmd/chronicledb-node`'s existing `-tags=integration` real-process
suite (`docs/testing-strategy.md` §4/§6.3) with
`cmd/chronicledb-node/membership_integration_test.go`:

- **Start a real 3-node cluster** — reuses existing `main_test.go`
  infrastructure unchanged.
- **Finalize to generation 2** — reuses the existing
  `/admin/upgrade/precheck`/`finalize` real-binary flow (`v0.4.0`,
  unchanged) as a precondition (§8.2). Assert a membership call
  *before* finalize returns `412` and one *after* succeeds, so the gate
  is proven rather than merely untested (§19 gate 5).
- **Assert the post-election not-ready window is real and transient**
  (§11): immediately after a forced election, assert
  `/admin/membership/status` reports `changesReady: false` with
  `notReadyReason: "no-current-term-commit"`, and that it flips to
  `true` without operator action. This is revision 2's most
  operator-visible behavior change and it must be observed against real
  processes, not only in the simulator.
- **Add a real fourth process as a learner** over genuine TCP/disk;
  poll `/admin/membership/status` (bounded polling, per
  `docs/testing-strategy.md` §4's existing discipline — never a fixed
  sleep) until caught up.
- **Continue writes throughout** — a background writer goroutine
  issuing ordinary `/propose` traffic across the entire test, asserting
  zero failed commits attributable to the membership change itself.
- **Continue SQL reads throughout** (new in revision 2) — a background
  goroutine issuing real SQL `SELECT`s across the entire test. Every
  SQL statement's `Session.Begin` calls `BeginReadIndex`
  (`internal/sql/engine.go`), so this is the only way to exercise the
  linearizable-read path against a changing voter set on real
  processes. Assert: no read blocks past its context deadline, and no
  read returns state older than a write acknowledged before it. Assert
  further that **every read failure is a clean `ErrLeadershipLost`-class
  error rather than a deadline expiry, for every step of this suite that
  runs against a reachable `C_new` majority** — which is every step, by
  construction: this suite never partitions a remaining voter away from
  the leader during a membership change. That scoping matters, because
  §4.2a's third row (no reachable `C_new` majority → the read blocks to
  the caller's deadline, with no `ErrLeadershipLost` to report) is a
  legitimate outcome that a blanket "never a timeout" assertion would
  wrongly call a bug; it is exercised deterministically by DM-17
  sub-case 3 phase A instead, where the partition can be injected
  exactly (§23/G2). Closes §23/P1 at the real-process level (DM-17
  closes it deterministically).
- **Promote the learner to voter**, **retrying on `425`** under the
  existing bounded-polling discipline (`docs/testing-strategy.md` §4 —
  bounded polling with a deadline, never a fixed sleep). The retry is
  not optional slack: the background writer runs throughout this suite,
  so `matchIndex == LastIndex()` is only momentarily true and a
  single-shot promote against `PromotionMaxLagEntries = 0` is a
  time-of-check/time-of-use race (§3.3, §23/G4). Retrying with the same
  `requestId` is always safe here because `425` is a pre-proposal
  refusal that records nothing (§10). The step's assertions, **stated
  against explicitly-set inputs rather than against any monotonicity of
  a moving observable (§23/H2)**, are:
  - the promote **eventually returns `200`** within the step's bounded
    polling deadline;
  - **every intervening refusal is a `425`** — never a `409`, `412` or
    `500`. This is the assertion that actually distinguishes the
    retryable pre-proposal refusal from every terminal one, and it is
    deterministic because the refusal's *kind* is a function of which
    check failed, not of timing;
  - **every reported `lag` is within a bound the test sets itself**
    (`maxAllowedLag`), from inputs it controls: the background writer is
    configured with a fixed in-flight write budget `W` (its `/propose`
    calls are issued with bounded concurrency, not free-running), the
    step asserts `lag <= W + 1` on every refusal, and the run fails if
    any refusal exceeds it. `lag` is `LastIndex() - matchIndex` against
    a `LastIndex()` the writer is still advancing, so it may legitimately
    **rise** between two attempts; revision 4's "decreasing-or-equal"
    assertion required a monotonicity nothing provides and is
    withdrawn. What matters operationally — and what this bound
    checks — is that the learner is *keeping up*, not that each
    observation improves on the last;
  - when the promote returns `200`, the learner's `matchIndex` **was**
    equal to `LastIndex()` at the instant `Core` evaluated it (§3.3),
    which is the state-based assertion "zero failed commits
    attributable to the membership change itself" rests on.
  A test that needs the refusal path itself to be deterministic sets
  its own threshold rather than relying on the shipped default: it
  configures `PromotionMaxLagEntries` explicitly (`0` to force the
  strict gate, or a value above `W + 1` to force first-try success)
  and asserts against the value it set. No assertion in this suite may
  depend on the shipped default remaining `0` (§23/G4, §23/H2).
- **Force a real leader failover** (`SIGKILL` the current leader,
  existing mechanism) with the 4-voter configuration active; assert a
  real election among the remaining 3 succeeds and writes resume.
- **Immediately after that failover, attempt a membership change** and
  assert it either succeeds or returns a retryable `503
  ErrConfigChangeNotReady` — never a `500`, never a hang, and never a
  second configuration entry stacked on an inherited one. This is
  DM-12's real-process counterpart.
- **Remove one of the original three voters** (not the new one) via a
  real admin call against the new leader.
- **Have the leader remove itself**, with the background SQL reader
  still running (§4.2, §4.2a). This step runs against a **healthy**
  cluster — every remaining voter reachable — which is precisely
  §4.2a's first and second table rows and excludes the third by
  construction. Assert: the removal commits only after
  acknowledgements from a genuine majority of `C_new` **not counting
  the leader**; the leader steps down exactly at commit; every SQL read
  in flight across that boundary either returns a correct result or
  fails with a clean `ErrLeadershipLost`, and none returns state older
  than a write acknowledged before it. Because the cluster is healthy,
  a read that ends at its context deadline instead is a **failure** of
  this step, and the assertion may say so — it is the reachable-quorum
  case, where §4.2a's third row does not apply. The blocked-read row
  is deliberately *not* exercised here; DM-17 sub-case 3 phase A
  exercises it deterministically, where a partition can be injected
  precisely. This is DM-17's third sub-case against real processes, and
  the only real-process exercise of §4.2a's self-exclusion on the read
  path.
- **Exercise the sub-three-voter guard** (§12.2): attempt a further
  removal taking the cluster to 2 voters without `confirmVoterCount`,
  assert `409`; retry with the correct value, assert success; assert
  both attempts appear in the audit log with the right reasons (§13.4).
- **Restart the surviving members** (`SIGKILL` + restart, existing
  mechanism) and assert: (a) every acknowledged `RequestID`'s recorded
  outcome is identical, on every surviving node, to what it was before
  the restart; (b) the recovered `Configuration` on every surviving
  node is byte-identical to what it was before the restart.
- **Snapshot/compaction across the whole sequence** — force enough
  writes that at least one real snapshot is created *while a membership
  change is in flight*, then restart from it and assert the recovered
  configuration matches `ConfigAt` at the snapshot boundary (DM-13's
  real-process counterpart, §23/C1).
- **Backup and restore, twice** (new in revision 2, §7.6):
  1. Take a real backup of the post-membership-change cluster —
     deliberately **without** forcing a snapshot first, so the
     membership entries are in the WAL suffix the backup copies, which
     is the §23/F1 case. Restore it onto **every** node of a new
     cluster with different NodeIDs and addresses (the identical-backup
     model `docs/backup.md` §5 already specifies). Assert: the restored
     cluster **elects a leader** and serves reads and writes; its
     `/admin/membership/status` reports exactly the operator-supplied
     membership; no source NodeID or address appears anywhere in it; no
     node reports `ErrNodeRemoved`; every committed row and every
     **non-membership** `RequestID` outcome is byte-identical to the
     source; and the source's own membership `RequestID`s are absent,
     per §7.6's scoped exception.
  2. Restore a **real `v0.4.0`-produced backup** (generated via the
     existing git-worktree technique) under the `v0.5.0` binary;
     assert it restores successfully and that the resulting cluster's
     configuration comes from the flags. This is the compatibility
     boundary §7.1 and §7.6 both depend on, proven with a genuine
     old-binary artifact rather than a re-implementation.
- **Mixed old/new binary variant** (mirroring `v0.4.0`'s own
  `mixed_version_test.go` git-worktree technique): start a cluster on
  the `v0.4.0` baseline binary, roll to `v0.5.0` binaries one at a
  time, **assert each `v0.5.0` node starts successfully against its own
  pre-existing `FormatVersion 1` snapshot** (§7.1 — the specific thing
  revision 1's format decision would have broken), finalize to
  generation 2, *then* perform the add/promote/remove/restart sequence
  above.


## 17. Safety invariants (to add to `docs/invariants.md` at implementation time)

Mirroring the existing catalog's exact format (statement, scope, why it
matters, mechanism, threatened by, proof/test obligations). Revision 2
added three invariants, rewrote two, and reclassified one from safety
to liveness. Revision 3 restates one of those three in the form that is
actually true (`CONFIGURATION BRANCH CONFINEMENT`, §23/F4), extends two
to boundaries they did not reach (`QUORUM CONTINUITY` to the read path,
§23/F3; `MEMBERSHIP RECOVERY DETERMINISM` to the WAL round trip,
§23/F2), and adds one (`RESTORE MEMBERSHIP ISOLATION`, §23/F1).
Revision 4 changes no invariant's statement; it clarifies the scope of
`QUORUM CONTINUITY`'s read-path half (§23/G2) and of
`RESTORE MEMBERSHIP ISOLATION`'s accept-side rule (§23/G1).

**Count, stated once and correctly (§23/G10).** Revisions 2 and 3
carried two different and both-wrong totals — §17 said "eleven in
total" and §19 gate 8 said "§17's ten invariants" — while this section
has always listed **thirteen** headings: **twelve** invariants this
phase adds to or rewrites in `docs/invariants.md`
(`LEADER-TERM CONFIGURATION GATE`, `CONFIGURATION BRANCH CONFINEMENT`,
`SERIALIZED MEMBERSHIP CHANGE`, `QUORUM CONTINUITY`,
`MEMBERSHIP RECOVERY DETERMINISM`, `RESTORE MEMBERSHIP ISOLATION`,
`LEARNER NON-INTERFERENCE`, `MEMBERSHIP-SCOPED VOTE ACCEPTANCE`,
`MINIMUM VOTER INVARIANT`, `SINGLE-SERVER TRANSITION SHAPE`,
`MEMBERSHIP CHANGE GENERATION GATE`,
`MEMBERSHIP REQUEST OUTCOME STABILITY`), plus one **pre-existing**
invariant this phase only *extends* (`NO SILENT FORMAT
MISINTERPRETATION`, which already exists in `docs/invariants.md` and is
amended rather than added). §19 gate 8 states the same numbers, and
§18's matrix has a row for each.

### `LEADER-TERM CONFIGURATION GATE` (new in revision 2)
**Statement**: a leader never appends an `EntryConfig` entry unless
`termAt(commitIndex) == currentTerm` **and**
`commitIndex >= pendingConfIndex` (the value `lastIndex()` had at
`becomeLeader`). **Why it matters**: this is the premise that makes
every leader's `activeConfig` a *committed* configuration, which is
what keeps the committed configurations on a single chain
(`CONFIGURATION BRANCH CONFINEMENT`'s W1) and therefore what makes the
single-server change proof valid at all. Without it, two leaders can hold
configurations differing by two voters with **disjoint** majorities,
elect conflicting leaders, and commit different entries at the same
index (§2.3's DM-12 trace). **Mechanism**: §2.2a's P1/P2, checked in
`Core.ProposeConfigChange` (§2.6a checks 2–3); converged by
`internal/node.proposeElectionNoOp`, which is hereby a documented
safety dependency and must not be removed. **Threatened by**: removing
or short-circuiting `proposeElectionNoOp`; any path that appends an
`EntryConfig` outside `ProposeConfigChange`; any future change that
lets `commitIndex` advance to an entry not in `currentTerm`.
**Proof/test obligations**: DM-12 (including its
mechanism-disabled negative-control step), DM-10; a direct unit test
per premise; §16's post-failover real-process assertion.

### `CONFIGURATION BRANCH CONFINEMENT` (restated in revision 3; was revision 2's `CONFIGURATION WINDOW`)
**Statement**, in two mechanically checkable parts:
**(W1)** the set of *committed* configurations is totally ordered by
the index of the `EntryConfig` that established each, forms exactly one
chain with no two committed configurations siblings, and every adjacent
pair differs by at most one voter; **(W2)** every live-but-uncommitted
configuration is a single-shape (§2.6) child of the newest committed
configuration present in the log of the node holding it.
**Why it matters**: adjacency is exactly the hypothesis Lemma 1 (§2.3)
needs; pairwise intersection says nothing about configurations two or
more apart, which can have fully disjoint majorities. W1+W2 are what
bound how far apart two live configurations can be, and Lemma 3 is what
converts that bound into safety when two of them are nonetheless
non-adjacent.
**Why it is no longer stated as a two-element window (§23/F4)**:
revision 2 claimed "at most two live configurations, and adjacent."
That is **false** — and the counterexample is revision 2's own repaired
DM-12 schedule, in which `C0`, `C1` and `C2` are simultaneously live
and `C1`/`C2` are two voters apart with disjoint majorities. That state
is safe, but by Lemma 3 (the superseded branch can never assemble a
majority, because every majority of it intersects a majority that
already holds a strictly newer term — or, when the superseded
configuration is a *committed* one whose successor has committed, a
majority holding the same term at a **higher index** — and
`isLogUpToDate` compares term before index and rejects on either),
**not** by any window. Revision 4 completes that lemma's case analysis:
revision 3 proved only the two-children-of-one-ancestor case and
asserted the rest away by a reduction that is false whenever two live
configurations hang off *different* committed ancestors (§23/G3).
An invariant that is false in a safe,
reachable state cannot be asserted by an oracle without producing false
alarms, which is why this restatement is a test-plan correction as much
as a prose one.
**Mechanism**: §2.3's theorem and Lemma 3, from premises P1/P2/P3
(§2.2a) plus §2.6's single-server shape restriction.
**Threatened by**: any relaxation of the proposal gates; any transition
shape that changes more than one voter; concurrent changes (§11); and —
for the oracle — any reintroduction of a live-set-size or adjacency-of-
all-live-configurations assertion.
**Proof/test obligations**: §2.3's proof and Lemma 3; DM-12; DM-10's
configuration-lineage oracle, which asserts W1 and W2 and reconstructs
configurations from durable bytes independently of `Core` (§15);
**DM-22**, which calibrates that oracle in both directions (quiet on
the safe three-configuration state, firing on an injected W1
violation); DM-16.

### `SERIALIZED MEMBERSHIP CHANGE` (rewritten in revision 2)
**Statement**: at most one membership-change command may be
outstanding at a time; a second is synchronously refused, never queued,
never entering the log, until the first resolves. **Mechanism**: three
independent checks, not one — P3 (`activeConfigIndex > commitIndex`,
covering a second proposal in one term), P2 (`pendingConfIndex`,
covering an inherited uncommitted suffix), P1 (current-term commit,
covering a change that exists only on *another* node and is invisible
locally). Revision 1 specified P3 alone and was insufficient.
**Threatened by**: any code path that appends a second `EntryConfig`
before the first commits, on any node, in any term. **Proof/test
obligations**: DM-5, DM-10, DM-12; a direct unit test per premise
asserting synchronous rejection with the correct error and an unchanged
log length.

### `QUORUM CONTINUITY` (rewritten in revision 2; extended to the read path in revision 3)
**Statement**: every **live** quorum decision — election, commit, **and
ReadIndex leadership confirmation** — uses the deciding node's current
`activeConfig`, and counts a node toward that quorum (its `matchIndex`,
or its own implicit self-ack) **iff that node is a Voter in that
`activeConfig`, with no self-exemption**; every **boundary** capture
(snapshot `Meta.Configuration`, `snapshotConfig` at compaction) uses
`ConfigAt` at that boundary's index. There is never ambiguity about
which configuration a given decision used, and the two rules are never
interchanged.
**Read-path extension (§23/F3)**: revision 2 stated the
no-self-exemption rule for `advanceLeaderCommit` only, while
`internal/node.checkPendingReads` counts `self` unconditionally
(`acked := 1`). Translated mechanically, a self-removing leader would
confirm a ReadIndex against a "majority" containing a non-member — in a
3→2 self-removal, one real voter standing in for two. §4.2a states the
rule once for both quorums.
**What this invariant does and does not claim about liveness
(§23/G2)**: it is a statement about the *basis* of a quorum decision,
never about a read always terminating in a ChronicleDB-level result. A
read held by a leader that cannot reach a majority of its current
`activeConfig` stays pending and ends at the caller's context deadline,
exactly as it does on a minority-partitioned leader today — §4.2a's
third table row. Blocking is the conservative direction and is not a
violation of anything here; **resolving** on a basis that is not a
genuine current-configuration majority would be.
**Why it matters**: the first half is required by §2.2
(append-time-effective); the second half is required because
`activeConfig` may reflect an uncommitted entry above the boundary that
can still be truncated. Revision 1 stated only the first half and
applied it to boundaries too, which was wrong (§23/C1).
**Mechanism**: `activeConfig.majority()` at every live call site,
including `checkPendingReads`, which reads the configuration once per
pass via `Core.ActiveConfig()` (§4.2a, §6.3a);
`ConfigAt(appliedIndex)`/`ConfigAt(uptoIndex)` at `maybeSnapshot`/
`Compact` (§6.3's call-site table, which is exhaustive).
**Threatened by**: any new call site that computes a majority from
something other than `activeConfig`; any boundary capture that reads
`activeConfig`; any quorum count that adds a node without testing that
node's voter status — `acked := 1` being the concrete example.
**Proof/test obligations**: §2.3's proof; DM-13, DM-16, DM-5, DM-10;
**DM-17's three sub-cases** including self-removal; §16's leader
self-removal step with the background SQL reader; a debug-build
assertion at both boundary call sites.

### `MEMBERSHIP RECOVERY DETERMINISM` (rewritten in revision 2; extended to the WAL round trip in revision 3)
**Statement**: `ConfigAt` (§6.3) is the **sole** mechanism by which any
configuration is ever derived, at every call site, with one fixed
priority order (latest retained configuration-establishing
`EntryConfig` → `snapshotConfig` when `snapshotHasConfig` →
`Config.Bootstrap` → zero value), and it is a pure, deterministic
function of durable state. After any restart, snapshot install, or
divergent-suffix repair, two nodes with byte-identical durable state
always compute identical configurations.
**The inputs must survive the round trip, not merely the algorithm
(§23/F2)**: `ConfigAt`'s step 1 selects entries **by `Entry.Type`**, so
`Entry.Type` must survive `encodeEntryPayload`/`decodeEntryPayload` for
every entry that has one. Revision 2's generation-gated header did not
guarantee that — a follower persists before it applies, so the first
`EntryConfig` after finalization was written untyped and vanished from
the scan on the next restart, producing exactly the divergent
configurations this invariant forbids. The gate is now the entry's own
type (§6.1a), which cannot be stale.
**Presence must be a fact, not an emptiness test (§23/F7)**: step 2 is
gated on the durable `Meta.HasConfiguration` bit, never on
`snapshotConfig` being non-empty. **Why it matters**: revision 1
described this algorithm three times with three subtly different
fallbacks, which disagreed on a never-snapshotted node — so restart and
truncation could produce different configurations from the same durable
state (§23/C2). **Mechanism**: §6.3, one function, no second
description anywhere in this document. **Threatened by**: any code path
that trusts a cached `activeConfig` across a restart; any second
reconstruction implementation (revision 2 grew one in §9's
`committedConfigIndex`, §23/F5); any call site added without being
added to §6.3's table; any encode-path decision gated on a value
updated on a different schedule from the write it gates; any test that
reconstructs configurations by asking `Core`.
**Proof/test obligations**: DM-16 (all four invariants,
property-tested), DM-7, DM-13, **DM-20** (the WAL round trip across the
finalize boundary, with its negative control), **DM-21** (a restored
directory), DM-10's independent lineage oracle; a direct test restarting
a node at each of §5's table rows.

### `RESTORE MEMBERSHIP ISOLATION` (new in revision 3)
**Statement**: a data directory produced by `-restore-from` derives its
`Configuration` **only** from the operator's `-cluster`/`-peers` flags.
No `Member.ID` and no address belonging to the source cluster is ever
adopted, replicated, or reported by the restored cluster, from either
durable carrier — the staged snapshot (`Meta.HasConfiguration = false`)
or the staged WAL suffix (every `EntryConfig` payload rewritten to the
`Voided` kind, at its original index and term).
**Why it matters**: `docs/backup.md` already documents restoring onto a
different peer set as supported, and §1.8 makes durable configuration
override the flags everywhere else. Without this invariant those two
rules compose into a silent, total failure: every restored node finds
its own `ID` absent from a non-empty configuration, is therefore
`selfRemoved()` (§3.1), never campaigns, never votes, and answers
`ErrNodeRemoved` — a permanently leaderless cluster, in precisely the
disaster-recovery scenario the feature exists for. Revision 2 stated
the rule and implemented only the snapshot half (§23/F1).
**Mechanism**: §7.6's two-part transform in
`internal/backup.buildStaging`; §6.3 step 1 skipping `Voided` entries;
§6.3 step 2 gated on `snapshotHasConfig`; §7.2's handling of an
installed snapshot that carries no configuration.
**Threatened by**: any restore path that copies WAL payloads through
without inspecting their type; treating snapshot-only clearing as
sufficient; letting `ProposeConfigChange` emit a `Voided` entry; making
`ConfigAt` step 1 match on `Type` alone without the kind check; **and —
in the opposite direction — rejecting or failing closed on a `Voided`
entry received on the replication path**, which halts every node that
joins a restored cluster before it has taken its own snapshot
(§23/G1).
**Proof/test obligations**: **DM-21** including its negative control
and its step 6 (a learner that receives voided entries from a live
leader appends them and stays up); §16's two restore steps, the first
of which deliberately backs up without snapshotting first; a
`buildStaging` unit test asserting index and term are preserved
entry-for-entry; a `ProposeConfigChange` debug-build assertion that no
emitted `EntryConfig` ever carries the `Voided` kind.

### `LEARNER NON-INTERFERENCE`
**Statement**: a learner never counts toward quorum, never votes, and
its absence/crash/slowness never blocks or delays commit progress for
the voting set — including never blocking a cluster-generation
finalization (§8.2a). **Mechanism**: `activeConfig.majority()` computed
over `Voters` only, everywhere (§2.2); `PrecheckResult.Ready` computed
over voters only (§8.2a). **Threatened by**: any commit-rule, election,
or precheck path that iterates `Learners` when computing a threshold.
**Proof/test obligations**: DM-3; a direct test asserting commit
progress is unaffected by an entirely absent/crashed learner; a direct
test asserting `Ready` is true with an unreachable learner present. Add
the explicit assertion that `internal/node.processOutput` persists
before consuming any peer acknowledgement (§2.3's final table row) —
previously undocumented, and load-bearing for every commit-count
argument in this document.

### `MEMBERSHIP-SCOPED VOTE ACCEPTANCE` (renamed and narrowed in revision 2; **liveness, not safety**)
**Statement**: a `RequestVoteRequest` from a sender that is not a Voter
in the receiver's own `activeConfig` is dropped before any term/log
state is touched; a node that is not a Voter in its own `activeConfig`
never grants a vote and never starts an election. Additionally, a node
that has heard from its current leader within the minimum election
timeout ignores any `RequestVoteRequest`, including a higher-term one
(Raft §4.2.3). **`AppendEntriesRequest`, `MsgInstallSnapshotRequest`,
and all responses are never filtered on a membership basis.**
**Why it matters, and what it does *not* provide**: this bounds the
disruption a removed-but-running node can cause. It is **not** a safety
mechanism — safety against a removed node comes entirely from
`CONFIGURATION BRANCH CONFINEMENT` + Lemmas 1 and 3, and holds with no
filtering at all.
Revision 1 classified this as safety and extended it to all message
classes, which broke legitimate transition traffic and could strand a
member awaiting log repair (§23/C3, §2.7's DM-14 trace).
**Mechanism ownership (§23/F6)**: Rule 1 is pure `Core` state. Rule 2
is `Core`'s single `heardFromLeader` boolean (§2.2), set when a
leader's `AppendEntries`/`InstallSnapshot` is accepted in the current
term and cleared on `InputElectionTimeout` or step-down — **`Core` owns
no clock and no tick counter**; `internal/node` owns the election
timer (`electionArmed`/`electionTicksLeft`), and revision 2's claim
that `Core` already tracked an election deadline as tick state was
simply false about this codebase.
**Threatened by**: extending the filter to replication traffic;
applying the "not in my configuration" test to a receiver's own
membership rather than to vote eligibility; conflating the `neverJoined`
and `selfRemoved` states (§3.1); moving Rule 2 into `internal/node`,
where `internal/fault`'s `Core`-level harness could not exercise it.
**Proof/test obligations**: DM-9 (both
rules, separately), DM-14, DM-15; a direct unit test with a stray,
never-a-member sender; a direct unit test asserting a learner's
election timeout is a no-op; a direct unit test asserting that a
non-Voter's disarmed election timer (no `ResetElectionTimer`, hence no
further `InputElectionTimeout`) cannot affect any vote decision,
because Rule 1's second clause is evaluated first.

### `MINIMUM VOTER INVARIANT`
**Statement**: a `RemoveServer` that would reduce the voter count to
`0` is deterministically refused by `Core`, on the leader at propose
time and on every replica at accept time; a zero-voter `Configuration`
is never reached or persisted. **Scope note (new in revision 2)**: this
is quorum mathematics and is **never** operator-overridable. The
separate requirement that any result below **three** voters carry an
explicit `confirmVoterCount` (§12.2) is operator policy, lives in
`internal/node`, is not re-validated by any replica, and appears in no
safety proof. The two must not be merged. **Proof/test obligations**:
a direct unit test attempting removal on a 1-voter cluster; DM-19 for
the policy layer.

### `SINGLE-SERVER TRANSITION SHAPE`
**Statement**: every `EntryConfig` entry that **establishes a
configuration** represents exactly one of the four transitions in §2.6,
relative to the immediately preceding active configuration; no other
transition is ever accepted by any replica.
**Scope (clarified in revision 4, §23/G1)**: the statement is about
configuration-**establishing** entries. A `Voided` entry (§2.5, §7.6)
establishes none — it carries no `Configuration` to validate — so it is
outside this invariant's scope entirely and is appended on the ordinary
replication path like any `EntryNormal` entry. Reading it *into* scope
is the failure mode: it halts every node that joins a restored cluster
before that cluster's first snapshot.
**Mechanism**: §2.6's leader-side check plus every
replica's defense-in-depth re-check, on establishing entries; plus a
`ProposeConfigChange` debug-build assertion that no emitted entry
carries the `Voided` kind, which is where the guarantee lives for
voided entries because it is the only place a discriminator exists.
**Threatened by**: a leader-side-only check with no follower-side
re-verification; **and, in the other direction, extending the
re-verification to voided entries.**
**Proof/test obligations**: a property test generating random
`(C_old, C_new)` pairs and asserting acceptance iff exactly one of the
four shapes holds; a direct test that a replica appends a received
`Voided` entry and does not fail (DM-21 step 6).

### `MEMBERSHIP CHANGE GENERATION GATE` (new in revision 2)
**Statement**: no `EntryConfig` entry is ever proposed by a node whose
committed cluster generation is `< 2`, and none is ever **applied** by
a node whose durable cluster generation is `< 2` — the latter fails
closed (`Node.fail`) rather than being silently accepted. **Why it
matters**: revision 1 gated only the proposer, making every replica's
correctness depend on a remote node's code being right (§23/P3).
**Mechanism**: §8.2's two gates. **Threatened by**: adding a propose
path that bypasses the admin API's check; treating the apply-side
refusal as recoverable. **Proof/test obligations**: DM-18 (both
sides); §16's before/after-finalize real-process assertions.

### `MEMBERSHIP REQUEST OUTCOME STABILITY`
**Statement**: a completed membership-change `RequestID` resolves to
the same recorded outcome after retry, restart, or leader failover,
indefinitely; and a request refused **before proposal** records nothing
at all, leaving its `RequestID` freshly usable. **Mechanism**:
§2.6/§10's `FSM`-owned, snapshotted outcome table, written only by
`applyCommitted`. **Threatened by**: recording an outcome from any
pre-proposal refusal path (§2.6a's eight checks); including
`confirmVoterCount` in the idempotency fingerprint (§12.2).
**Proof/test obligations**: DM-6, DM-12 step 5, DM-19; a direct
retry-after-restart and retry-after-failover test pair.

### `NO SILENT FORMAT MISINTERPRETATION` (extended by this phase)
Extended to four new surfaces, each with its own independent
fail-closed path and its own test (§18): the `EntryConfig` control-kind
range on the **wire** (§2.5), the `Entry.Type` payload sentinel on
**disk** (§6.1a), the snapshot `FormatVersion` **range** check (§7.1),
and — new in revision 3 — the snapshot frame's
`hasConfig`/`configLen` **cross-check** (§7.1: an invalid flag byte, or
a flag and length that disagree, is `ErrCorrupt` rather than
a shifted read). The `FormatVersion` surface relaxes strict version
equality to a bounded supported range; the invariant statement is
unchanged but its mechanism is not, and `docs/snapshots.md` §5 must say
so (§19). Note that the disk surface's fail-closed property now rests
on the type header being written for **every** typed entry (§6.1a's
corrected gate): a silently untyped `EntryConfig` would not be
misinterpreted by an old binary, but it *would* be misinterpreted by a
new one, which is the §23/F2 defect.


## 18. Test / proof matrix

| Failure mode / invariant | Unit | Deterministic fault | Property/random | Fuzz | Real-process | Race |
|---|---|---|---|---|---|---|
| `LEADER-TERM CONFIGURATION GATE` (new) | ✓ (one per premise: P1, P2, P3 — each asserting the error *and* an unchanged log length) | **DM-12** (incl. its mechanism-disabled negative control), DM-10 | ✓ (random election/propose interleavings) | — | §16's post-failover attempt | `-race` |
| `CONFIGURATION BRANCH CONFINEMENT` (restated in rev 3) | ✓ (majority-arithmetic table, all `n` 1–7; W1 chain-linearity checker; W2 single-shape-child checker) | DM-12, DM-10's lineage oracle (independent, durable-bytes reconstruction), **DM-22** (oracle calibrated both directions) | ✓ | — | §16 add/remove sequence | `-race` |
| `SERIALIZED MEMBERSHIP CHANGE` | ✓ (direct rejection test per premise) | DM-5, DM-10, DM-12 | ✓ (random interleavings) | — | §16 | `-race` on the chaos suite |
| `QUORUM CONTINUITY` (live half) | ✓ | DM-5, DM-10 | ✓ | — | §16 | `-race` |
| `QUORUM CONTINUITY` (boundary half — `ConfigAt` at snapshot/compact) | ✓ + debug-build assertion at both call sites | **DM-13** | DM-16 | — | §16's in-flight-snapshot step | — |
| `MEMBERSHIP RECOVERY DETERMINISM` / `ConfigAt`'s four invariants | ✓ (one test per §5 table row; `termAt(snapshotIndex) == snapshotTerm` immediately after `Compact`, the boundary P1 depends on, §2.2a) | DM-7, DM-13, **DM-20**, **DM-21** | **DM-16** (incl. the never-snapshotted bootstrap-fallback case, the `Voided`-entries-only case, and both `snapshotHasConfig` states) | — | §16 restart step | — |
| `LEARNER NON-INTERFERENCE` (incl. never blocking finalize) | ✓ (commit progress with a dead learner; `Ready` with an unreachable learner) | DM-3 | ✓ | — | §16 | `-race` |
| `MEMBERSHIP-SCOPED VOTE ACCEPTANCE` (Rule 1) | ✓ (stray never-a-member sender; learner never grants; learner election timeout is a no-op) | DM-9 | — | — | — | — |
| Leader-contact suppression (Rule 2, Raft §4.2.3; `Core`-owned `heardFromLeader`, node-owned clock) | ✓ (higher-term `RequestVote` ignored while leader contact is fresh; accepted once stale; flag cleared on `InputElectionTimeout` and on step-down; **a non-Voter's disarmed election timer cannot affect any vote decision**, because Rule 1's second clause is evaluated first; **and the composition with `PauseTicksForTest`/`electionTicksPaused`** — freeze ticks, accept a leader `AppendEntries`, then deliver a higher-term `RequestVote` and assert it is ignored with no term bump, §23/G9) | DM-9, DM-14 | — | — | — | — |
| Replication traffic is **never** membership-filtered | ✓ (self-removing leader's `AppendEntries` still accepted by a follower that adopted `C_new`) | **DM-14** | — | — | §16 | — |
| `MINIMUM VOTER INVARIANT` (Raft safety layer) | ✓ (1-voter removal refused by `Core`, leader and replica side) | — | — | — | — | — |
| Sub-three-voter confirmation (operator-policy layer) | ✓ (missing / stale / correct `confirmVoterCount`; fingerprint exclusion) | **DM-19** | — | — | §16's guard step | — |
| `SINGLE-SERVER TRANSITION SHAPE` | ✓ | DM-1..DM-19 (incidentally) | ✓ (the four-shape generator) | ✓ `FuzzDecodeEntryConfig` | — | — |
| `MEMBERSHIP CHANGE GENERATION GATE` (new) | ✓ (propose-side `412`; apply-side `Node.fail`) | **DM-18** | — | — | §16 before/after finalize | — |
| `MEMBERSHIP REQUEST OUTCOME STABILITY` (incl. "refused records nothing") | ✓ | DM-6, DM-12 step 5, DM-19 | — | — | §16 | — |
| `Entry.Type` WAL encoding (§6.1a) | ✓ `TestEntryPayloadSentinelNeverCollides`; ✓ round-trip for both forms; ✓ existing `v0.4.0` WAL payload decodes as `EntryNormal`; ✓ unknown type → `ErrUnknownEntryType`; ✓ **the header is written whenever `Type != EntryNormal` regardless of the node's durable generation** | **DM-20** (persist-before-apply across the finalize boundary, byte-level assertion, **with its generation-gated negative control**) | — | ✓ **`FuzzDecodeEntryPayload`** (never panics; never returns an unread type) | §16's mixed-binary variant | — |
| `NO SILENT FORMAT MISINTERPRETATION` — wire path (§2.5) | ✓ `TestControlKindRangesNeverCollide`; ✓ a real encoded `EntryConfig` payload fed to the *old* `DecodeSetClusterVersion` fails closed | — | — | ✓ | §16's mixed-binary variant | — |
| `NO SILENT FORMAT MISINTERPRETATION` — disk path (§6.1a) | ✓ a real **typed** `EntryConfig` payload fed to the *old* `DecodeCommitTxn` fails closed (independent of the wire path) | — | — | ✓ | §16's mixed-binary variant | — |
| Snapshot `FormatVersion` range and configuration presence (§7.1) | ✓ v2 round-trip incl. `Configuration`; ✓ **a real `v0.4.0`-produced snapshot fixture decodes with `HasConfiguration == false`**; ✓ `HasConfiguration == false` round-trips and is **distinguishable from a present-but-empty `Configuration`**; ✓ `hasConfig = 0x02` and `hasConfig = 0x00 with configLen != 0` each refused `ErrCorrupt`; ✓ synthetic `version=3` refused fail-closed; ✓ pre-finalize `Encode` byte-identical to `v0.4.0`'s | — | — | ✓ (fuzz `Decode` across both versions and both flag states) | §16's mixed-binary variant, incl. each node starting against its own pre-existing v1 snapshot | — |
| `InstallSnapshot` configuration authority (§7.2) | ✓ (`msg.Configuration` ≠ `Meta.Configuration` → `Node.fail`; **`msg.HasConfiguration` ≠ `Meta.HasConfiguration` → `Node.fail`, asserted separately with both configurations empty**, §23/G8) | DM-8 | — | — | — | — |
| `RESTORE MEMBERSHIP ISOLATION` (§7.6, new in rev 3) | ✓ (`buildStaging` sets `HasConfiguration = false`; **every `EntryConfig` in the staged suffix is rewritten to `Voided` at its original index and term**; restored `Open` reaches `ConfigAt` step 3; a `Voided` entry applies as a no-op and records no outcome; **a `Voided` entry is accepted unconditionally on the replication path** — the test asserts a follower that receives one from a live leader appends it and does **not** fail (§23/G1); `ProposeConfigChange` can never emit one, pinned by a debug-build assertion) | **DM-21** (both carriers, incl. the learner-added-before-first-snapshot step, **with its part-2-disabled negative control**) | — | ✓ (`FuzzDecodeEntryConfig` covers the `Voided` kind) | **§16's two restore steps** (new-identity restore of a backup taken *without* snapshotting first; real `v0.4.0` backup restored under `v0.5.0`) | — |
| Restored application state remains exact (§7.6) | ✓ (`BACKUP INTEGRITY`/`BACKUP CONSISTENCY` regression, unchanged assertions) | — | — | — | §16 | — |
| ReadIndex / linearizable reads across a configuration change (§4.2a) | ✓ (`checkPendingReads` against a changed voter set; newly promoted voter with `ackSeq == 0`; **self-removing leader's own self-ack not counted** — assert a 3→2 self-removal with one remaining acker does *not* resolve **and that no step-down and no `ErrLeadershipLost` follow, because the entry cannot commit either**, §4.2a's third row; one configuration read per pass) | **DM-17** (all three sub-cases; sub-case 3 in its **two phases plus the positive phase**, one per row of §4.2a's table, §23/G2; plus interleaved failover) | — | — | **§16's background SQL reader**, incl. the leader self-removal step (healthy-cluster rows 1–2 only, by construction) | `-race` |
| Leader self-removal (§4.2, corrected match-index boundary) | ✓ (propose self-removal, assert the leader's own `matchIndex` is **not** counted; assert commit requires a true `C_new` majority) | DM-2 (incl. the lost-remaining-voter variant), DM-14 | — | — | §16 | — |
| Stale removed node rejoin attempt (§4.4, §4.5) | ✓ | DM-9, DM-14 | — | — | ✓ (kill + restart-with-old-data-dir a removed node) | — |
| Uncommitted-promote learner campaigns (§2.3's "surprising but correct") | ✓ | **DM-15** | — | — | — | — |
| `ROLLBACK BOUNDARY HONESTY` (extended to generation 2 / snapshot v2 / entry-payload sentinel) | ✓ (byte-identical-at-generation-1 output for WAL payloads **and** snapshots) | — | — | — | §16's mixed-binary variant | — |
| Even-voter-count warning (§12.1) | ✓ (response field assertion) | — | — | — | — | — |
| RBAC / audit for all four endpoints, incl. every `rejected` reason (§13.4) | ✓ (decision-table test, existing pattern extended; one audit record per call per reason) | — | — | — | ✓ (real-process RBAC/audit test extended) | — |


## 19. Release acceptance gates (`v0.5.0`)

The phase is **not** complete on add/remove happy-path alone. Objective
gates, all required:

1. Every invariant in §17 has a passing test at the level(s) §18 maps
   it to; `go test ./... -race` green including the new suites.
2. The full §15 deterministic scenario list (**DM-1 through DM-22**)
   passes at the same seed-count discipline `docs/testing-strategy.md`
   §6.5 already establishes for Phase 7 (fast default in CI; a
   documented `CHRONICLEDB_CHAOS_SEEDS`-driven larger local/manual run
   clean).
3. **Every negative control passes** — this gate is about the tests,
   not the product, and a regression test that cannot fail when its
   mechanism is removed does not count:
   - **DM-12**: with the P1 gate disabled via the test-only build hook,
     the harness's `committedOracle` *detects* the two-values-at-one-index
     divergence.
   - **DM-20**: with the entry-payload type header re-gated on durable
     cluster generation (revision 2's rule), the byte-level assertion
     fails and the post-restart configuration is stale.
   - **DM-21**: with part 2 of the restore transform disabled, the
     restored node reports `selfRemoved()` and the restored cluster
     never elects a leader.
   - **DM-22**: the configuration-lineage oracle stays quiet on the
     safe three-configuration state **and** fires on an injected W1
     violation. An oracle that cannot do both is not calibrated, and
     one that reconstructs configurations by asking `Core` does not
     satisfy this gate at all (§15's independence requirement).
4. The §16 real-process proof passes end to end, including: the
   post-election `changesReady` window observed on real processes; the
   background SQL reader with zero hangs; the in-flight-snapshot
   restart; both restore steps; the sub-three-voter guard; and the
   mixed-`v0.4.0`/`v0.5.0`-binary variant in which **every node starts
   successfully against its own pre-existing `FormatVersion 1`
   snapshot**.
5. `FuzzDecodeEntryConfig`, `FuzzDecodeEntryPayload`,
   `TestControlKindRangesNeverCollide`, and
   `TestEntryPayloadSentinelNeverCollides` pass; **both** independent
   old-binary-fails-closed tests (wire path §2.5, disk path §6.1a)
   pass.
6. The real `v0.4.0`-produced snapshot fixture and the real
   `v0.4.0`-produced backup both decode/restore correctly under
   `v0.5.0` (§7.1, §7.6). These are generated once via the existing
   git-worktree technique and committed as artifacts; a re-implemented
   old encoder does **not** satisfy this gate.
7. Membership changes are proven refused pre-finalization on **both**
   sides (§8.2) by direct tests, and proven to work correctly
   immediately post-finalization by the same real-process suite.
8. Documentation landed in the same pass as the implementation
   (mirroring this project's established phase-completion discipline —
   every prior `v0.2.0`–`v0.4.0` release updated its full set of
   affected docs in the landing commit, never as a follow-up). The
   complete, authoritative list, expanded in revision 2:
   - `docs/invariants.md` — §17's **twelve** added-or-rewritten
     invariants, plus the amendment to the pre-existing
     `NO SILENT FORMAT MISINTERPRETATION` (thirteen headings in §17).
     All twelve are **new entries in the catalog** — none of them
     exists in `docs/invariants.md` today, which is why they are
     "(new, added when implemented)" in the sense
     `docs/enterprise-v1-plan.md` §5–§7 established; the "new in
     revision 2 / restated in revision 3" labels on §17's headings are
     relative to earlier revisions of **this plan**, not to the
     catalog. `NO SILENT FORMAT MISINTERPRETATION` is the sole
     pre-existing entry and is amended in place, never re-added.
     Revisions 2 and 3 gave two different wrong counts here and in
     §17; §23/G10.
   - `docs/raft.md` — a new "Dynamic membership" resolved-decisions
     section, mirroring the existing `§9`/`§10` Phase-implementation-note
     pattern; must cover `ConfigAt` and §6.3a's accessors, the §2.2a
     gates (including `termAt`'s snapshot-boundary behavior), §2.7's
     narrowed rules **and their ownership split** (`Core` owns
     `heardFromLeader`; `internal/node` owns the election clock), and
     §4.2a's one quorum-counting rule for commit and ReadIndex alike.
   - `docs/snapshots.md` — a new resolved-decisions section **plus** an
     amendment to §5 point 1: strict `FormatVersion` equality becomes a
     bounded `[MinReadVersion, FormatVersion]` range, with §7.1's
     reader-behavior table reproduced, including the
     `HasConfiguration` bit and the `hasConfig`/`configLen`
     cross-check.
   - **`docs/backup.md`** (new in revision 2, expanded in revision 3) —
     a subsection stating that restore re-bootstraps membership from
     operator flags, why data restoration and membership bootstrap are
     separate concerns (§7.6), that the re-bootstrap acts on **both**
     the staged snapshot and the staged WAL suffix, and what the one
     scoped exception to exact state restoration is (source membership
     `RequestID` outcomes); plus the replacement-hardware runbook
     step.
   - **`docs/recovery.md`** (new in revision 2) — §6.2's one new
     ordering step, and `ConfigAt` as the sole reconstruction
     mechanism.
   - `docs/upgrades.md` — generation 2 exists, what it gates, and
     §8.2a's learners-do-not-block-finalize decision.
   - **`docs/testing-strategy.md`** (new in revision 2) — DM-12's
     negative-control pattern, which is a reusable discipline this
     project has not previously written down.
   - A new `docs/membership.md` — operator runbook. Its **prose and
     ordering are the implementing session's to choose; its normative
     content is not** (§23/F-NB4). It must contain, at minimum:
     add/promote/remove procedures; the `confirmVoterCount` guard and
     the exact meaning of "state the resulting count"; the
     decommissioning runbook **including certificate revocation**
     (§13.3 — security-relevant: §2.7 makes revocation optional for
     safety, and the runbook must say that plainly rather than implying
     it is unnecessary); the ID-reuse guidance of §1.5 (security- and
     recovery-relevant); even-voter-count guidance; the 2-voter warning
     and the 1-voter warning **including that a 1-voter cluster's loss
     of its single disk is unrecoverable except from backup**
     (recovery-relevant); the fixed HTTP status codes and `reason`
     vocabulary of §9/§13.4, so operator scripts key on documented
     values; the post-election `changesReady: false` window (§11)
     with the instruction to poll rather than treat `503` as failure;
     and — the same instruction for the same reason — that **`425` on
     promote is a retryable, records-nothing refusal**, expected on a
     continuously written cluster because the shipped
     `PromotionMaxLagEntries` default is `0`, to be retried with the
     same `requestId` rather than treated as failure (§3.3, §23/G4).
   - A new `docs/adr/0018-dynamic-membership-architecture.md` —
     single-server vs. joint consensus; append-time-effective;
     `Core`-not-FSM placement; the §2.2a gates and why revision 1 was
     unsafe without them; `ConfigAt` vs. a single field; snapshot
     format Option A vs. Option B.
   - **`docs/adr/0017`** (new in revision 2) — a short "superseded for
     `v0.5.0`, and why" note on its format-version-not-bumped
     reasoning, rather than leaving it silently contradicted.
   - `docs/architecture.md` §1 — the "static three-node cluster" line
     updated; `internal/backup` added to the packages this phase
     touches.
   - `CHANGELOG.md`, `README.md`'s phase-summary paragraph.
9. `docs/enterprise-v1-plan.md` §8 itself is **not** rewritten (it
   remains the plan-level record it always was), consistent with how
   `v0.2.0`–`v0.4.0` never rewrote their own sections either.
10. No regression in any existing scenario: the full pre-existing test
    suite (Phases 1–10, `v0.2.0`–`v0.4.0`) remains green, unmodified in
    its own assertions (only extended where this phase's own changes —
    e.g. `Config.Peers` → `activeConfig.Voters`,
    `snapshot.Encode`'s new write-version parameter — mechanically
    require call-site updates, not assertion weakening).


## 20. Scope control — explicit non-goals

Restating and sharpening
[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §8's own
non-goals list, plus this document's own additions surfaced during
design:

- **Joint consensus / arbitrary concurrent multi-node changes** — §2.1,
  §11.
- **Automatic membership rebalancing or failure-triggered
  auto-replacement** — every change in this document is
  operator/admin-triggered; no control loop watches for a dead node
  and proposes anything on its own.
- **Multi-shard/range architecture, automatic placement/rebalancing
  across shards, cross-shard distributed transactions** — unaffected;
  still exactly one shard, always (§0).
- **Kubernetes operator, cloud control plane** — no such component is
  introduced or assumed by any admin endpoint here (they are plain
  HTTP, exactly like every existing admin endpoint).
- **In-place address mutation for an existing member** — §1.6;
  requires remove + re-add.
- **A permanent removed-`ID` tombstone/ledger** — §1.5; mTLS identity
  binding plus §2.7's two rules are the accepted defenses instead
  (and, per §4.5, the property they provide is bounded disruption —
  liveness — not safety, which never depended on them).
- **Certificate revocation mechanism (OCSP/CRL) triggered by removal**
  — §13.3; remains an operator/CA responsibility, unchanged from
  `v0.2.0`.
- **Arbitrarily large cluster sizes** — this phase targets and tests
  three-to-seven-voter clusters (matching
  [`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §8's own stated
  range); nothing prevents a larger cluster mechanically, but no
  performance/scaling claim is made or tested beyond this range.
- **Unrelated `v0.6+` admission-control or storage-lifecycle work** —
  untouched; this phase's only interaction with `v0.6.0`'s later
  Admission Control phase is the *design-review* dependency
  [`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §3 already
  names (membership changes become one more kind of control-plane
  proposal that phase's priority split must account for) — no code
  dependency in either direction.

---

## 21. Appendix: implementation slices and file-by-file change list

Suggested build order — each slice independently testable before the
next begins, mirroring this project's established
"implement → test against invariants → move on" discipline. Revision 2
adds slice 3b (`internal/backup`) and materially expands slices 1, 3,
and 4.

**Slice 1 — `internal/raft` core mechanism (no wire/durability change yet)**
- `types.go`: add `EntryType`, `Entry.Type`; add `Configuration`,
  `Member`.
- `config.go`: replace `Config.Peers []NodeID` with
  `Config.Bootstrap Configuration`; remove `Config.majority()`.
- `core.go`:
  - add `activeConfig`/`activeConfigIndex`/`snapshotConfig`/
    `bootstrapConfig`/`pendingConfIndex` fields;
  - **add `ConfigAt(index)` first** (§6.3) and implement every other
    configuration-derivation in terms of it — this ordering matters,
    because the whole point is that no second algorithm exists;
  - update `NewCore`/`NewCoreFromSnapshot` signatures to accept an
    initial `Configuration`;
  - rewrite **all eight** `cfg.Peers`/`cfg.majority()` call sites
    enumerated in §2.2's table to use `activeConfig` — including
    `handleRequestVoteResponse`'s election-win tally, which revisions
    1–3 omitted from the list (§23/G5); removing `Config.Peers` and
    `Config.majority()` in this same slice turns any remaining site
    into a compile error, which is the backstop, not the checklist;
  - `becomeLeader`: initialize `nextIndex`/`matchIndex` over voters
    **and** learners, `nextIndex = lastIndex()+1` for a member first
    seen mid-term, and `pendingConfIndex = lastIndex()` (§2.2a P2);
  - add `ProposeConfigChange` with §2.6a's checks 1–6 in order;
  - add append-time activation in `handlePropose` and
    `handleAppendEntriesRequest`, and revert-on-truncate via
    `ConfigAt(lastIndex())`;
  - add `neverJoined()`/`selfRemoved()` predicates (§3.1);
  - add `snapshotHasConfig` (§7.1) and thread it through `ConfigAt`
    step 2, `Compact`, and `handleInstallSnapshotRequest`;
  - add §6.3a's accessors — `ActiveConfig()` (**deep copy**),
    `ActiveConfigIndex()`, and the already-public `ConfigAt` — and no
    others;
  - add the `Voided` membership-change kind (§2.5) and make `ConfigAt`
    step 1 skip it; `ProposeConfigChange` must never be able to emit
    one (debug-build assertion), and the accept path must **never**
    reject or fail on one — it is appended like an `EntryNormal` entry
    and the four-shape check is not reached (§2.6's carve-out,
    §23/G1);
  - add `heardFromLeader` (§2.2) — set on accepted leader
    `AppendEntries`/`InstallSnapshot`, cleared on `InputElectionTimeout`
    and step-down — and **no tick state**: the election clock stays in
    `internal/node` (§2.7 Rule 2). Document the coupling with
    `PauseTicksForTest`/`electionTicksPaused` on **both** sides, add the
    frozen-clock unit test, and audit existing callers of that hook
    (§2.7's second interaction note, §23/G9);
  - `handleRequestVoteRequest`: §2.7 Rule 1;
  - `handleElectionTimeout`: no-op for a node that is not a Voter in
    its own `activeConfig` (§2.7 Rule 1's third clause) — a real
    behavior change, since it campaigns unconditionally today;
  - leader-contact suppression state and check (§2.7 Rule 2);
  - self-removal step-down on commit (§4.2).
- `messages.go`: add `Message.Configuration` **and
  `Message.HasConfiguration`** for `MsgInstallSnapshotRequest` — the
  pair, never the configuration alone (§7.2, §8.1, §23/G8).
- Unit tests: majority-arithmetic table; the four-shapes property test;
  **`ConfigAt`'s four invariants (DM-16)**, including both
  `snapshotHasConfig` states and a log containing only `Voided`
  entries; §2.2a's three premises, one test each, plus
  `termAt(snapshotIndex) == snapshotTerm` after `Compact`; §2.7's two
  rules and the non-Voter disarmed-timer interaction;
  revert-on-truncate; self-removal match-index exclusion;
  learner-election-timeout no-op; `ActiveConfig()` returns a copy that
  a subsequent append does not mutate.

**Slice 2 — `internal/fsm` outcome table (no `Core` dependency)**
- New file `internal/fsm/membership.go`: local mirror of the `0xF0`
  marker constant (with the `TestControlKindRangesNeverCollide` guard),
  `RecordMembershipOutcome`, the fingerprint-mismatch check (§10) over
  `{kind, nodeId, address}` only — **not** `confirmVoterCount` (§12.2).
- `FuzzDecodeEntryConfig`.

**Slice 3 — `internal/snapshot` format extension (Option B, §7.1)**
- `snapshot.go`: `Meta.Configuration` **and `Meta.HasConfiguration`**
  plus the package-local `Member`/`Configuration` types;
  `FormatVersion` `1`→`2`; **new `MinReadVersion = 1`**; replace
  `Decode`'s strict equality with the bounded range check and a v1
  decode path; decode the `hasConfig`/`configLen` pair with the
  cross-check of §7.1 (`ErrCorrupt` on an invalid flag or a
  flag/length disagreement); `Encode` takes the write-version as an
  explicit parameter (the generation gate).
- Tests: v2 round-trip; **a real `v0.4.0`-produced snapshot fixture in
  `testdata/`**; `version=3` refused; pre-finalize output
  byte-identical to `v0.4.0`'s; fuzz `Decode` across versions.

**Slice 3b — `internal/backup` (new in revision 2, both carriers in revision 3, §7.6)**
- `restore.go`/`buildStaging`, **part 1**: set
  `Meta.HasConfiguration = false` and zero `Meta.Configuration` on the
  staged snapshot, re-encoding a v2 source frame rather than copying
  bytes through.
- **Part 2**: while copying each source entry, inspect the payload
  framing (§6.1a) and rewrite any `EntryConfig` payload into the
  `Voided` form (§7.6), preserving the entry's index and term exactly,
  so `copyWALSuffix`'s existing `idx != rec.Index` assertion still
  holds. This is the half revision 2 omitted, and without it part 1
  achieves nothing (§23/F1). `internal/backup` does **not** import
  `internal/raft`; it matches on the documented payload framing, and a
  unit test in `internal/raft` pins that framing so the two cannot
  drift.
- **`copyWALSuffix` is shared by `Export` and `Restore`, so part 2 must
  be applied to the restore path only (§23/G6).** `copyWALSuffix`
  (`internal/backup/backup.go`) is called from `Export` — where it
  copies the live WAL into the backup — *and* from
  `buildStaging`/`restore.go`. Voiding inside `copyWALSuffix`
  unconditionally would strip membership out of the **backup itself**,
  destroying the source's own recoverable history and making the
  backup differ from the cluster it was taken from. Take the payload
  transform as an explicit parameter (a `func([]byte) []byte`, or a
  small `transformNone`/`transformVoidConfigEntries` pair), passed as
  identity by `Export` and as the voiding rewrite by `buildStaging`;
  a wrapper function around `copyWALSuffix` used only by restore is
  equally acceptable. What is **not** acceptable is an unparameterized
  edit inside the shared helper. A direct unit test asserts that a
  backup produced by `Export` still contains its `EntryConfig` entries
  unvoided, so the two directions can never be conflated.
- Tests: restore of a v1 (real `v0.4.0`) backup; restore of a v2 backup
  into a cluster with different NodeIDs/addresses; assertion that no
  source NodeID survives in **either** carrier; assertion that every
  staged entry's index and term are unchanged; assertion that restored
  application state and every **non-membership** `RequestID` outcome
  are byte-identical; **DM-21** including its negative control.

**Slice 4 — `internal/node` wiring**
- `storage.go`: `encodeEntryPayload`/`decodeEntryPayload` per §6.1a —
  the type header written **iff `Type != EntryNormal`**, never gated on
  the node's durable cluster generation (§23/F2) — including the
  `init()` sentinel non-collision guard and `ErrUnknownEntryType`.
- `node.go`: `Config.Bootstrap` (replacing `Peers`); §6.2's recovery
  ordering; `AddLearner`/`PromoteToVoter`/`RemoveServer` (mirroring
  `FinalizeUpgrade`'s shape, plus §2.6a checks 7–8 and §3.3's and
  §8.2a's promotion preconditions); `applyCommitted`'s `entry.Type`
  dispatch **with the follower-side generation gate** (§8.2);
  `maybeSnapshot` using `ConfigAt(appliedIndex)` **with the debug
  assertion** (§6.3, §7.4); `handleInstallSnapshot`'s
  `msg.Configuration`/`msg.HasConfiguration` vs.
  `Meta.Configuration`/`Meta.HasConfiguration` pairwise equality check
  (§7.2 — both fields, §23/G8);
  dial-address table updates on configuration change (§1.8);
  `ErrNodeRemoved` (§4.6); `Node.majority()` and `checkPendingReads`
  against `Core.ActiveConfig()` read **once per pass**, with the
  self-ack counted **iff self is a Voter** (§4.2a — the `acked := 1`
  line is the specific defect being closed, §23/F3);
  `computePrecheck`/`Status.Ready` per §8.2a; `refreshStatusLocked`
  populating `Status`'s new fields on the event loop, with
  `committedConfigIndex` from `ConfigAt(commitIndex)` and from nothing
  else (§23/F5); `Status()` field additions (§14).
- `internal/transport`: a mutex-guarded `SetPeers`-style method for
  live dial-table updates (the `addrs` map is already mutex-guarded;
  what is missing is a public mutation entry point); peer-identity-vs-
  configuration check (§13.2).

**Slice 5 — `cmd/chronicledb-node` admin surface**
- New `membership.go` (§9) including `confirmVoterCount` (§12.2) and
  the `changesReady`/`notReadyReason` status fields; `authz.go`
  decision-table rows; audit reasons (§13.4);
  `-tags=integration` real-process suite (§16).

**Slice 6 — deterministic fault harness (§15)**
- `internal/fault`: `Cluster.ProposeConfigChange`; DM-1 through
  **DM-22**, including the four test-only hooks their negative controls
  need (P1 disable for DM-12/DM-22, generation-gated payload encoding
  for DM-20, restore part-2 disable for DM-21); and the
  configuration-lineage oracle, which reconstructs configurations from
  each node's durable bytes **independently of `Core`** (§15).

**Slice 7 — documentation and release**
- §19 gate 8's complete doc-update list;
  `internal/version.MaxSupportedGeneration` bump to `2`;
  `CHANGELOG.md`; tag `v0.5.0` once every §19 gate is met.


## 22. Open questions — resolved during this planning pass

Every release-critical question this document's own instructions
required resolving has a concrete decision above; recorded here for
traceability rather than left as narrative. Entries marked **(rev 2)**
were either newly raised or materially changed by the architecture
review.

- *Where does membership state live, `Core` or FSM?* → `Core`, with FSM
  owning only the outcome-idempotency side-record (§2.4).
- *Effective-on-append or effective-on-commit?* → Append (§2.2),
  because §2.3's proof requires it — **but only for live decisions**;
  boundary captures use `ConfigAt` (§6.3). **(rev 2)**
- *Joint consensus or single-server?* → Single-server, serialized, with
  a formal proof (§2.3).
- **(rev 2)** *Is "at most one outstanding change" sufficient for that
  proof?* → **No.** It must be joined by the leader-term commit gate
  (§2.2a P1) and the inherited-suffix floor (P2); revision 1's
  single check was insufficient and a concrete counterexample exists
  (§2.3, DM-12).
- **(rev 2)** *Is one `activeConfig` field enough?* → **No.**
  `ConfigAt(index)` is required for snapshot creation and compaction
  (§6.3, §7.4); revision 1's `snapshotConfig = activeConfig` could
  durably capture a configuration that never committed (DM-13).
- **(rev 2)** *How many configuration-reconstruction algorithms are
  there?* → **Exactly one** (§6.3), with one fixed priority order and
  an exhaustive call-site table. Revision 1 had three, and they
  disagreed.
- *How does an old binary fail closed on a new entry type?* → Two
  independent paths: gob field-dropping plus the mirrored `0xF0`
  control marker on the **wire** (§2.5), and the `0xFF` entry-payload
  sentinel on **disk** (§6.1a). Both are separately tested. **(rev 2
  for the disk path, which revision 1 encoded ambiguously.)**
- *Does the snapshot format version bump?* → **Yes, Option B** (§7.1) —
  and the bump is only viable together with a real version-aware
  decoder retaining a v1 read path, which revision 1 omitted and which
  would have broken the `v0.4.0` → `v0.5.0` upgrade outright.
  **(rev 2)**
- **(rev 2)** *Does backup/restore carry membership?* → **No.**
  `-restore-from` re-bootstraps `Configuration` from operator flags
  (§7.6); data restoration and cluster-membership bootstrap are
  deliberately separate concerns, and `docs/backup.md` already
  documents restoring onto a new peer set as supported.
- **(rev 2)** *Should stale-member message rejection cover all message
  classes?* → **No.** `RequestVote` only, plus the standard Raft
  §4.2.3 leader-contact rule; replication traffic is never filtered on
  a membership basis, because a receiver's membership view is expected
  to be temporarily behind during a valid transition (§2.7).
- **(rev 2)** *Does a self-removing leader count its own `matchIndex`?*
  → **No.** It is excluded from the instant of append, like any other
  non-member (§4.2). Revision 1 said the opposite and would have been
  unsafe.
- *Are even-sized voter sets allowed?* → Yes, with a non-blocking
  warning (§12.1).
- *Is there a minimum voter count?* → `1`, enforced deterministically
  in `Core`; `0` is structurally forbidden. **(rev 2)** Separately and
  at a different layer, any result below **3** voters requires an
  explicit `confirmVoterCount` from the operator (§12.2) — policy, not
  quorum mathematics.
- *Do membership changes require finalization first?* → Yes,
  unconditionally. **(rev 2)** Enforced on **both** the propose side
  and the apply side (§8.2), with the exact legality condition stated
  as "the generation-2 finalize is committed and applied in the
  proposing leader's own FSM."
- **(rev 2)** *Do learners block generation finalization?* → **No.**
  They appear in precheck status with a `Role` field but `Ready` is
  computed over voters only; the residual risk is closed at the
  promotion boundary instead (§8.2a).
- **(rev 3)** *What gates the durable `Entry.Type` header?* → **The
  entry's own type**, never the node's cluster generation, which is
  updated at apply time and is therefore stale on every follower at the
  moment it persists (§6.1a, §23/F2).
- **(rev 3)** *Does clearing the staged snapshot's configuration make a
  restore membership-safe?* → **No.** The restored WAL suffix carries
  the source's `EntryConfig` entries and `ConfigAt` scans the log
  first. Restore voids those entries in place, at their original index
  and term, as part 2 of the same transform (§7.6, §23/F1).
- **(rev 3)** *Does the self-exclusion rule apply to ReadIndex?* →
  **Yes**, identically to commit: a node contributes to a quorum count
  iff it is a Voter in its own `activeConfig`, with no self-exemption
  on either path (§4.2a, §23/F3).
- **(rev 3)** *Are at most two configurations live at once?* → **No,
  and the design never required it.** Three can be live in a safe,
  reachable state. The checkable invariant is branch confinement
  (W1/W2) and the safety conclusion comes from it plus Lemma 3 (§2.3,
  §23/F4).
- **(rev 3)** *Is "`snapshotConfig` is empty" a usable test for "the
  snapshot carries no configuration"?* → **No.** Presence is an
  explicit durable bit, `Meta.HasConfiguration` (§7.1, §23/F7).
- **(rev 3)** *Who owns the election clock?* → **`internal/node`**, as
  it always has (`electionArmed`/`electionTicksLeft`). `Core` owns the
  vote decision and one derived boolean, and no tick state (§2.7,
  §23/F6).

### Non-blocking risks / residual open items for the implementing session

Revision 3 re-examined every item revision 2 left here against a single
test — *can this choice affect correctness, compatibility, recovery,
security, or test determinism?* — and **promoted four of the five out of
this list**. Only genuinely free choices remain.

**Promoted to plan-level decisions in revision 3:**

- **`PromotionMaxLagEntries` default → pinned at `0`, and `425` pinned
  as retryable** (§3.3, §2.6a, §9). Not because of safety (promoting a
  lagging learner is not a safety violation, §3.3) but because `0` is
  the conservative availability-protecting value and because the
  condition `Core` evaluates is then exact and observable. **Revision 3
  additionally claimed the pin made the gate "a pure function of
  observable state"; it does not** (§23/G4) — `lag == 0` is observed by
  one request and re-evaluated on the event loop afterwards, and §16's
  background writer moves `LastIndex()` in between, so a single-shot
  promote is a time-of-check/time-of-use race. The determinism is
  recovered by pinning the *other* half of the decision: `425` is a
  pre-proposal, records-nothing refusal and is therefore explicitly
  retryable with the same `RequestID`, retried by the handler within
  the request context and by §16's promote step under bounded polling.
  Still runtime-tunable; the *default* and the *retry semantics* are
  both fixed.
- **HTTP status codes → pinned** (§9). They are asserted by §16's
  real-process suite, published in `docs/membership.md`'s runbook, and
  mirrored into §13.4's audit reasons, so they are an interface with
  three consumers, not a cosmetic choice. Additionally, two distinct
  conditions share `412`, so every error body must carry a
  machine-readable `reason` from §13.4's fixed vocabulary, identical to
  the audit record's.
- **`Retry-After` → value free, semantics pinned** (§9, §2.6a). The
  number is tuning. The rules are not: the handler's internal retry
  window is bounded by the request context; a handler retries **only**
  pre-proposal refusals, never after an entry may have been appended,
  so at most one `EntryConfig` can result from one admin call; and a
  surfaced `503` is always safe to retry with the same `RequestID`
  because nothing was recorded (§10). **Revision 4 fixes the set of
  retryable refusals at exactly three** (§23/G4): P1's and P2's
  `ErrConfigChangeNotReady`, and §3.3's `ErrLearnerNotCaughtUp`/`425`
  — the last because `PromotionMaxLagEntries = 0` makes the promote
  gate a time-of-check/time-of-use test against a moving
  `LastIndex()`, not because the request is ever wrong. Every other
  refusal is terminal for that submission.
- **`docs/membership.md` → prose free, content pinned** (§19 gate 8).
  Its normative checklist is now enumerated, because three of its items
  are security- or recovery-relevant (certificate revocation on
  decommission, ID-reuse guidance, the 1-voter unrecoverability
  warning) and two are interface-relevant (status codes, the
  `changesReady` polling instruction).
- **Rule 2's placement → decided, not optional** (§2.7). Revision 2
  offered "inside `Core` as tick state or derived from the existing
  election-timer bookkeeping" as an implementer's choice; the first
  option does not exist (`Core` has no tick state) and the choice
  between layers changes whether `internal/fault` can exercise the
  rule at all. `Core` owns `heardFromLeader`; `internal/node` keeps the
  clock.

**What genuinely remains free for the implementing session:**

- The concrete `Retry-After` seconds value and the handler's internal
  retry budget, within the semantics above — for `425` as well as for
  `503`, whose retry *semantics* are now pinned identically (§23/G4).
- `docs/membership.md`'s prose, section ordering and examples, given
  the pinned content list.
- Log-line wording, metric help strings, and the exact shape of
  debug-build assertions.
- Whether `internal/backup`'s payload-framing matcher (§7.6 part 2) is
  a small local helper or a shared one, provided the framing contract
  it depends on is pinned by a test in `internal/raft`, and whether the
  restore-only application of it is expressed as a transform parameter
  to `copyWALSuffix` or as a restore-side wrapper around it — what is
  **not** free is applying it unconditionally inside that
  `Export`-shared helper (§23/G6).

---

## 23. Review traceability (revision 1 → revision 2 → revision 3 → revision 4 → revision 5)

Every finding from the correctness review of planning commit `6df4e68`
(revision 1 → 2, below), from the closure review of `d41ec09`
(revision 2 → 3), and from the final approval review of `8b4a7a7`
(revision 3 → 4, at the end of this section), with the section that
resolves it. Kept so a future reader can tell
which parts of this design are load-bearing *because something was
found to be wrong*, rather than merely asserted — and so no resolution
is silently dropped in a later revision.

### Architecture blockers

| ID | Finding | Resolution |
|---|---|---|
| **B1** | Serialization derived from `activeConfigIndex > commitIndex` does not survive leader failover; a new leader whose log lacks a predecessor's uncommitted `EntryConfig` proposes freely, producing two configurations two voters apart with disjoint majorities → two leaders, two different entries committed at one index. | §2.2a's P1 (leader-term commit gate) and P2 (`pendingConfIndex`); §2.3's rewritten proof with the configuration-window claim as an explicit theorem (restated in revision 3 as `CONFIGURATION BRANCH CONFINEMENT`, §23/F4); §2.6a's error/retry semantics; **DM-12** including its negative control; §17's new `LEADER-TERM CONFIGURATION GATE`. |
| **B2** | `snapshot.FormatVersion` `1`→`2` with `Decode`'s strict equality check would make `v0.5.0` unable to read any `v0.4.0` snapshot or backup — breaking the upgrade §16 is meant to prove. | §7.1's Option A/B decision with a real version-aware decoder, `MinReadVersion`, an exhaustive reader table, the generation gate on writes, and a **real `v0.4.0` snapshot fixture** as a test input; §19 gate 6. |
| **B3** | Durable `Meta.Configuration` overriding `-cluster`/`-peers` contradicts `docs/backup.md`'s documented "restore onto a new peer set" operation; "v0.3.0 unaffected / no guarantee weakened" was false. | §7.6 (restore re-bootstraps membership; why the concerns are separate; case table); §0's corrected non-weakening claim; §1.8's restore exception; slice 3b; §16's two restore steps; `docs/backup.md` added to §19's doc list. |

### Correctness gaps

| ID | Finding | Resolution |
|---|---|---|
| **C1** | Snapshot/compaction captured append-time-effective `activeConfig` rather than the configuration at `LastIncludedIndex`, durably recording configurations that may never commit and making revert-on-truncate a no-op. **Disproves "one field is enough."** | §6.3's `ConfigAt`; §7.1/§7.4's corrected captures; §7.4's before/at/after entry table; §5's new snapshot row; **DM-13**; debug assertions at both call sites. |
| **C2** | Two divergent fallback algorithms (§2.2's truncation revert vs. §6.2's recovery) disagreed on a never-snapshotted node. | §6.3 is now the **only** algorithm, with an exhaustive call-site table; §2.2 and §6.2 both call it; **DM-16** property-tests the bootstrap-fallback case specifically. |
| **C3** | `MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE` filtered replication traffic, breaking legitimate transition traffic (a self-removing leader is muted by its own followers; a stranded follower cannot be repaired). | §2.7 rewritten: Rule 1 (`RequestVote` only) + Rule 2 (Raft §4.2.3 leader-contact); replication traffic never filtered; §4.2/§4.4/§4.5/§13.3 rewritten consistently; **DM-14**; §17 reclassifies the rule as liveness, not safety. |
| **C4** | The §3.1 relaxation keyed on "`activeConfig` does not include itself," which is also the state of a *removed* node. | §3.1's relaxation **deleted** (unnecessary once §2.7 is narrowed) and replaced by the explicit `neverJoined()` / `selfRemoved()` predicates, with the rule that no condition is ever written against "does not include itself" alone. |
| **C5** | §4.2 claimed a self-removing leader's `matchIndex` still counts — contradicting §2.2 and unsafe (a 3→2 self-removal would "commit" with one real holder). | §4.2 rewritten with the exact boundary and the availability consequence stated honestly; §12's table gains a self-removal row; **DM-2**'s lost-remaining-voter variant; §18's dedicated unit row. |
| **C6** | The `Entry.Type` WAL encoding ("a trailing byte when non-zero") is undecodable. | §6.1a's leading `0xFF` sentinel header, generation-gated, with the full decode algorithm, collision guard, old-file compatibility, malformed/unknown behavior, and `FuzzDecodeEntryPayload`. |
| **C7** | Backup/DR format surface asserted unaffected but changed. | §7.6, slice 3b, §18's restore rows, §19's doc list. |
| **C8** | Minimum-size protections inverted: a non-blocking warning for *even* counts, nothing at all for dropping to 1 voter. | §12.2's two-layer split — `Core`'s absolute `>= 1` rule vs. the `confirmVoterCount` operator gate for any result `< 3`, with deterministic validation, fingerprint exclusion, audit record, and **DM-19**. |

### Proof / test gaps

| ID | Finding | Resolution |
|---|---|---|
| **P1** | No ReadIndex/linearizable-read coverage despite the quorum basis changing — the exact class of the Phase 8 `BeginReadIndex` hang. | **DM-17**; §16's background SQL reader; §18's dedicated row; slice 4's read-path note. |
| **P2** | Learners' role in `UpgradePrecheck`/`Status.Ready` undecided. | §8.2a: present in status with a `Role` field, never blocking `Ready`; residual risk closed at the promotion boundary. |
| **P3** | No follower-side generation gate. | §8.2's apply-side gate; §17's `MEMBERSHIP CHANGE GENERATION GATE`; **DM-18**. |
| **P4** | `Message.Configuration` duplicated `Meta.Configuration` with no authority rule. | §7.2: the installed snapshot's `Meta.Configuration` is authoritative; a mismatch is `Node.fail`; **DM-8** extended. |
| **P5** | Missing scenarios (in-flight-snapshot, post-truncation restart, uncommitted-promote campaign, the T1 shape). | **DM-12, DM-13, DM-15, DM-16**; §5's two new boundary rows feeding DM-7. |
| **P6** | §2.3's proof was pairwise only, with the inductive step asserted. | §2.3 rewritten: definitions, Lemma 1, Lemma 2, a theorem with P1/P2/P3 as explicit premises, plus the counterexample that defeats the revision-1 form. (Revision 2 stated that theorem over a two-element live-configuration window, which is false; revision 3 restates it as W1/W2 + Lemma 3 — §23/F4.) |
| **P7** | "A learner's election timeout is a no-op" was prose with no named guard; `handleElectionTimeout` campaigns unconditionally today. | §2.7 Rule 1's third clause; slice 1's explicit code-change note; §18's unit row. |
| **P8** | `nextIndex`/`matchIndex` initialization for a mid-term member unspecified; zero value would send the whole log tail in one message. | §2.2's `becomeLeader`/mid-term initialization paragraph; §7.3's note on the `InstallSnapshot` routing that follows from it. |

### Non-blocking items also addressed

| Finding | Resolution |
|---|---|
| `transport.addrs` has no public mutation entry point (the map is already mutex-guarded). | Slice 4's `SetPeers`-style method. |
| `internal/node.processOutput`'s persist-before-consume ordering is load-bearing for every commit-count argument but was undocumented. | §2.3's attempted-traces table; §17's `LEARNER NON-INTERFERENCE` proof obligations; §15's harness note. |
| §2.5's control-kind collision (`AddLearner = 1` vs. `controlKindSetClusterVersion = 1`). | Already caught and fixed within revision 1 itself (range `16+`, cross-package guard); retained unchanged, and confirmed correct against `internal/fsm/clusterversion.go` during the review. |

### Revision 2 → revision 3 (closure review of `d41ec09`)

The closure review confirmed 11 of the 15 revision-2 corrections fully
closed and found the remaining defects concentrated at three
*boundaries* where membership semantics were specified for
`internal/raft` but not carried across, plus one overstated theorem.
None of them required changing the consensus architecture.

#### Correctness gaps

| ID | Finding | Resolution |
|---|---|---|
| **F1** | §7.6's restore re-bootstrap acted on the staged snapshot only. `buildStaging` also copies the source's WAL suffix verbatim, and `ConfigAt` scans the log *before* the snapshot boundary — so any membership change committed after the source's last snapshot resurrects source membership. Every restored node then finds its own `ID` absent from a non-empty configuration, is `selfRemoved()`, never campaigns, and answers `ErrNodeRemoved`: a permanently leaderless cluster, silently, in the disaster-recovery path. | §7.6 rewritten as a **two-part** rule: part 1 sets `Meta.HasConfiguration = false` on the staged snapshot; part 2 rewrites every `EntryConfig` payload in the copied suffix to the new `Voided` kind (§2.5) at its original index and term. §6.3 step 1 skips `Voided` entries; §6.3's call-site table names `buildStaging`; §7.2 handles a configuration-less installed snapshot; new invariant `RESTORE MEMBERSHIP ISOLATION` (§17); **DM-21** with a part-2-disabled negative control; §16's first restore step now deliberately backs up *without* snapshotting first; slice 3b expanded (§21). |
| **F2** | §6.1a gated the durable `Entry.Type` header on the writing node's cluster generation — a value updated at **apply** time while entries are persisted **earlier in the same `processOutput` pass**. The first `EntryConfig` after finalization is therefore written type-less on every follower, so `ConfigAt`'s type-matching scan loses it on the next restart (stale configuration) and `applyCommitted` routes it to `ErrUnknownControlCommand` → `Node.fail`. The ordinary path, not an edge case. | §6.1a's gate is now the entry's own type (`Type != EntryNormal`), which cannot be stale. Rollback safety becomes structural (§8.2 means a pre-finalization node has no typed entry to write). `MEMBERSHIP RECOVERY DETERMINISM` extended to the WAL round trip (§17); **DM-20** with a generation-gated negative control; §18 and §21 slice 4 updated. |
| **F3** | §4.2 excluded a self-removing leader from its own **commit** quorum but said nothing about `internal/node.checkPendingReads`, which counts `self` unconditionally (`acked := 1`). Translated mechanically, a 3→2 self-removing leader confirms a ReadIndex with one real voter standing in for two — on the path this project has already had one real bug in. | New **§4.2a** states one rule for both quorums: a node contributes to any quorum count iff it is a Voter in its own `activeConfig`, no self-exemption; the configuration is read once per `checkPendingReads` pass via `Core.ActiveConfig()`. `QUORUM CONTINUITY` extended to the read path (§17); DM-17 gains a third, self-removal sub-case; §16 gains a leader-self-removal step with the background SQL reader; §18's read row rewritten. |

#### Proof / test gaps

| ID | Finding | Resolution |
|---|---|---|
| **F4** | `CONFIGURATION WINDOW` ("at most two live configurations, adjacent") is false as literally stated, and §2.3's induction step assumed a superseded uncommitted configuration stops being live — it does not, on the far side of a partition. Revision 2's **own** repaired DM-12 branch reaches three live configurations, two of them two voters apart with disjoint majorities. The design is safe, but by a different mechanism than the one proved; and an oracle asserting the stated invariant would raise false alarms on that safe schedule. | §2.3 restated: the checkable invariant is **branch confinement** (W1 committed-chain linearity, W2 uncommitted-branch confinement), and safety follows from W1+W2 plus the new **Lemma 3** (superseded-branch exclusion, which turns on `isLogUpToDate` comparing term before index). The three-configuration state is worked through explicitly as safe-and-reachable. §17's invariant renamed and restated; DM-10's oracle now asserts W1/W2 with an explicit **independence requirement** (reconstruct from durable bytes, never ask `Core`); new **DM-22** calibrates the oracle in both directions; §19 gate 3 extended to cover every negative control. |

#### Non-blocking items, all closed rather than deferred

| ID | Finding | Resolution |
|---|---|---|
| **F5** | §9 derived `committedConfigIndex` independently ("the previous configuration's index"), a second configuration derivation whose second clause named no mechanism, contradicting §6.3's exhaustive-call-site claim. | Deleted. `committedConfigIndex` is the second return value of `ConfigAt(commitIndex)` and nothing else; §6.3's call-site table and §6.3a's caller table both carry the row; §1.7 updated. |
| **F6** | §2.7 Rule 2 claimed `Core` already tracks an election-timer deadline as tick state. It does not: `internal/node` owns `electionArmed`/`electionTicksLeft` and `Core` sees only the discrete `InputElectionTimeout` event. | §2.7 rewritten with the ownership split stated against the actual code: `internal/node` keeps the clock unchanged; `Core` gains one derived boolean, `heardFromLeader`, set/cleared by events it already receives. The `internal/node`-side filtering alternative is explicitly rejected (it would put a vote decision where `internal/fault` cannot exercise it). The non-Voter disarmed-timer interaction is stated and given a unit test. §17 and §22 updated. |
| **F7** | `ConfigAt` step 2 tested "`snapshotConfig` is non-empty", overloading emptiness as "this snapshot has no configuration" — two different facts. | `Meta.HasConfiguration` is now an explicit durable bit in the v2 frame, cross-checked against `configLen` and fail-closed on disagreement (§7.1). `Core.snapshotHasConfig` mirrors it; `ConfigAt` step 2, `Compact`, and `handleInstallSnapshotRequest` all use the flag. Boundary agreement restated for both flag states. A test asserts "absent" is distinguishable from "present but empty". |
| **F8** | `termAt(commitIndex)` at the snapshot boundary was unstated, though P1 depends on it. | §2.2a states it explicitly as existing, unchanged behavior: the sentinel entry gives `snapshotTerm` at `i == snapshotIndex`, `commitIndex >= snapshotIndex` always holds, so the call is always defined and must **not** be "corrected" into an error. Unit test added (§18, §21 slice 1). |
| **F9** | No `Core` membership accessor was named, though §8.2a, §9, §12.2 and §14 all need one. | New **§6.3a** defines exactly four accessors, requires `ActiveConfig()` to return a **deep copy** (because `refreshStatusLocked` publishes into a struct another goroutine serves), states the event-loop-only rule, and gives the complete caller table. |
| **F-NB1..4** | `PromotionMaxLagEntries` default, HTTP status codes, `Retry-After`, and `docs/membership.md` prose were listed as free implementation choices. | Re-tested against "can this affect correctness, compatibility, recovery, security or test determinism?" and **four were promoted to plan-level decisions** (§22): the default is pinned at `0` for test determinism; status codes are pinned as a three-consumer interface and paired with a mandatory machine-readable `reason`; `Retry-After`'s semantics are pinned while its value stays free; `docs/membership.md`'s normative content is enumerated while its prose stays free. |

### Revision 3 → revision 4 (final approval review of `8b4a7a7`)

The final review confirmed **F1**–**F9** closed, verified every
statement revision 3 makes about the current codebase against the tree
(`checkPendingReads`'s `acked := 1`, `processOutput`'s
persist-before-apply ordering, the absence of tick state in `Core`,
`termAt`'s sentinel behaviour, `Decode`'s strict version equality,
`encodeEntryPayload`'s `term || data` shape, `copyWALSuffix`'s verbatim
payload copy and index assertion, and the `0xF0`/`2` control-byte
namespace), and re-confirmed the previously approved consensus
architecture intact on every point. It found four blocking items, all
of them **claims outrunning their mechanisms** or one rule
contradicting itself, and none of them requiring a mechanism change.

#### Correctness gaps

| ID | Finding | Resolution |
|---|---|---|
| **G1** | §7.6 declared, in consecutive sentences, that a voided `EntryConfig` "arriving from a **live leader**" is `Node.fail` **and** that a restored directory legitimately replicates its voided entries onward ("and the entries it replicates from it"), a claim §7.2 and DM-21 step 6 both depend on. Both cannot hold. The fail-closed reading halts any node that joins a restored cluster before that cluster has taken its own snapshot — it receives the restored boundary by `InstallSnapshot` (§7.2's blessed `HasConfiguration == false` path) and then the restored suffix's voided entries by ordinary `AppendEntries` — which is DM-21 step 6's exact schedule and the first operation a DR runbook performs. The rule is also not implementable: a receiver has no signal separating "replicated out of a restored directory's durable log" from "invented by a live leader", since both arrive as an `AppendEntries` from the current leader and no node knows another node's restore boundary. | §7.6's bullet rewritten: a voided entry is **accepted unconditionally on the replication path**, exactly like an `EntryNormal` entry, and fail-closed is confined to the emit side where a discriminator genuinely exists — `ProposeConfigChange` can never construct one (§2.6a check 5 admits only the four shapes), pinned by a debug-build assertion. `RESTORE MEMBERSHIP ISOLATION`'s "threatened by" list gains the **opposite** direction (rejecting a replicated voided entry) as a named hazard; DM-21 step 6 now asserts the acceptance explicitly, in order, as the reason the step exists; §18's row restated. |

#### Proof / test gaps

| ID | Finding | Resolution |
|---|---|---|
| **G2** | §4.2a claimed a read across a self-removal "either resolves against a genuine `C_new` majority or fails cleanly at step-down — never resolves against a phantom majority, and **never hangs**", and DM-17's self-removal sub-case asserted, of one schedule, both that the read does not resolve with one acking voter **and** that it then fails with `ErrLeadershipLost` "at the step-down that follows commit". Those are not simultaneously satisfiable: with `C_new = {b,c}`, one acking voter is `1 < 2` for `checkPendingReads` *and* `1 < 2` for `advanceLeaderCommit`, so the `EntryConfig` never commits, the leader never steps down, and no `ErrLeadershipLost` is ever produced — the read blocks to the caller's deadline. §16 carried the same overstatement ("every read failure is a clean `ErrLeadershipLost`-class error rather than a timeout"). | §4.2a now gives the **complete three-row case table** (resolve; `ErrLeadershipLost` at step-down; blocks to deadline when no `C_new` majority is reachable) and says plainly that the third row is not a hang in the Phase-8 sense but the ordinary behaviour of a linearizable read on a leader that cannot assemble its quorum — blocking is the conservative direction; resolving would be the bug. DM-17 sub-case 3 is restructured into **two phases plus a positive phase**, one per row (revision 5 additionally states which *view* of that one read each phase observes, and that no second read may be issued — §23/H3). §16's SQL-reader and self-removal assertions are **scoped to the reachable-majority case**, which is every step that suite runs, with the blocked-read row deliberately left to DM-17 where a partition can be injected exactly. `QUORUM CONTINUITY` gains an explicit "what this invariant does and does not claim about liveness" paragraph; §18's read row restated. |
| **G3** | §2.3's Lemma 3 reduced every non-adjacent pair of live configurations to "two distinct children of **one** common committed ancestor" by asserting that children of *different* committed elements "would differ by at most one voter along the chain". That is false: with a committed chain `{a,b,c} → {a,b,c,d} → {a,b,c,d,e}`, a partitioned node still holding the uncommitted child `{a,b,c,x}` of the first, and a child `{a,b,c,d,e,f}` of the third live on the majority side, the two are five voters apart and hang off different ancestors. The state is reachable (DM-22's shape plus two further committed transitions), so the proof covered nothing there — and the theorem's non-adjacent branch rests entirely on Lemma 3. | Lemma 3 rewritten with an explicit **anchor** definition and a complete case split: Case 1 (anchors coincide — revision 3's argument, retained verbatim and now correctly scoped); Case 2a (anchors differ, `C` a child of its anchor — reduced to Case 1 with `C' := C_{a+1}`, with the `term(L) < t` step proved from P1 plus `commitIndex` being a prefix bound); and **Case 2b** (anchors differ, `C` *is* its anchor — a committed configuration still live on a node that has not seen its successor), which revision 3 had no case for and which the term comparison alone cannot settle, because such a node may legitimately hold an entry of the successor's own term. Case 2b is closed by **index** domination: every node holding `C_a` necessarily lacks `C_{a+1}`'s entry at `(k, t)` (holding it would change its `activeConfig`), every majority of `C_a` meets the `C_{a+1}` majority that holds `(k, t)` by Lemma 1, and `isLogUpToDate` rejects on the index comparison at equal terms. A closing note records that a committed configuration is superseded only once its successor exists, which is why DM-22's three live configurations are safe *without* Lemma 3 being invoked on arrival. §17's `CONFIGURATION BRANCH CONFINEMENT` summary updated to name both rejection branches. **Case 1's own candidate-side term bound was still misstated by this revision and is corrected in revision 5 (§23/H1); the lemma's conclusion is unchanged.** |
| **G4** | §3.3 pinned `PromotionMaxLagEntries = 0` **for test determinism**, on the claim that with `0` "the gate is a pure function of observable state (poll until `lag == 0`, then promote)". It is a time-of-check/time-of-use race: `lag == 0` is observed by one HTTP request and re-evaluated by `PromoteToVoter` on the event loop afterwards, and §16 runs a background `/propose` writer across the entire test, so `LastIndex()` advances in between and the promote returns `425`. The stricter the threshold, the tighter the window — so the pin made §19 gate 4 less reliable, not more, in the name of determinism. It is an operational statement too: with the shipped default, promotion on a continuously written cluster needs retry or quiescence. | Both halves of the decision are now pinned. The default **stays `0`** — it is the conservative, availability-protecting value, and loosening it would trade a real guarantee for a test convenience — and `425`/`ErrLearnerNotCaughtUp` is pinned as an **explicitly retryable, records-nothing pre-proposal refusal**, joining §2.6a's checks 2–3 as the third such refusal the admin layer may retry on the operator's behalf under the identical `RequestID`-untouched discipline. §2.6a's retry paragraph now enumerates all three; §9's status-code table names `425`/`503` as the retryable pair and `400`/`409`/`412` as terminal; §16's promote step retries under the existing bounded-polling discipline and asserts every intervening refusal was a `425` (the non-increasing-`lag` half of that assertion is **withdrawn in revision 5** and replaced by a test-set bound — §23/H2); §19 gate 8's `docs/membership.md` content list requires the runbook to say so; §22's F-NB1 entry records that revision 3's stated justification was wrong and where the determinism actually lives. |

#### Non-blocking items, all closed rather than deferred

| ID | Finding | Resolution |
|---|---|---|
| **G5** | §2.2 and §21 slice 1 both said "seven" `cfg.Peers`/`cfg.majority()` call sites and enumerated a list omitting `handleRequestVoteResponse`'s election-win tally (`len(c.votesReceived) >= c.cfg.majority()`) — the **election** quorum, the one site this plan can least afford to leave on a stale configuration. Harmless in practice (slice 1 removes `Config.Peers`/`Config.majority()`, so every missed site is a compile error) but the plan presents this enumeration the same way it presents §6.3's and §6.3a's exhaustive tables. | §2.2 replaces the prose list with an **eight-row table**, `handleRequestVoteResponse` included and flagged, and records the structural backstop as a backstop rather than as the checklist; it also states which rows learner fan-out is added to and which it must never be added to. §21 slice 1 says "all eight" and names the previously omitted row. |
| **G6** | §21 slice 3b said "`copyWALSuffix`'s restore-side caller" for §7.6's part-2 transform, but `copyWALSuffix` is shared: `Export` calls it to copy the live WAL **into** the backup, and `buildStaging` calls it to copy the backup into staging. An unparameterized edit inside the shared helper would void membership out of the backup itself. | §7.6 and §21 slice 3b both now require the transform to be **passed in** (or applied through a restore-only wrapper), with `Export` passing identity, and add a unit test asserting a backup produced by `Export` still carries its `EntryConfig` entries unvoided. |
| **G7** | §1.1 translated `Config.validate()` as "`Bootstrap.Voters` must include `ID`" with the carve-out only implied in a parenthetical. Today's check is unconditional, so a literal translation would reject §3.1's never-joined learner, which is constructed with a deliberately **empty** `Bootstrap`. | §1.1 states the predicate explicitly as a conditional (non-empty `Bootstrap` must include `ID`; an entirely empty `Bootstrap` is valid), explains why it is a bootstrap-seed well-formedness check rather than a membership check, and requires a unit test pinning both branches. |
| **G8** | §7.2 described `MsgInstallSnapshotRequest` as gaining `Configuration` and cross-checked only that value against `Meta.Configuration`, while §8.1's generation-2 definition names `Configuration` **and** `HasConfiguration`. A flag-only disagreement with both configurations empty would pass a value-only check silently — and the flag is what decides whether the configuration is read at all (§6.3 step 2). | §7.2 now treats the two as a **pair** on the wire exactly as they are in the durable frame, and the `handleInstallSnapshot` check covers both fields; §21 slice 1's `messages.go` bullet, slice 4's check, DM-8 and §18's row all name the pair. |
| **G9** | `Core.heardFromLeader` is cleared by `InputElectionTimeout`, which the existing test-only `PauseTicksForTest`/`electionTicksPaused` hook suppresses. Under a frozen election clock the flag stays `true` indefinitely and that node ignores every `RequestVoteRequest`, including higher-term ones — correct as a composition, but a real behavioural change for any test that freezes ticks on one node and expects another's election to succeed. | §2.7 gains a second interaction note stating the coupling, requiring it to be documented on **both** sides (`heardFromLeader`'s and `PauseTicksForTest`'s doc comments), adding a direct unit test that pins the composition, and requiring an audit of existing `PauseTicksForTest` callers when slice 1 lands — resuming ticks rather than weakening Rule 2 where a vote must be granted. Scoped as an implementation-time obligation: no production path freezes ticks, and on a Voter or a Leader the flag can never go stale. §18's Rule 2 row and §21 slice 1 updated. |
| **G10** | §17 said "eleven in total" and §19 gate 8 said "§17's ten invariants", while §17 lists thirteen headings. | §17 states the count once and correctly — **twelve** added-or-rewritten catalog entries plus one pre-existing entry (`NO SILENT FORMAT MISINTERPRETATION`) amended in place — and notes that the "new in revision 2 / restated in revision 3" heading labels are relative to earlier revisions of *this plan*, not to `docs/invariants.md`, in which none of the twelve exists yet. §19 gate 8 repeats the same numbers. |

### Revision 4 → revision 5 (focused re-review of `1900617`)

The re-review confirmed **G1**–**G10** closed — the restore path's
accept-side rule consistent across §2.5, §2.6, §6.3, §6.4, §7.2, §7.6,
§17 and DM-21; §4.2a's three-row table and DM-17's matching phases
satisfiable; Lemma 3's previously missing Case 2b explicit and proved
by index domination; and the promotion default reduced to policy with
determinism relocated to `425`'s retry semantics — and it verified the
mechanical claims against the tree again (`copyWALSuffix` shared by
`Export` and `buildStaging`; `handleRequestVoteResponse`'s
`cfg.majority()` tally present at `internal/raft/core.go`;
`checkPendingReads`' `acked := 1`; `BeginReadIndex`'s caller-side
`ctx.Err()` return leaving `n.pendingReads` untouched). It returned one
proof gap and three wording/test-spec items. **No mechanism, and no
lemma conclusion, changes in revision 5.**

#### Proof gap

| ID | Finding | Resolution |
|---|---|---|
| **H1** | §2.3's Lemma 3 **Case 1** asserted that "any candidate holding `C` carries a log whose last entry is from a term `<= term(L)`", justified by "no leader of a later term extended that branch (a later leader extending it would have had `C` as *its* own anchor, which makes `C` committed)". The justification conflates a node's **anchor** (newest *committed* configuration) with its **`activeConfig`** (`ConfigAt(lastIndex())`, which may rest on an uncommitted entry). A leader elected on `C`'s uncommitted branch holds `C` as `activeConfig` while its anchor stays `C_a`, so the branch can carry terms above `term(L)` — reachable under this document's own gates, since P1/P2/P3 gate `EntryConfig` appends only: `a` appends `Remove(d)` at term 1 unreplicated, crashes, restarts as a Follower whose `activeConfig` is that uncommitted child, campaigns at term 2 (a shorter-logged `b` grants), appends a term-2 no-op, is partitioned; `c` then wins term 3 under `C_a`, commits its no-op (P1) and appends a different child. Case 1's stated bound is false there — the candidate's last term is 2, not 1. | Case 1 now proves the bound it actually needs, **`lastLogTerm < term(L')`**: every entry above `C`'s establishing entry in a `C`-holder's log was appended by a leader that itself held `C` (no configuration-establishing entry sits in between, or the holder's `activeConfig` would not be `C`), so the candidate's last term is `term(L'')` for some such leader, and `term(L'') > term(L')` is contradictory in both orderings — if `L''` was elected first, a majority of `C` sits at `term(L'')`, every majority of `C_a` meets it by Lemma 1, and `L'` can never commit its own term entry, so P1 fails and `C'` is never appended; if `L'` committed first, every majority of `C` already held `lastLogTerm >= term(L')` and `L''` could not have been elected. `isLogUpToDate`'s term-before-index comparison then rejects every `C`-candidate exactly as before. The reachable schedule is written out as a three-row table so the corrected bound is pinned against the state that breaks the old one. **Conclusion, Case 2a's reduction to Case 1, Case 2b, the theorem, and `CONFIGURATION BRANCH CONFINEMENT` are all unchanged.** |

#### Test-spec and wording items

| ID | Finding | Resolution |
|---|---|---|
| **H2** | §16's promote step asserted "every intervening refusal was `425` with a **decreasing-or-equal** reported `lag`". `lag` is `LastIndex() - matchIndex` against a `LastIndex()` that this suite's own background writer keeps advancing, so it may legitimately rise between two attempts; the assertion required a monotonicity nothing provides — the same "determinism from a moving observable" shape as G4, reintroduced by G4's own fix. | The monotonicity assertion is **withdrawn**. §16 now asserts: eventual `200` within the bounded polling deadline; **every intervening refusal is a `425`** (deterministic, because the refusal's kind is a function of which check failed); **`lag <= W + 1` on every refusal**, where `W` is the background writer's fixed in-flight write budget, an input the test sets itself; and, on success, that `matchIndex == LastIndex()` held at the instant `Core` evaluated it. A test that needs the refusal path itself to be deterministic **sets `PromotionMaxLagEntries` explicitly** rather than relying on the shipped default, and no assertion in the suite may depend on that default remaining `0`. |
| **H3** | DM-17 sub-case 3 said "two phases against one read" while Phase A asserted a caller-visible context error and Phase B asserted `ErrLeadershipLost` for "the pending read" — satisfiable only because the caller's deadline does not deregister the read, which the plan never said. An implementer could reasonably have issued a second `BeginReadIndex` for Phase B, which would register a read *after* the configuration change and silently lose §4.2a row 2's coverage. | DM-17 sub-case 3 now states the split explicitly: Phase A observes `ctx.Err()` **from `BeginReadIndex`**; the `pendingRead` entry stays registered in `n.pendingReads` with its buffered (capacity-1) `resultCh` until `checkPendingReads` resolves or fails it; Phase B observes `ErrLeadershipLost` **from that original read's own buffered channel**, retained by the harness from Phase A; and the test **must not** issue a second read, with the reason (row 2 requires a read whose `term`/`requiredSeq` predate the self-removal) stated. |
| **H4** | §2.2 said "**eight**, six in `internal/raft/core.go` and two mirrored copies in `internal/node`" above a table whose eight rows are seven `core.go` rows plus one `internal/node` row — two different counting conventions in one sentence, both landing on eight by coincidence. | §2.2 now states the convention once (a row is one quorum-or-fan-out **computation**: `handleElectionTimeout` contributes two rows, `becomeLeader`'s two loops are one, and `Node.majority()`/`checkPendingReads` are one), gives the per-function count as the alternative reading, and names the table as authoritative. The eight-row table itself, including row 3, is unchanged. |
