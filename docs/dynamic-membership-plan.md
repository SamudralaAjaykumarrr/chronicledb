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
`docs/adr/`, `docs/membership.md`, or `docs/raft.md`'s substantive
content**; those are implementation-time deliverables this plan
specifies precisely enough to write without further architectural
judgment calls.

## 0. Scope reaffirmation

Per [ADR-0001](adr/0001-v1-single-shard-static-cluster-scope.md) and
[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §0.2, this phase
changes *cluster membership* (which nodes host the one Raft group) —
never the number of *shards* (still exactly one, always). Nothing
below reintroduces sharding, cross-shard transactions, or multi-group
Raft. See §20 for the complete non-goals list.

Depends on: `v0.2.0` Security Foundation (mTLS node identity, RBAC,
audit — §13), `v0.3.0` Backup/DR (unaffected, reused as-is), `v0.4.0`
Compatibility/Rolling Upgrades (the generation-gating mechanism this
phase's own new wire/log-entry format rides on — §8). All three are
released; none of their guarantees is weakened by anything below (see
§8's explicit non-weakening argument).

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
    // all (a brand-new, never-before-run cluster — see §1.6). Every
    // later restart derives the active Configuration from durable
    // state instead (§6), never from this field again.
    Bootstrap Configuration
    ElectionTimeoutTicks, ElectionTimeoutJitterTicks, HeartbeatTimeoutTicks int
    Rand Rand
    // PromotionCatchUpTicks/... (leader-only tuning) live in
    // internal/node, not here — see §3.3.
}
```

`Config.validate()` changes from "`Peers` must include `ID`" to
"`Bootstrap.Voters` must include `ID`" (bootstrap-only check; a node
joining an *already-running* cluster as a learner never uses
`Bootstrap` at all — see §3.1).

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
- Is never sent `RequestVoteRPC` by a candidate, never grants a vote
  (see §1.7's new acceptance check), never becomes Candidate itself
  (its own election timeout is a no-op — see §2.6).
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
  2. **`MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE`** (§1.7): a stray old
     process that still believes it holds a removed `ID` and keeps
     sending Raft traffic under it is rejected by every current member
     the instant it is not present in the *receiver's own* current
     `Configuration` — see §4.5 for the complete "stale removed node"
     treatment.
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
| The currently active `Configuration` | Yes, derived | Not stored as its own record — reconstructed from (a) the latest `EntryConfig` log entry still retained (§2), or (b) the snapshot's `Meta.Configuration` if no such entry remains in the retained log (§7), or (c) `Config.Bootstrap` if neither exists yet (fresh cluster, §1.1). This mirrors exactly how `commitIndex`/`appliedIndex` are **not** independently persisted but are always reconstructed (`docs/raft.md` §5.1) — membership gets the identical treatment, for the identical reason (never trust a cached derived value; always recompute from the log/snapshot that is the actual source of truth). |
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
    activeConfig      Configuration // replaces cfg.Peers as the source of majority()/replication targets
    activeConfigIndex Index         // log index of the EntryConfig entry that produced activeConfig (or 0 if it came from snapshot/bootstrap — see §6)
    snapshotConfig    Configuration // the Configuration captured in this Core's own last snapshot boundary (§7); the fallback activeConfig reverts to when a truncation removes every EntryConfig entry above snapshotIndex
}
```

Every existing call site that iterated `c.cfg.Peers` or called
`c.cfg.majority()` is changed to `c.activeConfig.Voters` /
`c.activeConfig.majority()` (§2's implementation slice §21.1 lists
every one, all seven identified in the current `core.go`:
`handleElectionTimeout`, `handleHeartbeatTimeout`, `handlePropose`'s
peer fan-out, `becomeLeader`'s `nextIndex`/`matchIndex`
initialization, `advanceLeaderCommit`'s majority loop, plus
`internal/node.Node.majority()`/`checkPendingReads`'s mirrored copies).
Learner fan-out (`AppendEntriesRPC` sent to `activeConfig.Learners`
too, never `RequestVoteRPC`) is a small addition alongside each of
those loops.

**Reverting on divergent-suffix repair.** When `handleAppendEntriesRequest`
truncates `c.log` because of a log-matching conflict (existing
mechanism, unchanged), and that truncation removes the entry at
`activeConfigIndex` or later, `Core` must recompute `activeConfig`:
scan `c.log` backward from the new tail down to (not below)
`snapshotIndex` for the latest remaining `EntryConfig` entry; if one is
found, adopt its `Configuration` and index; if none is found, revert to
`snapshotConfig` and `activeConfigIndex = 0` (meaning "the snapshot
boundary's own configuration, no later change survives"). This is a
bounded scan over `c.log`, which is already bounded in size by "since
last snapshot" (the same assumption every other `c.log` operation
already makes).

**Reverting on crash.** No special handling needed: `activeConfig` is
pure in-memory `Step` state, exactly like `c.log` itself, and is always
recomputed from durable state (the reconstruction rule in §1.7/§6) on
every restart — a crash before an `EntryConfig` entry is durably
persisted simply means it never happened, identical to any other
uncommitted, unpersisted log content (`docs/failure-model.md` §2.1).

### 2.3 Proof: quorum intersection and split-brain prevention

**Claim**: for any two `Configuration` values `C_old` and `C_new` that
differ by the addition or removal of exactly one voter (the only two
voter-affecting transitions this mechanism permits — promotion is a
learner→voter move that also changes voter count by exactly one; see
§2.3's transition enumeration below), **every majority of
`C_old.Voters` intersects every majority of `C_new.Voters`.**

**Proof** (standard, restated for this system's exact parameters):
let `C_old` have `n` voters, majority `⌊n/2⌋+1`. `C_new` adds one voter
(`n+1` voters, majority `⌊(n+1)/2⌋+1`) or removes one voter (`n-1`
voters, majority `⌈(n-1)/2⌉`, i.e. `⌊(n-1)/2⌋+1` for the removal case
worked the same way). Take the add case (removal is symmetric): a
`C_old` majority excludes at most `⌊(n-1)/2⌋` of the `n` original
voters. A `C_new` majority of `n+1` members excludes exactly
`(n+1) - (⌊(n+1)/2⌋+1) = ⌈(n-1)/2⌉` members out of `n+1` total — of
which at most one exclusion can be the newly added voter (it is only
one member), so at least `⌈(n-1)/2⌉ - 1` of the exclusions fall among
the original `n`. Summing the two exclusion counts against the
original `n` voters: `⌊(n-1)/2⌋ + (⌈(n-1)/2⌉ - 1) < n` for every
`n ≥ 1` (the two exclusion sets cannot jointly cover all `n` original
voters), so the two majorities must share at least one original voter
in common. QED for add; the removal case follows by the identical
counting argument with `n-1` in place of `n+1`.

**Why serialization is required for this proof to keep holding.** The
proof above compares exactly two adjacent configurations differing by
one member. If a second, overlapping change could be proposed before
the first commits, the cluster could reach a state where two
*non-adjacent* configurations (differing by two or more members) are
both "live" candidates for majority computation at once — for which
the one-member-intersection proof does **not** apply (two
configurations differing by two members can, in the worst case, have
fully disjoint majorities — this is precisely the hazard joint
consensus's two-phase `C_old,new` intermediate exists to close for the
*general*, non-serialized case). **`SERIALIZED MEMBERSHIP CHANGE`**
(§17) — enforced by `Core.ProposeConfigChange` refusing to accept a
second proposal while `activeConfigIndex > commitIndex` (§2.4) — is
therefore not a convenience restriction; it is the specific
precondition under which the single-server-change proof above is
valid at all. This is the answer to this document's own §2 instruction
("do not choose based on implementation simplicity alone") stated as a
formal precondition, not an assertion.

**Split-brain prevention**: given quorum intersection holds at every
point (by the proof, inductively applied across the whole serialized
sequence of one-member-at-a-time transitions), the existing
`RAFT ELECTION SAFETY` argument (`docs/invariants.md`, unchanged
mechanism: vote-once-per-term, persisted before granting) continues to
guarantee at most one legitimate leader per term, and the existing
`QUORUM SAFETY`/current-term commit rule (`docs/raft.md` §4, unchanged
mechanism, now evaluated against `activeConfig.majority()` instead of
a fixed constant) continues to guarantee a minority partition can never
independently commit — because "minority" and "majority" are always
evaluated against a `Configuration` whose majority provably intersects
every other majority any other node in the cluster could simultaneously
be using. No two disjoint groups of nodes can ever both believe they
hold a majority of *the same* logical configuration lineage, because
every configuration transition in that lineage was itself only ever
committed by reaching a majority of its immediate predecessor.

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
`PromoteToVoter=17`, `RemoveServer=18`. Implementation must add a
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

### 2.7 `MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE` (new mechanism, closes §4.5)

Every inbound `Message` (`RequestVoteRequest`, `AppendEntriesRequest`,
`InstallSnapshotRequest`, and their responses) is checked, at the very
top of `Core.Step`'s message dispatch, before any term/log state is
touched: if `msg.From` is not `activeConfig.isMember(msg.From)`, the
message is dropped outright — no term bump, no state change, no
reply. Additionally, `handleRequestVoteRequest` denies (never grants)
a vote if the *responding* node's own `ID` is not currently a Voter in
its own `activeConfig` (a Learner, or a node that has observed its own
removal, never grants a vote — it is not entitled to participate in
leader election at all). This closes two things at once: a removed
node's stray, indefinitely-retried elections can never even register
with current members (no term bump, no disruption — closing the
liveness hole flagged in §4.5), and a Learner can never accidentally
grant a vote it should never have been asked to cast.

---

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
with an **empty** `Config.Bootstrap` (or, more precisely, `Open`
detects "empty data directory, and the operator has *not* passed the
existing `-cluster`/`-peers` bootstrap flags meant only for forming a
*brand-new* cluster" and constructs a `Core` whose `activeConfig` is
the zero-value `Configuration{}` — no voters, no learners, including
not itself). Such a `Core` is inert with respect to elections (its own
`ID` is not a Voter in `activeConfig`, so §2.7's rule already prevents
it from ever calling an election) but is fully able to *receive*
`AppendEntriesRPC`/`InstallSnapshotRequest` from the real leader once
the AddLearner entry reaches it (§2.7's message-acceptance check would
otherwise drop messages from a sender not in `activeConfig` — but note
the check is about the *sender*, not this node's own membership; a
brand-new node with an empty `activeConfig` would reject *every*
inbound message under a naive reading of §2.7, including the very
first `AppendEntriesRPC` that is about to deliver its own membership
to it). **Resolution**: §2.7's acceptance check is relaxed for exactly
one case — a node whose own `activeConfig` does not yet include itself
at all (a brand-new learner-to-be) accepts `AppendEntriesRPC`/
`InstallSnapshotRequest` from any sender unconditionally, precisely
because it has no legitimate way to know who to trust yet beyond
"whoever the operator pointed it at" (the same trust boundary that
already exists today for `-peers`/`-cluster` at first bootstrap,
enforced by mTLS at the transport layer, §13 — not by §2.7's
membership check). The instant such a node processes an `EntryConfig`
entry that includes itself, it adopts a real `activeConfig` and §2.7's
full check applies from then on, including to future messages from
that same sender.

### 3.2 Snapshot catch-up when required

Fully covered by §7 (the one snapshot-format extension: `Meta`
gains a `Configuration` field). No new catch-up *mechanism* — this
phase extends an existing one.

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

If the learner's current `matchIndex` (tracked by `Core`, read via a
new `Core.MatchIndexOf` call — the accessor already exists,
unmodified) is not within `PromotionMaxLagEntries` of `Core.LastIndex()`,
`PromoteToVoter` returns a distinct, synchronous error (`ErrLearnerNotCaughtUp`,
carrying the current lag) **without ever proposing anything** — no
wasted log entry, no ambiguous outcome, matching this document's
explicit instruction that a newly promoted node must not endanger
quorum. This check is deliberately **not** re-validated by followers
at accept time (§2.6's four-shapes check does not include it) — it is
a leader-only availability/safety judgment call using non-replicated
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

The leader may be asked to remove **itself**. This is handled exactly
per the well-known Raft treatment
([`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §8's own text,
made precise here): the leader **appends and replicates** the
self-removing `EntryConfig` entry normally (using the new
configuration's majority to decide its commit, per §2.2 — the leader's
own vote/matchIndex still counts toward this specific commit decision,
since the entry has not committed yet at the moment it is being
evaluated, even though the *resulting* configuration excludes the
leader — this is not a contradiction: `activeConfig` already excludes
the leader from *future* decisions the instant it's appended, but the
already-in-flight commit decision for *this* entry is evaluated against
the memberships as of that same instant, which is `C_new` — a leader
proposing its own removal is, from the moment of proposing, already
"a non-member acting as leader of a configuration that doesn't include
it," which is fine: nothing in Raft requires the leader to be a member
of the configuration whose commit decisions it is currently
brokering, only that a majority of *that* configuration's voters
persist the entry). Once the entry **commits** (not merely appends —
waiting for commit specifically avoids needlessly giving up leadership
for a change that might never actually succeed, e.g. a transient
network issue preventing the remaining voters from acking), the leader
immediately steps down to `Follower` (a new `Output.SteppedDown`-style
transition, forced the instant `advanceLeaderCommit` observes a newly
committed `EntryConfig` entry that excludes this node's own `ID` from
`Voters`) so the remaining voters can elect a new leader among
themselves. The `/admin/membership/remove` HTTP call itself, having
been issued against the (soon to no longer be) leader, still returns
the committed outcome normally — self-removal is not a special case
from the *caller's* point of view, only from the *node's own*
post-commit behavior.

### 4.3 Node unavailable during removal

No special case: removal is proposed and committed exactly like any
other membership change, using the **old** configuration's majority if
the target is a voter still counted at proposal time... **correction,
matching §2.2 precisely**: the removal entry's own commit is evaluated
against `C_new` (§2.2's "the new configuration governs even its own
commit" rule), so an unavailable (crashed, partitioned) *voter being
removed* does not need to acknowledge its own removal at all — its
absence never blocks the removal from committing, since `C_new`
already excludes it from the majority calculation. An unavailable
*learner* being removed is even simpler: learners never affect any
majority calculation regardless.

### 4.4 Removal followed by restart

The removed process, if it later restarts (having never learned of its
own removal — e.g. it was already offline when the entry committed),
restarts with whatever it had durably persisted before going offline —
which may still show it as a member (its own last-known
`activeConfig` reconstruction, §6, still includes itself, since it
never received the removal entry). It resumes as a `Follower`
(or, if it still believes it's entitled to, a `Candidate` — see below)
and attempts to reconnect. The instant it reconnects to any current,
legitimate member and receives real Raft traffic, one of two things
happens depending on how far behind it is:
- If the leader's retained log still spans back far enough, ordinary
  `AppendEntriesRPC` (§2.7's relaxed acceptance rule does **not** apply
  here — this node already has a non-empty `activeConfig` that
  includes itself, so the full §2.7 check applies; a legitimate current
  leader is, by construction, a member of the node's own eventually-adopted
  `activeConfig` lineage, so its messages are accepted) delivers the
  removal entry, and the node adopts the new configuration excluding
  itself (§2.2) — from that instant, `MEMBERSHIP-SCOPED MESSAGE
  ACCEPTANCE` (§2.7) means this node itself, observing its own removal,
  simply stops being an active participant: it never becomes Candidate
  again (its own `ID` is no longer a Voter in its own `activeConfig`)
  and any client that still points at it receives a clear
  "removed from cluster" error (§4.6) rather than silently timing out
  forever.
- If the leader has already compacted past the required range, a
  `MsgInstallSnapshotRequest` (carrying the current `Configuration`,
  §7) achieves the identical outcome in one step.
- **If it never reconnects at all** (fully, permanently partitioned):
  it may continue believing itself a Voter and periodically call
  elections forever. This is harmless to cluster **safety** by
  construction (§2.3's proof; a permanently isolated single node can
  never gather a majority of any configuration the rest of the cluster
  is actually using) and, by §2.7, is now also harmless to the *rest of
  the cluster's* **liveness** — its stray `RequestVoteRequest`s are
  dropped outright by every current member the instant they check
  `activeConfig.isMember(msg.From)`, so they never even bump anyone's
  term. This is the direct, mechanism-level resolution of this
  document's item-4 requirement to cover "stale removed node attempting
  to rejoin" and "removed node sending old Raft traffic" — see §4.5.

### 4.5 Stale removed node sending old Raft traffic (full treatment)

Covered completely by §2.7 (`MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE`):
every current member drops (no term bump, no reply, no state change)
any Raft message whose `From` is not in that receiver's own current
`activeConfig`. This closes the liveness hole that would otherwise
exist without it: absent this check, a removed-but-still-network-reachable
node retrying elections with an ever-increasing term could force every
real member to repeatedly step down (existing `stepDownTo` behavior on
any higher-term message, unconditional today), causing indefinite,
needless re-elections — a genuine liveness degradation this phase's own
new capability (a node that can be legitimately removed while still
running) introduces if not closed. §2.7 closes it structurally, at the
lowest possible layer (before any term/log state is touched), rather
than relying on the removed node's own good behavior or on the
transport layer alone.

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
| **After proposal, before commit** (leader appended the `EntryConfig` entry to its own log, possibly replicated to some but not a majority-of-`C_new`, then crashed) | The entry may or may not survive in the new leader's log (ordinary divergent-suffix repair reasoning, §2.2's revert-on-truncate rule, applies identically to `EntryConfig` entries — no special case). | If a new leader's log does **not** contain the entry (it never reached a majority-of-`C_new`): as if the proposal never happened — `activeConfig` reverts to whatever the new leader's own log/snapshot shows (§2.2's revert rule). If a new leader's log **does** contain it (it *did* reach a majority-of-`C_new`, satisfying `LEADER COMPLETENESS`): the new leader necessarily adopts it as part of normal log reconstruction (§6) and the entry proceeds to commit normally, exactly as if the original leader had not crashed — **this is not ambiguous**: `LEADER COMPLETENESS`'s existing vote-granting log-comparison rule (unchanged mechanism) guarantees only a candidate whose log already contains every entry a majority-of-`C_new` might have persisted can win an election, so there is no possible new-leader outcome that "loses" an entry that genuinely reached a majority. |
| **After commit, before apply** (entry committed — majority-of-`C_new` persisted it — but the committing node crashed before `internal/node.applyCommitted` ran `FSM.RecordMembershipOutcome`/updated its dial table) | The entry, being committed, is guaranteed present in every future legitimate leader's log (`LEADER COMPLETENESS`). | Identical to the existing, already-proven `docs/failure-model.md` §2.3 case ("leader crash after quorum, before reply") extended to this command kind: on restart/re-election, `applyCommitted` runs normally over the durable committed log, including this entry, producing the identical deterministic outcome it always would have — no ambiguity, no re-derivation needed beyond the existing, already-tested recovery path. |
| **Leader crash mid-change** (any point during propose/replicate/commit) | Covered by the two rows above, exhaustively — "mid-change" decomposes into exactly "before commit" or "after commit," nothing else. | See above. |
| **Candidate election during change** | An election can occur at any point regardless of an in-flight config change; §2.3's proof is precisely what makes this safe — a candidate's own log either does or does not contain the (possibly uncommitted) `EntryConfig` entry, and `LEADER COMPLETENESS`'s existing log-comparison rule decides eligibility exactly as it always does, config entries included with no special case. | Whichever candidate wins reconstructs its own `activeConfig` from its own (possibly config-entry-including, possibly not) log via the same §2.2 mechanism every node always uses — never ambiguous, always a pure function of that node's own now-current log. |
| **Network partition during change** | The proposal can only commit by reaching a majority of `C_new` (§2.2) — an isolated minority (whether it holds the old or new quorum's minority) can never independently commit it, by `QUORUM SAFETY`, unchanged mechanism now evaluated against `activeConfig.majority()`. | If the majority side includes enough of `C_new`'s voters, the change commits there normally and the healed minority catches up afterward via ordinary replication/snapshot (§2.2, §7) with **no** special reconciliation step — `activeConfig`'s revert-on-truncate rule (§2.2) already handles a minority node's stale, divergent, or simply-behind log correctly, uniformly with every other kind of entry. |
| **Snapshot during/involving a membership transition** | Fully covered by §7. A snapshot taken while an `EntryConfig` entry is committed-but-still-in-the-retained-log simply captures `activeConfig` as of the snapshot boundary (§7.1) — no special "is a change in flight" case, because `SERIALIZED MEMBERSHIP CHANGE` already guarantees at most one is ever outstanding, and an *uncommitted* `EntryConfig` entry is never included in a snapshot at all (snapshots only ever cover committed, applied history, unchanged existing rule). | The snapshot's `Meta.Configuration` is exactly `activeConfig` as of `LastIncludedIndex` — always unambiguous by construction. |

---

## 6. Durability / recovery

### 6.1 Where membership state is persisted

- **Raft log**: every `EntryConfig` entry, via the *existing*
  `WALStorage.Append`/`internal/wal.AppendLogEntry` path — no new WAL
  record type. The only change needed in `internal/node/storage.go` is
  to `encodeEntryPayload`/`decodeEntryPayload` (the opaque
  `Term + Data` blob `WALStorage` hands to `internal/wal`, entirely
  outside `internal/wal`'s own awareness — `docs/architecture.md` §5's
  existing "WAL must not know Raft semantics" boundary, unchanged): it
  must additionally carry `Entry.Type`, encoded **additively** exactly
  like every other `v0.4.0` generation field (§8) — a trailing byte
  appended only when `Type != EntryNormal` (the zero value), so a
  generation-0 entry payload remains byte-identical to today's exact
  encoding. `internal/wal` itself needs **no code change at all** —
  it already treats this payload as fully opaque bytes (mirroring
  `ADR-0017`'s own observation that `internal/snapshot` needed no
  change for the identical reason).
- **WAL metadata**: no new field needed. `Metadata.ClusterGeneration`
  (existing, `v0.4.0`) already gates whether membership commands may
  even be proposed (§8) — no separate membership-specific metadata
  field is required.
- **Snapshots**: `Meta.Configuration` (§7) — the fallback source of
  truth once the log no longer retains the relevant `EntryConfig`
  entry.
- **FSM/state machine**: the membership `RequestID → Outcome`
  idempotency table (§2.6, §10), via `FSM.EncodeState`/`DecodeState`
  exactly like every other outcome table, so it is included in every
  snapshot automatically.

### 6.2 Recovery ordering

Extends `docs/recovery.md`'s existing ordering (unchanged for every
step already there) with exactly one new step, inserted at the point
`Core` is reconstructed:

1. *(existing)* Open WAL, validate/replay to recover `HardState` and
   log entries.
2. *(existing)* Load and validate the most recent snapshot, if any.
3. **(new)** Determine `snapshotConfig`: `Meta.Configuration` from the
   loaded snapshot (§7.1), or the zero value if no snapshot exists yet.
4. **(new)** Reconstruct `activeConfig`/`activeConfigIndex`: scan the
   recovered log entries (from the snapshot boundary forward) for the
   latest `EntryConfig` entry; if found, adopt it; otherwise adopt
   `snapshotConfig`; if *that* is also the zero value and this is
   genuinely the very first-ever start of a data directory, adopt
   `Config.Bootstrap` (§1.1, §1.8) — exactly one of these three sources
   is ever consulted, deterministically, in this fixed priority order,
   which is what makes post-recovery configuration state **never
   ambiguous** (this document's item 5/6 requirement, restated as a
   recovery algorithm rather than an assertion).
5. *(existing, unchanged)* `commitIndex`/`appliedIndex` reconstruction
   via legitimate leader contact or this node's own election
   (`docs/raft.md` §5.1) — membership gets no special treatment here;
   a config entry that turns out to have been uncommitted is subject
   to exactly the same divergent-suffix repair as any other entry,
   with `activeConfig` reverting per §2.2's rule the moment that
   repair actually happens.

### 6.3 Corruption / fail-closed behavior

No new corruption class: an `EntryConfig` entry's payload is subject
to the *same* WAL-level checksum/framing validation every entry
already gets (`internal/wal` never inspects `Data`'s content, §6.1) —
a corrupted `EntryConfig` payload is caught by the existing checksum
mechanism before `internal/node` ever tries to decode it as a
`Configuration` at all (`RECOVERY NON-INVENTION`, unchanged). If
somehow a structurally-invalid (but checksum-valid — i.e., a genuine
bug, not corruption) `Configuration` payload is ever decoded during
replay, the fail-closed behavior is identical to §2.6's live-path
defense-in-depth check: refuse to adopt it, halt (`Node.fail`), never
guess or silently repair it.

---

## 7. Snapshots / compaction

### 7.1 Membership encoded in snapshots

```go
// internal/snapshot/snapshot.go — Meta gains one field
type Meta struct {
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    Configuration     Configuration // NEW; raft.Configuration re-exported/mirrored
                                     // as a snapshot-package-local type (this
                                     // package has no dependency on internal/raft
                                     // today, docs/snapshots.md §1 — kept that way;
                                     // internal/node converts at its own boundary,
                                     // exactly as it already does for
                                     // LastIncludedIndex/Term's raft.Index/raft.Term
                                     // <-> uint64 conversion)
}
```

`Encode`/`Decode`'s frame layout gains a `Configuration` section,
appended **additively** after the existing `fsmState` section and
**gated behind `snapshot.FormatVersion`** exactly like `v0.4.0` gated
the WAL/FSM generation fields — but here, unlike `v0.4.0`'s "append
only when nonzero" trick (which worked because the *old* format had no
concept of the new field at all and needed byte-identical generation-0
output), the snapshot's `Configuration` section is **never optional in
new writes**: `v0.5.0` bumps `snapshot.FormatVersion` from `1` to `2`
outright (unlike `v0.4.0`, which deliberately did *not* bump
`FormatVersion` because it introduced no new *outer-frame* content —
this phase does introduce new outer-frame content, so the version bump
is the correct, honest signal here, not an oversight relative to
`ADR-0017`'s stated reasoning for *not* bumping it last time). A
pre-`v0.5.0` binary's `Decode` sees `version=2 != FormatVersion(1)` and
fails closed with `ErrUnsupportedVersion` — exactly the mechanism
`NO SILENT FORMAT MISINTERPRETATION` already requires, via the
*pre-existing* strict version-equality check `docs/snapshots.md` §5
already documents, requiring no new logic, only the constant bump plus
the new field's encode/decode. **This is gated by cluster generation
exactly like everything else in §8**: a node never *writes* a
`FormatVersion 2` snapshot until the cluster has already finalized to
generation `>= 2` (mirroring how `v0.4.0`'s WAL/FSM generation fields
are never written until finalize) — so a pre-finalize snapshot remains
byte-identical to today's `FormatVersion 1` output, preserving rollback
safety (`ROLLBACK BOUNDARY HONESTY`) exactly as `v0.4.0` established.

### 7.2 Installing a snapshot with different membership

`MsgInstallSnapshotRequest`/`Response` gain explicit `Configuration`
fields (mirroring how `LastIncludedIndex`/`LastIncludedTerm` are
already explicit `Message` fields `Core` itself reads/writes directly,
unlike the opaque `SnapshotData`, §2's precedent). On successful,
durable installation (existing `internal/node.handleInstallSnapshot`
sequence, unchanged ordering — driver validates and durably installs
*before* ever calling `Core.Step` for this message, per the existing
documented contract), `Core.handleInstallSnapshotRequest` adopts the
snapshot's `Configuration` as both `activeConfig` and `snapshotConfig`
unconditionally — exactly mirroring the existing "always discard the
whole log" simplification this method already documents for the log
itself, now extended to configuration: whatever `activeConfig` this
node had before is entirely superseded, no merge, no reconciliation.

### 7.3 Catch-up of newly added nodes

No new mechanism beyond §3.1/§3.2 and this section — a learner far
enough behind to need a snapshot receives one exactly like any
existing far-behind follower always has (`docs/snapshots.md` §7,
completely reused), now additionally carrying `Configuration`.

### 7.4 Compaction across configuration changes

`Core.Compact(uptoIndex)` (existing method) additionally sets
`snapshotConfig = activeConfig` and, if `activeConfigIndex <=
uptoIndex` (the config entry that established the current
`activeConfig` is itself being compacted away), also sets
`activeConfigIndex = 0` — meaning "the config now lives only in the
snapshot boundary, no later log entry need be consulted." This can
never lose information: whoever calls `Compact` is, by existing
contract (`docs/snapshots.md` §3: `uptoIndex <= appliedIndex`), only
ever compacting already-applied, already-durable history, and the
caller (`internal/node`) is required to have already captured
`activeConfig` into the new snapshot's `Meta.Configuration` (§7.1)
*before* calling `Compact` — the same existing ordering discipline
`LOG COMPACTION SAFETY` already requires between "snapshot durable"
and "truncate" for every other kind of state.

### 7.5 Restarted nodes whose local configuration is stale

Fully resolved by §6.2's fixed-priority reconstruction algorithm — a
"stale local configuration" cannot exist as an ambiguous state after
restart, by construction: the node always adopts exactly what its own
durable log/snapshot says, deterministically. If that happens to be
genuinely behind the *cluster's* current configuration (this node
missed changes while it was down), that is not staleness in the node's
own recovery — it is ordinary catch-up, resolved the moment it
reconnects (§4.4), identical in kind to any node that missed ordinary
committed writes while offline.

---

## 8. Compatibility / rolling upgrades integration

### 8.1 New generation boundary: `MaxSupportedGeneration` `1` → `2`

`internal/version.MaxSupportedGeneration` bumps to `2` as part of this
phase's own implementation. Generation `2` is defined as: "this binary
understands `Entry.Type`, `EntryConfig` entries, `Message`'s new
`SnapshotConfiguration`/membership-change-related fields, and
`snapshot.FormatVersion 2`." This is a genuinely new, real correctness
gate — exactly the kind `MaxSupportedGeneration`'s own doc comment
already anticipates ("expected to move in lockstep with, at most, one
MINOR version at a time").

### 8.2 Membership changes are forbidden until finalization

**Yes, unconditionally.** Every admin-API entry point
(`AddLearner`/`PromoteToVoter`/`RemoveServer`, §9) checks
`n.clusterGeneration >= 2` (the existing, durable, Raft-replicated
value `v0.4.0` already introduced, read via the existing
`FSM.ClusterGeneration()`/`Node`-cached mirror, §8.1's continuation of
`v0.4.0`'s own mechanism, zero new plumbing needed for this specific
check) **before ever calling `Core.ProposeConfigChange`**, returning a
clear, actionable error otherwise ("cluster not yet finalized to
generation 2; run `/admin/upgrade/precheck` and
`/admin/upgrade/finalize` first"). This is the **primary** safety
mechanism (§2.5's `EntryConfig` encoding-level fail-closed behavior is
defense in depth, not the primary one) — it means a mixed-version
cluster mid-`v0.4.0`→`v0.5.0` rolling upgrade can never even attempt a
membership change, by the same operator-facing precheck/finalize
discipline `v0.4.0` already established and this phase adds no new
concept to, only a new generation number and a new class of gated
operation.

### 8.3 Old (`v0.4.0`) binary encountering dynamic-membership state

- **An `EntryConfig` entry on the wire/in the log**: §2.5's complete
  account — decoded as an ordinary opaque entry (its `Type` field
  silently gob-dropped), then fails closed at the existing
  `applyControlEntry`/`ErrUnknownControlCommand` path once committed,
  with zero code change to the old binary.
- **A `FormatVersion 2` snapshot**: §7.1's complete account — the old
  binary's existing strict `FormatVersion` equality check
  (`docs/snapshots.md` §5, unchanged) refuses it outright,
  `ErrUnsupportedVersion`, before decoding anything further.
- **Because of §8.2, neither of these should ever actually happen
  during a correctly-operated rolling upgrade** (an old binary is never
  still part of the cluster once membership changes are permitted at
  all) — both are genuine defense-in-depth, exercised only by an
  operator error (e.g. redeploying an old binary onto a node after the
  rest of the cluster has already moved past generation 2), exactly
  the posture `ADR-0017` already documents for its own equivalent
  cases.

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
  -> 200 { "status": "committed"|"aborted", "error": "", "leaderHint": "" }
  -> 409 (NotLeaderError, leaderHint set) | 412 (cluster generation < 2)
  RBAC: admin only. Audited.

POST /admin/membership/promote
  { "requestId": "<opaque>", "nodeId": "n4" }
  -> 200 { "status": "committed"|"aborted", ... }
  -> 425 { "error": "learner not caught up", "lag": <entries> }  (§3.3, never proposed)
  RBAC: admin only. Audited.

POST /admin/membership/remove
  { "requestId": "<opaque>", "nodeId": "n4" }
  -> 200 { "status": "committed"|"aborted", ... }
  RBAC: admin only. Audited.

GET /admin/membership/status
  -> 200 {
       "configIndex": 42,
       "voters":   [{"id":"n1","address":"...","matchIndex":41}, ...],
       "learners": [{"id":"n4","address":"...","matchIndex":30,"lag":11}],
       "inProgress": false
     }
  RBAC: admin, operator, and read-only (pure observability — mirrors
  /status's existing openness, unlike the three mutating endpoints
  above, which follow /admin/upgrade/*'s stricter admin-only precedent).
```

`authz.go` gains four new `Endpoint*` constants and decision-table rows
(`EndpointMembershipAdd`/`Promote`/`Remove`: `{admin: true, operator:
false, read-only: false}`; `EndpointMembershipStatus`: `{admin: true,
operator: true, read-only: true}`), added to `AllEndpoints` — the RBAC
decision-table test (`authz_test.go`, existing pattern) automatically
covers all four once added, no new test *shape* needed.

Every mutating endpoint requires and validates `requestId` (§10);
`NodeIDsMustBeValidatedNotFreeform` — `nodeId` is validated as
non-empty and, for `add`, `address` as a syntactically valid
`host:port` before ever reaching `Core` — an operator typo produces an
immediate `400`, never a proposed-then-rejected log entry.

No broader management platform is introduced — exactly these four
endpoints, matching this document's own item-9 closing instruction.

---

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

---

## 11. Concurrent reconfiguration

**Decision: rejected/serialized, not permitted.** `SERIALIZED
MEMBERSHIP CHANGE` (§2.3, §17) is enforced structurally by
`Core.ProposeConfigChange` refusing (a synchronous, immediate rejection
— `ErrConfigChangeInProgress`, never entering the log) any new proposal
while `activeConfigIndex > commitIndex` — i.e., while the most recent
configuration change has been appended but not yet committed. This is
not merely an API-level queueing rule; it is the specific,
load-bearing precondition the §2.3 safety proof depends on. The
enterprise plan's own instruction — "prefer explicit rejection/
serialization unless the architecture proves concurrent changes safe"
— is satisfied here in the strongest form: concurrent changes are not
merely undesirable, they are proven unsafe (§2.3) under this
mechanism, so serialization is not a preference but a requirement.

An operator attempting a second change while one is in flight receives
an immediate, clear rejection (`409`) with no ambiguity about whether
their request "queued" — it did not; they must retry once
`/admin/membership/status`'s `inProgress` field clears.

---

## 12. Quorum / availability semantics

`majority(n) = ⌊n/2⌋ + 1`. Concrete worked examples (voter counts
only — learners never affect any of these):

| Transition | Old majority | New majority | Availability during the transition |
|---|---|---|---|
| **3 → 4** (add) | 2 of 3 | 3 of 4 | No availability loss: the add itself commits under the *old* config's majority requirement is **not** how §2.2 works — correction: it commits under `C_new`'s majority (3 of 4), which the promotion-eligible new voter, having just been promoted from an already-caught-up learner, can immediately help satisfy. In practice this is momentarily *more* demanding (needs 3 acks instead of 2) but never blocking, since the new voter is by definition already caught up (§3.3) and can ack immediately. |
| **4 → 3** (remove) | 3 of 4 | 2 of 3 | The removal itself commits under `C_new`'s majority (2 of 3 remaining) — strictly *easier* to satisfy than the old 3-of-4, so removal can never be blocked by the very node being removed being unavailable (§4.3). |
| **3 → 2** (remove) | 2 of 3 | 2 of 2 | **Resulting cluster has zero fault tolerance** (`majority(2) = 2` — both remaining nodes must be up for any further progress). Permitted by the mechanism (no safety issue — §2.3's proof holds for any `n`), but `/admin/membership/remove`'s response includes a `"warning"` field when the resulting voter count is even (§12.1) — this is exactly the case flagged. |
| **2 → 3** (add) | 2 of 2 | 2 of 3 | Strictly improves fault tolerance (from zero tolerance to one-failure tolerance) — the add's own commit needs 2 of 3, satisfiable as soon as the new voter ack's, without requiring both original members simultaneously (a genuine availability improvement path out of a degenerate 2-node cluster, worth naming explicitly for operator guidance in `docs/membership.md`). |
| **Loss of a node during a transition** | — | — | Fully covered by §5's boundary table: the *only* thing that matters is whether the in-flight `EntryConfig` entry reached a majority of `C_new` before the loss — if yes, it is durably committed and unaffected by the loss (`LEADER COMPLETENESS`); if no, it is exactly as if it never happened. |

### 12.1 Even-sized voter sets: permitted, discouraged, warned

**Decision**: even-sized voter counts are **permitted** by the
mechanism (§2.3's proof places no parity requirement on `n` — it holds
for every `n ≥ 1`) but are **operationally discouraged**, because an
even-sized voter set never provides more fault tolerance than the next
smaller odd size while requiring a strictly larger quorum (`majority(4)
= 3` tolerates exactly one failure, identical to `majority(3) = 2`, at
the cost of one more required ack per commit) — standard, well-known
Raft/Paxos operational guidance, not a ChronicleDB-specific
consideration. **Mechanism**: `/admin/membership/add` and
`/admin/membership/remove`'s response includes a non-blocking
`"warning"` string field whenever the resulting voter count is even
(computed deterministically from `C_new.Voters`, entirely
diagnostic — never gates the operation, consistent with
`docs/observability.md`'s "diagnostic state is not a correctness
dependency" rule applied to an operational-quality signal rather than
a safety one). `docs/membership.md` (written at implementation time)
states this guidance plainly as the primary place operators encounter
it, with the API warning as a secondary, in-the-moment nudge.

### 12.2 Minimum cluster size

`MINIMUM VOTER INVARIANT` (§2.6 shape 3, §17): a `RemoveServer`
reducing `len(Voters)` to `0` is deterministically refused — a
zero-voter `Configuration` is a structurally broken state (undefined
`majority()`) that must never be reachable, not even transiently. A
`RemoveServer` reducing to exactly `1` voter is **permitted** (a
degenerate but well-defined single-node "cluster," `majority(1) = 1`)
— not specially forbidden, matching this project's general preference
for mechanism over invented policy (§20), though `docs/membership.md`
will note it removes all fault tolerance and Raft's own purpose along
with it.

---

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

1. **`MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE`** (§2.7, Raft-level): a
   removed node's Raft messages are dropped by every current member
   regardless of whether its TLS certificate is still technically
   valid — membership, not certificate validity, is the acceptance
   gate at this layer.
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
   background connection attempts*.

### 13.4 Audit completeness

Every one of the three mutating endpoints produces exactly one audit
record per call, following the identical existing invariant/mechanism
(`AUDIT COMPLETENESS`, `v0.2.0`, unchanged) — no new audit-log format,
just three new audited action kinds.

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
```

Per-node detail (which specific learner is lagging, by how much, its
address) belongs on `/admin/membership/status` (§9), **not** on a
metrics label — exactly the same precedent `PeerGenerationInfo`
already established for `v0.4.0`'s per-peer generation detail
(`/status`, not `/metrics`).

`Status` (existing `internal/node.Node.Status()` struct) gains
`VoterCount`/`LearnerCount`/`ConfigIndex` fields, mirroring how it
already gained `ClusterGeneration` in `v0.4.0`.

---

## 15. Deterministic fault harness scenarios

Extends `internal/fault` (the Phase 4/7 deterministic simulator,
`docs/testing-strategy.md` §3, §6) with a `Cluster.ProposeConfigChange`
method mirroring the existing `Cluster.Propose`/`Compact` wrapper
pattern. New scenarios (named here; implemented as
`internal/fault/membership_test.go` and additions to
`internal/fault/chaos_test.go`'s combined schedule, at
implementation time):

- **DM-1 Leader failure during add, before commit** — leader crashes
  after appending an `AddLearner` entry but before a majority acks;
  assert the entry is either fully committed (by the new leader) or
  entirely absent, never partially adopted by some nodes.
- **DM-2 Leader failure during remove, after commit, before self-observation**
  — the specific self-removal case (§4.2): leader commits its own
  removal then crashes before stepping down on its own; assert the
  remaining voters correctly elect among themselves regardless.
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
  outstanding change at any simulated instant, across all nodes'
  views).
- **DM-6 Repeated `RequestID` retry across a leader failover mid-change**
  — the exact `docs/enterprise-v1-plan.md` §8 "retry after leader
  failover" scenario, checked against the outcome table (§10).
- **DM-7 Crash/restart at every boundary in §5's table** — one
  subtest per row, each asserting the exact resulting state that row
  specifies, not merely "no crash."
- **DM-8 Snapshot install carrying a different `Configuration` than the
  receiver's own stale one** — assert unconditional adoption (§7.2),
  no merge attempted.
- **DM-9 Stale removed node retrying elections indefinitely post-removal**
  — assert zero term bumps and zero disruption to the live cluster
  (§2.7, §4.5), using the existing `committedOracle`-style
  reference-model pattern (§6.1 of `docs/testing-strategy.md`) extended
  to also track "current legitimate `Configuration`" as a second
  invariant to check after every scheduled action.
- **DM-10 Combined randomized schedule**: membership changes
  (add/promote/remove, each individually valid per §2.6) interleaved
  with the existing Phase 7 fault classes (elections, partitions,
  crashes, message drop/duplicate/delay, compaction) at the same
  seed-count discipline `docs/testing-strategy.md` §6.5 already
  establishes — checked after every action against `QUORUM CONTINUITY`,
  `SERIALIZED MEMBERSHIP CHANGE`, and the existing `committedOracle`,
  extended (per DM-9) with a configuration-lineage oracle.
- **DM-11 Disk-fault injection during an `EntryConfig` append** — reuses
  `MemoryStorage.FailNextAppends` (existing Phase 7 mechanism) targeted
  specifically at a config-change entry; assert the affected node never
  falsely reports success and the cluster continues correctly
  afterward (mirroring `TestChaos_DiskFaultDuringPersistence`'s
  existing shape).

---

## 16. Real-process proof plan

Extends `cmd/chronicledb-node`'s existing `-tags=integration` real-process
suite (`docs/testing-strategy.md` §4/§6.3) with
`cmd/chronicledb-node/membership_integration_test.go`:

- **Start a real 3-node cluster** — reuses existing `main_test.go`
  infrastructure unchanged.
- **Finalize to generation 2** — reuses the existing
  `/admin/upgrade/precheck`/`finalize` real-binary flow (`v0.4.0`,
  unchanged) as a precondition (§8.2).
- **Add a real fourth process as a learner** over genuine TCP/disk;
  poll `/admin/membership/status` (bounded polling, per
  `docs/testing-strategy.md` §4's existing discipline — never a fixed
  sleep) until caught up.
- **Continue writes throughout** — a background writer goroutine
  issuing ordinary `/propose` traffic across the entire test, asserting
  zero failed commits attributable to the membership change itself
  (isolating "the change caused a problem" from "the underlying system
  had a problem," per this project's existing benchmark-correctness
  discipline, `docs/testing-strategy.md` §9.1, applied here to
  correctness rather than performance).
- **Promote the learner to voter.**
- **Force a real leader failover** (`SIGKILL` the current leader,
  existing mechanism) with the 4-voter configuration active; assert a
  real election among the remaining 3 succeeds and writes resume.
- **Remove one of the original three voters** (not the new one) via a
  real admin call against the new leader.
- **Restart the surviving members** (`SIGKILL` + restart, existing
  mechanism) and assert: (a) every acknowledged `RequestID`'s recorded
  outcome is identical, on every surviving node, to what it was before
  the restart; (b) the recovered `Configuration` on every surviving
  node is byte-identical to what it was before the restart — the
  concrete, end-to-end proof this document's item 16 asks for
  ("retain exact membership and acknowledged state").
- **Mixed old/new binary variant** (mirroring `v0.4.0`'s own
  `mixed_version_test.go` git-worktree technique, §21 references this
  as reused infrastructure): start a cluster on the `v0.4.0` baseline
  binary, roll to `v0.5.0` binaries one at a time, finalize to
  generation 2, *then* perform the add/promote/remove/restart sequence
  above — proving dynamic membership genuinely requires and correctly
  waits for finalization (§8.2) rather than merely being untested
  pre-finalize.

---

## 17. Safety invariants (to add to `docs/invariants.md` at implementation time)

Mirroring the existing catalog's exact format (statement, scope, why
it matters, mechanism, threatened by, proof/test obligations):

### `SERIALIZED MEMBERSHIP CHANGE`
**Statement**: at most one membership-change command may be
outstanding (`activeConfigIndex > commitIndex`) at a time; a second is
synchronously refused, never queued, never entering the log, until the
first resolves. **Mechanism**: `Core.ProposeConfigChange`'s explicit
check (§2.3, §11). **Threatened by**: any code path that appends a
second `EntryConfig` entry before the first commits. **Proof/test
obligations**: DM-5, DM-10 (§15); a direct unit test attempting exactly
this and asserting synchronous rejection.

### `QUORUM CONTINUITY`
**Statement**: at every point in the committed log, there is a
well-defined, single quorum requirement (majority of the *current*
`activeConfig.Voters`) — no ambiguity about which configuration a given
commit decision used. **Mechanism**: §2.2's append-time-effective
single active configuration, used uniformly (never per-historical-index)
for every majority calculation at any given moment (§2.2's explicit
resolution of the enterprise plan's own "quorum as of the configuration
active at each log position" phrasing). **Threatened by**: any code
path that computes majority against a configuration other than the
current `activeConfig`. **Proof/test obligations**: §2.3's proof;
DM-5, DM-10.

### `LEARNER NON-INTERFERENCE`
**Statement**: a learner never counts toward quorum, never votes, and
its absence/crash/slowness never blocks or delays commit progress for
the voting set. **Mechanism**: `activeConfig.majority()` computed over
`Voters` only, everywhere (§2.2). **Threatened by**: any commit-rule or
election code path that iterates `Learners` when computing a majority
threshold. **Proof/test obligations**: DM-3; a direct test asserting
commit progress is unaffected by an entirely absent/crashed learner.

### `MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE`
**Statement**: a Raft message from a sender not currently a member
(`activeConfig.isMember`) of the receiver's own active configuration is
dropped before any term/log state is touched; a non-Voter never grants
a vote. **Mechanism**: §2.7. **Threatened by**: any message handler
that processes term/log effects before this check. **Proof/test
obligations**: DM-9; a direct unit test with a stray, never-a-member
sender.

### `MINIMUM VOTER INVARIANT`
**Statement**: a `RemoveServer` that would reduce the voter count to
`0` is deterministically refused; the mechanism never reaches or
persists a zero-voter `Configuration`. **Mechanism**: §2.6 shape 3's
explicit check. **Proof/test obligations**: a direct unit test
attempting exactly this on a 1-voter cluster.

### `SINGLE-SERVER TRANSITION SHAPE`
**Statement**: every accepted `EntryConfig` entry represents exactly
one of the four transitions in §2.6, relative to the immediately
preceding active configuration; no other transition is ever accepted
by any replica. **Mechanism**: §2.6's leader-side check plus every
replica's defense-in-depth re-check. **Threatened by**: a
leader-side-only check with no follower-side re-verification (a
leader bug could otherwise silently corrupt membership on followers
too). **Proof/test obligations**: a property test generating random
`(C_old, C_new)` pairs and asserting acceptance iff exactly one of the
four shapes holds.

### `MEMBERSHIP RECOVERY DETERMINISM`
**Statement**: after any restart, snapshot install, or divergent-suffix
repair, exactly one of {latest retained `EntryConfig` entry, snapshot's
`Meta.Configuration`, `Config.Bootstrap`} — in that fixed priority
order — determines `activeConfig`, with no other path ever setting it.
**Mechanism**: §6.2's fixed-priority reconstruction algorithm.
**Threatened by**: any code path that trusts a cached or
previously-computed `activeConfig` value across a restart instead of
re-deriving it. **Proof/test obligations**: DM-7; a direct test
restarting a node at each of §5's table rows and asserting the
resulting `activeConfig` matches the table's stated outcome exactly.

### `MEMBERSHIP REQUEST OUTCOME STABILITY`
**Statement**: a completed membership-change `RequestID` resolves to
the same recorded outcome after retry, restart, or leader failover,
indefinitely. **Mechanism**: §2.6/§10's `FSM`-owned, snapshotted
outcome table. **Proof/test obligations**: DM-6; a direct
retry-after-restart and retry-after-failover test pair, mirroring the
existing `CommitTxn`/`SetClusterVersionCommand` equivalents.

---

## 18. Test / proof matrix

| Failure mode / invariant | Unit | Deterministic fault | Property/random | Fuzz | Real-process | Race |
|---|---|---|---|---|---|---|
| `SERIALIZED MEMBERSHIP CHANGE` | ✓ (direct rejection test) | DM-5, DM-10 | ✓ (random interleavings) | — | — | `-race` on the chaos suite |
| `QUORUM CONTINUITY` / §2.3 proof | ✓ (unit-level majority-arithmetic table, all `n` 1–7) | DM-5, DM-10 | ✓ | — | §16 add/remove sequence | `-race` |
| `LEARNER NON-INTERFERENCE` | ✓ | DM-3 | ✓ | — | §16 | `-race` |
| `MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE` | ✓ | DM-9 | — | — | — | — |
| `MINIMUM VOTER INVARIANT` | ✓ | — | — | — | — | — |
| `SINGLE-SERVER TRANSITION SHAPE` | ✓ | DM-1..DM-11 (incidentally, via every scenario's own assertions) | ✓ (the four-shape generator) | ✓ `FuzzDecodeEntryConfig` (malformed `Data` never panics, mirroring `FuzzDecodeCommitTxn`'s existing discipline) | — | — |
| `MEMBERSHIP RECOVERY DETERMINISM` | ✓ (one test per §5 table row) | DM-7 | — | — | §16 restart step | — |
| `MEMBERSHIP REQUEST OUTCOME STABILITY` | ✓ | DM-6 | — | — | §16 | — |
| `NO SILENT FORMAT MISINTERPRETATION` (extended) | ✓ `TestControlKindRangesNeverCollide`; ✓ old-binary-decode-fails-closed unit test (feeding a real encoded `EntryConfig` payload into the *old* `DecodeSetClusterVersion`, mirroring `TestControlCommandMarker_NeverCollidesWithCommitTxnVersion`'s exact technique) | — | — | ✓ `FuzzDecodeEntryConfig` | §16's mixed-binary variant | — |
| `ROLLBACK BOUNDARY HONESTY` (extended to generation 2 / snapshot `FormatVersion 2`) | ✓ (byte-identical-at-generation-1 encoding test, mirroring the existing `v0.4.0` equivalents) | — | — | — | §16's mixed-binary variant | — |
| Leader self-removal (§4.2) | ✓ (direct unit test: propose self-removal, commit, assert step-down) | DM-2 | — | — | §16 | — |
| Stale removed node rejoin attempt (§4.4, §4.5) | ✓ | DM-9 | — | — | ✓ (kill+restart-with-old-data-dir a removed node in `membership_integration_test.go`) | — |
| Even-voter-count warning (§12.1) | ✓ (response field assertion) | — | — | — | — | — |
| RBAC / audit for all four endpoints | ✓ (decision-table test, existing pattern extended) | — | — | — | ✓ (real-process RBAC/audit test, existing `security_integration_test.go` pattern extended) | — |

---

## 19. Release acceptance gates (`v0.5.0`)

Per this document's own instruction, the phase is **not** complete on
add/remove happy-path alone. Objective gates, all required:

1. Every invariant in §17 has a passing test at the level(s) §18 maps
   it to; `go test ./... -race` green including the new suites.
2. The full §15 deterministic scenario list (DM-1 through DM-11) passes
   at the same seed-count discipline `docs/testing-strategy.md` §6.5
   already establishes for Phase 7 (fast default in CI; a documented
   `CHRONICLEDB_CHAOS_SEEDS`-driven larger local/manual run clean).
3. The §16 real-process proof (3→4 voters via learner catch-up →
   promote → forced failover → remove → restart-and-verify) passes,
   including its mixed-`v0.4.0`/`v0.5.0`-binary variant.
4. `FuzzDecodeEntryConfig` and `TestControlKindRangesNeverCollide` pass;
   the old-binary-fails-closed unit test (§18) passes.
5. Membership changes are proven refused pre-finalization (§8.2) by a
   direct test, and proven to work correctly immediately post-finalization
   by the same real-process suite.
6. `docs/invariants.md`, `docs/raft.md` (a new "§11 Dynamic membership"
   or equivalent resolved-decisions section, mirroring the existing
   `§9`/`§10` Phase-implementation-notes pattern that document already
   uses), `docs/snapshots.md` (a new resolved-decisions section, same
   pattern), `docs/upgrades.md` (a short note that generation 2 exists
   and what it gates), a new `docs/membership.md`, a new
   `docs/adr/0018-dynamic-membership-architecture.md`, `docs/architecture.md`
   §1 (the "static three-node cluster" line updated to "static unless
   changed via the documented membership mechanism"), `CHANGELOG.md`,
   and `README.md`'s phase-summary paragraph are all updated in the same
   pass that lands the implementation — mirroring exactly this
   project's own established phase-completion discipline (see this
   repository's own working convention: every prior `v0.2.0`–`v0.4.0`
   release updated its full set of affected docs in the landing commit,
   never as a follow-up).
7. `docs/enterprise-v1-plan.md` §8 itself is **not** rewritten (it
   remains the plan-level record it always was) — only this plan
   document, `docs/invariants.md`, and the phase's own primary docs are
   updated at implementation time, consistent with how `v0.2.0`–`v0.4.0`
   never rewrote their own `enterprise-v1-plan.md` sections either.
8. No regression in any existing scenario: the full pre-existing test
   suite (Phases 1–10, `v0.2.0`–`v0.4.0`) remains green, unmodified in
   its own assertions (only extended where this phase's own changes —
   e.g. `Config.Peers` → `activeConfig.Voters` — mechanically require
   call-site updates, not assertion weakening).

---

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
  binding plus `MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE` are the accepted
  defenses instead.
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
next begins, mirroring this project's own established
"implement → test against invariants → move on" discipline:

**Slice 1 — `internal/raft` core mechanism (no wire/durability change yet)**
- `types.go`: add `EntryType`, `Entry.Type`; add `Configuration`,
  `Member`.
- `config.go`: replace `Config.Peers []NodeID` with
  `Config.Bootstrap Configuration`; remove `Config.majority()`.
- `core.go`: add `activeConfig`/`activeConfigIndex`/`snapshotConfig`
  fields; update `NewCore`/`NewCoreFromSnapshot` signatures to accept
  an initial `Configuration` (from snapshot or bootstrap); rewrite the
  seven identified `cfg.Peers`/`cfg.majority()` call sites (§2.2) to use
  `activeConfig`; add `ProposeConfigChange`; add the append-time
  activation logic in `handlePropose` (leader) and
  `handleAppendEntriesRequest` (follower accept path); add the
  revert-on-truncate logic; add §2.7's message-acceptance check at the
  top of `Step`'s dispatch; add the §3.1 relaxed-acceptance case for a
  node whose own `activeConfig` doesn't yet include itself.
- `messages.go`: add `Message.SnapshotConfiguration` (or equivalent
  explicit fields) for `MsgInstallSnapshotRequest`.
- Unit tests: majority-arithmetic table, the four-shapes property test,
  `MEMBERSHIP-SCOPED MESSAGE ACCEPTANCE` unit tests, revert-on-truncate
  unit tests, self-removal step-down unit test.

**Slice 2 — `internal/fsm` outcome table (no Core dependency)**
- New file `internal/fsm/membership.go`: local mirror of the `0xF0`
  marker constant (with the `TestControlKindRangesNeverCollide` guard),
  `RecordMembershipOutcome`, the fingerprint-mismatch check (§10).
- `FuzzDecodeEntryConfig` target (decodes the raft-native payload
  shape for fuzz purposes even though `internal/fsm` never decodes it
  in production — mirrors this package's existing fuzz-coverage
  discipline for every other wire-adjacent format it's aware of).

**Slice 3 — `internal/snapshot` format extension**
- `snapshot.go`: `Meta.Configuration`; `FormatVersion` `1`→`2`;
  encode/decode extension.
- Byte-identical-at-`FormatVersion-1`-input round-trip regression test
  (there is no such thing here — `v0.5.0` *does* bump the version, per
  §7.1's honest divergence from `v0.4.0`'s own non-bump — so this test
  instead asserts the *reverse*: an old `FormatVersion 1` file remains
  fully readable forever, exactly as `docs/snapshots.md` §5 already
  requires for any version it still supports, alongside the new,
  separate assertion that a `FormatVersion 2` file is correctly refused
  by code that only knows version 1, per §7.1).

**Slice 4 — `internal/node` wiring**
- `storage.go`: `encodeEntryPayload`/`decodeEntryPayload` additive
  `Type` byte.
- `node.go`: `Config.Bootstrap` (replacing `Peers`); §6.2's recovery
  reconstruction call into `NewCoreFromSnapshot`; `AddLearner`/
  `PromoteToVoter`/`RemoveServer` methods (mirroring
  `FinalizeUpgrade`'s existing shape: precheck generation, idempotency
  check, propose, await); `applyCommitted`'s `entry.Type` dispatch
  (§2.5); dial-address table updates on configuration change (§1.8);
  `ErrNodeRemoved` (§4.6); `handleInstallSnapshot`'s `Configuration`
  adoption (§7.2); `Status()` field additions (§14).
- `internal/transport`: peer-identity-vs-configuration check (§13.2);
  no "remove peer" operation needed (§1.8).

**Slice 5 — `cmd/chronicledb-node` admin surface**
- New `membership.go` (§9); `authz.go` decision-table rows (§9);
  `-tags=integration` real-process suite (§16).

**Slice 6 — deterministic fault harness (§15)**
- `internal/fault`: `Cluster.ProposeConfigChange`; DM-1 through DM-11.

**Slice 7 — documentation and release**
- §19's complete doc-update list; `internal/version.MaxSupportedGeneration`
  bump to `2`; `CHANGELOG.md` entry; tag `v0.5.0` once every §19 gate is
  met.

---

## 22. Open questions — resolved during this planning pass

Every release-critical question this document's own instructions
required resolving has a concrete decision above; recorded here for
traceability rather than left as narrative:

- *Where does membership state live, Core or FSM?* → Core, with FSM
  owning only the outcome-idempotency side-record (§2.4).
- *Effective-on-append or effective-on-commit?* → Append (§2.2),
  because the §2.3 proof requires it.
- *Joint consensus or single-server?* → Single-server, serialized, with
  a formal proof (§2.3) rather than an assertion.
- *How does an old binary fail closed on a new entry type it cannot
  even represent (`Entry.Type` doesn't exist for it)?* → gob's
  field-dropping plus a deliberately mirrored `0xF0` marker byte
  reusing the existing FSM control-command fail-closed path (§2.5),
  with the marker-value collision caught and fixed during this planning
  pass itself (§2.5's "Correction" — control-kind byte ranges
  `internal/fsm` `1..15`, `internal/raft` `16+`, guarded by a
  cross-package test).
- *Does the snapshot format version bump, unlike `v0.4.0`'s?* → Yes,
  honestly, because unlike `v0.4.0` this phase adds new outer-frame
  content (§7.1).
- *Are even-sized voter sets allowed?* → Yes, with a non-blocking
  warning (§12.1).
- *Is there a minimum voter count?* → `1`, enforced deterministically;
  `0` is structurally forbidden (§12.2).
- *Do membership changes require finalization first?* → Yes,
  unconditionally, as the *primary* safety mechanism, with the
  wire-format fail-closed behavior as defense in depth only (§8.2).

### Non-blocking risks / residual open items for the implementing session

- The exact `PromotionMaxLagEntries` default (§3.3) is a tuning
  judgment call with no safety consequence either way (it only affects
  *when* an operator is allowed to promote, never whether a promotion
  that *is* allowed is safe) — the implementing session may pick `0`
  (fully caught up) as the conservative default and expose it as a
  flag without further architectural review.
- `docs/membership.md`'s exact prose/runbook structure is left to the
  implementing session (mirroring how `docs/backup.md`/`docs/upgrades.md`
  were each written fresh at their own implementation time, not
  pre-drafted by their planning predecessors) — this document specifies
  every fact that document must contain, not its prose.
- The precise HTTP status code choices in §9 (`409`, `412`, `425`) are
  reasonable, self-consistent picks but not load-bearing to any
  invariant — the implementing session may adjust them for consistency
  with whatever `cmd/chronicledb-node` convention has evolved to by
  then, provided the underlying `Outcome`/error semantics are
  unchanged.
