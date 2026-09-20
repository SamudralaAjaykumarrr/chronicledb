# ADR-0018: Dynamic Membership Architecture

Status: Accepted

## Context

Through `v0.4.0`, cluster membership was fixed at bootstrap
(`ADR-0001`): `internal/raft.Config.Peers` and `internal/node.Config.
Peers`/`PeerAddrs` were the sole, permanent source of truth for who
participates in the Raft group, re-read and re-trusted on every process
start. `docs/enterprise-v1-plan.md` §8 ("Dynamic Membership," target
`v0.5.0`) is the designated resolution point: adding a node, promoting
a caught-up learner to voter, and removing a node, all without cluster
downtime, while preserving every safety property `v0.1.0`-`v0.4.0`
already established. The full, adversarially-reviewed design lives in
`docs/dynamic-membership-plan.md` (five revisions, each closing findings
from an independent correctness review); this ADR records the
architecture actually implemented.

## Decision

**Single-server changes, serialized — not joint consensus.** A
membership change is proposed, replicated, and committed as an ordinary
Raft log entry of a new kind (`EntryConfig`), constrained to exactly one
of four shapes (add one learner, promote one learner to voter, remove
one voter, remove one learner) and to at most one outstanding change at
a time, enforced by three proposal-time gates (P1: this leader has
already committed an entry of its own current term; P2: this leader's
own inherited log tail has itself already committed; P3: this leader
has not itself already appended an uncommitted change). Quorum
intersection between any two *adjacent* configurations (Lemma 1) plus a
branch-confinement invariant over the committed configuration chain
(Lemma 3) together prove split-brain is impossible under this
mechanism — see the plan's §2.3 for the full proof, including the
counterexample (DM-12) that defeats the P3-only serialization an
earlier revision relied on.

**Append-time-effective, not commit-time-effective.** The moment an
`EntryConfig` entry is appended — whether by a leader proposing it or a
follower accepting it — that node's active configuration switches
immediately, before the entry commits. This is the property the safety
proof depends on: without it, entries proposed after a config-change
entry but before its own commit would be evaluated under a stale quorum
size, reintroducing exactly the "two simultaneously valid quorum
definitions" hazard joint consensus exists to solve.

**Membership is `Core`-native, never FSM-native.** Considered and
rejected: encoding membership changes as `internal/fsm` control
commands (mirroring `SetClusterVersionCommand`'s existing pattern),
applied only at commit time. Rejected because `internal/fsm.Apply` only
ever runs after an entry commits, but the append-time-effective property
above requires the new configuration to govern quorum computation
*before* commit. The membership `RequestID -> Outcome` idempotency
table, which has no bearing on quorum, still lives in `internal/fsm`
(`RecordMembershipOutcome`) — the identical role that package already
plays for every other command kind, without requiring `internal/raft`
to depend upward on it.

**`ConfigAt` is the single configuration-reconstruction algorithm.**
Every site that ever needs a configuration — append-time activation,
accept-time activation and revert-on-truncate, restart, leader
initialization, snapshot creation, compaction, status reporting —
calls one pure, deterministic function over durable log/snapshot/
bootstrap state, in one fixed priority order. An earlier revision of
this design described configuration reconstruction three times, with
subtly different fallbacks that disagreed on a never-snapshotted node;
`ConfigAt` replaces all three.

**Snapshot format: Option B (bump the outer frame version), not
Option A (encode inside the opaque FSM state blob).** Considered and
rejected: keeping `FormatVersion 1` and encoding `Configuration`
additively inside `internal/fsm`'s own serialized state, mirroring how
`v0.4.0` encoded `ClusterGeneration`. Rejected because that would place
Raft-level membership inside `internal/fsm`'s state — the same layering
violation the FSM-native membership placement above was rejected for —
and because the consensus boundary (`LastIncludedIndex`/
`LastIncludedTerm`) already lives in the outer frame, which is where a
configuration effective at that boundary belongs. Chosen: bump
`FormatVersion` `1` -> `2`, add `Meta.Configuration`/
`Meta.HasConfiguration` to the outer frame, and implement a genuinely
version-aware decoder (`[MinReadVersion, FormatVersion]`) rather than
the strict-equality check bumping the constant alone would have broken
(an early revision of this plan asserted "no new logic, only the
constant bump" and was wrong — see `docs/snapshots.md` §11).

**Restore re-bootstraps membership from operator flags, on both durable
carriers.** `-restore-from` never resurrects the source cluster's
runtime membership — cluster identity and membership are properties of
the deployment, not the data, and `docs/backup.md` already documents
restoring onto a different peer set as supported. The transform acts on
both the staged snapshot (`Meta.HasConfiguration` cleared) and the
staged WAL suffix (any `EntryConfig` entry rewritten to a `Voided` kind
at its original index and term) — acting on the snapshot alone, which
an early revision of this plan did, is not sufficient: `ConfigAt`'s log
scan finds a source cluster's post-snapshot reconfiguration in the WAL
suffix before it ever consults the snapshot boundary. See
`docs/backup.md` §10a.

**Two narrow liveness rules replace a blanket message filter.** A
removed-but-still-running node's disruption is bounded by exactly two
rules on `RequestVoteRequest` only (membership-scoped vote acceptance,
and standard Raft §4.2.3 leader-contact suppression) — never by
filtering `AppendEntriesRequest`/`MsgInstallSnapshotRequest`/responses,
which an early revision of this plan did and which broke legitimate
transition traffic (a receiver's membership view is expected to be
temporarily ahead of or behind the sender's during any valid
transition — filtering replication on that basis would strand a
follower awaiting exactly the repair that traffic delivers). Safety
against a removed node was never provided by message filtering at all —
it follows entirely from the quorum-intersection proof above, with or
without any filtering.

**Removed-node retirement is authoritative cluster-side; drain
replication is rejected.** A node is removed the instant the
`EntryConfig` entry removing it **commits** under a majority of the new
configuration. Removal is a property of the cluster's committed
configuration chain, **never** of the removed node's local durable
state, and it is never conditioned on the removed node observing,
receiving, or acknowledging that entry. A removed node whose own disk
never learns is nonetheless retired: it is not a member, it cannot
become one again without an explicit operator re-add, and its own
belief about its membership carries no authority.

This is a decision, not a description of a gap. Investigation during
this work established that `activeConfig` is append-time-effective, so
every replication fan-out site (`appendLeaderEntry`,
`handleHeartbeatTimeout`, `becomeLeader`) iterates a
`Voters`/`Learners` set that already excludes the target by the time
the removal entry is sent — the entry is addressed to the new
configuration only. This applies to **every removed follower, not only
one that happens to be offline at commit time**: an online, fully
reachable removed follower is equally never told. (One incidental
exception exists and is explicitly not a mechanism: the
`AppendEntriesResponse`/`InstallSnapshotResponse` reply paths do not
consult membership, so a response already in flight at append time can
draw a reply carrying the entry. It does not occur in the deterministic
harness and nothing is built on it.)

**Continued replication to a departing member ("drain") is rejected for
`v0.5.0`**, and an explicit durable `DRAINING`/`RETIRING` state doubly
so:

- It is **not required for safety.** The quorum-intersection proof
  above never invokes it. A stale removed node is denied by log
  comparison — by term when its last-log term is lower, by index when
  the terms are equal, since it provably lacks the removal entry — at
  every majority of its own stale configuration, with message
  filtering removed entirely.
- It is **not required for durability.** The removal entry commits
  under a majority of the new configuration, and `commitIndex` is a
  prefix bound, so every previously committed entry is durable on a
  majority of the new configuration by log matching, independent of
  the departing node.
- It is **incompatible with a load-bearing requirement.** "Remove the
  node that died" is the primary reason removal exists, and it works
  precisely because the departing voter's acknowledgement is never
  required. A drain that waited for that acknowledgement would break
  it; one that did not wait would not be a drain.
- It **cannot help the case that motivated it.** An offline removed
  node is unreachable by definition. Drain would close only the online
  case, which the two liveness rules already bound.
- It is **disproportionate.** A per-peer lifetime, a termination
  condition tied to an acknowledgement that may never arrive, a
  durability decision, leader-change handoff, snapshot suppression, and
  a proof that the draining peer cannot leak into `majority()` — a new
  replication state machine, to buy liveness already bought twice.

`docs/dynamic-membership-plan.md` §1.8's reference to "§4.4's drain
semantics" named a mechanism §4.4 never specified and no code ever
implemented; it is deleted, and §4.4 is rewritten to state retirement
normatively. §4.1 now states the append-time replication boundary
directly, since leaving it implicit is what allowed the contradictory
prose to accumulate across five revisions in §1.8, §4.4, §4.5, §7.5
and §13.3.

**Consequences accepted.** A retired process does not shut itself down,
does not report itself retired, and does not stop campaigning;
`ErrNodeRemoved` is reachable for a self-removing leader and a
re-added-then-removed node, and **not** for an ordinary removed
follower. Terminating the process and wiping or archiving its data
directory are operator steps (`docs/membership.md` §7) — its durable
log still asserts the pre-removal configuration and is re-adopted
verbatim on any restart. One residual is recorded rather than implied
away: a member simultaneously behind on membership *and* out of leader
contact escapes both liveness rules and may grant one vote and bump its
term. That cannot elect the removed node, and no drain would change it,
because the escaping node is a current member that is behind.

**Deferred, explicitly not `v0.5.0`.** The online case could be closed
by a one-shot best-effort notification — a single extra
`appendEntriesMessage` to the departing node alongside the new
configuration's fan-out, built from its pre-append replication state.
Any such mechanism is bounded to: best-effort only (one message, no
retry, no acknowledgement tracking); never counted for quorum;
never required for safety or progress; no durable `DRAINING` state; no
replication-state-machine extension; failure to deliver does not
invalidate or delay the removal; and never escalating to a snapshot.
Recorded in the plan's §4.4 so the option is not re-litigated as a
design question.

## Alternatives Considered

- **Joint consensus** (the Raft paper's general two-phase `C_old,new`
  mechanism, supporting arbitrary concurrent reconfigurations).
  Rejected: this project's target cluster sizes (three to seven voters)
  and non-goal of concurrent reconfiguration make that generality
  unnecessary; single-server changes, serialized, are exactly as safe
  for every mutation this system needs (add one learner, promote one
  learner, remove one member) at a fraction of the implementation and
  proof surface.
- **FSM-native membership commands.** Rejected — see Decision above.
- **Snapshot Option A (encode inside the FSM state blob).** Rejected —
  see Decision above.
- **A permanent removed-ID tombstone/ledger**, to prevent ID reuse.
  Rejected as exactly the kind of policy this project's non-goals
  already reject; mTLS identity binding plus the two liveness rules are
  the accepted defenses instead (`docs/membership.md`).
- **In-place address mutation for an existing member.** Rejected as
  out of scope: re-IP'ing a node requires remove + re-add, which needs
  no new mechanism at this project's target cluster sizes.
- **Drain / `DRAINING` / `RETIRING` replication state for a departing
  member.** Rejected — see Decision above. Retirement is authoritative
  cluster-side and needs no delivery to the departing node.

## Consequences

- `internal/raft.Config.Peers`/`majority()` are removed outright,
  turning any unmigrated call site into a compile error rather than a
  silent stale-quorum bug.
- `internal/node.Config`'s own `Peers`/`PeerAddrs` fields are
  unchanged in shape, but become advisory-only past a data directory's
  first bootstrap — `docs/dynamic-membership-plan.md` §1.8.
- `internal/snapshot.FormatVersion` bumps to 2; `internal/version.
  MaxSupportedGeneration` bumps to 2; a fresh cluster now needs two
  `/admin/upgrade/finalize` calls to reach this binary's current max.
- `internal/backup` gains a dependency on internal/raft's *documented
  byte layout* (never an import) for its restore-side transform.
- Several real, previously-latent defects were found and fixed while
  implementing this design, all now regression-tested — see
  `CHANGELOG.md`'s `[0.5.0]` entry for the complete list. Two are
  worth calling out here because real-cluster testing (not unit tests)
  is specifically what found them: `raft.Core.handleInstallSnapshotRequest`
  accepted a stale/duplicated `InstallSnapshotRequest` already below
  `CommitIndex`, discarding log down to a lower boundary and leaving
  `CommitIndex() > LastIndex()`; and three reply-driven continuation
  sites kept replicating to a removed member off stale
  `nextIndex`/`matchIndex` bookkeeping after the two proactive fan-out
  sites had already correctly stopped. Both were found by DM-10's
  randomized combined fault schedule, not by any hand-written scenario
  — the reason that schedule is part of this phase's proof obligations
  rather than an optional extra.

## Correctness Implications

See `docs/invariants.md`'s "Dynamic Membership invariants" section for
the complete catalog (twelve new entries, one pre-existing entry
amended) and `docs/dynamic-membership-plan.md` §2.3 for the full
quorum-intersection / branch-confinement safety proof this ADR
summarizes.

## Testing and Proof Obligations

All of DM-1 through DM-22 (`docs/dynamic-membership-plan.md` §15) and
the full §16 real-process proof plan are implemented and passing as of
this ADR. By level:

- Unit level (`internal/raft`, `internal/fsm`, `internal/snapshot`,
  `internal/backup`): the four-shape validator, `ConfigAt`'s four
  invariants (DM-16, `TestConfigAtInvariantsProperty`, a 40-seed
  randomized property test), all three proposal gates, self-removal
  quorum exclusion, the two liveness rules, the membership outcome
  table's idempotency and generation-gated persistence, the snapshot v2
  format including its `hasConfig`/`configLen` cross-check, the
  restore membership-isolation transform including its negative
  control, `FuzzDecodeEntryConfig`, `FuzzDecodeEntryPayload`, and
  `TestControlKindRangesNeverCollide`.
- Cross-package format compatibility (`internal/node`, the smallest
  package that legitimately imports both `internal/raft` and
  `internal/fsm`): `TestEntryPayloadSentinelNeverCollides`, plus the two
  independent old-binary-fails-closed proofs §19 gate 5 requires —
  `TestOldBinaryFailsClosedOnEntryConfigWirePayload` (the §2.5 wire
  path: `gob` drops `Entry.Type`, so a pre-`v0.5.0` binary routes the
  entry on `Data[0] == 0xF0` into `fsm.DecodeSetClusterVersion`, which
  must fail closed on the membership kind byte) and
  `TestOldBinaryFailsClosedOnTypedEntryConfigWALPayload` (the §6.1a disk
  path, independent of the wire path because it exercises the typed
  header the wire never carries: an old `decodeEntryPayload` returns
  everything after the term, so the sentinel lands in
  `fsm.DecodeCommitTxn`'s version position and must be refused). Both
  assert against a genuine durable WAL record for an entry
  `Core.ProposeConfigChange` produced, not a synthesized payload
  (`internal/node/entry_payload_compat_test.go`). These three landed
  after the rest of this ADR, in the `v0.5.0` release-qualification
  pass that found them missing.
- Real-process level (`internal/node`, `cmd/chronicledb-node`): the
  full add/promote/remove lifecycle against a genuinely new process
  joining with an empty peer list, the sub-three-voter confirmation
  guard, a leader removing itself with the remaining voters electing a
  successor, the HTTP admin surface's status-code/reason mapping
  (including `ErrLeadershipLost` -> `409`), the post-election
  `changesReady` window observed on real processes, a real
  snapshot-in-flight-during-config-change restart, both required
  backup/restore steps against real processes
  (`membership_snapshot_backup_test.go`), a real backup produced by the
  actual, previously-released `v0.4.0` binary restoring cleanly under
  this binary (`TestRealMembership_BackupRestoreRealV040Binary`), and
  the mixed-`v0.4.0`/`v0.5.0`-binary dynamic-membership variant in
  which every node starts against its own genuine `FormatVersion 1`
  snapshot before rolling
  (`TestMixedVersionMembership_RollThenAddPromoteRemoveRestart`) —
  together closing out §16 end to end.
- Deterministic multi-node level (`internal/fault`): DM-1 through
  DM-9, DM-11 through DM-15, and DM-22, against real
  crash/partition/election dynamics, including DM-12's P1-disabled
  negative control (`TestDM12Step7_P1DisabledProducesADoubleCommitTheOracleDetects`,
  proving `committedOracle` actually detects the resulting double
  commit rather than merely passing on the happy path) and DM-14's
  self-removal replication-continuity scenarios (ordinary and
  abandoned-change/partition-repair variants). DM-10
  (`TestDM10_CombinedRandomizedMembershipSchedule`) extends the
  existing combined chaos schedule with membership actions and an
  independently-derived configuration-lineage oracle
  (`internal/fault/lineage_oracle_test.go`) that never asks `Core` for
  the answer it is checking; it found two real product bugs (the
  `InstallSnapshot`-below-`CommitIndex` and reply-driven-replication-
  to-a-removed-member defects listed under Consequences below) and four
  bugs in the harness itself before both were trusted, all now fixed.
  DM-22 (`internal/fault/membership_dm22_test.go`) calibrates that
  oracle in both directions: quiet on §2.3's genuine three-live-
  configuration safe state, and firing on a synthetic W1 violation.
- Configuration-change-aware read path (`internal/node`): DM-17's three
  §4.2a sub-cases (promote, remove, self-removal — the last as its two-
  phase-blocked-then-clean-failure schedule plus a positive phase),
  each also repeated with a real interleaved leader failover
  (`internal/node/dm17_test.go`, `dm17_failover_test.go`).
- Generation/format boundary (`internal/node`): DM-18's follower-side
  half (a synthetic `EntryConfig` fed to `applyConfigEntry` on a node
  pinned below generation 2, alongside the already-covered leader-side
  refusal), DM-19's stale-`confirmVoterCount` and zero-voter-refusal-
  despite-a-"correct"-confirmation cases, and DM-20's byte-level
  `Entry.Type` WAL round trip across the finalization boundary with its
  negative control (`internal/node/membership_test.go`'s
  `TestDM18_MembershipChangeGenerationGateBothSides` and
  `TestDM19_SubThreeVoterConfirmationStaleAndZeroVoterCases`;
  `internal/node/dm20_test.go`).
- Restore (`internal/node`, `internal/backup`): DM-21's full real-
  cluster scenario, both directions — the positive proof (a restored
  cluster with a different peer set electing a leader and serving
  traffic, plus a learner joining it before its own first snapshot and
  accepting voided entries) and the negative control (voiding disabled
  reproduces the exact `selfRemoved()`/never-elects failure) —
  `internal/node/dm21_test.go`.
- SQL / linearizable reads: §16 literally specifies a background
  goroutine issuing real SQL `SELECT`s against real OS processes, but
  `cmd/chronicledb-node` has no SQL wire protocol to issue them through
  (`docs/sql.md` §8 — SQL is a Go-library-only surface, by design, not
  a gap this phase closes). `internal/sql/dynamic_membership_test.go`
  substitutes the equivalent proof at this package's own real-TCP/
  real-disk/single-process tier: a background reader issues real
  `SELECT`s (each a real `BeginReadIndex` call) against the current
  leader throughout a full add/promote/remove/self-removal/crash-
  failover sequence, asserting every read either succeeds or fails with
  one of the documented clean error classes, never a bare deadline
  expiry. See `docs/testing-strategy.md` §12 for why this substitution
  is the accurate description of what was built rather than a gap.

**Two items remain genuinely open, not implemented in this release:**

- The peer-TLS-identity-vs-configured-membership transport-layer check
  §13.2 of the plan describes (verifying a connection's already-mTLS-
  verified identity against the `Member.ID` the current `Configuration`
  names for that address) was not built; only the pre-existing `v0.2.0`
  mTLS certificate-subject-to-`NodeID` binding applies. Tracked as a
  known limitation, not a safety gap — see `docs/membership.md` §7 and
  the Consequences section below.
- The `internal/node` driver-level cross-check between an
  `InstallSnapshotRequest`'s carried `Configuration`/`HasConfiguration`
  and the installed file's own `Meta` (§23/G8) is implemented in
  `handleInstallSnapshot` and unit-tested at the `internal/raft`
  unconditional-adoption level via `internal/fault`'s DM-8
  (`TestDM8_InstallSnapshotAdoptsConfigurationUnconditionally`), but has
  no dedicated driver-level test against a live `Node`'s own event-loop
  goroutine — the same class of difficulty DM-18's follower-side half
  had before it was solved by feeding the entry directly rather than
  through live replication. Similarly, a direct unit test asserting
  `computePrecheck`'s `Ready` stays `true` with an unreachable learner
  present exercises `LEARNER NON-INTERFERENCE` only indirectly today
  (through the full add/promote/remove lifecycle test), not as its own
  targeted case.

  (The "drain semantics" question raised during this work is no longer
  a remaining item; it is resolved as a decision — see
  **Removed-node retirement is authoritative cluster-side** below.)
