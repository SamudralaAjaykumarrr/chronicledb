# ADR-0017: Compatibility / Rolling Upgrades Architecture

Status: Accepted

## Context

Through `v0.3.0`, `internal/version` reported a build-time version
string that nothing else in the codebase read to make a compatibility
decision: there was no wire-protocol version handshake, no WAL/
snapshot/FSM-command format generation concept, and no tested
mixed-version behavior — every existing test ran one binary version
against itself. `docs/versioning.md` documented SemVer policy for what
counts as a *breaking change*, but not how a *live cluster* survives a
breaking change during a rolling deployment. `docs/enterprise-v1-plan.md`
§7 ("Compatibility / Rolling Upgrades") is the designated resolution
point, targeted at release `v0.4.0` (not yet tagged). This ADR records
the architecture actually implemented for that phase.

This phase exists specifically so that a *later* phase (Dynamic
Membership, §8, which adds new Raft/FSM command kinds of its own) has a
tested, general mechanism to roll out format changes safely, instead of
inventing one under the pressure of that later phase's own,
higher-risk, Raft-touching work.

## Decision

Five surfaces each gain an explicit version/generation concept, checked
at the point it is interpreted — extending the pre-existing
per-frame/per-record version-byte discipline (`docs/wal.md` §3,
`docs/snapshots.md` §5) rather than replacing it:

1. **`internal/version.MaxSupportedGeneration`** (new `const uint32 =
   1`): the one genuinely new correctness input this phase adds to a
   package that was previously purely diagnostic (see that constant's
   own doc comment for why its doc comment had to change). Generation 0
   is, by definition, every format exactly as it existed through
   `v0.3.0` — never redefined.
2. **Wire protocol** (`internal/raft.Message.SenderGeneration`): rides
   on every ordinary Raft message rather than a dedicated handshake
   message. `internal/transport` already gob-encodes `Message`
   end-to-end (its own package doc comment), and `encoding/gob` is
   self-describing per struct field — a pre-`v0.4.0` peer simply never
   populates this field (a new binary decodes it as generation 0) and
   silently ignores it when a new binary sets it on a message sent to
   an old peer. This was chosen deliberately over a one-time preamble
   exchanged before any Raft message: a preamble would have required an
   unmodified pre-`v0.4.0` binary to already understand a brand-new
   message kind it was never built to parse, which the "build and
   execute actual old/new binaries, do not fake it" proof requirement
   this phase was built under makes impossible by construction.
   `internal/raft.Core` itself never reads or writes this field —
   `internal/node` populates it on every outbound `Output.Messages`
   entry and records each peer's last-reported value on receipt,
   exactly mirroring the pre-existing `Message.Seq` field's own
   driver-owned, Core-opaque pattern.
3. **FSM command format** (`internal/fsm.ControlCommandMarker`,
   `SetClusterVersionCommand`): a brand-new FSM command kind — the
   *cluster-version finalize command itself* — introduced via a
   discriminator byte (`0xF0`) chosen to never collide with
   `commitTxnCommandVersion`'s own small, sequential range (currently
   `2`). Every existing `CommitTxnCommand` remains encoded exactly as
   before; the new command kind is dispatched on *before* falling
   through to `DecodeCommitTxn`. This buys unmodified backward
   compatibility for free: a pre-`v0.4.0` node's own unmodified
   `applyCommitted`, which unconditionally decodes every committed
   entry as a `CommitTxnCommand`, already fails closed
   (`ErrUnsupportedCommandVersion` -> `Node.fail`, which halts that
   node's event loop and — since `main.go`'s shutdown loop returns once
   `Node.Done()` fires — exits its process) on any control-command
   entry, with zero code change to that binary. Proven directly, not
   assumed, by `TestMixedVersion_OldBinaryRejectedAfterFinalize`.
4. **WAL/snapshot-state format**: `internal/wal.Metadata.ClusterGeneration`
   and `fsm.FSM`'s own `clusterGeneration` field (serialized inside
   `EncodeState`'s opaque state blob, which `internal/snapshot`'s outer
   frame already carries via an explicit length-prefixed field — so
   `internal/snapshot` itself needed **no code change at all**: it was
   already generation-agnostic by construction). Both trailing fields
   are appended **only when nonzero** — generation 0 (the default, and
   the only value any pre-`v0.4.0` binary ever produced or expected)
   encodes byte-identical output to every prior release. This is the
   actual mechanism, not merely a documented promise, behind "before
   finalize, new nodes must still write old-generation formats (so an
   old node can still be rolled back and rejoin)."
   `wal.decodeMetadata` additionally already tolerated trailing bytes
   it did not recognize (it never validated `len(b)` against
   `11+idLen`) — a pre-existing, previously-incidental property this
   phase now relies on deliberately: a pre-`v0.4.0` binary's own
   unmodified `decodeMetadata` reads a post-finalize metadata record's
   `NodeID`/`FormatVersion`/`LatestSnapshotIndex` correctly and simply
   never learns `ClusterGeneration` exists.
5. **Cluster version / finalize**: a cluster-wide, Raft-replicated
   "minimum understood generation," itself an FSM command (`SetClusterVersionCommand`,
   surface 3 above). `internal/node.UpgradePrecheck` reports every
   configured peer's last-known generation (learned from live Raft
   traffic — surface 2); `FinalizeUpgrade` checks leadership *first*
   (a follower's own peer-generation view is architecturally
   incomplete — see its doc comment — so leadership is never decided
   from that possibly-partial view), then re-confirms precheck, then
   proposes `SetClusterVersionCommand{TargetGeneration:
   currentGeneration+1}` through the ordinary Raft log — exactly the
   same `Propose`-shaped, event-loop-dispatched path `CommitTxnCommand`
   already uses (`ProposeControl`/`handleControlPropose`), so finalize's
   crash-atomicity is inherited from the pre-existing Raft/FSM
   machinery, not reimplemented. `FSM.ApplySetClusterVersion` accepts
   only `TargetGeneration == current+1` (single-step; N/N+1 only) and
   `TargetGeneration > current` (strictly forward) — both evaluated
   identically by every replica from the replicated command and prior
   state alone, never a per-node judgment call (`STATE MACHINE SAFETY`
   preserved). Idempotency is a dedicated `RequestID -> Outcome` table
   distinct from `CommitTxnCommand`'s own (simpler semantics — no
   fingerprint-mismatch detection — are sufficient and appropriate for
   a rare, single-purpose admin command).

`/admin/upgrade/precheck` (GET, read-only, `admin`-gated) and
`/admin/upgrade/finalize` (POST, `admin`-gated, audited via the
pre-existing `security.wrap` middleware chain — see `ADR-0015`) are the
new HTTP surface, plus a standalone `-upgrade-precheck=<addr>` CLI dry
run that never opens `-datadir`/calls `node.Open`.

## Alternatives Considered

- **A dedicated wire-handshake message exchanged before any Raft
  message.** Rejected: an old, unmodified binary cannot be taught to
  recognize a new message kind after the fact, and this phase's own
  binding constraint ("do not fake mixed-version proof using one binary
  with different flags... build and execute actual old/new binaries")
  meant the mechanism had to work against a real, already-built,
  unmodifiable pre-`v0.4.0` binary. Piggy-backing on `gob`'s existing
  field-tolerant encoding of the *ordinary* message stream both
  satisfies that constraint and is simpler (one new struct field, not a
  second message-handling code path).
- **A single, unified "protocol version" integer covering all five
  surfaces at once.** Rejected: the five surfaces evolve independently
  in practice (a future phase might need a new FSM command without
  touching the WAL frame format at all) — `docs/enterprise-v1-plan.md`
  §7 explicitly frames them as five separately-versioned surfaces, and
  collapsing them into one shared counter would force every surface to
  bump together even when only one actually changed.
- **Bumping `wal.FormatVersion`/`snapshot.FormatVersion`'s existing
  strict-equality-checked constants for this phase's own new content.**
  Rejected: this phase introduces no new WAL frame content or outer
  snapshot-frame content at all (the one new durable thing —
  `SetClusterVersionCommand` — travels as an ordinary, already-opaque
  `RecordTypeLogEntry`/FSM-state payload); bumping either constant would
  have made every pre-finalize file unreadable by an old binary for no
  reason, directly breaking the rollback-safety requirement.
- **Trusting a node's own persisted log tail as "committed" after
  restart**, to make an isolated/solo restarted node immediately
  re-apply whatever is already durably on disk. Rejected — not even
  considered as a change, since `raft.NewCoreFromSnapshot` already,
  correctly, resets `commitIndex` to the local snapshot boundary on
  every restart (classical Raft safety: an uncommitted tail must not be
  trusted until a leader reconfirms it). This phase's own integration
  test (`TestMixedVersion_OldBinaryRejectedAfterFinalize`) had to be
  designed around this existing, correct behavior — see that test's
  comments for the reasoning.

## Consequences

- Every later Enterprise V1 phase that changes a wire/FSM-command/WAL/
  snapshot/schema format (Dynamic Membership, §8, first) now has a
  tested, general "gate the change behind a generation bump, finalize
  only once every live node has upgraded" mechanism to reuse, rather
  than inventing one under that phase's own, higher-risk pressure.
- `docs/upgrades.md`'s runbook (precheck -> roll nodes one at a time ->
  verify -> finalize) is now an operationally real procedure, not
  aspirational documentation — it is exactly what
  `TestMixedVersion_CriticalUpgradeProofScenario` executes end-to-end
  against real binaries.
- A rollback attempted after finalize is not silently accepted and not
  merely operationally discouraged: it is refused by the same
  determinism-preserving mechanism that made the command safe to apply
  in the first place (`applyControlEntry`'s capability check,
  `DecodeCommitTxn`'s free backward-incompatibility for a pre-`v0.4.0`
  binary).

## Correctness Implications

- `STATE MACHINE SAFETY` extends to a new command kind
  (`SetClusterVersionCommand`) under the identical discipline
  `CommitTxnCommand` already satisfies: `FSM.ApplySetClusterVersion` is
  a pure function of its own arguments and the FSM's own prior state,
  called once, in order, for every committed index, live or replayed.
- `RAFT ELECTION SAFETY`/`QUORUM SAFETY` are unmodified — this phase
  adds no new Raft message type and no change to `internal/raft.Core`'s
  own decision logic at all; `Message.SenderGeneration` is populated and
  read entirely by the driver (`internal/node`), never by `Core`.
- See `docs/invariants.md`'s new "Compatibility / Rolling Upgrades
  invariants (`v0.4.0`)" section (`NO SILENT FORMAT
  MISINTERPRETATION`, `ROLLBACK BOUNDARY HONESTY`, `MIXED-VERSION
  QUORUM SAFETY`) for the complete, itemized correctness argument.

## Testing and Proof Obligations

- Deterministic: `internal/fsm/clusterversion_test.go` (encode/decode
  round-trip, marker-never-collides, monotonic single-step, rollback-
  boundary rejection, idempotent retry, a `FuzzDecodeSetClusterVersion`
  target); `internal/fsm/snapshot_test.go`'s
  `TestEncodeStateDecodeStateRoundTrip_ClusterGeneration` and
  `TestDecodeState_RejectsGenerationFieldWrongLength`;
  `internal/wal/generation_test.go` (metadata round-trip,
  backward-compatible-trailing-bytes regression pin, `Open`'s
  `ErrUnsupportedGeneration` rejection, `SetClusterGeneration`'s own two
  guardrails, restart persistence).
- In-process real-cluster: `internal/node/upgrade_test.go`
  (`TestUpgradePrecheckAndFinalize_RealCluster`,
  `TestFinalizeUpgrade_RestartPersistsGeneration`) — real WAL-backed
  storage and real TCP transport, one binary throughout (this package
  proves the mechanism converges/commits/persists correctly; it does
  not, by itself, prove old/new interoperation).
- Real mixed-binary (`cmd/chronicledb-node/mixed_version_test.go`,
  `-tags=integration`): two actual, independently-built binaries — the
  current working tree, and the pre-`v0.4.0` baseline commit checked
  out into a temporary `git worktree` and built from there —
  `TestMixedVersion_CriticalUpgradeProofScenario` (the full numbered
  scenario: old-binary cluster start, continued traffic, one-node-at-
  a-time upgrade, a forced real leader failover while versions are
  mixed, continued traffic and a RequestID retry across that failover,
  final-node upgrade, precheck, finalize, a full-cluster restart, and
  every acknowledged RequestID's outcome verified on every node),
  `TestMixedVersion_RollbackBeforeFinalizeIsSafe`, and
  `TestMixedVersion_OldBinaryRejectedAfterFinalize`.
- RBAC/audit: `authz`'s decision table and
  `cmd/chronicledb-node/auth_test.go`'s HTTP-layer RBAC test both extend
  to the two new endpoints, admin-only, matching every other mutating
  administrative surface.
