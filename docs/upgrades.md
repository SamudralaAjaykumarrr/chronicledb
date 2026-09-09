# Compatibility / Rolling Upgrades

Status: `v0.4.0` (not yet tagged/released), `docs/enterprise-v1-plan.md`
§7, [`ADR-0017`](adr/0017-compatibility-and-rolling-upgrades.md).
Implemented in `internal/version` (`MaxSupportedGeneration`),
`internal/raft` (`Message.SenderGeneration`), `internal/fsm`
(`ControlCommandMarker`, `SetClusterVersionCommand`,
`ApplySetClusterVersion`), `internal/wal` (`Metadata.ClusterGeneration`,
`SetClusterGeneration`, `Open`'s `ErrUnsupportedGeneration` check),
`internal/node` (`UpgradePrecheck`, `FinalizeUpgrade`), and
`cmd/chronicledb-node` (`/admin/upgrade/precheck`,
`/admin/upgrade/finalize`, `-upgrade-precheck`).

## 1. What this is, and is not

Through `v0.3.0`, ChronicleDB had no wire-protocol version negotiation,
no WAL/snapshot/FSM-command format generation concept, and no tested
mixed-version behavior — every test and every real deployment ran one
binary version against itself. This phase is what makes an in-place,
one-node-at-a-time rolling upgrade a *supported, tested* operation
instead of an unverified assumption.

**Is**: a mechanism that lets a live, replicated cluster run a mix of
adjacent binary versions (N and N+1) safely, with an explicit,
operator-controlled point (`finalize`) at which the cluster commits to
the newer generation's format understanding, and an explicit, honest
boundary past which rollback is no longer supported.

**Is not**: automatic/unattended upgrade orchestration (an operator or
external tool drives node-by-node restart — ChronicleDB provides the
safety mechanism, not the orchestration); skip-version (N/N+2) upgrade
support; live schema migration (`ALTER TABLE`) tooling. See §7 Non-goals.

## 2. The five versioned surfaces

Each surface gains an explicit version/generation field, checked at the
point it is interpreted — see
[`ADR-0017`](adr/0017-compatibility-and-rolling-upgrades.md) for the
full design and rationale of each:

1. **Wire protocol** — every `internal/raft.Message` carries
   `SenderGeneration`, the sender's `internal/version.MaxSupportedGeneration`.
   This rides on the ordinary message stream (via `encoding/gob`'s
   field-tolerant encoding), not a separate handshake — a peer built
   before this field existed simply never sets it (read as generation
   0) and silently ignores it when present.
2. **WAL format** — `internal/wal.Metadata.ClusterGeneration`, appended
   to the metadata record only once nonzero (so a not-yet-finalized
   node's metadata is byte-identical to every pre-`v0.4.0` release).
   `wal.Open` refuses to start (`ErrUnsupportedGeneration`) if a data
   directory's recorded generation exceeds this binary's own.
3. **Snapshot format** — no code change was needed: the FSM state a
   snapshot carries is opaque to `internal/snapshot`'s own outer frame,
   which already carries an explicit length-prefixed field. The
   generation-aware content lives one layer down, in FSM state (next
   point).
4. **FSM command / state format** — `fsm.ControlCommandMarker` (`0xF0`)
   distinguishes a brand-new command kind, `SetClusterVersionCommand`,
   from every existing `CommitTxnCommand` (whose own version byte stays
   in a small, disjoint, sequential range). `fsm.FSM`'s own serialized
   state (`EncodeState`/`DecodeState`) carries the agreed cluster
   generation as a trailing field, again appended only when nonzero.
5. **Metadata/schema format** — `internal/sql/schema.go`'s
   `schemaRecordVersion` already carried an explicit version byte before
   this phase (Phase 8) and needed no change; it is documented here as
   the fifth surface for completeness, using the identical
   discipline the other four now formalize.

## 3. Cluster version / finalize

The cluster's **agreed generation** is itself Raft-replicated state,
changed only by a committed `SetClusterVersionCommand`
(`fsm.ApplySetClusterVersion`):

- Accepts only `TargetGeneration == currentGeneration + 1` — single
  step, matching the N/N+1-only support policy (no skip-version
  upgrades).
- Accepts only `TargetGeneration > currentGeneration` — the agreed
  generation only ever moves forward (`ROLLBACK BOUNDARY HONESTY`,
  `docs/invariants.md`).
- Both rejections are deterministic `StatusAborted` outcomes — every
  replica evaluates the identical command against the identical prior
  state and reaches the identical decision, never a per-node judgment
  call.

**Precheck** (`GET /admin/upgrade/precheck`, `admin`-gated,
read-only): reports this node's currently-applied generation, this
binary's own max supported generation (the generation finalize would
target), and every configured peer's last-known generation as observed
via live Raft traffic. `Ready` is true only once every peer is known
and reports at least the target generation. **Precheck's view from a
follower can be incomplete** — a follower only ever exchanges Raft
messages directly with the leader (plus whichever peers it happened to
vote for/against during a past election), never a full mesh of
pairwise traffic with every other follower — so always run precheck
against the current leader for the authoritative picture (also true of
`-upgrade-precheck`, below).

**Finalize** (`POST /admin/upgrade/finalize`, `admin`-gated, audited):
checks leadership first (a non-leader is refused with the standard
`NotLeaderError`/leader-hint shape `/propose` already uses — its own
precheck view is never consulted for this decision, since it can be
incomplete), then re-confirms precheck, then proposes
`SetClusterVersionCommand{TargetGeneration: currentGeneration+1}`
through the ordinary Raft log — the same crash-atomic,
event-loop-dispatched path `CommitTxnCommand` already uses. A crash
between precheck passing and the HTTP response returning leaves the
command either fully committed or not proposed at all, never partially
applied; a retried finalize call is idempotent (deterministic
per-target `RequestID`).

**CLI dry run**: `chronicledb-node -upgrade-precheck=<host:port>`
queries an already-running node's `/admin/upgrade/precheck` and prints
the result, then exits — it never opens `-datadir` and never calls
`node.Open`. (Plain HTTP only in this release; see §7 Non-goals.)

## 4. Runbook

1. **Precheck** against the current leader before touching anything:
   `chronicledb-node -upgrade-precheck=<leader-http-addr>` (or `GET
   /admin/upgrade/precheck` directly). Confirm every configured peer is
   already reachable and running a compatible version, or is about to
   be upgraded in the steps below.
2. **Roll one node at a time**: stop that node's process, replace its
   binary with the new version, start it again against its *same* data
   directory. It rejoins, catches up via ordinary Raft replication, and
   — until finalize — continues writing and reading exactly generation
   0's format, indistinguishable on the wire from a pre-`v0.4.0` peer.
3. **Verify** via `/status` (`ClusterGeneration`,
   `MaxSupportedGeneration`) and `/admin/upgrade/precheck` after each
   node. Traffic (reads/writes) continues normally throughout — there is
   no cluster-wide pause.
4. **Repeat** for every remaining node.
5. **Finalize only when safe**: once precheck reports `Ready` against
   the leader, `POST /admin/upgrade/finalize`. This is the one
   irreversible step (see §5).
6. A **crash at any point before finalize** (a node down between
   individual restarts, a partition, an operator's own process kill) is
   safe by construction: the cluster continues operating in generation
   0's semantics regardless of which physical binary each live node
   happens to be running, exactly as if no upgrade were in progress.

## 5. Rollback / downgrade semantics

**Before finalize**: redeploying the older binary onto a node that was
already upgraded is safe. It rejoins, replicates, and serves normally —
proven by a real two-binary test
(`TestMixedVersion_RollbackBeforeFinalizeIsSafe`,
`cmd/chronicledb-node`, `-tags=integration`). This works because every
format byte a not-yet-finalized cluster ever produces is, by
construction, identical to what the old binary already understands (§2,
§3) — rollback safety here is a property of the bytes on disk, not an
operational promise layered on top of them.

**After finalize**: rollback is not supported, and is not silently
accepted. Starting an older binary against a data directory (or
rejoining it to a live cluster) that has been finalized past what it
understands fails closed:

- If the old binary's data directory itself already carries a
  generation beyond what a hypothetical newer-but-still-limited binary
  supports, `wal.Open`'s own `ErrUnsupportedGeneration` check refuses
  to start before touching Raft/FSM replay at all. (A genuine
  pre-`v0.4.0` binary has no such check compiled in at all — its
  rejection happens at the next point instead.)
- Rejoining a live, already-finalized cluster: the old binary receives
  ordinary AppendEntries traffic like any reconnecting follower,
  attempts to apply the already-committed `SetClusterVersionCommand`
  entry, and its own unmodified `DecodeCommitTxn` fails with
  `unsupported CommitTxn command version` (see `ADR-0017` for why this
  requires no code change to the old binary at all) — the node's event
  loop stops and its process exits. Proven directly, with a real
  pre-`v0.4.0` binary, by `TestMixedVersion_OldBinaryRejectedAfterFinalize`.

This is the honest rollback boundary: **exactly the last successful
finalize**, surfaced via `/status`'s `ClusterGeneration` field, never a
vaguer "recent version" claim.

## 6. Failure semantics

- A node encountering a committed control command it cannot decode, or
  whose target generation exceeds its own capability, halts (`Node.fail`)
  rather than guessing — its process exits (`main.go` returns once
  `Node.Done()` fires), never silently continuing in a possibly-
  inconsistent state.
- `finalize` called while any configured peer still reports an
  insufficient/unknown generation fails outright — no Raft proposal is
  even attempted — rather than partially finalizing.
- A crash mid-rolling-upgrade (some nodes upgraded, some not, no
  finalize yet) is safe by construction (§4 point 6).
- Killing the process that issued `finalize` mid-call leaves the
  underlying command either fully committed (the normal Raft/FSM
  machinery already guarantees this) or not committed at all — a retry
  (same deterministic `RequestID`) is idempotent either way.

## 7. Non-goals

Automatic/unattended rolling upgrades (an operator or external
orchestrator drives node-by-node restart); N/N+2 (skip-version) upgrade
support; live schema migration tooling beyond existing SQL DDL;
authenticated/TLS support for the `-upgrade-precheck` CLI dry-run
specifically (it issues a plain HTTP GET — a deployment requiring
`-auth-mode`/TLS on its control-plane HTTP surface should query
`/admin/upgrade/precheck` directly with an authenticated HTTP client
instead of this convenience flag).

## 8. Deviations from `docs/enterprise-v1-plan.md` §7's literal text

- **Wire-protocol handshake mechanism**: §7 describes "`internal/transport`'s
  connection handshake exchanges each peer's supported version range
  before any Raft message." The implementation instead stamps
  `SenderGeneration` on every ordinary message (§2 point 1) rather than
  using a dedicated preamble exchanged once per connection. This is a
  functionally equivalent (every message a peer is version-checked
  against is itself version-tagged, continuously, not just at connection
  setup) but structurally different mechanism, chosen specifically
  because a one-time preamble would have required an already-built,
  unmodifiable pre-`v0.4.0` binary to parse a message kind it was never
  built to understand — impossible without violating this phase's own
  "do not fake mixed-version proof... build and execute actual old/new
  binaries" requirement. See `ADR-0017`'s Alternatives Considered.
- **"Range" of supported versions**: the implementation tracks a single
  `MaxSupportedGeneration` per binary (the highest generation it
  understands) plus the implicit minimum of generation 0 (every binary
  understands generation 0, by definition), rather than an explicit
  `[min, max]` range. Since only N/N+1 adjacent-generation operation is
  supported at all (§7's own explicit non-goal excludes skip-version
  upgrades), a single maximum plus an always-0 minimum is the complete,
  exact representation of what any V1 binary ever needs to express.

## 9. Related documents

[`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §7 (the original
plan); [`ADR-0017`](adr/0017-compatibility-and-rolling-upgrades.md)
(architecture and alternatives); [`docs/versioning.md`](versioning.md)
(SemVer policy this phase's binary-format compatibility rules extend);
[`docs/wal.md`](wal.md) §14, [`docs/snapshots.md`](snapshots.md) §10
(this phase's generation sections in each format's own primary doc);
[`docs/invariants.md`](invariants.md)'s "Compatibility / Rolling
Upgrades invariants (`v0.4.0`)" section.
