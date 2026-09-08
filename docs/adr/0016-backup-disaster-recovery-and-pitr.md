# ADR-0016: Backup / Disaster Recovery / PITR Architecture

Status: Accepted

## Context

Through `v0.2.0`, ChronicleDB's only recovery path is Raft/WAL replay
from surviving nodes' own persisted state
([`docs/recovery.md`](../recovery.md)); simultaneous loss of a majority
of nodes' storage is explicitly out of guarantee scope
([`docs/failure-model.md`](../failure-model.md) §5). There is no
portable export format, no way to recover from total cluster storage
loss, and no point-in-time recovery.
`docs/enterprise-v1-plan.md` §6 ("Backup / Disaster Recovery") is the
designated resolution point, targeted at release `v0.3.0` (not yet
tagged). This ADR records the architecture actually implemented for
that phase.

The task that authorized this phase carried one binding correctness
constraint: **a Raft snapshot is not, by itself, an enterprise backup.**
Backup must be proven to satisfy every requirement in
`docs/enterprise-v1-plan.md` §6 (portable format, manifest, checksums,
consistent MVCC/RequestID/schema capture, clean-cluster restore,
corruption rejection, crash safety, PITR) — not merely rename or copy an
internal snapshot file and call it done.

## Decision

A backup is a **self-describing, versioned export of a consistent
snapshot boundary plus the WAL log suffix needed to reach a chosen point
in time**, built entirely on `internal/snapshot` and `internal/wal`'s
own existing, validated mechanisms — never a second, independently-
evolving on-disk format (`docs/enterprise-v1-plan.md` §6's "reuse
mechanism, not invent" instruction, and `docs/vision.md`'s "smallest
technically real design" principle).

1. **Package `internal/backup`** (new): `Export`/`Restore` plus a JSON
   manifest with a trailing hex-CRC32 checksum line. It knows nothing
   about `internal/node` or `internal/raft` — `Export`/`Restore` both
   operate on a `snapshot.Meta`/`fsm.FSM` pair and a `*wal.WAL`, exactly
   the shape `internal/node` already manages internally, preserving
   `docs/architecture.md` §5's dependency layering.
2. **Export** writes a brand-new, self-contained directory:
   `outDir/snapshot` (via `internal/snapshot.Manager.Create`, unmodified)
   holding the base boundary's full FSM state (MVCC version chains,
   tombstones, and the complete RequestID/idempotency outcome table —
   already everything `internal/fsm.EncodeState` captures, since SQL
   schema/catalog state is itself stored as ordinary MVCC key/value
   pairs under a table's schema key, per `internal/sql/schema.go` — no
   separate catalog capture was needed), and `outDir/wal` (a freshly
   written WAL, via `internal/wal`, unmodified) holding exactly the
   requested suffix: every log entry copied through
   `wal.WAL.AppendLogEntry` using the identical encode path a live
   node's own log uses, entry payloads treated as fully opaque bytes
   (never decoded — internal/backup does not need to know they are
   Raft-term-prefixed `CommitTxnCommand`s to copy them correctly). A
   manifest (`manifest.json`) records format version, producer version,
   a diagnostic cluster ID, the base boundary
   (`LastIncludedIndex`/`LastIncludedTerm`), the WAL range actually
   captured, and a size+CRC32 checksum for the snapshot file and every
   WAL segment file — written durably last, via the same temp-file/
   fsync/atomic-rename sequence `internal/storage.WriteFileDurable`
   already provides, so a crash mid-export leaves no manifest at all
   (BACKUP INTEGRITY).
3. **Two backup schedules**, both using the same `Export` call with a
   different `UntilIndex`: **snapshot-only** (`UntilIndex` == the base
   boundary itself — no trailing WAL suffix; RPO bounded by "time since
   the source's own last snapshot boundary") and **continuous WAL
   archiving** (`UntilIndex` == `backup.UntilLatest` — every WAL entry
   currently durable; RPO bounded only by "time since this export ran").
   A live node's `Node.Backup(ctx, dir, continuous bool, clusterID)`
   exposes exactly this choice as a boolean, not a caller-supplied index
   — a caller cannot know the node's current base boundary in advance to
   ask for "exactly that" by number.
4. **Restore** validates the manifest's own checksum and format version,
   then every referenced component's size+checksum, then
   `internal/snapshot.Decode`'s own internal consistency check — all
   *before* a single byte reaches the target data directory (BACKUP
   INTEGRITY). It then builds the complete replacement data directory —
   snapshot installed via `internal/snapshot.Manager.Install` (the exact
   validated-install path a follower already uses for a peer-provided
   snapshot) and a freshly rebased WAL replayed from the backup's own
   WAL copy via `internal/wal.Replay` — in a **staging directory** beside
   the real target, made visible only via a single atomic directory
   rename as the last step (ATOMICITY / BACKUP CONSISTENCY): a crash at
   any point before that rename leaves the real target exactly as
   Restore found it, so re-running Restore after an interruption is
   always safe. The resulting directory is in *exactly* the layout
   `internal/node.Open` already expects and validates — WAL segments at
   the directory root, `snapshot/` as the only subdirectory — so restore
   is architecturally "recovery from a portable source" reusing the
   exact same validated recovery code path as an ordinary restart, never
   a new decode path.
5. **PITR boundary is a committed log index, never a timestamp.**
   `ExportOptions.UntilIndex`/`RestoreOptions.UntilIndex` both take a
   log index — the same index space as `internal/fsm`'s `CommitSeq`
   (`docs/architecture.md` §4: `internal/node`'s WAL log index *is* the
   FSM `CommitSeq`, by construction — `FSM.Apply(index, cmd)` is always
   called with the WAL/Raft log index as `index`). There is no
   wall-clock/timestamp restore boundary anywhere in this design: the
   manifest's own `CreatedAtUnixNano` field, like every other timestamp
   in this codebase, is diagnostic bookkeeping only, never a correctness
   input (`docs/architecture.md` §4's existing "never wall-clock
   timestamps" rule for `CommitSeq`/`StartSeq`, applied here verbatim).
   This is a deliberate, documented narrowing of
   `docs/enterprise-v1-plan.md` §6's own "-restore-until=<index|time>"
   CLI text: ChronicleDB has no wall-clock-to-log-index mapping to
   invent one from, and the task authorizing this phase explicitly
   forbids inventing precision the system does not have.
6. **DESTRUCTIVE RESTORE ISOLATION**: `Restore` refuses to run against a
   target data directory that already contains any WAL/snapshot state
   unless `RestoreOptions.Force` is set; `cmd/chronicledb-node`'s
   `-restore-from` flag requires the matching `-force-overwrite` flag
   for that case and always attempts an audit record (fatal to startup
   if that record cannot be written, mirroring AUDIT COMPLETENESS).
7. **Live-node backup is event-loop-serialized, exactly like
   `maybeSnapshot`.** `Node.Backup` dispatches to `handleBackup` on the
   node's own single event-loop goroutine (the same request/response
   channel pattern `Propose`/`BeginReadIndex` already use), so it reads
   a genuinely consistent view of `n.core`/`n.walog`/`n.snapMgr` — never
   a torn view racing a concurrent proposal or the node's own
   `maybeSnapshot` compaction cycle. It never mutates the live node's own
   retained WAL/snapshot state (no compaction, no pointer update) —
   backup is architecturally separate from Raft snapshot/compaction
   semantics, reusing only the *encoding*, never the *retention
   decision*.
8. **Backup/restore are administrative actions.** `/admin/backup` is
   RBAC-gated (`admin` or `operator`, per `docs/enterprise-v1-plan.md`
   §5's own RBAC section naming "future backup-trigger" as an operator-
   permitted action) and audited through the existing middleware chain
   (`docs/enterprise-v1-plan.md` §5). `-restore-from` is a startup-only
   CLI flag with no HTTP-authenticated principal available at that point
   in the process lifecycle; its audit record uses a fixed
   `"cli-operator"` principal, honestly reflecting that the operator's
   shell-level access to invoke the binary with these flags is itself
   the authorization boundary (the same reasoning
   `-enable-fault-endpoint` already relies on).

## Alternatives Considered

1. **Treat a Raft snapshot file as the backup artifact directly (rename/
   copy).** Rejected outright per this phase's binding constraint: a
   bare snapshot has no manifest, no checksum-of-the-whole-artifact, no
   PITR range, and no restore-into-a-clean-directory semantics — it is
   not portable evidence of anything beyond "the bytes `internal/snapshot`
   already trusted." `docs/enterprise-v1-plan.md` §6's every acceptance
   criterion (destructive restore drill, corruption rejection, RPO/RTO
   for two schedules, PITR-to-an-arbitrary-boundary) requires the
   manifest+WAL-suffix design actually implemented.
2. **Copy source WAL segment files verbatim (raw `os.ReadFile`/
   `os.WriteFile` of `*.seg` files) instead of re-appending through
   `wal.AppendLogEntry`.** Rejected: a source segment holds whatever mix
   of LogEntry/HardState/Metadata records and rotation boundaries it
   happens to have accumulated, which has no necessary relationship to
   the backup's own chosen boundary/suffix range. Re-appending through
   `internal/wal`'s own encode path produces a WAL whose segment layout
   is entirely this package's own — deterministic, minimal, and
   independent of the source's rotation history — while still never
   duplicating `internal/wal`'s decode logic (only its already-public
   `Open`/`Replay`/`AppendLogEntry`/`AppendMetadataSnapshot`/`Truncate`
   API).
3. **A live-node HTTP `/admin/restore` endpoint** (named in
   `docs/enterprise-v1-plan.md` §6's "Formats/APIs affected" list).
   Rejected: restore's own architecture (§6's "Architecture" prose, not
   merely the terse API-surface bullet) is unconditionally "a new
   node/cluster started with `-restore-from=<backup-dir>`... only then
   joins/forms a Raft group" — restore targets a not-yet-opened, clean
   data directory by construction. A live, already-running,
   already-Raft-joined node has no clean directory to restore into
   without first tearing itself down, which is a materially different
   (and riskier) operation than what §6's own architecture describes; a
   startup-only flag matches the documented design exactly and is
   strictly safer. `docs/enterprise-v1-plan.md` §0 itself states that
   "packages/files expected... is a plan, not a promise of exact shape."
4. **An offline `-backup-to=<dir>` CLI mode reading a stopped node's
   on-disk directory directly**, as the plan's CLI bullet also suggests.
   Deferred (not built): the plan's own text hedges this exact point
   ("`-backup-to=<dir>` (or a new `chronicledb-ctl backup` subcommand —
   see §11)"), signalling the CLI shape was intentionally left open to a
   later phase (§11, Operations). Building it now would mean either (a)
   scraping a directory that might be concurrently owned by a live node
   process with no locking primitive anywhere in this codebase to make
   that safe, or (b) opening a full `node.Open` (Raft core, transport,
   election timers) purely to take one backup — disproportionate
   machinery for what §5's RBAC/audit dependency already implies should
   be an authenticated action against a *running* node. The tested,
   implemented path (`POST /admin/backup` against a live node) satisfies
   every acceptance criterion this phase requires, including "real
   backup of a live three-node cluster under concurrent write load."
5. **Wall-clock PITR boundaries** (`-restore-until=<time>`, per the
   plan's literal CLI text). Rejected per this phase's explicit
   instruction not to invent precision the system does not have:
   ChronicleDB has no commit-timestamp concept anywhere
   (`docs/architecture.md` §4) — only a logical commit/log-index
   boundary, which is exactly what `RestoreOptions.UntilIndex` uses.

## Consequences

- New package `internal/backup` (manifest, export, restore) with no
  dependency on `internal/node`/`internal/raft`.
- `internal/node.Node` gains `Backup` (public) and `handleBackup`
  (event-loop-only), a new `backupCh` request/response channel
  alongside the existing `proposeCh`/`readIndexCh`, and two new metrics
  (`BackupsTotal`, `BackupsFailedTotal`).
- `internal/authz` gains `EndpointBackup` (`admin`+`operator` allowed,
  `read-only` denied) in the existing decision table.
- `cmd/chronicledb-node` gains `/admin/backup` (audited, RBAC-gated) and
  three new startup flags: `-restore-from`, `-restore-until`,
  `-force-overwrite`.
- No change to any existing WAL/snapshot on-disk format — backup reuses
  both as-is; this phase does not trigger a `docs/wal.md`/
  `docs/snapshots.md` format version bump.
- `docs/failure-model.md` §5's "simultaneous majority storage loss"
  bullet is narrowed (not removed): it remains true that ChronicleDB's
  own live-replication guarantees do not cover it, but a documented,
  tested resolution path (backup/restore) now exists outside that
  guarantee, per this ADR and `docs/backup.md`.

## Correctness Implications

- **No changes to any existing Raft/WAL/MVCC/Snapshot-Isolation/
  RequestID/snapshot-compaction invariant.** `internal/backup` only ever
  calls already-existing, already-validated public entry points of
  `internal/wal`/`internal/snapshot`; it never re-implements framing,
  checksums, or replay.
- New invariants added to `docs/invariants.md`: `BACKUP INTEGRITY`,
  `BACKUP CONSISTENCY`, `DESTRUCTIVE RESTORE ISOLATION`.
- Backup creation never mutates a live node's own retained WAL/snapshot
  state — `handleBackup` never calls `AppendMetadataSnapshot`,
  `Compact`, or `CompactBefore` on the node's own `n.walog`/`n.storage`;
  those remain exclusively `maybeSnapshot`'s responsibility.
- Restore never touches the target data directory until every
  referenced backup component has already passed checksum and internal-
  consistency validation, and even then only via a single atomic
  directory rename — there is no code path that partially writes into
  the real target directory.
- PITR restore boundaries are exact: `Restore`'s WAL replay loop stops
  strictly after copying the entry whose index equals the requested
  boundary, so the resulting WAL's own `NextIndex()` is provably
  `boundary+1` — no entry beyond the boundary can ever be present,
  independent of what the source backup itself contains.

## Testing and Proof Obligations

- `internal/backup`: `Export`/`Restore` round-trip for both snapshot-
  only and continuous-archiving backups, independently reconstructing
  expected state via a from-scratch reference-model helper (not
  `internal/backup`'s own internals) and comparing exported MVCC state
  structurally (`TestExportRestore_SnapshotOnlyRoundTrip`,
  `TestExportRestore_ContinuousPITRRoundTrip`); PITR restored to every
  boundary in a 10-entry history, with an explicit assertion that the
  restored WAL's own `NextIndex()` proves no post-boundary entry leaked
  in; out-of-range `UntilIndex` rejection; corrupted/truncated manifest,
  corrupted/missing snapshot, corrupted/missing WAL segment — each
  proven to leave the target directory absent or empty (never partially
  written); non-clean-target rejection and forced-overwrite replacement;
  simulated interrupted-restore retry (stale staging directory) proven
  safe/idempotent; repeated restore into fresh directories proven
  deterministic; `FuzzRestoreManifest` (arbitrary manifest bytes, never
  panics); `BenchmarkExport`/`BenchmarkRestore` for both schedules (the
  published RPO/RTO evidence, `docs/backup.md`).
- `internal/node`: a real, live, in-process three-node cluster's
  `Node.Backup` exercised under concurrent write load with disjoint
  RequestIDs cross-checked against the live leader's own recorded
  CommitSeq (`TestBackup_LiveClusterUnderConcurrentWriteLoad`); the
  destructive disaster-recovery drill — seed 20 commits, back up, `Stop`
  and never reopen every node's original data directory, restore three
  brand-new directories from the backup alone, start a brand-new cluster
  against them, confirm every pre-loss commit's outcome survived, and
  confirm the restored cluster accepts a genuinely new write afterward
  (`TestBackup_DestructiveDisasterRecoveryDrill`) — both run with
  `-race`.
- `internal/authz`: the existing role×endpoint decision-table test
  extended with `EndpointBackup`.
- `cmd/chronicledb-node`: `runRestore`/`recordRestoreAudit` unit tests
  (default/explicit/invalid `-restore-until`, non-clean-target/force,
  audit-entry verification via `internal/audit.Verify`); the RBAC
  decision table and unauthenticated-denied tests extended to
  `/admin/backup`; a real-OS-process (`integration` build tag) backup
  under concurrent write load and a real-OS-process destructive
  disaster-recovery drill using the actual `-restore-from` CLI flag on
  genuine subprocess invocations
  (`cmd/chronicledb-node/backup_integration_test.go`).
