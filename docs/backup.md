# Backup / Disaster Recovery / PITR

Status: `v0.3.0` (not yet tagged/released), `docs/enterprise-v1-plan.md`
§6, [`ADR-0016`](adr/0016-backup-disaster-recovery-and-pitr.md).
Implemented in `internal/backup` (format, export, restore),
`internal/node.Node.Backup` (live-node export), and
`cmd/chronicledb-node` (`/admin/backup`, `-restore-from`,
`-restore-until`, `-force-overwrite`).

## 1. What this is, and is not

Through `v0.2.0`, ChronicleDB's only recovery path was Raft/WAL replay
from a surviving node's own local disk — simultaneous loss of a majority
of nodes' storage was, and remains, outside ChronicleDB's own live-
replication guarantee ([`docs/failure-model.md`](failure-model.md) §5).
This document describes the resolution path this phase adds: a
**backup** is a self-describing, versioned, checksummed export of a
consistent point in ChronicleDB's committed history, restorable into a
brand-new, empty cluster.

**A Raft snapshot is not, by itself, a backup.** A snapshot
([`docs/snapshots.md`](snapshots.md)) is an internal compaction
mechanism: it has no manifest, no portable format guarantee independent
of the node that produced it, and no PITR range. A backup, as
implemented here, reuses the exact same validated snapshot/WAL encoding
underneath, but wraps it in a manifest, a checksum layer above and
beyond each format's own internal checksums, and an explicit, selectable
committed-history range.

## 2. Backup format

A backup is a directory:

```
<backup-dir>/
  manifest.json       # written last, atomically — see §4
  snapshot/
    <index>.snap       # the base boundary's full FSM state
  wal/
    <segment files>     # the WAL suffix beyond the base boundary
```

- **`snapshot/`** holds exactly one file, written via
  `internal/snapshot.Manager` unmodified: the full committed state
  (every key's MVCC version chain, including tombstones, and the
  complete RequestID/idempotency outcome table) as of the backup's base
  boundary (`LastIncludedIndex`/`LastIncludedTerm`). SQL schema/catalog
  state needs no separate handling: it is stored as ordinary MVCC
  key/value pairs under each table's schema key
  (`internal/sql/schema.go`), so it is already part of this same MVCC
  export.
- **`wal/`** holds a freshly-written WAL, via `internal/wal` unmodified,
  containing exactly the log entries from `LastIncludedIndex+1` through
  whatever boundary the backup captured. Every entry is copied through
  `internal/wal.WAL.AppendLogEntry` using the identical encode path a
  live node's own log uses — the payload bytes themselves are never
  decoded or reinterpreted (they are already opaque to `internal/wal`;
  `internal/backup` preserves that boundary). This directory's own
  segment-file layout is independent of whatever segment rotation the
  *source* WAL happened to have — it is written fresh, from scratch.
- **`manifest.json`** — see §3.

This layout is *not* a live node's own data directory (that has WAL
segments directly at its root, with `snapshot/` alongside — see
[`docs/storage.md`](storage.md) §4): a backup directory is a portable
archive with its own two-subdirectory shape. `Restore` (§5) is what
turns a backup directory into a real, immediately-usable node data
directory.

## 3. Manifest

`manifest.json` is JSON, followed by a trailing line: an 8-hex-digit
CRC32 of exactly the JSON bytes preceding it. Fields:

| Field | Meaning |
|---|---|
| `formatVersion` | This manifest format's version (currently `1`). Checked before anything else is trusted. |
| `chronicledbVersion` | The producing binary's `internal/version.Version` string. Diagnostic only — Restore never checks this against its own version; there is no cross-version compatibility story yet ([`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §7, out of this phase's scope). |
| `clusterId` | The producing cluster's `-cluster` flag value, verbatim. **Diagnostic only** — restoring a backup into a differently-configured cluster (a different peer set, e.g. onto replacement hardware) is a legitimate, supported use of this feature, so this is never validated against a restore target. |
| `lastIncludedIndex`, `lastIncludedTerm` | The backup's base snapshot boundary — the same fields `internal/snapshot.Meta` carries. |
| `snapshotFile`, `snapshotSize`, `snapshotChecksum` | The `snapshot/` file's name, byte size, and CRC32. |
| `walFromIndex`, `walUntilIndex` | The inclusive range of log indices the `wal/` directory covers (`walFromIndex` is always `lastIncludedIndex+1`). |
| `walSegments` | Each `wal/` segment file's name, size, and CRC32. |
| `createdAtUnixNano` | Wall-clock creation time, **diagnostic only, never a correctness input** — identical in spirit to `docs/architecture.md` §4's "`CommitSeq`/`StartSeq` are never wall-clock timestamps" rule, applied here to the manifest's own bookkeeping. |

Every checksum here is layered *above* each underlying format's own
internal checksum (WAL's per-record CRC32, the snapshot's own CRC32) —
this lets `Restore` reject a truncated or replaced whole file cheaply,
before ever asking `internal/wal`/`internal/snapshot` to parse it.

## 4. Failure semantics

- **Crash mid-export**: `manifest.json` is written last, via the same
  temp-file/fsync/atomic-rename/directory-fsync sequence
  `internal/snapshot` already uses for its own file
  (`internal/storage.WriteFileDurable`). A crash at any point before
  that succeeds leaves a backup directory with no manifest at all —
  `Restore` (and any manual inspection) must, and does, refuse it
  outright.
- **Corrupted backup** (bad manifest checksum, bad manifest format
  version, a snapshot or WAL segment file that does not match its
  manifest-recorded size/checksum, a snapshot that fails
  `internal/snapshot`'s own internal-consistency check): `Restore`
  refuses to start and returns a specific error — it never partially
  restores, and it validates every referenced component *before*
  writing a single byte to the target data directory.
- **Interrupted restore** (process killed mid-replay): `Restore` builds
  the complete replacement data directory in a staging directory beside
  the real target, and only makes it visible via one atomic directory
  rename as its last step. A crash before that rename leaves the real
  target exactly as `Restore` found it (empty/absent, or, for a forced
  restore, its pre-existing content still there) — re-running `Restore`
  with the same arguments is always safe.
- **Repeated/idempotent restore**: restoring the same backup (and the
  same `-restore-until` boundary) into two independent, empty
  directories produces byte-for-byte identical committed state in both
  — proven by `TestRestore_RepeatedRestoreIntoFreshDirsIsDeterministic`.
- **Destructive restore isolation**: `Restore` refuses to run against a
  target data directory that already contains *any* WAL/snapshot state,
  unless the caller explicitly forces it
  (`RestoreOptions.Force`/`-force-overwrite`) — this never silently
  overwrites a live cluster's data.

## 5. Restore

`Restore(backupDir, dataDir, opts)`:

1. Reads and checksum-validates `manifest.json`.
2. Reads and validates the snapshot file (manifest checksum, then
   `internal/snapshot.Decode`'s own internal-consistency check) and
   every WAL segment file's checksum — entirely before touching
   `dataDir`.
3. Resolves the requested PITR boundary (`opts.UntilIndex`, or
   `backup.UntilLatest` for "everything this backup captured") against
   the manifest's own `[lastIncludedIndex, walUntilIndex]` range,
   rejecting anything outside it.
4. Refuses to proceed if `dataDir` already contains WAL/snapshot state,
   unless `opts.Force` is set.
5. Builds the complete replacement directory in a staging location:
   installs the validated snapshot via
   `internal/snapshot.Manager.Install` (the same validated-install path
   a follower already uses for a peer-provided snapshot), then replays
   the backup's own WAL copy and re-appends every entry up to and
   including the resolved boundary into a freshly-rebased WAL.
6. Atomically promotes staging into `dataDir` (§4).

The result is, byte-for-byte, what `internal/node.Open` already expects:
WAL segments at `dataDir`'s root, `dataDir/snapshot/` alongside. Restore
is therefore "recovery from a portable source," reusing
`internal/node.Open`'s own already-validated recovery path — there is no
separate decode path a restored node's *own* startup exercises that a
normal restart does not also exercise.

### Restoring a whole cluster

Restore operates on one data directory at a time. To rebuild an
`N`-node cluster from a single backup, restore that same backup into
`N` fresh, empty data directories (one per node), then start all `N`
node processes normally (same `-cluster`/`-peers` as before, or a new
membership if this is disaster recovery onto replacement hosts — since
the manifest's `clusterId` is diagnostic-only, this is fully supported).
Because every node restores the identical backup, all `N` copies start
with byte-identical committed history; Raft's own election then
proceeds exactly as it would after any other simultaneous restart.

### CLI

```bash
./chronicledb-node -id=n1 -listen=... -http=... -cluster=n1,n2,n3 -peers=... \
  -datadir=/var/lib/chronicledb/n1 \
  -restore-from=/mnt/backups/2026-09-08T12:00:00 \
  -restore-until=8500   # omit for "everything this backup captured"
```

Restore runs once, at startup, before the node ever opens
`-datadir` for real or joins the cluster. `-restore-from` requires
`-datadir` to be empty/absent unless `-force-overwrite` is also passed
(§4). On success, startup continues normally against the now-restored
directory — there is no separate "restore-only" exit mode.

## 6. Taking a backup

Backup is an admin/operator-gated, audited action against a **live,
already-running** node (`docs/enterprise-v1-plan.md` §5's RBAC section
names "future backup-trigger" as an operator-permitted operational
action, unlike `/fault` or membership changes):

```bash
curl -X POST 'http://127.0.0.1:8001/admin/backup?dir=/mnt/backups/2026-09-08T12:00:00&continuous=true'
```

- `dir` (required): a local filesystem path this node process can write
  to. V1 ships local-filesystem-path backup only — no built-in
  cloud-object-store integration (§9); copy the resulting directory
  wherever it needs to live yourself.
- `continuous`: `true` for continuous WAL archiving (RPO bounded only by
  "time since this call ran"); omitted/anything else for a
  snapshot-only backup (RPO bounded by "time since this node's own last
  snapshot boundary" — see §7).

`Node.Backup` reads this node's *currently-adopted* snapshot boundary
(whatever `internal/snapshot.Manager` already durably holds — or the
empty boundary, if this node has never snapshotted) and, for continuous
mode, every WAL entry currently durable beyond it. It never creates a
new snapshot or mutates this node's own retention (no compaction, no
pointer update) — backup is architecturally separate from Raft
snapshot/compaction (`docs/enterprise-v1-plan.md` §6). Like local
snapshot creation, the export itself runs on the node's single
event-loop goroutine and briefly blocks ticks/heartbeats/proposals for
its duration — an existing, accepted tradeoff (`maybeSnapshot` already
has the same characteristic), not something this phase introduces.
Large backups (a large WAL suffix) should therefore prefer more frequent
continuous-archiving exports over one very large deferred one, or accept
the brief pause.

Backup can be taken from any node — leader or follower — since it only
ever reads that node's own already-durable local state.

## 7. RPO/RTO model

**RPO** ("how much committed history could a backup lose") is bounded
by which schedule produced it:

- **Snapshot-only** (`continuous=false`): RPO is "time since this node's
  own last snapshot boundary" — every commit since then is not in the
  backup at all.
- **Continuous WAL archiving** (`continuous=true`): RPO is "time since
  this backup call ran" — every commit durable at that instant is
  captured.

**RTO** ("how long does restore take") is "manifest validation +
snapshot install + WAL suffix replay" — measured directly by
`internal/backup`'s own benchmarks
(`go test ./internal/backup/... -run '^$' -bench . -benchmem`). Measured
on this repository's development hardware (Intel Core Ultra 7 258V,
`GOMAXPROCS=8`; results vary by hardware and are not a guaranteed SLA —
republish these numbers against target hardware before relying on them
operationally):

| Schedule | Keys in base snapshot | WAL suffix | Export | Restore |
|---|---|---|---|---|
| Snapshot-only | 1,000 | 0 entries | ~17.7 ms | ~14.4 ms |
| Continuous WAL archiving | 1,000 | 1,000 entries | ~22.3 ms | ~16.5 ms |

(`BenchmarkExport`/`BenchmarkRestore`, `internal/backup/bench_test.go`.)
Both scale with the size of what they process — a snapshot-only backup's
Export/Restore cost tracks the base snapshot's key count; a continuous
backup's additionally tracks the WAL suffix's entry count. There is no
theoretical upper bound published here beyond what the benchmarks
measure directly; an operator with a much larger dataset should run the
same benchmark against representative data before depending on a
specific RTO number.

## 8. PITR boundary model

The restore boundary (`-restore-until` / `RestoreOptions.UntilIndex`) is
a **committed log index** — the same index space as `internal/fsm`'s
`CommitSeq` (`internal/node`'s WAL log index *is* the FSM `CommitSeq`;
`FSM.Apply(index, cmd)` is always called with that index). There is
**no wall-clock/timestamp restore boundary**: ChronicleDB has no
commit-timestamp concept anywhere in its design
([`docs/architecture.md`](architecture.md) §4 — `CommitSeq`/`StartSeq`
are logical, never derived from wall-clock time), so a
`-restore-until=<time>` form would require inventing precision the
system does not have. This is a deliberate, documented narrowing of
`docs/enterprise-v1-plan.md` §6's own CLI-flag bullet text — see
[`ADR-0016`](adr/0016-backup-disaster-recovery-and-pitr.md) Alternative
5.

A restore boundary is exact: `Restore`'s WAL replay stops strictly after
copying the entry whose index equals the requested boundary, so the
restored WAL's own `NextIndex()` is provably `boundary+1` — no
transaction committed after the selected boundary can ever appear, and
every transaction at or before it does (subject to it having been
included in the backup being restored from at all — see §7's RPO
discussion).

## 9. Non-goals

Consistent with `docs/enterprise-v1-plan.md` §6's own explicit
non-goals:

- **No encryption of the backup artifact.** A backup directory contains
  the same sensitive data the live cluster does; operators must apply
  their own access control/encryption-at-rest to wherever they store it.
- **No built-in cloud object-store integration.** Backup/restore operate
  on local filesystem paths only; copying a backup directory to S3/GCS/
  etc. is the operator's own responsibility.
- **No continuous/streaming replication to a warm standby** — a
  different feature from point-in-time backup.
- **No in-process backup scheduler.** Operators drive `/admin/backup` on
  whatever schedule they choose (`cron` or equivalent) against the CLI/
  HTTP surface.
- **No cross-version compatibility guarantee.** `chronicledbVersion` in
  the manifest is diagnostic only; there is no format-migration story
  yet (`docs/enterprise-v1-plan.md` §7, a later phase).

## 10. Deviations from `docs/enterprise-v1-plan.md` §6's literal text

`docs/enterprise-v1-plan.md` §0 states its own "packages/files
expected" lists are "a plan, not a promise of exact shape." Three
deviations, all reasoned and documented (see
[`ADR-0016`](adr/0016-backup-disaster-recovery-and-pitr.md) for the full
alternatives-considered analysis):

1. **No `-backup-to=<dir>` offline CLI flag.** The plan's own text
   hedges this ("`-backup-to=<dir>` (or a new `chronicledb-ctl backup`
   subcommand — see §11)"). Backup is implemented as the tested,
   RBAC/audit-gated `/admin/backup` HTTP endpoint against a live node
   instead — this is what §6's dependency on §5 (RBAC + audit) actually
   implies, and it is what the "real backup of a live three-node cluster
   under concurrent write load" acceptance criterion exercises.
2. **No `/admin/restore` HTTP endpoint.** Restore's own architecture
   description (§6 "Architecture", not the terse API-surface bullet) is
   unconditionally startup/CLI-shaped: "a new node/cluster started with
   `-restore-from=<backup-dir>`... only then joins/forms a Raft group."
   `-restore-from`/`-restore-until`/`-force-overwrite` implement exactly
   that; a live-node HTTP restore endpoint would require tearing down an
   already-running node first, which the plan's own architecture text
   never actually describes.
3. **`-restore-until` accepts a log index only, never a timestamp**
   (§8).

## 11. Related documents

- [`docs/enterprise-v1-plan.md`](enterprise-v1-plan.md) §6 — the
  authoritative plan this phase implements.
- [`ADR-0016`](adr/0016-backup-disaster-recovery-and-pitr.md) — full
  architecture decision record.
- [`docs/snapshots.md`](snapshots.md) — the underlying snapshot
  mechanism backup reuses.
- [`docs/wal.md`](wal.md) — the underlying WAL mechanism backup reuses.
- [`docs/recovery.md`](recovery.md) — ordinary restart recovery, the
  code path restore ultimately reuses.
- [`docs/failure-model.md`](failure-model.md) §5 — the "simultaneous
  majority storage loss" scope this phase's backup/restore path
  resolves.
- [`docs/security.md`](security.md) — RBAC/audit model `/admin/backup`
  and `-restore-from` build on.
- [`docs/invariants.md`](invariants.md) — `BACKUP INTEGRITY`, `BACKUP
  CONSISTENCY`, `DESTRUCTIVE RESTORE ISOLATION`.
