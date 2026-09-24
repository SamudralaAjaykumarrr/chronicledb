# Invariant Catalog

Status: Architecture Foundation. These invariants are design
requirements to be proven by future implementation and tests, not
claims about existing code (none exists yet).

Each invariant lists: identifier, statement, scope, why it matters,
enforcing mechanism, threatening failures, and proof/test obligations.
Scenario references point into
[`docs/scenario-corpus.md`](scenario-corpus.md).

---

## DURABILITY

**Statement**: An acknowledged committed transaction survives every
crash/restart scenario permitted under the documented durability model
([`docs/replication.md`](replication.md) §1, §2).

**Scope**: Standalone and replicated modes.

**Why it matters**: This is the load-bearing promise of "database" —
without it, ChronicleDB is a cache.

**Mechanism**: Explicit `Sync()` boundary before acknowledgment
([`docs/wal.md`](wal.md) §4); in replicated mode, additionally quorum
persistence before acknowledgment ([`docs/replication.md`](replication.md) §1.2).

**Threatened by**: Acknowledging before `Sync()`/quorum completes;
crash timing bugs (§1.3-1.8 of [`docs/failure-model.md`](failure-model.md)).

**Proof/test obligations**: Crash-injection tests at every pipeline
stage (`docs/scenario-corpus.md` §Local Durability, §Raft/Replication);
verify no acknowledged write is ever lost across the documented
failure classes.

---

## ATOMICITY

**Statement**: A successful multi-key transaction becomes visible
atomically — no reader ever observes a partial application of its
mutation set.

**Scope**: All committed transactions.

**Why it matters**: Multi-key correctness (e.g. transferring a value
between two keys) depends on this.

**Mechanism**: Single deterministic `Apply` step per `CommitTxn`
command ([`docs/transactions.md`](transactions.md) §5).

**Threatened by**: Crash mid-apply without idempotent re-apply from a
clean starting state; incorrect partial-write code paths.

**Proof/test obligations**: Concurrent-reader test observing a
committing transaction's keys never sees a mix of old/new values
(`docs/scenario-corpus.md` §Transactions); crash-mid-apply replay test.

---

## ABORT SAFETY

**Statement**: Aborted or never-committed writes never become
committed through recovery.

**Scope**: Standalone and replicated recovery.

**Why it matters**: Otherwise a client-visible abort could silently
turn into a committed write after a crash — a direct correctness
violation.

**Mechanism**: Uncommitted writes live only in ephemeral session state
([`docs/transactions.md`](transactions.md) §2); recovery only replays
durably committed commands ([`docs/recovery.md`](recovery.md) §2).

**Threatened by**: Recovery misclassifying an uncommitted durable
suffix as committed (§2 of `docs/recovery.md`).

**Proof/test obligations**: Restart-after-abort test confirms aborted
transaction's writes are absent; restart with an uncommitted durable
suffix confirms it is not applied until legitimately committed
(`docs/scenario-corpus.md` §Raft/Replication).

---

## RECOVERY NON-INVENTION

**Statement**: Recovery never manufactures committed state unsupported
by legitimate committed history.

**Scope**: All recovery paths (local durable log, Raft log, snapshots).

**Why it matters**: This is the master rule preventing silent data
corruption from masquerading as successful recovery.

**Mechanism**: Mandatory checksum verification with fail-closed
behavior on mid-log corruption ([`docs/wal.md`](wal.md) §6); committed
boundary determined via legitimate leader/commit-rule information, not
log presence alone ([`docs/recovery.md`](recovery.md) §2).

**Threatened by**: Any "best-effort" repair of corrupted or ambiguous
durable state.

**Proof/test obligations**: Corruption-injection tests confirm startup
refusal rather than guessed recovery (`docs/scenario-corpus.md` §Local
Durability).

---

## MVCC VISIBILITY

**Statement**: A transaction reads exactly the committed versions
allowed by its `StartSeq`, plus its own write set.

**Scope**: All reads.

**Why it matters**: This is the precise, checkable definition of
Snapshot Isolation's read guarantee ([`docs/mvcc.md`](mvcc.md) §3).

**Mechanism**: The visibility rule in [`docs/mvcc.md`](mvcc.md) §3,
applied uniformly by `internal/mvcc`.

**Threatened by**: Off-by-one errors in the `CommitSeq <= StartSeq`
comparison; own-write shadowing bugs; tombstone mishandling.

*(`v0.6.0`)* Narrowed in domain, not weakened, by MVCC GC: this rule now
applies to snapshots at or above the applied GC watermark, and a read
below the horizon returns an explicit `ErrSnapshotTooOld` rather than a
value — a refused read is not a wrong read (see the new "Admission
Control / Storage Lifecycle invariants" section below).

**Proof/test obligations**: Property-based tests generating random
interleavings of writers and readers, checked against a reference
model of the visibility rule (`docs/scenario-corpus.md` §Transactions).

---

## CONFLICT CORRECTNESS

**Statement**: Write-write conflict behavior follows the documented
first-committer-wins rule exactly.

**Scope**: All committing transactions with overlapping write sets.

**Why it matters**: Without this, Snapshot Isolation's "no lost
updates" guarantee does not hold.

**Mechanism**: Deterministic conflict check at apply time
([`docs/mvcc.md`](mvcc.md) §4).

**Threatened by**: Leader-side-only conflict checks that are not
re-verified deterministically at apply time on every replica.

**Proof/test obligations**: Concurrent conflicting-transaction
scenario tests, replicated-mode tests confirming all replicas reach
the identical `COMMITTED`/`ABORTED` decision (`docs/scenario-corpus.md`
§Transactions, §Raft/Replication).

---

## IDEMPOTENCY

**Statement**: A completed `RequestID` is never applied twice.

**Scope**: All mutating requests carrying a `RequestID`.

**Why it matters**: Without this, network retries could double-charge,
double-write, or otherwise duplicate effects.

**Mechanism**: `RequestID` outcome table checked before re-evaluating
a command ([`docs/transactions.md`](transactions.md) §6).

**Threatened by**: Recording the outcome outside the atomic apply step
(a crash between apply and outcome recording could allow a re-apply).

**Proof/test obligations**: Duplicate-request-before-response and
duplicate-request-after-restart tests
(`docs/scenario-corpus.md` §Idempotency).

---

## REQUEST OUTCOME STABILITY

**Statement**: A completed `RequestID` resolves to the same logical
terminal outcome after retry, restart, or replay.

**Scope**: All completed requests, indefinitely (V1 retains outcomes
indefinitely — [`docs/transactions.md`](transactions.md) §6).

**Why it matters**: This is what makes "uncertain outcome" resolvable
by the client at all ([`docs/transactions.md`](transactions.md) §7).

**Mechanism**: Outcome table is part of durable, snapshotted state
machine state ([`docs/snapshots.md`](snapshots.md) §2).

**Threatened by**: Outcome table not included in snapshots (would lose
outcomes on log compaction); non-deterministic `Apply` producing a
different outcome on replay.

**Proof/test obligations**: Retry-after-snapshot-and-compaction test;
retry-after-full-node-replacement-via-snapshot test.

---

## RAFT ELECTION SAFETY

**Statement**: At most one legitimate leader exists per term.

**Scope**: Replicated mode.

**Why it matters**: Two leaders in the same term could each believe
they can commit, directly threatening every other Raft invariant.

**Mechanism**: Vote-once-per-term rule, persisted before voting
([`docs/raft.md`](raft.md) §2, §5).

**Threatened by**: Granting a vote without first persisting
`votedFor`; a node voting twice in the same term after a crash that
lost an unpersisted vote record.

**Proof/test obligations**: Simulator tests asserting at most one
leader per observed term across all nodes at all times
(`docs/scenario-corpus.md` §Raft/Replication).

---

## RAFT LOG MATCHING

**Statement**: Matching `(term, index)` positions across two logs
imply identical preceding history in both logs.

**Scope**: Replicated mode.

**Why it matters**: This is what allows a leader to reason about a
follower's log using only a single `(prevLogIndex, prevLogTerm)` check
instead of comparing entire logs.

**Mechanism**: `AppendEntriesRPC` consistency check and divergent
suffix repair ([`docs/raft.md`](raft.md) §3).

**Threatened by**: Incorrectly accepting an `AppendEntriesRPC` whose
prefix doesn't actually match; incorrect truncation logic.

**Proof/test obligations**: Simulator tests with induced leader
changes and divergent logs, asserting eventual convergence to a single
consistent log across all nodes.

---

## LEADER COMPLETENESS

**Statement**: Legitimately committed entries survive leadership
changes — every future leader's log contains every previously
committed entry.

**Scope**: Replicated mode.

**Why it matters**: Without this, a leadership change could silently
lose committed data.

**Mechanism**: Vote-granting log-comparison rule
([`docs/raft.md`](raft.md) §2) ensures only a node with an
up-to-date-or-better log can become leader.

**Threatened by**: A relaxed or incorrect vote-granting comparison.

**Proof/test obligations**: Simulator tests that commit entries, force
a leadership change, and assert the new leader's log/state contains
every previously committed entry (`docs/scenario-corpus.md`
§Raft/Replication).

---

## STATE MACHINE SAFETY

**Statement**: Replicas applying the same committed history reach
equivalent logical state.

**Scope**: All replicas, all time.

**Why it matters**: This is the core promise of a replicated state
machine — without it, "replication" doesn't mean anything.

**Mechanism**: `Apply` determinism constraints
([`docs/architecture.md`](architecture.md) §5 dependency rules;
[`docs/raft.md`](raft.md) §1 — no wall clock, randomness, environment
queries, network calls, unordered map iteration, or uncontrolled
global mutable state inside `Apply`).

**Threatened by**: Any nondeterminism leaking into `internal/fsm.Apply`
or `internal/mvcc`.

**Proof/test obligations**: Deterministic simulator replay tests: same
input log applied on independently constructed state machines must
produce byte-identical (or defined-equivalent) resulting state
(`docs/testing-strategy.md` §Deterministic Simulation).

---

## QUORUM SAFETY

**Statement**: A minority partition cannot acknowledge
majority-required commits.

**Scope**: Replicated mode under partition.

**Why it matters**: This is the consistency side of Raft's
consistency-over-availability trade-off.

**Mechanism**: Current-term commit rule requires majority
`matchIndex` ([`docs/raft.md`](raft.md) §4).

**Threatened by**: A leader miscounting its reachable peers, or
acknowledging before quorum confirmation.

**Proof/test obligations**: Partition scenario test
(`docs/scenario-corpus.md` §Raft/Replication, "minority partition")
verifying the isolated side never acknowledges a new commit.

---

## APPLIED-PREFIX SAFETY

**Statement**: A durable entry is not applied merely because it
exists on disk; only legitimately committed entries may become applied
state.

**Scope**: Restart/recovery, standalone and replicated.

**Why it matters**: Prevents an uncommitted durable suffix from
becoming visible, committed-looking state.

**Mechanism**: Recovery's committed-boundary determination
([`docs/recovery.md`](recovery.md) §2) — never inferred purely from
log presence.

**Threatened by**: Recovery code that "replays everything in the log"
without regard to commitment.

**Proof/test obligations**: Restart-with-uncommitted-suffix scenario
test (`docs/scenario-corpus.md` §Raft/Replication).

---

## SNAPSHOT SAFETY

**Statement**: A valid, installed snapshot preserves every committed
state transition represented through its included index.

**Scope**: Snapshot creation and installation.

**Why it matters**: A lossy or inconsistent snapshot would silently
corrupt recovered/caught-up state.

**Mechanism**: Atomic creation (temp file + fsync + atomic rename,
[`docs/snapshots.md`](snapshots.md) §3), mandatory validation before
use (§5), atomic all-or-nothing installation on followers (§7).

**Threatened by**: Partial writes being trusted; installation being
observed as "in progress" state by concurrent reads.

**Proof/test obligations**: Crash-during-creation and
crash-during-installation scenario tests confirming no partial
snapshot is ever trusted (`docs/scenario-corpus.md` §Snapshots).

---

## LOG COMPACTION SAFETY

**Statement**: Compaction never removes history still required to
reconstruct legitimate state.

**Scope**: WAL segment deletion after snapshot.

**Why it matters**: Premature deletion would make recovery or
follower catch-up impossible for the discarded range.

**Mechanism**: Deletion strictly ordered after confirmed-durable,
validated snapshot ([`docs/snapshots.md`](snapshots.md) §8).

**Threatened by**: Deleting segments before the corresponding snapshot
is confirmed durable.

**Proof/test obligations**: Crash-immediately-after-snapshot-before-
truncation scenario test; crash-during-truncation test
(`docs/scenario-corpus.md` §Snapshots).

---

## ISOLATION TRUTHFULNESS

**Statement**: ChronicleDB does not claim SERIALIZABLE isolation while
only Snapshot Isolation has been implemented and proven.

**Scope**: All documentation, client-facing messaging, and code
comments.

**Why it matters**: A false isolation-level claim is a correctness bug
in the documentation itself, and could cause application-level data
corruption for any user who relies on a guarantee ChronicleDB does not
actually provide (e.g. assuming write skew cannot happen — see
[`docs/mvcc.md`](mvcc.md) §1.1).

**Mechanism**: Documentation review discipline (this repository); a
future explicit SERIALIZABLE mode, if built, must ship with its own
proof obligations and its own ADR before any such claim changes.

**Threatened by**: Marketing-style language creeping into README or
docs ahead of implementation evidence — see
[`docs/vision.md`](vision.md) §Guiding principle.

*(`v0.6.0`)* Strengthened, not threatened, by MVCC GC: a naive GC
implementation reclaiming a version a live snapshot still needed would
have made the system quietly less isolated than it claims, and the
horizon guard (`internal/mvcc`'s `Visible`/`ScanVisible`) forbids that
structurally (see the new "Admission Control / Storage Lifecycle
invariants" section below).

**Proof/test obligations**: Documentation review checklist item in
every future architecture change; write-skew example test
(`docs/scenario-corpus.md` §Transactions) kept passing/demonstrated
indefinitely as a living counterexample to any accidental
SERIALIZABLE claim.

---

## Additional invariants required by this architecture

### CONSISTENT LOG RESPONSIBILITY

**Statement**: At any point in time, there is exactly one logical
ordered history (the Raft/local-durable-log history) and exactly one
physical persistence mechanism for it; no independently-authoritative
second history (e.g. a separate transaction log) exists.

**Scope**: Whole-system architecture.

**Why it matters**: This is the specific failure mode called out in
[`docs/architecture.md`](architecture.md) §2 — three competing sources
of truth is a classic distributed-database design bug.

**Mechanism**: `CommitTxn` is encoded as a single command in the one
logical history ([`docs/transactions.md`](transactions.md) §3); no
package other than `internal/wal` performs durable persistence of
ordered history.

**Threatened by**: A future feature adding its own ad hoc durable log
"for convenience" instead of encoding through the existing command
history.

**Proof/test obligations**: Architecture review obligation — any new
durable-history mechanism proposed in a future ADR must justify why it
is not better expressed as a command in the existing log.

### DETERMINISM BOUNDARY

**Statement**: `internal/raft` and `internal/fsm` never depend on wall
clock, randomness, environment variables, filesystem queries, network
calls, external services, process-local timing, or unordered map
iteration for any decision that affects committed or applied state.

**Scope**: `internal/raft` core logic, `internal/fsm.Apply`,
`internal/mvcc`.

**Why it matters**: This is the enabling precondition for
`STATE MACHINE SAFETY` and for deterministic simulation testing.

**Mechanism**: Dependency-inversion package boundaries
([`docs/architecture.md`](architecture.md) §5); nondeterministic
values (randomized election timeouts, wall-clock-derived data if ever
needed) are generated outside the deterministic core and passed in as
explicit inputs when required.

**Threatened by**: A convenience import of `time.Now()`,
`math/rand`'s global source, or map iteration order inside `Apply` or
the Raft core.

**Proof/test obligations**: Deterministic replay test comparing two
independently constructed state machines fed the identical input
sequence (`docs/testing-strategy.md` §Deterministic Simulation);
static-analysis/lint rule forbidding forbidden imports inside those
packages, once code exists.

---

## Security Foundation invariants (`v0.2.0`, `docs/enterprise-v1-plan.md` §5)

See [`docs/security.md`](security.md) for the full operational guide.
These invariants apply whenever TLS/authentication is configured — see
each invariant's Scope for how it applies when it is not (`v0.2.0` is
secure-by-configuration, not yet secure-by-default).

### NO UNAUTHENTICATED ADMIN ACTION

**Statement**: Every state-changing administrative HTTP endpoint
requires a successful authentication check before any side effect, with
no code path that performs the side effect first and authenticates
after.

**Scope**: `cmd/chronicledb-node`'s control-plane HTTP endpoints,
whenever `-auth-mode` is not `none`. (When `-auth-mode` is `none`, no
endpoint is authenticated at all — the pre-`v0.2.0` trusted-network
posture, unchanged and loudly warned about; see
[`docs/security.md`](security.md) §1.)

**Why it matters**: Every later Enterprise V1 phase adds a new
administrative surface (backup/restore, membership changes, upgrade
orchestration); this is the foundation those phases depend on rather
than each separately bolting on authentication.

**Mechanism**: The middleware chain `TLS termination -> authn -> authz
-> audit -> handler` (`cmd/chronicledb-node/auth.go`'s `security.wrap`)
wraps every route; the underlying handler is invoked only after authn
and authz both succeed.

**Threatened by**: A new endpoint registered directly on the mux
without going through `security.wrap`.

**Proof/test obligations**: RBAC decision-table test covering every
role x every endpoint through the actual HTTP middleware chain, and an
unauthenticated-request-denied test for every endpoint
(`cmd/chronicledb-node/auth_test.go`).

### NO PLAINTEXT PEER REPLICATION

**Statement**: Once peer TLS is configured for a node, `internal/transport`
never sends or accepts an unencrypted Raft message on that node; there
is no runtime toggle to disable it without a restart with different
flags.

**Scope**: `internal/transport`'s peer connections, whenever
`-peer-tls-cert`/`-peer-tls-key`/`-peer-tls-ca` are configured. A
Transport is either constructed via `New` (plaintext, matching
pre-`v0.2.0` behavior) or `NewTLS` (mTLS) — never both, and never
partially (`internal/node.Config.validate` refuses a partial peer-TLS
configuration at startup).

**Why it matters**: A silent plaintext fallback would defeat the entire
purpose of configuring peer mTLS, exactly at the moment a node believes
itself protected.

**Mechanism**: `NewTLS` wraps the listener in `tls.NewListener` with
`RequireAndVerifyClientCert`; outbound dials use `tls.DialWithDialer`
with an explicit chain-and-identity `VerifyPeerCertificate` callback.
Every message on an inbound connection is additionally checked against
that connection's own TLS-verified identity.

**Threatened by**: A future change that reads `Message.From` before
identity verification completes, or that falls back to a plaintext
`net.Dial` on a TLS handshake failure.

**Proof/test obligations**: `internal/transport/tls_test.go` — valid
mTLS round-trip; expired/wrong-CA/self-signed/no-certificate/plaintext-
to-TLS-listener rejection; wrong-identity message spoofing rejection;
certificate rotation with zero dropped connections. A real three-node
cluster with peer mTLS enabled (`internal/node/tls_test.go`) proving
replication and failover are unaffected.

### FAULT SURFACE OFF BY DEFAULT

**Statement**: `/fault` is unreachable unless both the
`-enable-fault-endpoint` build/run flag and `admin` authentication (when
auth is configured) are satisfied; proven by a test asserting the route
is unregistered, not merely that calling it returns an error status.

**Scope**: `cmd/chronicledb-node`'s HTTP mux.

**Why it matters**: `/fault` injects real network faults against a live
process — reachable-by-default in `v0.1.0`, this is the exact kind of
surface that must not ship open in a security-conscious default.

**Mechanism**: `newControlServer` registers `/fault`'s route
conditionally on `enableFault`, never checked-and-rejected after
unconditional registration.

**Threatened by**: Registering `/fault` unconditionally and relying on
RBAC alone to gate it.

**Proof/test obligations**: `TestFaultEndpoint_UnregisteredByDefault`
and `TestFaultEndpoint_RegisteredFlagCombinations`
(`cmd/chronicledb-node/auth_test.go`) assert `http.ServeMux.Handler`
returns an empty pattern when the flag is unset, across every
flag/auth-mode combination; a real-process integration test
(`cmd/chronicledb-node/security_integration_test.go`) confirms `404`,
not `403`, against a real binary invocation with an admin credential
but no `-enable-fault-endpoint` flag.

### AUDIT COMPLETENESS

**Statement**: Every action gated by RBAC produces exactly one audit
record (never zero, never more than one for a single logical action),
and audit-write failure blocks the action rather than silently
succeeding without a record.

**Scope**: `cmd/chronicledb-node`'s control-plane HTTP endpoints,
whenever `-auth-mode` is not `none`.

**Why it matters**: An audit trail with silent gaps, or one that can be
bypassed by a slow/failing disk, is not a tamper-evident record of
administrative activity — it is decoration.

**Mechanism**: `security.wrap` writes exactly one `audit.Entry` per
request, synchronously, before ever invoking the handler; a non-nil
`Append` error short-circuits with `500` and the handler never runs.
`internal/audit.Log`'s on-disk format is hash-chained (each record's
`recordHash` covers its header/payload/checksum; the next record embeds
it as `prevHash`) so tampering after the fact is independently
detectable even without trusting the writer.

**Threatened by**: Moving the audit write after the handler call (an
audit-log outage would then silently permit unrecorded actions);
logging only failures, not successes.

**Proof/test obligations**: `internal/audit`'s exhaustive single-byte-
tamper-detection test, forged-but-internally-consistent-record
detection, and `FuzzDecodeFrame`; `cmd/chronicledb-node/auth_test.go`'s
`TestAudit_ExactlyOneRecordPerDecision` and
`TestAudit_WriteFailureBlocksAction`.

## Backup / Disaster Recovery invariants (`v0.3.0`, `docs/enterprise-v1-plan.md` §6)

See [`docs/backup.md`](backup.md) and
[`ADR-0016`](adr/0016-backup-disaster-recovery-and-pitr.md) for the full
architecture. These invariants govern `internal/backup`,
`internal/node.Node.Backup`, and `cmd/chronicledb-node`'s
`/admin/backup`/`-restore-from` surfaces.

### BACKUP INTEGRITY

**Statement**: A restore never trusts a backup whose manifest checksum,
per-component (snapshot/WAL-segment) checksum, or internal
snapshot-consistency check fails validation; corruption is rejected
outright, before any byte reaches the target data directory.

**Scope**: `internal/backup.Restore`, and every caller of it
(`cmd/chronicledb-node`'s `-restore-from` preflight).

**Why it matters**: A backup is, by construction, an external input —
mirrors `RECOVERY NON-INVENTION`'s "never trust local disk beyond what
validates" discipline, applied to a portable artifact that may have
been copied, transferred, or stored by means entirely outside
ChronicleDB's own control.

**Mechanism**: `readManifest` validates the manifest's own trailing
CRC32 and format version before parsing its JSON body;
`verifySnapshotComponent`/`verifyWALSegmentComponents` check every
referenced file's size and CRC32 against the manifest's own recorded
values, then `internal/snapshot.Decode`'s own internal-consistency
check, all before `Restore` ever creates or writes into a staging
directory.

**Threatened by**: Trusting a component's bytes because the manifest
merely *names* it, without also checking its recorded checksum; writing
to the target directory before every component has passed validation.

**Proof/test obligations**: `internal/backup`'s corrupted/truncated/
missing-manifest, corrupted/missing-snapshot, and corrupted/missing-
WAL-segment tests, each asserting the target directory is left absent
or empty; `FuzzRestoreManifest`.

### BACKUP CONSISTENCY

**Statement**: A restored data directory's state is exactly the state a
legitimate node would reach by replaying the identical committed
history through the same boundary — no partial-transaction, no
reordered-command, and no post-boundary-transaction restore is ever
possible.

**Scope**: `internal/backup.Export`/`Restore`'s WAL-suffix copy/replay
loop.

**Why it matters**: Mirrors `ATOMICITY`/`RECOVERY NON-INVENTION` for the
backup/restore path specifically — a backup that silently reordered,
dropped, or fabricated a committed entry would be worse than no backup
at all, since it would appear to succeed.

**Mechanism**: Every WAL entry is copied through
`internal/wal.WAL.AppendLogEntry`'s own strictly-sequential index
assignment, with an explicit assigned-index-equals-source-index check
on every entry (`copyWALSuffix`); a PITR boundary stops the copy/replay
loop strictly after the requested index, so the resulting WAL's own
`NextIndex()` is provably `boundary+1`.

**Threatened by**: Copying WAL segment files as raw bytes instead of
through `AppendLogEntry`'s own index assignment (a source segment's
physical layout has no necessary relationship to a backup's chosen
range); an off-by-one in the PITR boundary check letting one
post-boundary entry through.

**Proof/test obligations**: `TestExportRestore_ContinuousPITRRoundTrip`
(restored to every boundary in a 10-entry history, each asserting
`NextIndex() == boundary+1`); `internal/node`'s
`TestBackup_DestructiveDisasterRecoveryDrill` (a real, live, three-node
restore proving every pre-loss commit's outcome survives and a
post-recovery write commits normally).

### DESTRUCTIVE RESTORE ISOLATION

**Statement**: Restoring into a data directory never silently overwrites
an existing, live cluster's data; restore targets an explicitly clean
(empty or absent) data directory only, refusing to run against one
containing existing WAL/snapshot state without an explicit force flag
that itself produces an audit record.

**Scope**: `internal/backup.Restore` (`RestoreOptions.Force`) and
`cmd/chronicledb-node`'s `-restore-from`/`-force-overwrite` startup
preflight.

**Why it matters**: A restore that could accidentally target a live
node's own in-use data directory would be one of the most destructive
possible operator mistakes this feature could introduce.

**Mechanism**: `dataDirIsClean` treats any existing directory entry at
all as "not clean"; `Restore` returns `ErrTargetNotClean` unless
`Force` is set. `cmd/chronicledb-node` additionally requires
`-force-overwrite` to be passed explicitly for that case, and treats a
failure to write the resulting audit record as fatal to startup
(mirroring `AUDIT COMPLETENESS`'s fail-closed discipline) — a
non-destructive restore into an already-clean directory logs a warning,
rather than failing startup, on an audit-write failure, since blocking
a legitimate disaster-recovery restore over a diagnostic-log issue would
itself be a worse availability tradeoff.

**Threatened by**: Checking "is the directory clean" only once, long
before the actual destructive `os.RemoveAll`, with other state changes
possible in between (`internal/backup.Restore` re-derives nothing else
in between: the check and the destructive removal are both inside one
synchronous call with no other actor able to interleave).

**Proof/test obligations**: `TestRestore_TargetNotCleanRequiresForce`;
`cmd/chronicledb-node`'s `TestRunRestore_NonCleanTargetRequiresForce`
and `TestRecordRestoreAudit_WritesVerifiableEntry`.

## Compatibility / Rolling Upgrades invariants (`v0.4.0`, `docs/enterprise-v1-plan.md` §7)

See [`docs/upgrades.md`](upgrades.md) and
[`ADR-0017`](adr/0017-compatibility-and-rolling-upgrades.md) for the
full architecture. These invariants govern `internal/version`'s
`MaxSupportedGeneration`, `internal/wal`/`internal/fsm`'s generation-
aware encode/decode, `internal/fsm`'s `ControlCommandMarker`/
`SetClusterVersionCommand`, `internal/raft.Message.SenderGeneration`,
and `internal/node`'s `UpgradePrecheck`/`FinalizeUpgrade`.

### NO SILENT FORMAT MISINTERPRETATION

**Statement**: A node never applies, replays, or installs a record/
command/snapshot-state whose version or generation it does not
recognize; it fails closed with a diagnostic rather than guessing at an
unrecognized layout.

**Scope**: `internal/wal.Open` (record/metadata format version, and
`Metadata.ClusterGeneration` against `version.MaxSupportedGeneration`),
`internal/fsm.DecodeCommitTxn`/`DecodeSetClusterVersion`/`DecodeState`,
`internal/node.applyCommitted`/`applyControlEntry` (a committed control
command whose `TargetGeneration` exceeds this binary's own
`MaxSupportedGeneration` is refused even though it is already
committed and durable — a determinism-preserving *local* refusal, never
a divergent *replicated* decision).

**Why it matters**: This is the same posture `RECOVERY NON-INVENTION`
already takes for corrupted data, extended to "correctly framed but
newer than this binary understands" data — the specific hazard a
rolling upgrade introduces that a single-version deployment never
faced.

**Mechanism**: Every one of the five versioned surfaces
(`docs/enterprise-v1-plan.md` §7) rejects an unrecognized version/
generation with a distinct sentinel error
(`wal.ErrUnsupportedVersion`/`ErrUnsupportedGeneration`,
`fsm.ErrUnsupportedCommandVersion`/`ErrUnknownControlCommand`) rather
than a generic decode failure, and — critically — this rejection is
free on a pre-`v0.4.0` binary for the one new wire format this phase
introduces: `fsm.ControlCommandMarker` (`0xF0`) is chosen to never
collide with `commitTxnCommandVersion`'s own small, sequential range, so
an unmodified pre-`v0.4.0` `DecodeCommitTxn` already fails closed on any
control-command payload with no code change at all (see
`ControlCommandMarker`'s doc comment).

**Threatened by**: A future format change that reuses an existing
version/generation value for genuinely different content, or that
widens a decoder's tolerance (e.g. "any trailing byte count") instead of
enumerating exactly which generations are understood.

**Proof/test obligations**: `internal/fsm/clusterversion_test.go`
(`TestControlCommandMarker_NeverCollidesWithCommitTxnVersion` — proves
the free backward-compatibility property directly by feeding a real
encoded control command into the *old* `DecodeCommitTxn` decoder);
`internal/wal/generation_test.go`'s
`TestOpenRefusesDataDirectoryFinalizedBeyondThisBinary`; the real
mixed-binary `TestMixedVersion_OldBinaryRejectedAfterFinalize`
(`cmd/chronicledb-node`, `-tags=integration`) — an actual pre-`v0.4.0`
binary, rejoining a live, already-finalized cluster, fails closed with
exactly this mechanism and its own process exits.

### ROLLBACK BOUNDARY HONESTY

**Statement**: The system never claims rollback safety past the last
finalize boundary; a redeploy of an older binary is safe strictly before
finalize and is deterministically, not merely operationally, refused
once the cluster's agreed generation has moved forward.

**Scope**: `internal/fsm.ApplySetClusterVersion` (the single place the
agreed generation ever changes), `internal/wal.SetClusterGeneration`/
`Open`, `internal/fsm.EncodeState`/`DecodeState`'s and
`internal/wal.encodeMetadata`/`decodeMetadata`'s generation-0-is-
byte-identical encoding discipline (the actual mechanism that makes
pre-finalize rollback safe — not merely undocumented-but-working).

**Why it matters**: An upgrade mechanism that quietly stops being
reversible, without the system itself being able to say exactly when
that happened, turns "rolling upgrade" into an unbounded risk instead of
a bounded one.

**Mechanism**: `ApplySetClusterVersion` accepts only
`TargetGeneration == currentGeneration + 1` (single-step) and
`TargetGeneration > currentGeneration` (strictly forward) — both a
skip-ahead and a downgrade/sideways attempt are deterministic
`StatusAborted` outcomes, evaluated identically by every replica from
the same replicated command and prior state, never a Go error and never
a per-node judgment call. Every generation-0 encoding
(`fsm.encodeState`, `wal.encodeMetadata`) is required to be
byte-identical to the pre-`v0.4.0` format — proven, not assumed — so
"rollback is safe before finalize" is a property of the bytes on disk,
not an operational promise layered on top of them.

**Threatened by**: Allowing `ApplySetClusterVersion` to accept an
arbitrary target (opening the door to skip-version upgrades this phase
explicitly does not support), or unconditionally appending the
generation field regardless of its value (which would silently break
pre-finalize rollback compatibility — see `encodeState`'s and
`encodeMetadata`'s own doc comments).

**Proof/test obligations**:
`TestApplySetClusterVersion_RollbackBoundaryHonesty`,
`TestApplySetClusterVersion_MonotonicSingleStep`
(`internal/fsm/clusterversion_test.go`);
`TestEncodeStateDecodeStateRoundTrip_ClusterGeneration` and
`TestEncodeDecodeMetadata_ClusterGeneration` (byte-identical-at-
generation-0 assertions); the real mixed-binary
`TestMixedVersion_RollbackBeforeFinalizeIsSafe` (safe side) and
`TestMixedVersion_OldBinaryRejectedAfterFinalize` (refused side),
`cmd/chronicledb-node`, `-tags=integration`.

### MIXED-VERSION QUORUM SAFETY

**Statement**: During an N/N+1 rolling-upgrade window, Raft quorum
safety (`RAFT ELECTION SAFETY`, `QUORUM SAFETY`) continues to hold
regardless of which subset of the cluster is on which binary version —
a new-binary leader with an old-binary follower (or vice versa) never
produces a state divergence.

**Scope**: `internal/raft.Message.SenderGeneration`,
`internal/node`'s wiring of it, and — the property that actually makes
this safe rather than merely observed — every command a leader proposes
before finalize is written in exactly generation-0's existing wire/
command format, which every peer, old or new binary alike, already
understands unchanged.

**Why it matters**: This is the specific safety property that lets
every later phase's own format changes (docs/enterprise-v1-plan.md §7's
listed dependents) be rolled out one node at a time instead of requiring
full-cluster downtime.

**Mechanism**: `raft.Message.SenderGeneration` rides on every ordinary
Raft message (not a separate preamble) via `encoding/gob`'s
self-describing, field-tolerant wire encoding — a pre-`v0.4.0` peer
simply never sets it (decoded as generation 0) and silently ignores it
when present on a message a new binary sends (see that field's doc
comment for why this was chosen over a dedicated handshake message,
which an unmodified old binary could not have parsed at all). Separately
and independently, `internal/node.FinalizeUpgrade`'s precheck refuses to
propose a `SetClusterVersionCommand` at all unless every configured peer
already reports the target generation — so no new-format command is
ever proposed while a peer that could not understand it might still be
part of the cluster.

**Threatened by**: Proposing any command whose encoding depends on
generation *before* finalize (this phase deliberately introduces none —
every CommitTxn command a leader proposes pre-finalize is
byte-identical to generation 0, regardless of which binary the leader
is running).

**Proof/test obligations**: The real mixed-binary
`TestMixedVersion_CriticalUpgradeProofScenario`
(`cmd/chronicledb-node`, `-tags=integration`) — the full numbered
scenario: old-binary cluster, one-node-at-a-time upgrade, a forced real
leader failover while versions are mixed, continued traffic and a
RequestID retry across that failover, final-node upgrade, finalize, a
full-cluster restart, and every acknowledged RequestID's outcome
verified on every node throughout.

## Dynamic Membership invariants (`v0.5.0`, `docs/dynamic-membership-plan.md`)

See [`docs/dynamic-membership-plan.md`](dynamic-membership-plan.md) for
the full design and proofs, and
[`ADR-0018`](adr/0018-dynamic-membership-architecture.md) for the
architecture decision. These invariants govern
`internal/raft.Configuration`/`Core.ConfigAt`/`ProposeConfigChange`,
`internal/fsm`'s membership outcome table, `internal/snapshot`'s
`FormatVersion 2`, `internal/backup`'s restore membership-isolation
transform, and `internal/node`'s membership admin surface. Twelve are
new entries in this catalog; `NO SILENT FORMAT MISINTERPRETATION`
(above) is amended in place, not re-added.

### LEADER-TERM CONFIGURATION GATE

**Statement**: A leader never appends an `EntryConfig` entry unless it
has already committed an entry of its own current term
(`termAt(commitIndex) == currentTerm`, P1) and its inherited log tail as
of its own election has itself already committed
(`commitIndex >= pendingConfIndex`, P2).

**Scope**: `raft.Core.ProposeConfigChange` (checks 2-3),
`internal/node.proposeElectionNoOp` (the existing, `v0.1.0`+ mechanism
that makes P1 converge after every election).

**Why it matters**: This is the premise that keeps every committed
configuration on a single chain (`CONFIGURATION BRANCH CONFINEMENT`'s
W1) rather than letting two leaders each append a sibling child of one
common ancestor with disjoint majorities — the single most serious
defect an early revision of the design plan contained (traced as DM-12
in the plan's own review history).

**Mechanism**: `Core.ProposeConfigChange`'s ordered precondition list;
`proposeElectionNoOp`'s synthetic current-term entry, proposed on every
`BecameLeader` output, which this phase promotes from "a ReadIndex
liveness fix" to "a documented safety dependency that must never be
removed."

**Threatened by**: Removing or short-circuiting `proposeElectionNoOp`;
any path that appends an `EntryConfig` entry outside
`ProposeConfigChange`; any future change letting `commitIndex` advance
to an entry not in `currentTerm`.

**Proof/test obligations**: `TestProposeConfigChangeP1NoCurrentTermCommit`,
`TestProposeConfigChangeP2InheritedSuffix`
(`internal/raft/membership_test.go`);
`TestDM12_NewLeaderRefusesSecondTransitionOnInheritedUncommittedTail`
(`internal/fault/membership_test.go`, real multi-node election/
partition dynamics); `TestDM12Step7_P1DisabledProducesADoubleCommitTheOracleDetects`
(`internal/fault`), the P1-disabled negative control proving
`committedOracle` actually detects the resulting two-committed-siblings
divergence rather than merely passing with the gate left enabled — see
`docs/testing-strategy.md` §11.

### CONFIGURATION BRANCH CONFINEMENT

**Statement**, in two mechanically checkable parts: **(W1)** the set of
*committed* configurations is totally ordered by the index of the
`EntryConfig` entry that established each, forms exactly one chain (no
two committed configurations are siblings), and every adjacent pair
differs by at most one voter; **(W2)** every live-but-uncommitted
configuration is a single-shape child of the newest committed
configuration present in the log of the node holding it.

**Why it matters**: adjacency is exactly the hypothesis quorum-
intersection needs; two configurations more than one voter apart can
have fully disjoint majorities. This is *not* a claim that at most two
configurations are ever live at once, or that all live configurations
are pairwise adjacent — three can be simultaneously live in a safe,
reachable state (a partitioned node holding a stale branch, a healthy
majority holding two further committed transitions), made safe by the
superseded-branch exclusion lemma rather than by any window bound.

**Scope**: `raft.Core.activeConfig`/`ConfigAt`/`ProposeConfigChange`'s
four-shape check (§2.6 of the plan).

**Threatened by**: Any relaxation of the P1/P2/P3 proposal gates; any
transition shape changing more than one voter at a time; concurrent
(unserialized) changes; reintroducing a live-set-size or adjacency-of-
all-live-configurations test into any test oracle.

**Proof/test obligations**: `TestClassifyTransitionFourShapes` (all four
shapes plus deliberate violations), `TestConfigAtBoundaryAgreementAfterCompact`,
`TestConfigAtNeverSnapshottedFallsBackToBootstrap` (`internal/raft`);
`TestDM12_...` (`internal/fault`); `TestDM10_CombinedRandomizedMembershipSchedule`
(`internal/fault/membership_dm10_test.go`), whose
`configLineageOracle` (`internal/fault/lineage_oracle_test.go`)
independently reconstructs each node's configuration from raw durable
log/snapshot bytes with its own from-scratch decoder — never asking
`Core`, `ConfigAt`, or any of its unexported helpers for the answer it
is checking; `TestDM22_ThreeLiveConfigurationsIsSafeAndOracleStaysQuiet`,
`TestDM22_SyntheticW1ViolationDetectedByLineageOracle`, and
`TestDM22_CorruptedOracleExpectationDetectedAgainstCorrectSystem`
(`internal/fault/membership_dm22_test.go`) calibrate that oracle in
both directions: quiet on §2.3's genuine three-live-configuration safe
state, and firing on both a real W1 violation and a corrupted
expectation.

### SERIALIZED MEMBERSHIP CHANGE

**Statement**: At most one membership-change command may be outstanding
at a time; a second is synchronously refused — never queued, never
entering the log — until the first resolves.

**Scope**: `raft.Core.ProposeConfigChange` checks 2-4 (P1, P2, P3 —
three independent checks, not one: P3 alone covers only a second
proposal within one stable leadership term).

**Threatened by**: Any code path that appends a second `EntryConfig`
entry before the first commits, on any node, in any term.

**Proof/test obligations**: `TestProposeConfigChangeP3SerializationInProgress`
(`internal/raft`); `TestRemoveServerRequiresConfirmationBelowThreeVoters`
and the full add/promote/remove lifecycle test (`internal/node`),
which exercise the idempotency pre-check that makes a retried request
never re-enter this path at all.

### QUORUM CONTINUITY

**Statement**: Every *live* quorum decision — election, commit, and
ReadIndex leadership confirmation — uses the deciding node's current
`activeConfig`, and counts a node toward that quorum (its `matchIndex`,
or its own implicit self-ack) **iff that node is currently a Voter in
that `activeConfig`**, with no self-exemption on either path. Every
*boundary* capture (a snapshot's `Meta.Configuration`, or
`snapshotConfig` at compaction) uses `ConfigAt` evaluated at that
boundary's own index — never the live `activeConfig`, which may reflect
an uncommitted entry above the boundary that can still be truncated
away.

**Scope**: `raft.Core.advanceLeaderCommit` (majority computed over
`activeConfig.Voters` only — a self-removing leader is simply absent
from that range from the instant of append, with no special case);
`internal/node.checkPendingReads` (reads `Core.ActiveConfig()` once per
pass, and counts its own self-ack only `iff cfg.IsVoter(n.cfg.ID)`);
`internal/node.maybeSnapshot`/`raft.Core.Compact` (both use
`ConfigAt(appliedIndex)`/`ConfigAt(uptoIndex)`, never `activeConfig`).

**Why it matters**: A self-removing leader that kept counting its own
`matchIndex`/self-ack toward a quorum it is no longer a member of would
let an entry "commit," or a read resolve, against a basis that is not a
genuine majority of the configuration actually in force — the specific
defect that made an early revision of this design unsafe for 3->2
self-removal.

**Threatened by**: Any new quorum-counting call site that iterates
something other than `activeConfig.Voters`; any boundary capture that
reads `activeConfig` instead of calling `ConfigAt`; a self-ack/self-
matchIndex counted unconditionally (`acked := 1`) rather than gated on
current voter status.

**Proof/test obligations**: `TestSelfRemovingLeaderExcludedFromOwnCommitQuorum`,
`TestSelfRemovalDoesNotPanicOnLateArrivingResponse` (`internal/raft`);
`TestSelfRemovingLeaderStepsDownAndClusterElectsNewLeader`
(`internal/node`, real processes, asserting the remaining voters elect
a successor and the removed node answers `ErrNodeRemoved`). The full
DM-17 three-sub-case ReadIndex-across-a-configuration-change scenario —
promote (`TestDM17Promote_NewVoterWithZeroAckSeqDoesNotCount`), remove
(`TestDM17Remove_RemovedVoterStopsCounting`), and self-removal's two-
phase-blocked-then-clean-failure-plus-positive schedule
(`TestDM17SelfRemoval_TwoPhasesAgainstOneReadPlusPositive`) — is
implemented in `internal/node/dm17_test.go`, each repeated with a real
interleaved leader failover in `internal/node/dm17_failover_test.go`.

### MEMBERSHIP RECOVERY DETERMINISM

**Statement**: `ConfigAt` is the **sole** mechanism by which any
configuration is ever derived, at every call site, with one fixed
priority order (latest retained configuration-establishing `EntryConfig`
-> `snapshotConfig` when `snapshotHasConfig` -> `Config.Bootstrap` ->
the zero value), and it is a pure, deterministic function of durable
state. After any restart, snapshot install, or divergent-suffix repair,
two nodes with byte-identical durable state always compute identical
configurations.

**Scope**: `raft.Core.ConfigAt` and its complete call-site table
(append-time activation, accept-time activation/revert-on-truncate,
`NewCoreFromSnapshot`, `becomeLeader`, `maybeSnapshot`, `Compact`,
`refreshStatusLocked`); `internal/node`'s `encodeEntryPayload`/
`decodeEntryPayload` typed-entry framing, which `Entry.Type` must
survive intact for `ConfigAt`'s step-1 scan to ever find it.

**Why it matters**: The typed-entry header is gated on the entry's own
`Type`, never on this node's durable cluster generation — the latter is
updated at a point in the event loop's single pass that is
systematically stale relative to when a follower persists the very
entry that gate would apply to, which would otherwise silently write
the first post-finalization `EntryConfig` entry untyped and make it
vanish from the scan on the next restart.

**Threatened by**: Any code path trusting a cached `activeConfig` across
a restart; any second, independent reconstruction implementation; any
call site added without updating `ConfigAt`'s call-site table; gating
the typed-entry header on a value updated on a different schedule from
the write it gates.

**Proof/test obligations**: `TestConfigAtNeverSnapshottedFallsBackToBootstrap`,
`TestConfigAtNeverJoinedIsZeroConfiguration`,
`TestConfigAtBoundaryAgreementAfterCompact`,
`TestNewLearnerFirstCatchUpBatchIncludingItsOwnAddEntry`,
`TestConfigAtInvariantsProperty` (`internal/raft` — DM-16, 40 randomized
seeds, with/without a snapshot boundary and a bootstrap seed, covering
determinism, monotone provenance, prefix stability, and boundary
agreement); `TestFinalizeUpgrade_FollowerAdoptsGenerationViaSnapshotInstall`-
style restart/catch-up coverage, `TestDM20_EntryTypeSurvivesWALRoundTripAcrossFinalizationBoundary`
and its negative control `TestDM20_NegativeControlGenerationGatedEncodingFailsTheProof`
(`internal/node/dm20_test.go` — the byte-level, negative-controlled
`Entry.Type` WAL round trip across the finalization boundary).

### RESTORE MEMBERSHIP ISOLATION

**Statement**: A data directory produced by `-restore-from` derives its
`Configuration` **only** from the operator's `-cluster`/`-peers` flags.
No source-cluster `Member.ID` or address is ever adopted, replicated,
or reported by the restored cluster, from either durable carrier — the
staged snapshot (`Meta.HasConfiguration = false`) or the staged WAL
suffix (every `EntryConfig` payload rewritten to the `Voided` kind, at
its original index and term).

**Scope**: `internal/backup.stripSnapshotConfiguration` (part 1),
`internal/backup.voidConfigEntryPayload`/`copyWALSuffix`'s transform
parameter (part 2, applied by `buildStaging`/`Restore` only — never by
`Export`, which must retain a backup's own recoverable membership
history unchanged).

**Why it matters**: `docs/backup.md` already documents restoring onto a
different peer set as supported, and durable configuration overrides
the bootstrap flags everywhere else in this system; without this
invariant those two rules compose into a silent, total failure — every
restored node finds its own ID absent from a non-empty configuration,
becomes permanently self-removed, and the restored cluster never elects
a leader, in precisely the disaster-recovery scenario the feature
exists for.

**Threatened by**: A restore path that copies WAL payloads through
without inspecting their type; clearing the snapshot's configuration
without also voiding the WAL suffix's `EntryConfig` entries (a defect
an early revision of this design actually had); voiding unconditionally
inside the `Export`-shared `copyWALSuffix` helper; failing closed on a
`Voided` entry received on the ordinary replication path (which halts
every node that joins a restored cluster before its own first
snapshot).

**Proof/test obligations**: `TestVoidConfigEntryPayloadMatchesRaftFraming`,
`TestStripSnapshotConfigurationClearsV2Configuration`,
`TestRestoreVoidsConfigEntryInStagedWAL`,
`TestRestoreNegativeControlWithoutVoiding` (`internal/backup`) — the
last of these is DM-21's negative control at the package level. The
full DM-21 real-cluster scenario — a restored cluster with different
NodeIDs actually electing a leader and serving traffic, plus a learner
added before the restored cluster's own first snapshot receiving voided
entries from a live leader
(`TestDM21_RestoreCarriesNoSourceMembershipFromEitherCarrier`), and its
real-cluster negative control asserting the restored node never elects
without part 2 of the transform
(`TestDM21_NegativeControlWithoutVoidingSelfRemovedNeverElects`) — is
implemented in `internal/node/dm21_test.go`.

### LEARNER NON-INTERFERENCE

**Statement**: A learner never counts toward quorum, never votes, and
its absence/crash/slowness never blocks or delays commit progress for
the voting set — including never blocking cluster-generation
finalization.

**Scope**: `activeConfig.Majority()` computed over `Voters` only,
everywhere; `internal/node.computePrecheck`'s `Ready` field computed
over voters only (a learner appears in `Peers` with a `Role` field for
diagnostics, but can never gate finalization).

**Threatened by**: Any commit-rule, election, or precheck path that
iterates `Learners` when computing a threshold.

**Proof/test obligations**: `TestAddLearnerPromoteRemoveFullLifecycle`
(`internal/node`, asserting ordinary write traffic and catch-up proceed
throughout); a direct unit test asserting `Ready` is true with an
unreachable learner present is a documented remaining item.

### MEMBERSHIP-SCOPED VOTE ACCEPTANCE (liveness, not safety)

**Statement**: A `RequestVoteRequest` from a sender that is not a Voter
in the receiver's own `activeConfig` is dropped before any term/log
state is touched (Rule 1); a node that has heard from its current
leader within the minimum election timeout ignores any
`RequestVoteRequest`, including a higher-term one (Rule 2, Raft
§4.2.3). `AppendEntriesRequest`, `MsgInstallSnapshotRequest`, and every
`*Response` are never filtered on a membership basis.

**Why it matters, and what it does not provide**: This bounds the
disruption a removed-but-running node can cause. It is **not** a safety
mechanism — safety against a removed node comes entirely from
`CONFIGURATION BRANCH CONFINEMENT` plus quorum intersection, and holds
with no filtering at all. Filtering replication traffic on a membership
basis would strand a legitimate member awaiting log repair (the exact
defect an early revision of this design had).

**Scope**: `raft.Core.handleRequestVoteRequest` (Rule 1);
`raft.Core.heardFromLeader`, set on an accepted leader
`AppendEntries`/`InstallSnapshot` and cleared on `InputElectionTimeout`
or step-down (Rule 2) — `Core` owns no clock and no tick counter;
`internal/node` owns the election timer unchanged.

**Threatened by**: Extending the filter to replication traffic; moving
Rule 2's logic into `internal/node`, where `internal/fault`'s `Core`-
level harness could not exercise it.

**Proof/test obligations**: `TestRequestVoteDroppedFromNonMember`,
`TestLearnerNeverGrantsVoteAndNeverCampaigns`,
`TestHeardFromLeaderSuppressesHigherTermRequestVote` (`internal/raft`);
`TestDM9_StaleRemovedNodeCannotDisruptCluster` (`internal/fault`, both
rules exercised together against a real election timer).

### MINIMUM VOTER INVARIANT

**Statement**: A `RemoveServer` that would reduce the voter count to
`0` is deterministically refused by `Core`, on the leader at propose
time and on every replica at accept time; a zero-voter `Configuration`
is never reached or persisted, transiently or otherwise. This is pure
quorum mathematics and is **never** operator-overridable — distinct
from the separate, policy-layer requirement that any result below
*three* voters carry an explicit `confirmVoterCount` (`internal/node`,
not re-validated by any replica, appearing in no safety proof).

**Scope**: `raft.Core.ProposeConfigChange` check 6;
`raft.classifyTransition`'s `RemoveServer(voter)` shape, which requires
`len(C_new.Voters) >= 1`.

**Proof/test obligations**: `TestMinimumVoterInvariantRefusesLastVoterRemoval`
(`internal/raft`); `TestRemoveServerRequiresConfirmationBelowThreeVoters`
(`internal/node`, the policy layer); `TestDM19_SubThreeVoterConfirmationStaleAndZeroVoterCases`
(`internal/node/membership_test.go`), which additionally proves a
*stale* `confirmVoterCount` — correct for an earlier cluster size — is
refused exactly like a missing one, and that `Core`'s own absolute
`ErrLastVoterRemoval` refuses a zero-voter removal even when the
policy-layer confirmation is satisfied, pinning the two-layer split
directly.

### SINGLE-SERVER TRANSITION SHAPE

**Statement**: Every `EntryConfig` entry that **establishes** a
configuration represents exactly one of four transitions (AddLearner,
PromoteToVoter, RemoveServer-voter, RemoveServer-learner) relative to
the immediately preceding active configuration; no other transition is
ever accepted by any replica. A `Voided` entry establishes none and is
outside this invariant's scope entirely — it is appended on the
ordinary replication path like any `EntryNormal` entry, and reading it
*into* scope (failing closed on it) would halt every node that joins a
restored cluster before that cluster's first snapshot.

**Scope**: `raft.classifyTransition` (leader-side, via
`ProposeConfigChange`, and replica-side, via
`activateFromAppendedEntries`) — with one deliberate, documented
exception: a replica's own re-check is skipped when its own prior
configuration is still the zero value (a never-joined node's very first
catch-up batch can legitimately contain, among older ordinary entries,
the very `EntryConfig` entry that adds it, and that node has no
independent basis to validate the transition's "before" state against
— see `activateFromAppendedEntries`'s doc comment for the real bug this
closes).

**Threatened by**: A leader-side-only check with no follower-side
re-verification; extending the re-verification to `Voided` entries;
applying the re-verification against a receiver's own zero prior
configuration.

**Proof/test obligations**: `TestClassifyTransitionFourShapes`,
`TestNewLearnerFirstCatchUpBatchIncludingItsOwnAddEntry`
(`internal/raft`); `FuzzDecodeEntryConfig`.

### MEMBERSHIP CHANGE GENERATION GATE

**Statement**: No `EntryConfig` entry is ever proposed by a node whose
committed cluster generation is `< 2`, and none is ever **applied** by
a node whose durable cluster generation is `< 2` — the latter fails
closed (this node stops) rather than being silently accepted.

**Scope**: `internal/node.handleMembership` (propose-side gate, before
`Core.ProposeConfigChange` is ever called);
`internal/node.applyConfigEntry` (apply-side gate, checked after
decoding but before recording any outcome).

**Why it matters**: A leader-side-only check would make every replica's
correctness depend on a remote node's code being right — the apply-side
gate is defense in depth mirroring `ADR-0017`'s identical posture for
`SetClusterVersionCommand`.

**Proof/test obligations**: `TestAddLearnerRefusedBeforeGeneration2`
(`internal/node`, and its HTTP-layer counterpart in
`cmd/chronicledb-node`, both the leader-propose-side gate);
`TestDM18_MembershipChangeGenerationGateBothSides`
(`internal/node/membership_test.go`), which additionally pins the
apply-side `Node.fail` path — a synthetic committed `EntryConfig` fed
directly to `applyConfigEntry` against a hand-built, never-started
`Node` pinned at generation 1, since genuine replication can never
reach this path (every node necessarily applies the generation-2
finalize before any later `EntryConfig` can exist).

### MEMBERSHIP REQUEST OUTCOME STABILITY

**Statement**: A completed membership-change `RequestID` resolves to
the same recorded outcome after retry, restart, or leader failover,
indefinitely; a request refused **before proposal** records nothing at
all, leaving its `RequestID` freshly usable.

**Scope**: `internal/fsm.RecordMembershipOutcome`/`GetMembershipOutcome`
(fingerprinted over `{kind, nodeId, address}` only — `confirmVoterCount`
is deliberately excluded, since it is an authorization gesture about
one submission, not part of the operation's identity); the membership
outcomes' `EncodeState`/`DecodeState` trailing block, gated on
`clusterGeneration >= 2` (not `> 0`) so a pre-`v0.5.0`-finalization
snapshot stays byte-identical to `v0.4.0`'s own output.

**Threatened by**: Recording an outcome from any pre-proposal refusal
path; including `confirmVoterCount` in the idempotency fingerprint.

**Proof/test obligations**: `TestRecordMembershipOutcomeIdempotentAndConflictDetected`,
`TestGetMembershipOutcomePrecheck`,
`TestMembershipOutcomesSurviveEncodeDecodeRoundTrip`,
`TestMembershipOutcomeGatedOnGeneration2NotJustNonZero` (`internal/fsm`);
the idempotent-retry assertion inside
`TestAddLearnerPromoteRemoveFullLifecycle` (`internal/node`).

### NO SILENT FORMAT MISINTERPRETATION (extended by `v0.5.0`)

Extended to four new surfaces, each with its own independent
fail-closed path and its own test: the `EntryConfig` control-kind range
on the **wire** (`internal/raft`'s local mirror of
`fsm.ControlCommandMarker`, disjoint from `internal/fsm`'s own range —
`TestControlKindRangesNeverCollide`), the `Entry.Type` payload sentinel
on **disk** (`internal/node`'s `entryPayloadTypeSentinel`, disjoint from
both `fsm.ControlCommandMarker` and `commitTxnCommandVersion` — guarded
by an `init()` panic), the snapshot `FormatVersion` **range** check
(`internal/snapshot.Decode`, `[MinReadVersion, FormatVersion]` replacing
strict equality — `TestUnsupportedVersionRange`), and the snapshot
frame's `hasConfig`/`configLen` **cross-check** (an invalid flag byte,
or a flag and length that disagree, is `ErrCorrupt` rather than a
shifted read — `TestV2RejectsInvalidHasConfigByte`,
`TestV2RejectsHasConfigFalseWithNonZeroLen`). The `FormatVersion`
surface's mechanism changes from equality to a bounded range; the
invariant's statement is unchanged.

## Admission Control / Storage Lifecycle invariants (`v0.6.0`)

See [`docs/admission-control.md`](admission-control.md) and
[`docs/storage-lifecycle.md`](storage-lifecycle.md) for the full
architecture, and [`ADR-0019`](adr/0019-admission-control-architecture.md)/
[`ADR-0020`](adr/0020-mvcc-gc-replicated-watermark.md)/
[`ADR-0021`](adr/0021-storage-lifecycle-retention-and-scrub.md) for the
design decisions. Ten new entries below; `NO SILENT FORMAT
MISINTERPRETATION` (above) is amended in place, not re-added; two
existing invariants (`MVCC VISIBILITY`, `ISOLATION TRUTHFULNESS`) each
gain one sentence.

### BOUNDED ADMITTED WORK

**Statement**: The number of client proposals concurrently
proposed-but-unapplied on a node never exceeds
`-max-inflight-proposals`; the number of pending `ReadIndex` requests
never exceeds `-max-concurrent-reads`; the number of live read leases
never exceeds `-max-live-read-leases`; and the number of goroutines
concurrently inside any admission gate never exceeds that gate's
`MaxConcurrent + MaxQueueDepth`.

**Scope**: Every node, every moment, replicated mode.

**Why it matters**: An unbounded admitted set is an unbounded memory
commitment and an unbounded event-loop backlog — the failure mode that
turns a slow cluster into a dead one.

**Mechanism**: Fixed-capacity channels in `internal/admission.Gate`
bound goroutines and resident payload memory. Three independent
event-loop ceilings — `len(n.waiters)`, `len(n.pendingReads)`, and the
live-lease count — bound the per-node state a request leaves behind.
These are not interchangeable: the gate does **not** bound `waiters` or
`pendingReads`, because a caller that cancels its context frees its
gate slot while its waiter/pending-read entry survives until it
resolves. The event-loop ceilings are the authoritative mechanism for
this invariant; the gates are the authoritative mechanism for the
admission-side memory bound.

**Threatened by**: A new client entry point added without a gate; a
gate constructed with `MaxConcurrent <= 0`; a `release` that is not
deferred; a new piece of per-request node state introduced without its
own event-loop ceiling.

**Proof/test obligations**: AC-1 (exact boundary), AC-2 (`-race`, 1000
iterations), AC-3 (`len(waiters)` never exceeds the bound under real
saturation), AC-14 (the defense counter stays 0 with no client
cancellation), AC-20 (cancel-heavy workload: the defense counter *does*
fire and the ceiling still holds), AC-21 (`pendingReads`/lease ceilings
hold under a never-resolving minority-partition read flood, `minLease`
is `O(1)`), AC-18 (structural AST test asserting no admission
identifier is reachable from the event loop).

### CONTROL-PLANE NON-STARVATION

**Statement**: The latency of processing one inbound Raft message, and
of dispatching one heartbeat round, is not a function of client
admission-queue depth, client request concurrency, or client rejection
rate. This invariant does not claim the event loop is never blocked:
snapshot creation, backup export, and snapshot installation still run
to completion on it, bounded and measured but not eliminated.

**Scope**: `internal/node`'s event loop, replicated mode.

**Why it matters**: `RAFT ELECTION SAFETY` is a safety property, but
Raft's availability depends on timely heartbeat processing; starving
it because of client load turns an overload into an election storm.

**Mechanism**: Structural lane separation with no shared structure
between Lane K (consensus, no gate at all), Lane A1/A2 (control/
maintenance), and Lane B (client reads/writes); no admission identifier
reachable from the event loop. Plus three preconditions that would
otherwise make the statement false: `FSM.mu` as an `RWMutex` (so the
ungated `/outcome`/`/status` read path never blocks behind a client
write holding it exclusively), the `pendingReads` ceiling bounding the
per-message pending-reads scan, and `MaxKeys`-bounded GC `Apply`.

**Threatened by**: Adding a gate acquisition inside the event loop "for
symmetry"; sharing one gate between Lane A and Lane B, or merging Lane
A1 and Lane A2 back together; adding an ungated endpoint that takes
`FSM.mu` in exclusive mode; adding per-message loop work proportional
to an unbounded set.

**Proof/test obligations**: AC-6 (`internal/node` `testCluster` —
saturated client gate vs. heartbeat latency), AC-10 (real cluster, zero
elections under sustained saturation), AC-19 (ungated `/outcome`/
`/status` flood vs. Raft message-processing p99), AC-21
(`checkPendingReads` cost bounded), AC-22 (concurrent backup + scrub
does not delay a membership change), SL-2b (GC Apply cost flat in
total key count), AC-18 (AST, lane separation), plus the negative
controls AC-7 (lane separation disabled by a test hook produces
elections) and AC-19's exclusive-mutex control.

### REJECTION SAFETY

**Statement**: A request rejected for capacity has never been
proposed, replicated, or applied; no `RequestID` was recorded; the
rejection is always safe to retry and never produces a duplicate
effect.

**Scope**: Every admission rejection, every lane.

**Why it matters**: Extends `IDEMPOTENCY` to the new rejection path
explicitly. A rejection that might have partially happened is worse
than no rejection at all.

**Mechanism**: Every gate acquisition strictly precedes the channel
send into the event loop, and therefore strictly precedes
`Core.Step(InputPropose)`.

**Threatened by**: Moving a gate acquisition after the propose;
recording a metric keyed by `RequestID` on the rejection path.

**Proof/test obligations**: AC-8 (50%-rejection stress with recycled
`RequestID`s, checked by `internal/oracle` for zero duplicate effects
and zero `RequestID`s with two outcomes), AC-9 (a rejected request's
`RequestID` is `ErrRequestIDUnknown` at `/outcome`).

### ADMISSION FAILS CLOSED

**Statement**: Any failure, misconfiguration, or absence of the
admission mechanism results in rejecting work, never in admitting
unbounded work.

**Scope**: Configuration parsing, gate construction, and every request
path.

**Why it matters**: An availability cost is preferred over resource
exhaustion — the same posture `AUDIT COMPLETENESS` already takes for
the audit log.

**Mechanism**: `MaxConcurrent <= 0` is a startup error; there is no
"unlimited" value; the event-loop ceiling rejects independently of the
gate; `Acquire` never returns a nil error without a held slot.

**Threatened by**: Adding a `-disable-admission` flag; treating a
gate-construction error as a warning rather than fatal.

**Proof/test obligations**: AC-4 (each invalid configuration refuses
startup with a specific message), AC-5 (a deliberately no-op gate
injected via a test hook still cannot exceed the bound, because the
event-loop ceiling holds).

### GC SAFETY

**Statement**: A version is removed only when
[`docs/mvcc.md`](mvcc.md) §6's rule permits it, and no transaction ever
observes a version GC has removed: any read or commit whose `StartSeq`
is below the applied GC watermark is refused, never silently served
from the surviving chain.

**Scope**: `internal/mvcc`, `internal/fsm`, every replica, all time.

**Why it matters**: Reclaiming a version a live snapshot still needs is
the canonical MVCC-GC correctness bug, and — worse than an error — it
manifests as a silent Snapshot Isolation violation.

**Mechanism**: The reclamation predicate applied inside `fsm.Apply`
against the post-`max()` `f.gcWatermark` (the single authoritative
horizon); the horizon guard inside `mvcc.Store` under the same lock as
the chain read, reached by every committed-read path including prefix
scans; the equality `Store.GCWatermark() == FSM.gcWatermark` after
restore, install, and restart; deterministic commit-side abort
(`StatusAbortedStale`).

**Threatened by**: A read path reaching `chains` without going through
`Visible`/`ScanVisible` (`Store.Export` is the one that historically
did, which is why it is restricted to snapshot encoding and asserted
structurally); a restored `Store` left at watermark 0; lowering the
watermark; computing the watermark from anything but committed state;
reclaiming against `cmd.Watermark` rather than `f.gcWatermark`.

**Proof/test obligations**: SL-1 (property test: randomized chains ×
randomized snapshot sets × randomized watermarks, asserting no
surviving snapshot's required version was removed and every
removed-version read is refused), SL-2/SL-12 (the same property under
combined chaos with GC active), SL-3 (negative control: with the
horizon guard disabled by a test hook, the property test detects the
silent stale read).

### GC DETERMINISM

**Statement**: The set of versions reclaimed by a node, and its
resulting GC cursor and watermark, are a pure function of the
committed log prefix that node has applied — identical on every
replica, and identical across replay, restart, and snapshot restore.

**Scope**: `internal/fsm.ApplyAdvanceGCWatermark`, `internal/mvcc`.

**Why it matters**: It is what keeps `STATE MACHINE SAFETY` and
`DETERMINISM BOUNDARY` true after GC exists.

**Mechanism**: Reclamation only inside `Apply`; explicitly sorted key
iteration via `Store.KeysFrom`; both `MaxVersions` and `MaxKeys`
carried in the replicated command rather than read from local config;
the post-`max()` `f.gcWatermark` as the single comparison horizon;
monotone `max()` on the watermark; `gcPassSeq` advanced in `Apply` so
the leader's next `RequestID` is itself a function of replicated
state.

**Threatened by**: A background goroutine mutating `Store`; map
iteration order; reading `-gc-max-versions-per-pass`/
`-gc-max-keys-per-pass` inside `Apply`; a per-node "skip GC when busy"
heuristic; deriving the GC `RequestID` from anything a replica cannot
recompute from its own applied state.

**Proof/test obligations**: SL-4 (two independently constructed FSMs
fed the identical history including GC commands produce byte-identical
`EncodeState`), SL-4a (a command whose `Watermark` is below
`f.gcWatermark` reclaims against the current watermark, not the
command's), SL-2b (Apply cost flat as total key count grows 10×),
SL-12 (chaos: every live node's `EncodeState` at the same applied
index is byte-identical, with GC active), SL-13 (a node that caught up
by `InstallSnapshot` and one that caught up by log replay agree byte
for byte, including `gcPassSeq` and the `Store`/`FSM` watermark
equality).

### SNAPSHOT HORIZON ENFORCEMENT

**Statement**: Every read of committed MVCC state, and every commit
decision, is refused if its `StartSeq` is below the applied GC
watermark — structurally, at the `internal/mvcc` boundary, with no
bypass.

**Scope**: `internal/mvcc.Store` (all read entry points),
`internal/fsm.Apply`.

**Why it matters**: It is the mechanism that makes `GC SAFETY`
unconditional rather than "sound as long as the leader knew about
every reader."

**Mechanism**: `Visible`/`ScanVisible` return `ErrSnapshotTooOld`;
`Apply` returns `StatusAbortedStale`; both read the watermark under the
same lock as the data. `Store.Export` is restricted to snapshot
encoding and `internal/sql`'s duplicate visibility helper is deleted,
so there is exactly one implementation of the rule. `DecodeState`
propagates the watermark into the restored `Store`, so the guard is
live on a restored or snapshot-installed node from its first read.

**Threatened by**: A new read accessor added to `Store` without the
check; a caller re-deriving visibility from `Export` outside
`internal/mvcc`; exposing `chains` directly; a restored `Store`
constructed without a watermark.

**Proof/test obligations**: SL-5 (ordering: idempotency before horizon
before conflict), SL-6 (every committed-read entry point the product
actually has — `replicatedTxn.Get`/`ScanPrefix`,
`standaloneTxn.Get`/`ScanPrefix`, and every exported `mvcc.Store` read
method — refuses below the horizon), SL-6a (`Store.GCWatermark() ==
FSM.gcWatermark` as a `DecodeState` post-condition), SL-6b
(`TestExportOnlyCalledFromSnapshotEncoding`, AST), SL-7 (a stale
transaction's commit aborts identically on every replica and is
recorded stably).

### RECLAMATION BOUNDARY

**Statement**: No durable WAL segment or snapshot file is deleted
before the snapshot that supersedes it is fully written, fsync'd,
recorded in durable WAL metadata, and has had `HardState`
re-affirmed; and nothing above that boundary is ever deleted.

**Scope**: `internal/wal.CompactBefore`/`CompactBeforeRetaining`,
`internal/snapshot.Manager.Prune`, `WALStorage.InstallSnapshot`.

**Why it matters**: This is `LOG COMPACTION SAFETY` restated at the
exact ordering level, made explicit because `v0.6.0` introduces new
retention knobs touching the same code.

**Mechanism**: The unchanged six-step `maybeSnapshot` ordering
(asserted by a call-order test); prune-after-durable in
`Manager.Prune`; whole-segments-only; never the current segment.

**Threatened by**: An "optimization" that compacts before the pointer
write; a retain count of 0; partial-segment reclamation.

**Proof/test obligations**: SL-8 (crash injection at each of the six
ordering points, restart, assert recoverable and no committed entry
lost, using the `internal/node` crash-injection facility — not
`internal/fault`, which has no snapshot manager, no `internal/wal`,
and no `internal/storage`), SL-26 (crash at each of the three
`handleInstallSnapshot` points), SL-9 (`-snapshot-retain-count=0`
refuses startup).

### DISK-FULL EXPLICITNESS

**Statement**: An out-of-space condition is always surfaced as an
explicit, distinguishable failure — never conflated with a transaction
conflict, a generic I/O error, or an admission rejection — and never
silently treated as success.

**Scope**: `internal/wal`, `internal/storage`, `internal/node`, the
HTTP surface.

**Why it matters**: A client that cannot distinguish "the cluster is
full" from "you lost a conflict" will retry the wrong thing forever.

**Mechanism**: `errors.Is(err, syscall.ENOSPC)` classification into
`wal.ErrOutOfSpace`/`storage.ErrOutOfSpace`, reclassified rather than
lost at each package boundary; the distinct `disk_critical` admission
reason; distinct `/health` and `/status` fields.

**Threatened by**: Wrapping an `ENOSPC` into a generic
`fmt.Errorf("write failed: %w")` that loses the classification at a
package boundary.

**Proof/test obligations**: SL-14 (real small filesystem, real
`ENOSPC`: the classified error reaches the client and `/health`),
SL-14b (`ENOSPC` is never reported as an SI abort), SL-15 (recovery
after space is freed, with no restart).

### SCRUB NON-DESTRUCTIVE

**Statement**: The integrity-verification scrub never modifies on-disk
state; running it against a live node is always safe.

**Scope**: `internal/node.Scrub` and everything it calls.

**Why it matters**: A verification tool that can damage what it
verifies will not be run when it is most needed.

**Mechanism**: `storage.OpenSegmentReadOnly` (`O_RDONLY`;
`Append`/`Sync`/`Truncate` return `ErrReadOnlySegment`); no `FSM.mu`,
no `Core` access; scrub runs on the caller's own goroutine, touching
only directory paths, never a live `*wal.WAL`/`*snapshot.Manager`.

**Threatened by**: A future "scrub can also fix the torn tail" idea.

**Proof/test obligations**: SL-10 (directory hashed before and after a
scrub over corrupted and clean data — byte-identical), plus
`TestScrubOnlyOpensReadOnly` (AST, with a negative control).

### NO SILENT FORMAT MISINTERPRETATION (extended by `v0.6.0`)

Amended in place, never re-added. The `v0.6.0` extension: control-kind
byte `2` (`AdvanceGCWatermark`, disjoint from `controlKindSetClusterVersion
= 1` and asserted so by `TestControlKindRangesNeverCollide`), outcome
status byte `3` (`StatusAbortedStale`), and the generation-3 snapshot
trailing block (`gcWatermark`/`gcCursor`/`gcPassSeq`) are each rejected
— never guessed at — by any binary that does not understand them; and
`v0.6.0`'s own `DecodeState` newly rejects any unrecognized status
byte, closing a gap in the `v0.5.0` decoder.

### `MVCC VISIBILITY` and `ISOLATION TRUTHFULNESS` (amended by `v0.6.0`)

`MVCC VISIBILITY` (above) is narrowed in domain, not weakened: it now
applies to snapshots at or above the GC horizon, and below the horizon
the answer is an explicit `ErrSnapshotTooOld`/`StatusAbortedStale`
rather than a value — a refused read is not a wrong read.
`ISOLATION TRUTHFULNESS` (above) is strengthened: a naive GC
implementation would have made the system quietly less isolated than
it claims by resurrecting a version a live snapshot still needed, and
the horizon guard (`internal/mvcc`'s `Visible`/`ScanVisible`) forbids
it structurally.
