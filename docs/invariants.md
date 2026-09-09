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
