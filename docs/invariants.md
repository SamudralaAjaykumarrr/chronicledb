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
