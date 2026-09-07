# Break ChronicleDB — External Review Challenge

Status: Phase 12 review infrastructure (`docs/roadmap.md` Phase 12).
This is an open invitation, published on `main`, for independent
database/storage/distributed-systems engineers to try to find genuine
correctness or reliability violations in ChronicleDB. **No external
review has occurred as of this writing** — this document is the
process, not a report of results. Any actual finding will be recorded,
honestly and regardless of outcome, in
[`docs/external-review-findings.md`](external-review-findings.md).

This guide does not duplicate the existing documentation tree; it maps
a reviewer directly onto it. Every command shown below is a real
command that runs against this repository today — none are aspirational
or hypothetical.

## Table of contents

1. [Purpose](#1-purpose)
2. [Architecture at a glance](#2-architecture-at-a-glance)
3. [Threat / correctness model](#3-threat--correctness-model)
4. [Guarantees in scope — what you are invited to try to break](#4-guarantees-in-scope--what-you-are-invited-to-try-to-break)
5. [Explicit non-guarantees — what is not in scope](#5-explicit-non-guarantees--what-is-not-in-scope)
6. [Reviewer personas — start here](#6-reviewer-personas--start-here)
7. [Challenge matrix](#7-challenge-matrix)
8. [Build, test, and deterministic reproduction](#8-build-test-and-deterministic-reproduction)
9. [How to report a correctness finding](#9-how-to-report-a-correctness-finding)
10. [How to report a security finding](#10-how-to-report-a-security-finding)
11. [What happens after you report](#11-what-happens-after-you-report)
12. [Where findings are recorded](#12-where-findings-are-recorded)

## 1. Purpose

ChronicleDB is a from-scratch distributed transactional database (see
[`vision.md`](vision.md)). Phases 1-11 built and tested the engine,
packaged it for open-source use, and published `v0.1.0` — see
[`roadmap.md`](roadmap.md) for the complete phase-by-phase account and
its evidence-based maturity model. This phase asks a harder question
than any internal test suite can answer on its own: **does an
independent reviewer, with no stake in the project succeeding, find a
way to violate a documented invariant?**

This is not a bug bounty program (`SECURITY.md` explicitly does not
operate one) and no reviewer, finding, or outcome is fabricated,
predicted, or implied here. Publishing this guide only means the
*process* is open; it says nothing about whether anyone has used it yet
(see [`docs/external-review-findings.md`](external-review-findings.md)
for the current, honest count).

## 2. Architecture at a glance

ChronicleDB is a single logical shard, a static three-node-style Raft
cluster, one durable ordered history per node. The full system map,
binding terminology, and package/dependency structure live in
[`architecture.md`](architecture.md) — read that first if you are
unfamiliar with the codebase; this section only orients you to where
the pieces reviewers attack actually live:

```
client
  -> internal/sql (optional; constrained SQL, never bypasses the below)
  -> internal/txn.Manager (standalone) / internal/node.Node (replicated)
  -> internal/mvcc, internal/fsm (deterministic Apply boundary)
  -> internal/raft (consensus core, transport/clock/storage abstracted)
  -> internal/wal, internal/storage (durable log)
  -> internal/snapshot (log compaction, follower catch-up)
```

`internal/fault` (deterministic distributed-systems simulator) and
`internal/oracle` (independent reference model, test-only) are never
imported by production code — they exist purely to let you, and the
project's own test suites, attack the above without real time, real
sockets, or real disks getting in the way.

## 3. Threat / correctness model

ChronicleDB assumes **crash/partition faults, not Byzantine faults**
(nodes that lie or behave arbitrarily maliciously) — this is Raft's own
assumption, inherited unchanged (`failure-model.md` §5). It also
assumes **a trusted network** between clients and the cluster and among
cluster nodes — there is no authentication, authorization, or TLS
(`SECURITY.md` §Deployment assumptions). Within that trust boundary,
however, ChronicleDB makes specific, checkable correctness claims (§4)
that hold regardless of how adversarially a *trusted* client or a
*crashing/partitioning* node behaves — that is exactly what you are
invited to try to falsify.

A finding that requires an untrusted network participant to do
something already documented as possible without auth/TLS (e.g.
"anyone on the network can propose a write") is not a new finding — see
§5 and §10 for exactly where that boundary sits.

## 4. Guarantees in scope — what you are invited to try to break

Every row below is a real claim this repository already has test
evidence for — the primary evidence links to the exact scenario
IDs/tests in [`scenario-corpus.md`](scenario-corpus.md); if you can
construct a case where the claim doesn't hold, that is a genuine
finding regardless of whether that exact scenario ID exists yet.

| Invariant ([`invariants.md`](invariants.md)) | Plain statement | Primary evidence |
|---|---|---|
| DURABILITY | An acknowledged committed write survives every documented crash/restart class. | `scenario-corpus.md` §Local Durability, §Raft/Replication |
| ATOMICITY | A multi-key transaction becomes visible all-or-nothing. | §Transactions (TX-3) |
| ABORT SAFETY | Aborted/never-committed writes never surface after recovery. | §Transactions, §Raft/Replication |
| RECOVERY NON-INVENTION | Recovery never manufactures state beyond legitimate committed history. | §Local Durability (LD-4/5/6) |
| MVCC VISIBILITY | A read sees exactly its `StartSeq`-visible committed versions plus its own writes. | §Transactions (TX-6, TX-7) |
| CONFLICT CORRECTNESS | Write-write conflicts follow first-committer-wins exactly, identically on every replica. | §Transactions (TX-5), §Raft/Replication |
| IDEMPOTENCY | A completed `RequestID` is never re-applied. | §Idempotency (ID-1..7) |
| REQUEST OUTCOME STABILITY | A `RequestID`'s terminal outcome never changes across retry/restart/replay, indefinitely. | §Idempotency, §Raft/Replication |
| RAFT ELECTION SAFETY | At most one leader per term. | §Raft/Replication (RF-9..13) |
| RAFT LOG MATCHING | Matching `(term,index)` implies identical preceding history. | §Raft/Replication (RF-9) |
| LEADER COMPLETENESS | Committed entries survive every future leadership change. | §Raft/Replication (RF-5,6,9,12,13) |
| STATE MACHINE SAFETY | All replicas applying the same history reach equivalent state. | model-based suites, `adversarial-testing.md` §3 |
| QUORUM SAFETY | A minority partition cannot acknowledge majority-required commits. | §Raft/Replication (RF-11) |
| APPLIED-PREFIX SAFETY | A durable-but-uncommitted suffix is never applied. | §Raft/Replication |
| SNAPSHOT SAFETY | An installed snapshot is atomic and never partially trusted. | §Snapshots (SN-1..5) |
| LOG COMPACTION SAFETY | Compaction never removes history still needed. | §Snapshots (SN-6) |
| ISOLATION TRUTHFULNESS | No accidental SERIALIZABLE claim anywhere. | `mvcc.md` §1.1, `si_history_test.go` |
| CONSISTENT LOG RESPONSIBILITY | One logical ordered history, one durable persistence mechanism. | `architecture.md` §2 |
| DETERMINISM BOUNDARY | `internal/raft`/`internal/fsm`/`internal/mvcc` never depend on wall clock, randomness, env, or unordered iteration for committed/applied decisions. | deterministic replay tests |

Also in scope: the constrained SQL layer's documented statement
semantics ([`sql.md`](sql.md) SQ-1..9), and the robustness properties
in [`failure-model.md`](failure-model.md) §6 (no panic on malformed
disk/network/SQL input, bounded allocation, checksum/version
enforcement) — correctness/robustness properties independent of the
deferred auth/TLS work, and fair game to attack.

## 5. Explicit non-guarantees — what is not in scope

Reporting one of these as a "bug" will be triaged, recorded honestly as
**not-a-bug (working as documented)** in the findings ledger, and
closed — not silently ignored, but also not a confirmed correctness
finding:

- **SERIALIZABLE isolation.** Only Snapshot Isolation is claimed
  (`mvcc.md` §1.1). The three-way write-skew ring in
  `si_history_test.go::TestSIHistory_ThreeWayWriteSkewRing` is a
  *documented, intentional* SI-allowed outcome, not a bug.
- Any SQL feature [`sql.md`](sql.md) §8 lists as not built: joins,
  subqueries, secondary indexes, `ORDER BY`/`LIMIT`, PostgreSQL wire
  protocol, a CLI, `NULL`/defaults, predicates beyond primary-key
  equality.
- Sharding, multi-shard transactions, dynamic membership, cross-region
  replication ([`non-goals.md`](non-goals.md)).
- Byzantine fault tolerance — Raft, and therefore ChronicleDB, assumes
  crash/partition faults, not malicious/lying nodes
  (`failure-model.md` §5).
- Simultaneous loss of a majority of nodes' persistent storage
  (standalone: the single node's storage).
- Security properties that require authentication/TLS ChronicleDB does
  not implement — "an unauthenticated client can propose writes" is
  expected, documented behavior (`SECURITY.md`), not a vulnerability,
  *unless* it demonstrates a correctness violation reachable even from
  a trusted client (which **is** in scope — see §10's decision aid).
- Performance/throughput claims beyond what [`benchmarks.md`](benchmarks.md)
  actually measured — a slower-than-expected number is feedback, not a
  correctness bug, unless it reveals a genuine algorithmic defect (e.g.
  another O(n²) pattern like `benchmarks.md` §8.1's).
- Windows/macOS/non-amd64 *runtime* behavior
  ([`support-matrix.md`](support-matrix.md): cross-compiled, not
  runtime-tested) — a *build* failure there is in scope; a runtime
  behavior difference is a known, disclosed gap.

## 6. Reviewer personas — start here

Pick the row closest to what you want to attack; each entry points at
the subsystem, docs, and existing tests without requiring you to read
the whole documentation tree cold.

| Persona | Targets | Entry points |
|---|---|---|
| Distributed-systems/Raft reviewer | Election safety, log matching, leader completeness, quorum safety, partitions, message reordering/duplication/delay | `internal/fault` (deterministic simulator), `internal/raft/adversarial_test.go`, [`raft.md`](raft.md), [`replication.md`](replication.md) |
| Storage/durability reviewer | WAL framing, torn-tail/corruption handling, fsync ordering, snapshot atomicity, compaction safety | `internal/wal`, `internal/storage`, `internal/snapshot`, [`wal.md`](wal.md) §6, [`snapshots.md`](snapshots.md) |
| Transactions/MVCC reviewer | Visibility, conflict detection, atomicity, tombstones, write skew | `internal/mvcc`, `internal/txn`, [`mvcc.md`](mvcc.md), `si_history_test.go` |
| Idempotency/client-contract reviewer | `RequestID` outcome stability across retries, failovers, restarts, snapshots | [`transactions.md`](transactions.md) §6-7, [`failure-model.md`](failure-model.md) §4 |
| SQL/frontend reviewer | The constrained SQL surface's statement semantics and its use of the real transaction machinery (never a shortcut around it) | `internal/sql`, [`sql.md`](sql.md), [ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md) |
| Generalist "just try to break it" | No specific subsystem — run chaos/adversarial suites at higher seed counts, drive `/fault` against a real cluster, try unusual input via the SQL fuzz corpus | §7's matrix and §8's commands, used as a menu, not a checklist |
| Security reviewer | Anything suggesting an untrusted network participant can do more than `SECURITY.md` already documents | [`SECURITY.md`](../SECURITY.md) — private reporting, not this public workflow (§10) |

A reviewer is explicitly encouraged to combine rows (e.g. an asymmetric
partition *during* a snapshot install) — this is exactly the style
Phase 7's and Phase 10's own most valuable testing came from (see
[`testing-strategy.md`](testing-strategy.md) §7,
[`adversarial-testing.md`](adversarial-testing.md) §4).

## 7. Challenge matrix

Every row is a named break-scenario, mapped to the invariant(s) it
threatens, the subsystem, and the *existing* deterministic reproduction
surface you should drive further — not a new mechanism to build. "Existing
coverage" cites the nearest proven test so you can see exactly what's
already checked and go looking for what isn't.

| # | Scenario | Subsystem | Invariant(s) | Reproduce via | Existing coverage (nearest proof) |
|---|---|---|---|---|---|
| 1 | Leader crash before replication | Raft | DURABILITY (n/a — never acked) | `internal/fault` Crash action; `/fault` on a real cluster | RF-4 |
| 2 | Leader crash after replication (quorum, before reply) | Raft | LEADER COMPLETENESS, REQUEST OUTCOME STABILITY | `internal/fault`; `cmd/chronicledb-node -tags=integration` SIGKILL | RF-5/6 |
| 3 | Follower crash/restart | Raft/Recovery | DURABILITY | `internal/node/chaos_test.go`; real SIGKILL | RF-3 |
| 4 | Quorum loss (majority down) | Raft | QUORUM SAFETY, availability (not safety) | `internal/fault` isolate N-1 nodes | RF-11 |
| 5 | Asymmetric partitions | Raft transport | RAFT ELECTION SAFETY, liveness | `Transport.IsolateLink`/`BlockSend`/`BlockRecv` | `TestChaos_AsymmetricPartitionSafety` (found/fixed a real bug — `testing-strategy.md` §7.1) |
| 6 | Delayed/duplicated messages | Raft transport | RAFT LOG MATCHING | `internal/fault` delay/duplicate | RF-7/8 (simulator-only, documented) |
| 7 | Stale Raft responses | Raft core | RAFT ELECTION SAFETY | `Cluster.Deliver`/`Transport.Take` held-message replay | `TestChaos_StaleAppendEntriesAfterSnapshotInstall` |
| 8 | Repeated elections | Raft | liveness, ELECTION SAFETY | `internal/fault` adversarial timer scheduling | RF-15, `failure-model.md` §2.8 |
| 9 | `RequestID` retries across failover | Idempotency/Raft | IDEMPOTENCY, REQUEST OUTCOME STABILITY | real cluster retry after kill | ID-2..5, SQ-8 |
| 10 | `UNKNOWN` client outcomes | Client contract | REQUEST OUTCOME STABILITY | crash between apply and response | `failure-model.md` §1.6/§4.4 |
| 11 | WAL truncation / torn-tail | Storage/WAL | RECOVERY NON-INVENTION | byte-level on-disk corruption injection | LD-3/4 |
| 12 | Corrupt WAL handling | Storage/WAL | RECOVERY NON-INVENTION | direct on-disk byte manipulation | LD-5/6 |
| 13 | Snapshot installation interrupted | Snapshot | SNAPSHOT SAFETY | crash mid-install (real or simulated) | SN-3 |
| 14 | Stale records around snapshot boundary | Snapshot/Raft | SNAPSHOT SAFETY, RAFT LOG MATCHING | held-then-delivered stale `AppendEntries` at boundary | `TestStaleAppendEntriesExactlyAtSnapshotBoundaryIsAccepted` |
| 15 | Compaction / rejoin | Snapshot/WAL | LOG COMPACTION SAFETY | repeated compact/restart cycles | SN-6, `TestRepeatedSnapshotCompactRestartCycleKeepsIndexesCorrect` |
| 16 | Transaction write/write conflicts | MVCC | CONFLICT CORRECTNESS | concurrent conflicting writers | TX-5 |
| 17 | Snapshot visibility (MVCC, not Raft snapshot) | MVCC | MVCC VISIBILITY | property test / randomized `StartSeq` | TX-6 |
| 18 | Tombstones | MVCC | MVCC VISIBILITY | delete then read at various `StartSeq` | TX-7 |
| 19 | Atomic multi-key behavior | Transactions | ATOMICITY | concurrent reader during multi-key commit | TX-3 |
| 20 | SQL path through real transaction machinery | SQL | CONFLICT CORRECTNESS, ISOLATION TRUTHFULNESS | SQL write-skew demo, distributed SQL tests | SQ-7, SQ-8 |

Combining rows (e.g. row 5 + row 13: asymmetric partition during a
snapshot install) is explicitly encouraged, not just tolerated — single-
fault schedules are the easy case; ChronicleDB's own hardest-won bugs
(`testing-strategy.md` §7, §8.1) came from combinations.

## 8. Build, test, and deterministic reproduction

Everything below is a real command that runs against this exact
repository — no external dependencies beyond the Go toolchain
([`dependencies.md`](dependencies.md)).

### 8.1 Build and run the fast test suite (under a minute)

```bash
go build ./...
go test ./...
```

### 8.2 Race detector and the real-process integration suite

```bash
go test ./... -race
go build -tags integration ./...
go test -tags=integration ./cmd/chronicledb-node/... -race -count=1
```

### 8.3 Drive the deterministic Raft-core chaos simulator

`internal/fault`'s deterministic simulator (`testing-strategy.md` §3)
is the cheapest-per-iteration layer — every run is fully determined by
its seed, so a failure is always reproducible:

```bash
CHRONICLEDB_CHAOS_SEEDS=5000 go test ./internal/fault/... -run TestChaos -timeout 300s
```

Reproduce one specific failing seed (printed by the failing test):

```bash
go test ./internal/fault/... -run 'TestChaos_AsymmetricPartitionSafety/seed=609'
```

Targeted Raft-core/snapshot-boundary adversarial scenarios
(`adversarial-testing.md` §4):

```bash
CHRONICLEDB_CHAOS_SEEDS=2000 go test ./internal/fault/... -run 'TestChaos_StaleAppendEntriesAfterSnapshotInstall|TestChaos_RepeatedCompactRestartCycle|TestChaos_LeaderChangeImmediatelyAfterSnapshot' -race -timeout 300s
```

### 8.4 Drive the model-based (independent-oracle) suites

Each of these checks real ChronicleDB behavior against
`internal/oracle` — a reference model that reimplements none of
ChronicleDB's own MVCC/FSM/Raft logic
([`adversarial-testing.md`](adversarial-testing.md) §2-3):

```bash
CHRONICLEDB_ADVERSARIAL_SEEDS=200 go test ./internal/node/... -run TestModel_AdversarialHistoryAgainstIndependentOracle -race -timeout 300s
CHRONICLEDB_ADVERSARIAL_SEEDS=3000 go test ./internal/sql/... -run TestModel_SQLAdversarialHistoryAgainstIndependentModel -race -timeout 300s
CHRONICLEDB_ADVERSARIAL_SEEDS=2000 go test ./internal/txn/... -run TestModel_SIHistoryRandomizedAgainstIndependentModel -race -timeout 300s
```

You can write a *new* `TestChaos_*`/model-based test against this same
harness to explore a scenario not yet covered — this is the expected
way to attack row 6/8's message-level classes further, without touching
any production code.

### 8.5 Fuzz the untrusted-input boundaries

Every decoder that touches disk/network/SQL bytes is fuzz-tested
(`failure-model.md` §6). Example (the snapshot decoder,
`adversarial-testing.md` §11):

```bash
go test ./internal/snapshot/... -fuzz FuzzDecode -fuzztime 30s
```

The same pattern applies to `internal/wal` (frame decoder),
`internal/fsm` (`FuzzDecodeCommitTxn`), `internal/raft` (two fuzz
targets), `internal/transport`, and `internal/sql`
(`FuzzParse`/`FuzzDecodeSchema`/`FuzzDecodeRow`) — run
`go test ./<package>/... -list 'Fuzz.*'` in any of these to see the
exact target names.

### 8.6 Drive a real cluster

```bash
./scripts/demo-local-cluster.sh   # scripted: real 3-node cluster, one command
```

For manual, step-by-step multi-process startup and the complete flag
reference, see [`configuration.md`](configuration.md) and
[`quickstart.md`](quickstart.md).

Against a running real cluster (`cmd/chronicledb-node`), inject and
heal a genuine network partition between real OS processes via the
`/fault` control-plane endpoint (`main.go`'s `handleFault`):

```bash
curl -X POST "http://<node-http-addr>/fault?action=block&peer=<peer-id>"
curl -X POST "http://<node-http-addr>/fault?action=unblock&peer=<peer-id>"
# directional-only (asymmetric) variants: blocksend / unblocksend / blockrecv / unblockrecv
```

`/propose`, `/status`, `/outcome`, `/metrics`, and `/health` are the
other real HTTP control-plane endpoints — see
[`observability.md`](observability.md) and [`configuration.md`](configuration.md).

### 8.7 Reproduction is not optional

Any report not accompanied by a reproduction — a seed + command, a
scripted sequence of `/propose`/`/fault` calls, or a new failing test —
will be asked for one before triage proceeds past initial reading. This
matches this repository's existing standard for outbound bug fixes
([`CONTRIBUTING.md`](../CONTRIBUTING.md) "How to add a regression test
for a bug fix": reproduce first, root-cause, then fix) applied to
inbound reports.

## 9. How to report a correctness finding

Use the **Correctness / safety bug** GitHub issue template
(`.github/ISSUE_TEMPLATE/correctness_bug.yml`) — the same template used
for any ordinary correctness report, extended with two optional fields
for Break-ChronicleDB submissions specifically:

- **Is this a response to the Break ChronicleDB challenge?** (checkbox)
- **Reviewer handle / affiliation (optional, for the findings log)** —
  leave blank to stay anonymous; the findings ledger (§12) will record
  "anonymous" if so.

The template already asks for the evidence a real correctness report
needs: commit/version, category of violation, expected-vs-actual cited
against [`invariants.md`](invariants.md), exact reproduction steps, a
seed if chaos/fuzz/adversarial-found, logs, deployment mode, and
environment. Fill in as much as you genuinely know — leave a field
blank rather than guess.

## 10. How to report a security finding

Unchanged from the existing project-wide policy
([`SECURITY.md`](../SECURITY.md)): a **suspected security
vulnerability** is reported via GitHub's private Security Advisory
process, **not** as a public issue. Decision aid, directly from
`SECURITY.md`'s own framing:

- Suspect a violation of a documented invariant (data loss, durability,
  wrong read, Raft safety, idempotency)? → the public
  **Correctness / safety bug** template (§9).
- Suspect the *lack* of authentication/TLS lets an untrusted network
  participant do something worse than what's already documented in
  `SECURITY.md` §Deployment assumptions (e.g. a way to corrupt another
  node's on-disk state, not just "can propose writes," which is already
  known/documented)? → private advisory.
- Not sure? → private advisory; a maintainer will redirect it to a
  public issue if it turns out not to be security-sensitive
  (`SECURITY.md` already documents this fallback path).

No bug-bounty program exists (`SECURITY.md` explicitly disclaims one);
reports are handled best-effort, with no SLA.

## 11. What happens after you report

1. A maintainer reproduces the report, or asks for a reproduction per
   §8.7 if one wasn't included.
2. It is classified per §12's schema — including, honestly, as
   **not-a-bug (working as documented)** if it matches §5.
3. If confirmed: a regression test is written proving the bug fails
   pre-fix and passes post-fix, mirroring
   [`CONTRIBUTING.md`](../CONTRIBUTING.md)'s existing rule for every bug
   fix in this repository.
4. Root cause and fix are documented, and the entry in
   [`docs/external-review-findings.md`](external-review-findings.md) is
   added/updated.
5. If applicable, a `CHANGELOG.md` entry and, for a release,
   [`releasing.md`](releasing.md)'s existing checklist.

There is no SLA (`SECURITY.md`'s "best-effort" precedent applies here
too) — an untriaged report is visible in the ledger, not silently
dropped.

## 12. Where findings are recorded

Every report received — confirmed or not — is recorded in
[`docs/external-review-findings.md`](external-review-findings.md)
against a fixed schema (reviewer, commit/environment, reproduction,
expected invariant, observed result, classification, confirmation
status, root cause, regression test, fix commit, release). As of this
writing that ledger has **zero entries** — no external review has
occurred yet. It will be updated honestly regardless of outcome,
including if the honest outcome is "no reports received."
