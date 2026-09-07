# Phase 12 Implementation Plan — External Review / "Break ChronicleDB" Challenge

Status: **planning only**. This document is the Phase 12 brief a future
implementation session executes; nothing in this document has been
implemented yet, no review has been invited, and no external finding
exists. Written against baseline commit `e13095d` (`docs: mark v0.1.0
release and open-source readiness`) — `main` clean, matching
`origin/main`, CI/tag/release workflows green, current maturity
`OPEN-SOURCE READY` (see [`roadmap.md`](roadmap.md) §Maturity Model).

This plan follows the same discipline every other phase in this
repository is held to (see [`vision.md`](vision.md) §Guiding
principle): no claim of external review, external validation, or a
higher maturity level is made anywhere in this document — only a plan
for how such a claim could later become true, evidenced.

There is no pre-existing "phase plan" document convention in this
repository (Phases 1-11 were executed directly against
[`roadmap.md`](roadmap.md)'s own phase sections); this document was
created per the task's explicit instruction to add one, at
`docs/phase-12-plan.md`, since none already existed to follow.

---

## 1. Objective

Make ChronicleDB genuinely reviewable by independent
database/storage/distributed-systems engineers, and build a disciplined
process for receiving, reproducing, classifying, fixing, and honestly
documenting externally discovered correctness/reliability problems —
without manufacturing adoption, fabricating reviewers or findings, or
claiming external validation before it has actually occurred.

Concretely, this phase's implementation work publishes:

1. A public reviewer guide (`docs/break-chronicledb.md`) that lets a
   Staff/Principal-level reviewer answer, from the guide alone plus
   linked existing docs, exactly the twelve questions in the Phase 12
   task brief (§2 of this plan reproduces them as the guide's own
   table of contents).
2. A correctness-reporting workflow built on the GitHub issue
   templates Phase 11 already shipped
   (`.github/ISSUE_TEMPLATE/correctness_bug.yml`), extended minimally
   for review provenance, not replaced.
3. A challenge/test matrix mapping named break-scenarios to the
   invariant catalog ([`invariants.md`](invariants.md)) and the exact
   existing deterministic reproduction tooling
   (`internal/fault`'s chaos suites, `internal/oracle`'s model-based
   suites, fuzz targets, `cmd/chronicledb-node`'s `/fault` endpoint) a
   reviewer can drive further, per
   [`testing-strategy.md`](testing-strategy.md) and
   [`adversarial-testing.md`](adversarial-testing.md) — not new
   mechanism.
4. An evidence ledger (`docs/external-review-findings.md`) — initially
   containing zero entries, in the same honest style Phase 10 reported
   "zero new production defects" rather than omitting the section —
   that records every report received against a fixed schema, whether
   confirmed or not.
5. A `roadmap.md`/`README.md`/`non-goals.md` update recording that
   Phase 12's review process is *open*, not that it has *concluded* or
   *found/not found* anything yet.

## 2. Non-goals

This phase must **not**:

- Modify database production code (`internal/*`, `cmd/*`) — Phase 12 is
  a review-and-response *process*, not a feature phase. Any actual bug
  fix arising from a real finding is separate follow-up work, done the
  normal way (fix + regression test + doc update + PR), after this
  plan's infrastructure exists and after a real report exists to fix.
- Modify existing tests or weaken any existing assertion, seed count,
  or documented invariant.
- Create a new git tag or publish a new GitHub Release.
- Claim, anywhere, that external review has occurred, that a
  Staff/Principal engineer has reviewed the project, that a bug bounty
  program exists, or that any specific number of reviewers/findings is
  expected or guaranteed.
- Fabricate reviewer names, GitHub issues, findings, stars, forks, or
  any other adoption/validation signal.
- Advance the maturity claim past what this phase's own actual
  completed work supports (see §17-18).
- Move SQL or deployment/infrastructure work ahead of the
  correctness-foundation phases (Phases 1-7) — `roadmap.md`'s Phase 12
  section states this is a standing constraint on the whole roadmap,
  not just a default ordering preference, and nothing in this plan
  proposes doing so.
- Begin a `v0.2.0` feature phase, an authentication/TLS implementation,
  or any other work that isn't the review infrastructure itself. (A
  confirmed finding may of course later motivate a fix in any
  subsystem — that fix is scoped to the finding, not an invitation to
  do general v0.2 feature work under Phase 12's name.)
- Open a public issue, PR comment, discussion, or any other
  externally-visible channel *inviting* reviewers as part of this
  planning session, or any future session executing this plan without
  the repository owner's explicit go-ahead — see §20 Step 7.

## 3. Current baseline (what Phase 12 builds on, unchanged)

Everything below is real, tested, and already true as of `e13095d` —
Phase 12 adds a review *process* on top, it does not need to (and must
not) re-prove any of this itself:

- Durable WAL + crash recovery (`internal/storage`, `internal/wal`) —
  [`storage.md`](storage.md), [`wal.md`](wal.md), [`recovery.md`](recovery.md).
- MVCC transactions under Snapshot Isolation, first-committer-wins
  conflict detection (`internal/mvcc`, `internal/txn`) —
  [`mvcc.md`](mvcc.md), [`transactions.md`](transactions.md).
- Deterministic FSM `Apply` boundary + durable `RequestID` idempotency
  (`internal/fsm`) — [`transactions.md`](transactions.md) §6-7,§10.
- Raft core + deterministic transport/clock/fault harness
  (`internal/raft`, `internal/fault`) — [`raft.md`](raft.md).
- Real replicated storage, quorum commits, leader failover
  (`internal/node`, `internal/transport`) — [`replication.md`](replication.md).
- Snapshots + log compaction (`internal/snapshot`) —
  [`snapshots.md`](snapshots.md).
- Partition/crash/restart chaos testing (`internal/fault`,
  `internal/node`, `cmd/chronicledb-node`) —
  [`testing-strategy.md`](testing-strategy.md) §6-7.
- Constrained SQL frontend (`internal/sql`) — [`sql.md`](sql.md).
- Observability + benchmarks (`internal/metrics`, `/metrics`,
  `/health`) — [`observability.md`](observability.md),
  [`benchmarks.md`](benchmarks.md).
- Adversarial/model-based correctness verification
  (`internal/oracle`) — [`adversarial-testing.md`](adversarial-testing.md).
- Open-source packaging + release automation, `v0.1.0` published —
  `LICENSE`, [`CONTRIBUTING.md`](../CONTRIBUTING.md),
  [`SECURITY.md`](../SECURITY.md), [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md),
  `.github/ISSUE_TEMPLATE/`, `.github/workflows/release.yml`.

Known, explicitly-documented limitations that must remain visible and
**unweakened** by anything Phase 12 writes (cross-checked against
[`non-goals.md`](non-goals.md), [`README.md`](../README.md) §Known
limitations, [`SECURITY.md`](../SECURITY.md)):

- Snapshot Isolation, not Serializable.
- Constrained SQL only (no joins/subqueries/secondary indexes/wire
  compatibility).
- One logical/static shard; static three-node-style V1 architecture.
- No authentication/TLS suitable for untrusted deployment.
- Synchronous snapshot fsync can delay the Raft event loop
  (`benchmarks.md` §8's measured latency spike; `snapshots.md`'s
  documented V1 limitation).
- Non-Linux release binaries are cross-compiled, not runtime-tested
  ([`support-matrix.md`](support-matrix.md)).
- No production-readiness claim; no external-validation claim before
  Phase 12 actually happens ([`non-goals.md`](non-goals.md) §Staff/
  Principal validation and equivalence claims).

## 4. Externally reviewable guarantees (what reviewers are being asked to try to break)

These are the claims this repository actually makes evidence for today
— the reviewer guide's "in scope" list is exactly this, no more:

| Invariant (`invariants.md`) | Plain statement | Primary evidence |
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

Also in scope: SQL-layer statement semantics as documented in
[`sql.md`](sql.md) (SQ-1..9), and the robustness properties in
[`failure-model.md`](failure-model.md) §6 (no panic on malformed
disk/network/SQL input, bounded allocation, checksum/version
enforcement) — these are correctness/robustness properties independent
of the deferred auth/TLS work, and are fair game to attack.

## 5. Explicit non-guarantees (what is NOT in scope to "break")

Restated from §3/§4 for the reviewer guide's own explicit "out of
scope" section — reporting one of these as a "bug" will be closed as
working-as-documented, not silently ignored, but also not treated as a
correctness finding:

- SERIALIZABLE isolation (only Snapshot Isolation is claimed —
  `mvcc.md` §1.1; the three-way write-skew ring in
  `si_history_test.go` is a *documented, intentional* SI-allowed
  outcome, not a bug).
- Any SQL feature `sql.md` §8 lists as not built (joins, subqueries,
  secondary indexes, `ORDER BY`/`LIMIT`, PostgreSQL wire protocol, a
  CLI, `NULL`/defaults).
- Sharding, multi-shard transactions, dynamic membership, cross-region
  replication (`non-goals.md`).
- Byzantine fault tolerance — Raft, and therefore ChronicleDB, assumes
  crash/partition faults, not malicious/lying nodes
  (`failure-model.md` §5).
- Simultaneous loss of a majority of nodes' persistent storage
  (standalone: the single node's storage) — outside any practical
  system's guarantee scope (`failure-model.md` §5).
- Security properties that require authentication/TLS ChronicleDB does
  not implement — e.g. "an unauthenticated client can propose writes"
  is expected, documented behavior (`SECURITY.md`), not a
  vulnerability, *unless* it demonstrates a correctness violation
  reachable even from a trusted client (which **is** in scope).
- Performance/throughput claims beyond what `benchmarks.md` actually
  measured — a slower-than-expected number is feedback, not a
  correctness bug, unless it reveals a genuine algorithmic defect
  (e.g. another O(n²) pattern like `benchmarks.md` §8.1's).
- Windows/macOS/non-amd64 runtime behavior (`support-matrix.md`:
  cross-compiled, not runtime-tested — a build failure there *is* in
  scope; a runtime behavior difference is a known, disclosed gap, not
  a surprise finding).

## 6. Reviewer personas

The reviewer guide is written to let each of these personas navigate
directly to what they'd test, without reading the whole documentation
tree cold:

1. **Distributed-systems/Raft reviewer** — targets election safety,
   log matching, leader completeness, quorum safety, network
   partitions, message reordering/duplication/delay. Entry points:
   `internal/fault` (deterministic simulator, cheapest to iterate on),
   `internal/raft/adversarial_test.go`, `raft.md`, `replication.md`.
2. **Storage/durability reviewer** — targets WAL framing, torn-tail/
   corruption handling, fsync ordering, snapshot atomicity, compaction
   safety. Entry points: `internal/wal`, `internal/storage`,
   `internal/snapshot`, `wal.md` §6, `snapshots.md`.
3. **Transactions/MVCC reviewer** — targets visibility, conflict
   detection, atomicity, tombstones, write skew. Entry points:
   `internal/mvcc`, `internal/txn`, `mvcc.md`, `si_history_test.go`.
4. **Idempotency/client-contract reviewer** — targets `RequestID`
   outcome stability across retries, failovers, restarts, snapshots.
   Entry points: `transactions.md` §6-7, `failure-model.md` §4.
5. **SQL/frontend reviewer** — targets the constrained SQL surface's
   documented statement semantics and its use of the real transaction
   machinery (never a shortcut around it). Entry points: `internal/sql`,
   `sql.md`, ADR-0013.
6. **Generalist "just try to break it" reviewer** — no specific
   subsystem; runs the chaos/adversarial suites at higher seed counts,
   drives `/fault` against a real cluster, tries unusual input via the
   SQL fuzz corpus, or otherwise free-forms. Entry point: §7's matrix
   and §8's reproduction commands, used as a menu rather than a
   checklist.
7. **Security reviewer** — routed to [`SECURITY.md`](../SECURITY.md)
   for private reporting rather than the public correctness workflow
   (§10).

## 7. Challenge / test matrix

Every row below is a named break-scenario from the task brief, mapped
to the invariant(s) it threatens, the subsystem, and the *existing*
deterministic reproduction surface a reviewer should drive (not a new
mechanism to build). "Existing coverage" cites the nearest proven test
so a reviewer can see exactly what's already checked and go looking for
what isn't.

| # | Scenario | Subsystem | Invariant(s) | Reproduce via | Existing coverage (nearest proof) |
|---|---|---|---|---|---|
| 1 | Leader crash before replication | Raft | DURABILITY (n/a — never acked) | `internal/fault` Crash action; `/fault` on a real cluster | RF-4 |
| 2 | Leader crash after replication (quorum, before reply) | Raft | LEADER COMPLETENESS, REQUEST OUTCOME STABILITY | `internal/fault`; `cmd/chronicledb-node` `-tags=integration` SIGKILL | RF-5/6 |
| 3 | Follower crash/restart | Raft/Recovery | DURABILITY | `internal/node/chaos_test.go`; real SIGKILL | RF-3 |
| 4 | Quorum loss (majority down) | Raft | QUORUM SAFETY, availability (not safety) | `internal/fault` isolate N-1 nodes | RF-11 |
| 5 | Asymmetric partitions | Raft transport | RAFT ELECTION SAFETY, liveness | `Transport.IsolateLink`/`BlockSend`/`BlockRecv` | `TestChaos_AsymmetricPartitionSafety` (found/fixed a real bug, §testing-strategy.md §7.1) |
| 6 | Delayed/duplicated messages | Raft transport | RAFT LOG MATCHING | `internal/fault` delay/duplicate | RF-7/8 (simulator-only, documented) |
| 7 | Stale Raft responses | Raft core | RAFT ELECTION SAFETY | `Cluster.Deliver`/`Transport.Take` held-message replay | `TestChaos_StaleAppendEntriesAfterSnapshotInstall` |
| 8 | Repeated elections | Raft | liveness, ELECTION SAFETY | `internal/fault` adversarial timer scheduling | RF-15, §2.8 failure-model |
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

A reviewer is explicitly encouraged to combine rows (e.g. row 5 + row
13: asymmetric partition during a snapshot install) — this is exactly
the style Phase 7/10's own most valuable findings came from (see
`testing-strategy.md` §7, `adversarial-testing.md` §4) and the matrix
says so directly rather than implying only one fault at a time is
interesting.

## 8. Deterministic reproduction strategy

The reviewer guide must make three things true, none of which require
new engineering — all already exist:

1. **Build/test in under a minute**: `go build ./...` && `go test
   ./...` (no external deps — `dependencies.md`).
2. **Drive the deterministic simulator directly**: point at
   `internal/fault`'s `Cluster` API and the exact
   `CHRONICLEDB_CHAOS_SEEDS=N go test ./internal/fault/... -run TestChaos
   -timeout 300s` pattern (`testing-strategy.md` §6.5) and the
   model-based `CHRONICLEDB_ADVERSARIAL_SEEDS` pattern
   (`adversarial-testing.md` §3) — a reviewer can write a *new*
   `TestChaos_*`/model-based test using the same harness without
   touching production code, and a failing run always prints its
   reproducing seed.
3. **Drive a real cluster**: `scripts/demo-local-cluster.sh` for the
   scripted path, `docs/configuration.md` for manual multi-process
   startup, and `cmd/chronicledb-node`'s `/fault` HTTP endpoint
   (`Block`/`Unblock`/`BlockSend`/`BlockRecv`/`UnblockSend`/`UnblockRecv`)
   for injecting a real partition against real processes, exactly as
   `cmd/chronicledb-node/chaos_test.go` already does.

The guide states plainly that **any report not accompanied by a
reproduction** (a seed + command, a scripted sequence of `/propose`/
`/fault` calls, or a new failing test) will be asked for one before
triage proceeds past initial reading — matching this repository's
existing standard (`CONTRIBUTING.md` "How to add a regression test for
a bug fix": reproduce first, root-cause, then fix) applied to inbound
reports instead of only outbound PRs.

## 9. Correctness-reporting workflow

Reuse, do not replace, Phase 11's existing
`.github/ISSUE_TEMPLATE/correctness_bug.yml` — it already asks for
commit/version, violation category (data loss / durability / incorrect
read / safety / liveness / idempotency / other), expected-vs-actual
cited against `invariants.md`, exact repro steps, seed (if
chaos/fuzz/adversarial-found), logs, and deployment mode. This already
covers requirement brief items 6-8 (how to report, what evidence is
needed) almost completely. Phase 12 extends it minimally rather than
duplicating a second template:

- Add one optional field: **"Is this a response to the Break
  ChronicleDB challenge?"** (checkbox) plus an optional freeform
  **"Reviewer handle / affiliation (optional, for the findings log —
  leave blank to stay anonymous)"** field, so a report can be linked
  into the evidence ledger (§14) without requiring identification.
- Add a `break-chronicledb` label (alongside the existing `bug`/
  `correctness` labels) so review-sourced reports are filterable
  separately from ordinary community-filed correctness bugs, without
  a separate workflow.
- No change to `.github/ISSUE_TEMPLATE/bug_report.yml`,
  `feature_request.yml`, or `config.yml` — an ordinary (non-correctness)
  bug found during review still goes through the existing bug-report
  path; the config's existing "Security vulnerability" contact link
  is unchanged.

Triage flow (documented in the reviewer guide, not a new tool):
report filed → maintainer reproduces (or asks for a reproduction per
§8) → classified per §10's taxonomy → if confirmed, a regression test
is written proving the bug fails pre-fix and passes post-fix (mirroring
`CONTRIBUTING.md`'s existing rule for all bug fixes) → root cause and
fix documented → entry added/updated in
`docs/external-review-findings.md` (§14) → if applicable, a follow-up
`CHANGELOG.md` entry and, for a release, `docs/releasing.md`'s existing
checklist.

## 10. Security-reporting boundary

Unchanged from Phase 11, restated prominently in the new reviewer
guide rather than re-specified: a **suspected security vulnerability**
(as opposed to a correctness bug reachable by a trusted client) is
reported via [`SECURITY.md`](../SECURITY.md)'s private GitHub Security
Advisory process, **not** as a public issue. The reviewer guide
includes a short decision aid, directly quoting `SECURITY.md`'s own
framing:

- Suspect a violation of a documented invariant (data loss, durability,
  wrong read, Raft safety, idempotency)? → `correctness_bug.yml`
  (public).
- Suspect the *lack* of authentication/TLS lets an untrusted network
  participant do something worse than what's already documented in
  `SECURITY.md` §Deployment assumptions (e.g. a way to corrupt another
  node's on-disk state, not just "can propose writes," which is
  already known/documented)? → private advisory.
- Not sure? → private advisory; a maintainer will redirect it to a
  public issue if it turns out not to be security-sensitive
  (`SECURITY.md` already documents this fallback path).

No new security-reporting mechanism is created; `SECURITY.md` is not
edited except, if needed, to add one cross-reference sentence pointing
to the new reviewer guide (informational only — no change to the
reporting process itself, no change to "no SLA / best-effort" language,
no claim of a bug-bounty *program* since `SECURITY.md` explicitly
disclaims operating one and this phase does not change that).

## 11. Documentation changes

New files:

- **`docs/break-chronicledb.md`** — the public reviewer guide. Sections
  (mirroring the task brief's twelve questions so a Staff/Principal
  reader can map guide→question 1:1): Scope, Threat/correctness model,
  Architecture at a glance (links to `architecture.md`, does not
  duplicate it), Guarantees in scope (§4 above), Explicit non-guarantees
  (§5), Quick build/test, Deterministic chaos entry points (§8),
  Reviewer personas as a "start here" table (§6), Challenge matrix (§7,
  reproduced or linked), How to report a correctness finding (§9), How
  to report a security finding (§10), What happens after you report
  (triage flow), Where to find the findings log.
- **`docs/external-review-findings.md`** — the evidence ledger (§14).
  Starts with zero entries and an explicit "no findings recorded yet;
  this section will be updated honestly regardless of outcome" line,
  matching Phase 10's "zero new production defects" precedent for
  reporting a null result plainly rather than omitting the section.

Updated files:

- **`docs/roadmap.md`** — Phase 12's section gets a status paragraph
  (not a "COMPLETE" marker — see §17) stating the review
  infrastructure has been published and the process is open, with the
  exact date and a link to `docs/break-chronicledb.md`. No maturity
  table row is edited beyond what's already there (`EXTERNAL-REVIEW
  READY` / `STAFF/PRINCIPAL DISCUSSION READY` are already correctly
  specified — see §17).
- **`README.md`** — one short paragraph after the existing Phase 11
  writeup, pointing to the new guide, using language that mirrors
  Phase 11's own "did not claim X" honesty pattern (e.g. "an external
  review process is now open; no external review has concluded, and no
  maturity claim beyond `OPEN-SOURCE READY` is made until it does").
- **`docs/README.md`** — add the two new docs to the documentation map
  (a new "External review (Phase 12)" subsection, consistent with the
  existing "Packaging and releases (Phase 11)" subsection's pattern).
- **`non-goals.md`** — no content change needed; its existing §Staff/
  Principal validation and equivalence claims entry already correctly
  anticipates this phase ("Revisit when: an actual external review
  occurs... even then, claims are scoped to what the review actually
  covered") and needs no edit, only continued adherence.
- **`CHANGELOG.md`** — an `[Unreleased]` entry noting the review
  infrastructure was added (process/docs only, no behavior change).
- **`CONTRIBUTING.md`** — one sentence in its existing "Reporting bugs"
  section pointing external reviewers specifically to
  `docs/break-chronicledb.md` in addition to the issue templates it
  already names.

## 12. GitHub metadata/template changes

- `.github/ISSUE_TEMPLATE/correctness_bug.yml` — add the two optional
  fields described in §9 (challenge-response checkbox, optional
  reviewer handle). No required-field changes; existing required
  fields (commit, category, expected-vs-actual, repro, cluster-mode)
  are unchanged.
- Add a `break-chronicledb` label to the repository's label set
  (documented in this plan; actual label creation is a `gh` CLI or web
  UI action taken by a maintainer at implementation time, not a
  file-based change this plan can commit).
- `.github/ISSUE_TEMPLATE/config.yml` — no change (the security contact
  link is already correct and sufficient).
- No new GitHub Discussions category, no new repository topic/badge
  claiming "externally reviewed," and no CODEOWNERS change are part of
  this plan — none are needed for the workflow above, and adding them
  would risk implying a status not yet earned.

## 13. Scripts / tooling needed

None beyond what already exists. Explicitly considered and rejected as
unnecessary for this phase:

- A new fuzzing/chaos harness — `internal/fault` and `internal/oracle`
  already provide everything §8 needs; a reviewer extends them with a
  new test function, not a new framework.
- A "reproduction bundle" generator/CLI — the existing seed+command
  convention (`testing-strategy.md` §6.5) is already sufficient and
  matches how every real bug in this repo's history has been reported
  and fixed (see `testing-strategy.md` §7, §8.1; `benchmarks.md` §8.1).
- A hosted/managed bug-bounty platform — `SECURITY.md` already
  explicitly disclaims running a bug bounty program; this phase does
  not change that.

If a specific reviewer's report later reveals a genuine gap in
reproduction tooling (e.g. a fault class the simulator can't express),
that is itself potentially a valid, separate finding to record in
`docs/external-review-findings.md` — not something to pre-build
speculatively now.

## 14. Evidence-recording format

`docs/external-review-findings.md` uses one table row per report,
matching the task brief's structure exactly:

| Field | Notes |
|---|---|
| Report ID | GitHub issue number (or, if a security advisory, "private advisory — see below" with only the resolution summarized publicly once appropriate, never the advisory's private content). |
| Date received | ISO date. |
| Reviewer / handle | As given, or "anonymous" if the reporter declined. |
| Environment / commit | Exact commit hash, OS/arch, Go version, deployment mode. |
| Deterministic seed or reproduction | Seed+command, or a description of the manual/real-cluster sequence. |
| Expected invariant | Cited from `invariants.md` (or `sql.md`/`failure-model.md` for non-catalog properties). |
| Observed result | What actually happened. |
| Classification | One of: safety / liveness / durability-recovery / isolation / idempotency / raft / snapshot-compaction / sql-frontend / observability-benchmark / security (routed away, not detailed here) / not-a-bug (working as documented). |
| Confirmed? | Yes / No / Not yet triaged. |
| Root cause (if confirmed) | One paragraph, matching the style of `testing-strategy.md` §7's existing bug write-ups. |
| Regression test | Test name + file, or "N/A — see reasoning" if genuinely not applicable (mirroring `testing-strategy.md` §7.2's precedent for when a dedicated test would add no coverage). |
| Fix commit | Hash, once merged. |
| Release containing fix | Version, once released, or "unreleased." |

A closing prose section restates the running honest tally (e.g.
"N reports received, M confirmed, K fixed" — starting at all zeros) —
mirroring `scenario-corpus.md`'s own closing summary-paragraph pattern
and `adversarial-testing.md`'s "Bugs found" section style, so the
number is never allowed to silently drift from the per-row detail
above (the same discipline `feedback_chronicledb_phase_completion_doc_updates`
already enforces for other status lines in this repository).

## 15. Acceptance criteria (for the infrastructure-publishing work itself)

The implementation session executing this plan is done with its own
scope when:

1. `docs/break-chronicledb.md` exists, answers all twelve brief
   questions, and every claim in it is traceable to an existing,
   currently-true doc/test (no forward-looking claim stated as
   present-tense fact).
2. `docs/external-review-findings.md` exists with the schema above and
   zero fabricated entries.
3. `.github/ISSUE_TEMPLATE/correctness_bug.yml` has the two new
   optional fields, is valid YAML, and does not weaken any existing
   required field.
4. `roadmap.md`, `README.md`, `docs/README.md`, `CHANGELOG.md`,
   `CONTRIBUTING.md` are updated per §11, with no maturity claim beyond
   "the process is open" (never "reviewed," "validated," or a
   maturity-level change).
5. Every existing "what this does not claim" statement
   (`non-goals.md`, `mvcc.md` §1.1, `sql.md` §8, `SECURITY.md`) is
   unchanged in substance.
6. All quality gates in §16 pass.
7. Exactly one commit (or a small, reviewable set, at the implementing
   session's discretion) is created, with no tag and no release.

This is **not** the same as "Phase 12 is complete" — see §17.

## 16. Quality gates

Documentation/metadata-only change, so the applicable gates are the
ones this repository already uses for doc-only phases (Phase 11's own
precedent), run against the diff:

```bash
git diff --check
grep -rn '<<<<<<<\|=======$\|>>>>>>>' --include='*.go' --include='*.md' --include='*.yml' .
gofmt -l .
go vet ./...
go build ./...
go test ./...
```

The last four confirm the doc/template-only diff did not accidentally
touch anything Go-compiled (it should not touch any `.go` file at all
in this phase's own scope — §22). If the issue-template YAML is
touched, additionally validate it parses as YAML (e.g. `python3 -c
"import yaml,sys; yaml.safe_load(open(sys.argv[1]))" .github/ISSUE_TEMPLATE/correctness_bug.yml`
or equivalent) before committing, since GitHub does not offer local
schema validation for issue forms.

A secret scan (`git diff` review for credentials, tokens, private
advisory content) is also required before commit, per this
repository's standing "no committed secrets" rule
(`failure-model.md` §6).

## 17. Phase 12 completion criterion

`roadmap.md` §Maturity Model already defines this precisely — it is
**not ambiguous** and this plan does not invent a rule:

> `EXTERNAL-REVIEW READY` — `OPEN-SOURCE READY` plus a specific,
> documented invitation/process for Phase 12 review is in place.
>
> `STAFF/PRINCIPAL DISCUSSION READY` — Phase 12 has actually occurred
> and its actual findings (not a prediction of findings) are
> documented and, where applicable, resolved.

Mapping this plan's work onto those two gates:

- **Publishing this plan's infrastructure** (the reviewer guide,
  challenge matrix, reporting workflow, evidence ledger, and the
  roadmap/README update stating the process is open) is exactly what
  satisfies `EXTERNAL-REVIEW READY`. This is real, checkable evidence
  (the documents exist and are accurate) — not a prediction.
- **Phase 12 itself being "COMPLETE (at this phase's own scope)"** —
  the same marker every other phase in `roadmap.md` uses — requires
  more than infrastructure: `roadmap.md`'s own Phase 12 brief frames
  the phase as "inviting outside engineers to attempt to find
  correctness violations," i.e. the review actually happening, not
  merely being invitable. Consistent with `STAFF/PRINCIPAL DISCUSSION
  READY`'s explicit requirement for *actual* findings (including a
  documented null result — "the review ran for period X, Y reports
  were received, Z confirmed" is a valid, honest completion, exactly
  as Phase 10 honestly reported zero new defects rather than treating
  that as failure or as something to hide).

**Proposed clarification** (flagged explicitly, since `roadmap.md`
does not itself state this two-step mapping in so many words, even
though it defines both endpoints precisely): this plan proposes that
`roadmap.md`'s Phase 12 section be split into two explicitly labeled
sub-milestones when the roadmap is next updated —
"Phase 12a — review infrastructure published (`EXTERNAL-REVIEW READY`)"
and "Phase 12b — review conducted, findings documented
(`STAFF/PRINCIPAL DISCUSSION READY`)" — so that a reader cannot
mistake completion of the infrastructure alone for completion of the
phase. This split is this plan's own proposal, not a rule already
written in `roadmap.md`; a future implementation session should apply
it only if it does not conflict with how a maintainer already intends
to word that section.

**This planning session does not mark either sub-milestone complete.**
Publishing the infrastructure is the next session's implementation
work; even that only achieves `EXTERNAL-REVIEW READY`, not
`STAFF/PRINCIPAL DISCUSSION READY` — no external review has occurred
as of this plan.

## 18. Maturity implications

- No maturity claim changes as a result of *this planning session* —
  current maturity remains `OPEN-SOURCE READY`.
- Once the infrastructure in this plan is actually implemented and
  published (a future session), `README.md`/`roadmap.md` may
  accurately state `EXTERNAL-REVIEW READY`, evidenced by the published
  guide/process — not before.
- `STAFF/PRINCIPAL DISCUSSION READY` may only be claimed once real
  external findings (confirmed or honestly-reported-as-none, over a
  stated review window) are documented in
  `docs/external-review-findings.md`. No word implying this
  (`externally validated`, `staff-reviewed`, `production proven`,
  `battle-tested`) may appear anywhere in the repository before that
  evidence exists, per `non-goals.md` §Staff/Principal validation and
  this repository's standing evidence-over-declaration principle.

## 19. Risks

- **No one reviews it.** A realistic, likely outcome for a
  from-scratch portfolio project with no existing audience. The plan
  treats this honestly: `EXTERNAL-REVIEW READY` only requires the
  *process* existing; it is not falsified by zero reviewers showing
  up, and `docs/external-review-findings.md` can honestly say "no
  reports received as of [date]" indefinitely without that being a
  defect in the documentation (unlike a false claim, which would be).
- **A report is filed but never fixed.** The evidence ledger's
  "Confirmed / Not yet triaged" states handle this — a report sitting
  untriaged is visible, not silently dropped, but this plan does not
  create an SLA (matching `SECURITY.md`'s own "best-effort, no SLA"
  precedent).
- **A "finding" is actually a documented non-guarantee (§5).** The
  classification taxonomy's explicit "not-a-bug (working as
  documented)" bucket exists specifically so this is recorded
  honestly (a real report, correctly triaged) rather than either
  silently closed with no trace or miscounted as a confirmed defect.
- **Scope creep**: a reviewer's finding tempts fixing unrelated things
  "while we're in there." §2's non-goals and the standing SQL/
  deployment-ordering constraint guard against this; any fix PR
  stemming from Phase 12 should be scoped to the specific confirmed
  finding, per this repository's existing focused-commit discipline
  (`CONTRIBUTING.md`).
- **Template friction**: adding fields to `correctness_bug.yml` could
  make it feel heavier for an ordinary (non-challenge) contributor
  filing a correctness bug. Mitigated by making both new fields
  explicitly optional (§9/§12).

## 20. Exact implementation sequence (for the session that executes this plan)

1. Re-read this plan and re-verify the baseline (git status clean,
   `main` == `origin/main`, CI green) has not changed since.
2. Write `docs/break-chronicledb.md` per §11's section list, drawing
   only from already-true facts in `roadmap.md`, `invariants.md`,
   `testing-strategy.md`, `adversarial-testing.md`, `failure-model.md`,
   `sql.md`, `SECURITY.md` — no new claims.
3. Write `docs/external-review-findings.md` with the §14 schema and
   zero entries.
4. Update `.github/ISSUE_TEMPLATE/correctness_bug.yml` per §9/§12.
5. Update `docs/roadmap.md`, `README.md`, `docs/README.md`,
   `CHANGELOG.md`, `CONTRIBUTING.md` per §11.
6. Run §16's quality gates; fix anything that fails.
7. **Pause for explicit maintainer confirmation before actually
   inviting anyone** — publishing the infrastructure (this commit) is
   a documentation change; *inviting* reviewers (a public
   announcement, a forum post, a tweet, opening a "Help wanted:
   external review" issue) is a separate, externally-visible action
   this plan does not authorize by itself, per this project's general
   rule that visible-to-others actions get confirmed, not assumed.
8. Commit (single commit, or a small reviewable set) with a message in
   this repository's existing style, e.g. `docs: publish external
   review / break-ChronicleDB infrastructure`.
9. Only after a real report arrives: follow §9's triage flow, add the
   ledger entry, and — if confirmed — fix, test, document, and update
   the ledger's "Confirmed"/"Fix commit" fields, per this repository's
   existing bug-fix discipline (`CONTRIBUTING.md`).

## 21. Expected files to add/change

Add:

- `docs/break-chronicledb.md`
- `docs/external-review-findings.md`
- `docs/phase-12-plan.md` (this document — added by the current
  planning session, not the future implementation session)

Change:

- `docs/roadmap.md` (Phase 12 section status paragraph only)
- `README.md` (one new paragraph)
- `docs/README.md` (documentation map: one new subsection)
- `CHANGELOG.md` (`[Unreleased]` entry)
- `CONTRIBUTING.md` (one sentence in "Reporting bugs")
- `.github/ISSUE_TEMPLATE/correctness_bug.yml` (two optional fields)

Not touched: any `.go` file, `go.mod`, any workflow file, `LICENSE`,
`CODE_OF_CONDUCT.md`, `SECURITY.md` (unless a single cross-reference
sentence is added, per §10), `docs/versioning.md`,
`docs/support-matrix.md`, `docs/releasing.md`, any ADR.

## 22. Things implementation must never do

- Never claim external review occurred, concluded, or produced a
  specific result before it actually did.
- Never fabricate a reviewer, report, issue, finding, star, fork, or
  any adoption/validation signal.
- Never weaken, remove, or soften an existing non-guarantee, known
  limitation, or "what this does not claim" statement.
- Never modify `internal/*` or `cmd/*` production code as part of
  publishing the review infrastructure itself.
- Never modify an existing test's assertions, seed counts, or scope in
  the course of this documentation work.
- Never create or push a git tag, or trigger `.github/workflows/release.yml`.
- Never advance the maturity claim past `EXTERNAL-REVIEW READY` without
  a real, documented review outcome (`STAFF/PRINCIPAL DISCUSSION
  READY`'s own gate).
- Never invite reviewers publicly without the explicit step in §20.7
  having actually happened.
- Never treat a report matching §5's explicit non-guarantees as a
  confirmed defect, and never silently discard it either — record it
  as "not-a-bug (working as documented)" in the ledger.
- Never move SQL or deployment/infrastructure work ahead of the
  correctness-foundation phases without a new ADR providing a strong,
  specific architectural reason, per `roadmap.md`'s standing
  constraint on this phase.
