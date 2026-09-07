# Roadmap and Maturity Model

Status: this document defines the phase sequence and the evidence
gates that govern when a maturity claim is allowed. Phases 1-11 are
complete (each at its own documented scope — see that phase's own
section below for exactly what it does and does not claim); current
maturity is `EXTERNAL-REVIEW READY` (§Maturity Model below) — Phases 8
and 9 together satisfy the underlying `PORTFOLIO READY` gate, with the
Authentication/TLS gap (`docs/non-goals.md` §Authentication and TLS)
explicitly, prominently documented as a deployment prerequisite rather
than resolved (the gate's own "resolved or explicitly, prominently
documented" wording permits either), Phase 11's packaging plus the
actual publication of the `v0.1.0` tagged release satisfy `OPEN-SOURCE
READY` on top of that, and Phase 12's published external-review
infrastructure (`docs/break-chronicledb.md`,
`docs/external-review-findings.md`) satisfies `EXTERNAL-REVIEW READY`
on top of that — **no external review has actually occurred yet**; see
that phase's own section below for the precise distinction between
"the process is open" and "the review has happened." Phase 10 added
deeper adversarial correctness evidence on top of the `PORTFOLIO READY`
gate — no new named maturity level was defined for it in §Maturity
Model below.

## Phase sequence

### Phase 0 — Architecture Foundation

Rigorous, internally consistent architecture documentation and ADRs
covering every system boundary listed in
[`docs/architecture.md`](architecture.md). No implementation. This is
the phase this repository is in as of this document.

### Phase 1 — Single-node durable storage + WAL + crash recovery — COMPLETE

`internal/storage`, `internal/wal`, and the recovery sequence in
[`docs/recovery.md`](recovery.md) (minus Raft-specific parts) are
implemented and tested against the scenarios in
[`docs/scenario-corpus.md`](scenario-corpus.md) §Local Durability
(LD-1 through LD-6), including a real subprocess kill-based crash test
(not merely a function-return simulation) and direct on-disk byte
manipulation to reproduce torn-tail and corruption scenarios. See
[`docs/storage.md`](storage.md) and [`docs/wal.md`](wal.md) for the
design as implemented.

### Phase 2 — MVCC + transactions + Snapshot Isolation — COMPLETE

`internal/mvcc` and `internal/txn` implement the visibility and
conflict rules in [`docs/mvcc.md`](mvcc.md) and
[`docs/transactions.md`](transactions.md), running on top of Phase 1's
durable log in standalone (Raft-less) mode. Tested and passing against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Transactions (TX-1
through TX-8), including: a property test checking MVCC visibility
against a reference model over randomized version chains; concurrent
(goroutine-level, race-detector-clean) non-conflicting and conflicting
writer tests; a real subprocess kill-based crash test proving a
committed transaction survives an ungraceful process kill; and restart-
recovery tests for committed single- and multi-key transactions,
aborted transactions, never-committed workspace state, tombstones, and
CommitSeq ordering, all via `internal/txn.Manager`'s real recovery path
(no in-memory shortcut). `internal/fsm` (the deterministic Apply
boundary) does not exist yet — Phase 2 plays that role directly inside
`internal/txn.Manager` for `CommitTxn` commands (docs/transactions.md
§9); factoring it out into `internal/fsm` is Phase 3 scope. Per
[`docs/roadmap.md`](roadmap.md) §Maturity Model, the `TRANSACTIONAL
ENGINE` maturity level formally requires Phase 3 (`RequestID`
idempotency) as well, which is explicitly out of scope for this phase
— see the maturity claim in [`README.md`](../README.md).

### Phase 3 — Deterministic state-machine boundary + `RequestID` idempotency — COMPLETE

`internal/fsm` is factored out as the deterministic `Apply` boundary
described in [`docs/architecture.md`](architecture.md) §5-6, and the
`RequestID` outcome table ([`docs/transactions.md`](transactions.md)
§6-7, §10) is implemented and durable — including detection of
mismatched-payload `RequestID` reuse and a `GetRequestOutcome` query,
both exercised by real disk-backed recovery, not just in-memory
shortcuts. Tested and passing against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Idempotency
(ID-1 through ID-7; the snapshot+compaction leg of ID-4 is proven as of
Phase 6), including deterministic-replay tests (two independently
constructed `internal/fsm.FSM` instances applying the identical
command history reach byte-identical outcomes) and restart-recovery
tests proving a conflicted `RequestID`'s `ABORTED` outcome — not just a
committed one — survives restart and is never re-evaluated on retry.

### Phase 4 — Raft state machine + deterministic transport/clock simulator — COMPLETE (at this phase's own scope)

`internal/raft` core (§1 of [`docs/raft.md`](raft.md); implementation
notes in §9 there) and `internal/fault`'s deterministic simulator
([`docs/testing-strategy.md`](testing-strategy.md) §3) are implemented
and proven against small in-simulator cluster scenarios — election
safety, vote safety (including across restart), log matching/divergent
suffix repair, the current-term commit rule, quorum safety under
partition, stale-leader step-down, and determinism/reproducibility —
per the Phase-4 subset of [`docs/scenario-corpus.md`](scenario-corpus.md)
§Raft/Replication (see that document's Phase 4 note for exactly what
"passing in the deterministic simulator" does and does not claim).
Production transport/disk are explicitly not wired in yet: `Storage` in
Phase 4 is `internal/fault.MemoryStorage`, not `internal/wal` (see
[`docs/raft.md`](raft.md) §9.4 for why — `internal/wal` does not yet
support the truncation Raft's log-matching repair needs). This phase
does **not** by itself advance the [`README.md`](../README.md) maturity
claim past `TRANSACTIONAL ENGINE` — per §Maturity Model below,
`REPLICATED PROTOTYPE` requires Phase 5 (a real multi-process
deployment) as well.

### Phase 5 — Real replicated storage + quorum commit + leader failover — COMPLETE (at this phase's own scope)

`internal/transport` (production TCP) and `internal/node` wire the
unchanged Phase 4 `internal/raft.Core` to a real `internal/wal`-backed
`raft.Storage` adapter (`internal/node.WALStorage`) and `internal/fsm`
in a real three-node deployment — both in-process against real
disk/sockets (`internal/node/node_test.go`) and via genuine separate OS
processes with real persistent data directories and a real `SIGKILL`
(`cmd/chronicledb-node`). `internal/wal` gained the suffix-truncation
capability Phase 4 identified as missing (`docs/wal.md` §10), making
divergent-suffix repair (`docs/raft.md` §3) durable for real, not just
in the deterministic simulator. The full client mutation path —
leader-only acceptance, `Propose` → Raft replication → quorum commit →
`internal/fsm.Apply` → `RequestID` outcome → response — is implemented
and proven, including the central Phase 5 acceptance scenario
(`RequestID` retry against a new leader after the original leader
crashes resolves to the identical, non-duplicated outcome) and the full
network-partition contract (`docs/replication.md` §5). `ReadIndex`
(`docs/replication.md` §4, ADR-0010) is also implemented
(`internal/node.Node.BeginReadIndex`) and proven safe under partition.
Tested against [`docs/scenario-corpus.md`](scenario-corpus.md)
§Raft/Replication (RF-1 through RF-15) — see that document's Phase 5
note for the precise, honest accounting of which of RF-1 through RF-15
were re-proven against real disk/network/processes versus remain
correctly simulator-only (RF-2, RF-7, RF-8, RF-14, RF-15 — deliberate
scope, not a gap; see [`docs/raft.md`](raft.md) §10.2). This phase does
**not** implement snapshots, log compaction, follower catch-up via
snapshot, dynamic membership, or a general client wire protocol
(`internal/protocol`) — those remain out of scope per the phase
boundary below and, where applicable, Phase 6.

### Phase 6 — Snapshots + restart recovery + log compaction — COMPLETE (at this phase's own scope)

`internal/snapshot` is implemented per
[`docs/snapshots.md`](snapshots.md): checksum/version-framed encoding
of `internal/fsm` state, crash-safe temp-file/fsync/atomic-rename
creation and load-with-fallback-on-corruption. `internal/node` wires
the driver-side lifecycle — restart restore (§Open, extending
[`docs/recovery.md`](recovery.md) §7 to start `commitIndex`/
`appliedIndex` at a restored snapshot's boundary instead of always 0),
live creation/compaction once durable log growth crosses
`Config.SnapshotThreshold`, and the `MsgInstallSnapshotRequest`/
`MsgInstallSnapshotResponse` wire protocol `internal/raft.Core` now
implements (a new `Core.Compact` for a node's own local compaction,
distinct from installing a peer's snapshot). `internal/wal` gained the
durable snapshot pointer and physical segment compaction
(`AppendMetadataSnapshot`/`CompactBefore`/`FirstIndex`, see
[`docs/wal.md`](wal.md) §11). Full recovery including snapshot-based
follower catch-up and log truncation is proven against
[`docs/scenario-corpus.md`](scenario-corpus.md) §Snapshots (SN-1
through SN-6) — see `internal/raft/snapshot_test.go`,
`internal/wal/snapshot_test.go`, and
`internal/node/node_test.go::TestSN1_RestartRestoresFromSnapshotAndCompactsLog`/
`TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction`. Per
§Maturity Model below, `STRONG DISTRIBUTED V1` additionally required
Phase 7 (chaos/combined fault schedules), now also complete — see that
phase's own section below.

### Phase 7 — Network partitions + crash lab + fault injection / chaos — COMPLETE (at this phase's own scope)

Combined, randomized fault schedules (partitions, crashes, disk
faults, message-level faults, all together, at scale, over many
iterations) are run via `internal/fault`, per
[`docs/testing-strategy.md`](testing-strategy.md) §3.3 "chaos tests."
This phase is about *breadth and adversarial combination* of scenarios
already individually proven in Phases 1-6, not new mechanism — and, per
that guiding principle, no new Raft/replication/snapshot mechanism was
added; the additions are: deterministic disk-fault injection and a
graceful (non-panicking) failure path in `internal/fault`, a
directional-only partition hook (`Transport.BlockSend`/`BlockRecv`) in
`internal/transport` needed to express an asymmetric partition against
real sockets (a purely symmetric `Block`/`Unblock` cannot), and a
minimal `/fault` control-plane endpoint in `cmd/chronicledb-node` to
drive that same hook against genuine separate OS processes. Seeded,
reproducible randomized property suites
(`internal/fault/chaos_test.go`) combine elections, proposals,
crashes/restarts, partitions/isolation/heals, message drop/duplicate/
delay, local log compaction, asymmetric partitions, and injected disk
faults, checked after every action against RAFT-ELECTION-SAFETY,
RAFT-LOG-MATCHING, and a simplified reference-model oracle for
COMMITTED-PREFIX-SAFETY; `internal/node/chaos_test.go` and
`cmd/chronicledb-node/chaos_test.go` (the latter `-tags=integration`,
genuine SIGKILL) prove the same combined-fault shapes end to end
against real disk/network/processes. This chaos work found and fixed
two genuine bugs (a Raft liveness bug where a node stepping down from
Leader/Candidate without granting the triggering vote/response could be
left with no election timer ever armed again, under an adversarial
asymmetric partition; and a WAL bug where installing a snapshot that
advances a node past a gap it never physically held any entries for did
not durably-equivalent-ly advance the WAL's own next-log-index counter,
fatally erroring the next live append) and one genuine data race (a
plain-pointer `internal/node.Node.fsmachine` field read/written from
different goroutines during snapshot install, fixed with
`atomic.Pointer`) — see
[`docs/testing-strategy.md`](testing-strategy.md) §7 for the full,
honest account of each, its regression test, and what remains
untested. See [`docs/scenario-corpus.md`](scenario-corpus.md)'s Phase 7
note for the precise chaos-variant accounting of RF-11/RF-13/RF-15 and
the SN-\* scenarios.

### Phase 8 — Constrained SQL using real transaction machinery — COMPLETE (at this phase's own scope)

The SQL surface defined in [`docs/non-goals.md`](non-goals.md) §SQL
surface (`CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`,
`BEGIN`/`COMMIT`/`ROLLBACK`, one primary key per table, a single
equality-on-primary-key predicate, three scalar types) is implemented
in `internal/sql` — a hand-written lexer/parser producing an explicit
typed AST, a binder that resolves and type-checks against a table's
real committed schema, and execution that flows exclusively through
`internal/txn.Manager` (standalone mode) or `internal/node.Node`
(replicated mode) via a small `Engine`/`Txn` seam
(`internal/sql/engine.go`) — never touching `internal/mvcc` or
`internal/storage` directly, per
[ADR-0013](adr/0013-sql-boundary-and-deferred-functionality.md). See
[`docs/sql.md`](sql.md) for the full grammar, data model, execution
semantics, and explicit compatibility boundaries.

Tested against [`docs/scenario-corpus.md`](scenario-corpus.md) §SQL
(SQ-1 through SQ-9), including: parser/lexer malformed-input and fuzz
coverage (`FuzzParse`, `FuzzDecodeSchema`, `FuzzDecodeRow`); every
documented error case for each of the five DML/DDL statements against
a real `internal/wal`-backed standalone engine; `RequestID` retry
safety for auto-commit mutations; explicit-transaction commit/rollback/
abort-on-statement-error semantics; a living Snapshot-Isolation
write-skew demonstration (no accidental SERIALIZABLE claim,
`docs/invariants.md` ISOLATION TRUTHFULNESS); schema-and-row survival
across a real WAL close/reopen restart; and, against a real three-node
`internal/node` cluster over genuine TCP/disk: SQL `INSERT` → Raft
commit → replicated state → `SELECT` returning the identical row on
every node, `RequestID` retry against a newly elected leader after the
original leader crashes, and SQL-visible state surviving a real
snapshot-create-and-install/compaction cycle plus a follower
crash/restart.

Building those real-cluster tests found and fixed one genuine,
previously-undiscovered Phase 5 liveness bug — not new SQL-layer
mechanism, but an existing mechanism (`internal/node.Node.BeginReadIndex`)
this phase's testing happened to newly exercise (every SQL statement
calls it, including the first one after a failover) — see
[ADR-0014](adr/0014-election-no-op-for-readindex-liveness.md) and
[`docs/replication.md`](replication.md) §4.3 for the fix
(`internal/node.Node.proposeElectionNoOp`) and its regression test.

Not built in this phase, per [`docs/sql.md`](sql.md) §8 and
[`docs/non-goals.md`](non-goals.md): joins, subqueries, views,
triggers, foreign keys, any predicate beyond primary-key equality,
aggregation, `ORDER BY`/`LIMIT`, `NULL`/defaults, secondary indexes, a
cost-based optimizer, PostgreSQL wire-protocol compatibility, or a SQL
CLI — the roadmap did not place a CLI requirement in this phase, so
`internal/sql` remains a library, consumed directly by tests, not a
client-facing tool.

### Phase 9 — Benchmarks + observability + performance engineering — COMPLETE (at this phase's own scope)

Diagnostic state (§Observability below, fully implemented in
[`docs/observability.md`](observability.md)) and benchmark targets
(§Performance Targets below, fully measured in
[`docs/benchmarks.md`](benchmarks.md)) are implemented and measured for
real, on real hardware, against real durable/replicated code paths —
never a mocked-persistence or fsync-disabled variant.

Microbenchmarks were added beside every layer named in this phase's
brief: `internal/wal` (append with/without the fsync durability
boundary, sequential append, replay at 100/1,000/10,000 entries),
`internal/mvcc` (point read at varying version-chain depth, write,
conflict check — no I/O), `internal/fsm` (deterministic `Apply` for
single-key/multi-key/duplicate-`RequestID` commands, command
encode/decode), `internal/txn` (the real, WAL-backed standalone commit
path, including the conflict path), `internal/snapshot` (encode/decode
at 100/1,000/10,000 keys), `internal/sql` (lexer/parser and
binding/planning cost kept explicitly separate from execution, per
this phase's own brief; every DML statement executed end-to-end
against a real standalone engine; a predicate-less full-table scan at
three row counts), and `internal/fault` (the Raft proposal/replication
path in the deterministic simulator, isolated from real I/O).
End-to-end/macro benchmarks were added in `internal/node`: a
single-node durable write, a three-node quorum-committed replicated
write (with a `internal/benchutil` p50/p95/p99/max latency-distribution
variant), a `ReadIndex`-backed primary-key read, an explicitly named
80% read / 20% write mixed workload, and node restart-recovery time
both from the full log and from a snapshot plus a retained log suffix.
A dedicated `TestSnapshotLatencyImpact` scripted experiment (not a
`-bench` benchmark, since it needs exact control over exactly when the
snapshot threshold is crossed) measured the documented Phase 6
synchronous-fsync-in-the-event-loop limitation directly: a real,
measured latency spike (baseline p99 ≈2.3ms vs. the snapshot-crossing
operation's max ≈6.4ms on the measurement machine) — reported honestly,
not hidden, and not "fixed" (the underlying design is unchanged;
`docs/snapshots.md`'s documented V1 limitation stands).

CPU/memory profiling (`go tool pprof`) of the SQL INSERT and
three-node replicated write paths found one genuine, evidence-backed
hotspot: `internal/wal.readFrame` read every remaining byte in a
segment (bounded only by the 64 MiB max record size, not by the actual
next frame's size) before decoding a single record, making every
segment scan — `Open`'s recovery scan, `Replay`, and `Truncate`'s
`locateLogIndex` — cost O(remaining bytes) per record, O(n²) total for
n records. At 10,000 128-byte entries this measured as ~1.4 seconds and
~7.3 GB allocated to replay ~1.3 MB of actual data. The fix (read the
fixed-size header first, then exactly the declared frame length,
falling back to the original behavior for any oversized/corrupt/torn
case) is a bounded, evidence-driven, non-semantic-changing change:
every torn-tail/corruption decision `decodeFrameBytes` makes is
byte-for-byte unchanged, verified by the complete pre-existing
`internal/wal` test suite (crash, corruption, truncation, and
`FuzzDecodeFrameBytes`) passing unchanged, plus the full repository
suite (`go test ./...`, `-race`, `-tags=integration`). Measured
improvement at 10,000 entries: ~273x latency, ~2,180x allocation — see
[`docs/benchmarks.md`](benchmarks.md) §8.1 for the full before/after
evidence. A second profiling pass (fsync's share of CPU time on both
the standalone SQL and replicated write paths) found no further
code-level hotspot — the dominant cost is the fsync durability boundary
itself, a correctness requirement, not a bug — and this phase
correctly did not force an optimization where profiling evidence did
not justify one (`docs/benchmarks.md` §8.2).

Observability (`internal/metrics`, `internal/node/metrics.go`,
`internal/txn`'s `Manager.Metrics`) adds counters for elections, leader
changes, Raft messages sent/received, proposals by outcome
(total/rejected/committed/aborted/unknown), `RequestID` duplicates,
and snapshots created/installed — every counter proven to move on a
real event (not a no-op) and race-safe under `-race`, and never
consulted by any correctness decision. `cmd/chronicledb-node` gained
`/metrics` (Prometheus text exposition format) and `/health` (JSON) —
the latter deliberately omits a cluster-wide "quorum available"
boolean, since a Follower/Candidate cannot reliably know that and a
Leader only knows it as of its own last heartbeat round; publishing a
heuristic disguised as a fact was rejected per this phase's own brief.
See [`docs/observability.md`](observability.md) for the complete
metric catalog, health/status API, restart-reset semantics, and test
evidence.

No optimization in this phase weakened any invariant in
[`docs/invariants.md`](invariants.md); the one optimization made
(§WAL replay, above) changed only I/O access pattern, not any
durability, ordering, or corruption-detection semantics, and needed no
new ADR since it reconsiders no documented trade-off — it closes an
unintentional performance gap in an existing one.
`cmd/chronicledb-bench` was not built: every benchmark this phase
needed was expressible as a standard Go benchmark or a small scripted
test against existing test infrastructure (see
[`docs/benchmarks.md`](benchmarks.md) §9 for the explicit reasoning and
the concrete trigger for revisiting that decision).

### Phase 10 — Deep adversarial correctness pass — COMPLETE (at this phase's own scope)

A dedicated verification pass specifically hunting for violations of
the invariant catalog, using the accumulated testing infrastructure
from Phases 1-9 combined with a new, structurally-independent
reference-model package (`internal/oracle`) that predicts committed
key/value state and verifies `RequestID` terminal-outcome stability
without reusing any ChronicleDB implementation code. No new product
functionality was added — per this phase's own brief, `internal/oracle`
is a test-only support package (like `internal/fault`), never imported
by production code.

Three new model-based history-testing suites drive real ChronicleDB
components (a real three-node disk/TCP cluster, a real standalone SQL
engine, real MVCC transactions) through long, deterministic, seeded
histories and check every outcome against the independent model:
`internal/node/model_test.go`, `internal/sql/model_test.go`, and
`internal/txn/si_history_test.go` (the last also adding five named,
deterministic tests documenting specific histories Snapshot Isolation
explicitly allows or forbids, extending TX-8 rather than repeating it).
Three new targeted Raft-core adversarial scenarios
(`internal/fault/adversarial_test.go`), two direct single-`Core` unit
tests for a previously-unit-untested-but-already-correct code path
(`internal/raft/adversarial_test.go`), one new cross-layer scenario not
covered by any prior phase — a follower catching up via a real snapshot
install, immediately followed by the leader crashing and that same
follower participating in the ensuing election
(`internal/node/crosslayer_test.go`) — a new WAL test proving *repeated*
(not merely single) snapshot/compact/restart cycles stay correct
(`internal/wal/adversarial_test.go`), and a new fuzz target for the
previously-unfuzzed snapshot decoder
(`internal/snapshot/fuzz_test.go`) round out the phase. Every new
suite's exact seed counts (validated locally at 200-3,000 seeds per
suite with `-race`, beyond each suite's smaller CI default) and
reproduction commands are itemized in
[`docs/adversarial-testing.md`](adversarial-testing.md), the phase's
own scenario-corpus-style honest accounting document — including its
explicit "Correctness boundaries" section (no SERIALIZABLE claim, no
broadened linearizability claim, no formal-verification claim) and its
"Bugs found" section, which reports honestly that this phase's testing
found **zero new ChronicleDB production defects** (Phases 1-9's own
chaos/benchmark work already found and fixed five genuine ones) while
finding and fixing two genuine defects in this phase's own new test
harness, each documented with its root cause and fix.

Per §Maturity Model below, no new named maturity level is defined for
Phase 10 — it strengthens the evidence behind the existing
`PORTFOLIO READY` claim rather than advancing to a new gate.

### Phase 11 — Open-source / portfolio packaging + releases — COMPLETE (at this phase's own scope)

No product functionality was added — per this phase's own brief,
Phase 11 is packaging, documentation polish for external readers, and
release infrastructure on top of the unchanged Phases 1-10 engine,
not new database mechanism. The deployment infrastructure
[`docs/non-goals.md`](non-goals.md) explicitly defers (Kubernetes
operator, etc.) was not pursued — nothing in this phase's brief
required it, and it remains deferred per that document's own trigger
condition.

Added: [`LICENSE`](../LICENSE) (Apache-2.0),
[`CONTRIBUTING.md`](../CONTRIBUTING.md),
[`SECURITY.md`](../SECURITY.md) (GitHub private vulnerability
reporting, documented deployment assumptions — no auth/TLS, matching
[`docs/non-goals.md`](non-goals.md) §Authentication and TLS),
[`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md) (Contributor Covenant
2.1), GitHub issue templates (bug report, correctness/safety bug,
feature request) and a pull request template
(`.github/ISSUE_TEMPLATE/`, `.github/pull_request_template.md`), and
[`.github/dependabot.yml`](../.github/dependabot.yml) (`gomod` and
`github-actions` ecosystems — see
[`docs/dependencies.md`](dependencies.md) for why `gomod` currently has
nothing to update).

A small `internal/version` package plus a `-version` flag on
`chronicledb-node` (`cmd/chronicledb-node/main.go`) report a
build-time-injected semantic version/commit/date, defaulting to
`dev`/`none`/`unknown` for a plain `go build` so a locally-built binary
is never mistaken for a tagged release — read by nothing on the
correctness path, per [`docs/observability.md`](observability.md)'s
existing rule that diagnostic state is never a correctness dependency.
`scripts/build-release.sh` reproduces the exact cross-compiled,
checksummed release archives
(`docs/support-matrix.md`'s five build targets: linux/amd64,
linux/arm64, darwin/amd64, darwin/arm64, windows/amd64) that
`.github/workflows/release.yml` builds — a workflow triggered
**only** by pushing a tag matching `v*.*.*` (never a branch push or
pull request), which re-runs the full quality-gate suite against the
exact tagged commit, then publishes a GitHub Release via the `gh` CLI
already present on GitHub-hosted runners (no third-party release
action). `scripts/demo-local-cluster.sh` starts a real local
three-node cluster (three genuine OS processes, real TCP, real disk)
end to end and was actually run to confirm leader election, a
replicated write, and cross-node status all work, printing cleanup
instructions rather than deleting anything itself. Two runnable
examples (`examples/basic-transaction`, `examples/sql-basics`) exercise
the real transaction engine and the real SQL frontend respectively,
standalone (no Raft) — both were actually run to produce the exact
output shown in [`docs/quickstart.md`](quickstart.md).

New reference documentation: [`docs/quickstart.md`](quickstart.md)
(every command actually executed against this repository),
[`docs/configuration.md`](configuration.md) (the complete
`chronicledb-node` flag reference — there is no config-file format,
deliberately, per [`docs/dependencies.md`](dependencies.md)),
[`docs/versioning.md`](versioning.md) (SemVer policy, what pre-1.0
compatibility means here, what counts as a breaking change),
[`docs/releasing.md`](releasing.md) (the release checklist),
[`docs/support-matrix.md`](support-matrix.md) (developed/tested vs.
cross-compiled-only platforms — Linux amd64 is the only platform
actually tested), and [`docs/dependencies.md`](dependencies.md) (the
zero-external-Go-dependency policy). [`docs/README.md`](README.md) was
reorganized into a topic-grouped map (previously a single flat reading
order last updated at Phase 0) and its stale "Architecture Foundation"
status line corrected. `README.md`'s own maturity statement and
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml)'s
`actions/checkout`/`actions/setup-go` versions were updated to their
current stable majors (`v7`, verified via the GitHub API at the time,
not guessed) with module caching explicitly disabled (there is no
`go.sum` to key it on).

Per §Maturity Model below, `OPEN-SOURCE READY` requires "license,
contribution docs, versioned release, no known unresolved correctness
gaps." This phase's own scope satisfied the first two (license,
contribution docs) and built the *capability* to cut a versioned
release; the fourth is unaffected by packaging work either way.
**This phase itself deliberately did not create or push a release
tag** — packaging infrastructure existing is not the same evidence as
an actual published release existing (this document's own
"a benchmark command existing does not prove performance" principle
applies identically here: a release *workflow* existing does not prove
a release). A maintainer subsequently completed
[`docs/releasing.md`](releasing.md)'s checklist and published the
first tag, `v0.1.0` (per [`docs/versioning.md`](versioning.md)), as a
separate, later action — see [`CHANGELOG.md`](../CHANGELOG.md) for what
that release contains. With that actual publication now in evidence,
the maturity claim advances to `OPEN-SOURCE READY`.

### Phase 12 — External review / "Break ChronicleDB" challenge

An explicit external-review phase: inviting outside
database/storage/distributed-systems engineers to attempt to find
correctness violations, with a bug-bounty-style "break it" framing.
Maturity claims about external validation (see §Maturity Model) are
only permitted based on the actual outcome of this phase, not before.

**Phase 12a — review infrastructure published.** As of this writing,
the review *process* is open and published, per
[`docs/phase-12-plan.md`](phase-12-plan.md): a public reviewer guide
([`docs/break-chronicledb.md`](break-chronicledb.md)) maps the
externally reviewable guarantees, explicit non-guarantees, reviewer
personas, and a 20-scenario challenge matrix onto the exact existing
deterministic reproduction tooling (`internal/fault`'s chaos suites,
`internal/oracle`'s model-based suites, fuzz targets,
`cmd/chronicledb-node`'s `/fault` endpoint) — no new mechanism was
built. The existing correctness-reporting workflow
(`.github/ISSUE_TEMPLATE/correctness_bug.yml`, shipped in Phase 11) was
extended, not replaced, with two optional fields linking a report to
this challenge. An evidence ledger
([`docs/external-review-findings.md`](external-review-findings.md))
was published starting with zero entries. Publishing this
infrastructure is exactly what satisfies `EXTERNAL-REVIEW READY` (see
§Maturity Model) — it is real, checkable evidence (the documents exist
and are accurate), not a prediction.

**Phase 12b — review conducted, findings documented — not yet
reached.** `EXTERNAL-REVIEW READY` is not the same as Phase 12 being
"COMPLETE" the way Phases 1-11 are marked above: that requires the
review to have actually happened — real reports (or an honestly
documented null result after a stated period) recorded in
[`docs/external-review-findings.md`](external-review-findings.md) —
which `STAFF/PRINCIPAL DISCUSSION READY` requires and which does not
exist as of this writing. No reviewer, finding, or review outcome is
claimed here; publishing the invitation is not the same as anyone
having accepted it.

**SQL and deployment work is not moved ahead of the correctness
foundations (Phases 1-7) without a new ADR providing a strong,
specific architectural reason** — this is a standing constraint on
this roadmap, not just a default ordering preference.

## Maturity Model

Maturity is determined by **evidence**, never by the mere existence of
files, commands, or infrastructure. The examples below are binding
interpretation guidance, not exhaustive:

- A WAL file existing does not prove durability — a passing
  crash-injection test against the documented durability contract
  does.
- `BEGIN`/`COMMIT` parsing does not prove transactions — passing tests
  against the MVCC visibility and conflict rules do.
- Leader election alone does not prove Raft safety — passing
  simulator tests against the full invariant catalog's Raft invariants
  do.
- Three processes running simultaneously does not prove correct
  replication — passing partition/crash/failover scenario tests do.
- A benchmark command existing does not prove performance — an actual
  measured, reported number does, and even then it proves only what
  was measured, not a general claim.
- Documentation claiming correctness does not prove correctness —
  only tests exercising the claimed property do.

| Maturity level | Evidence gate |
|---|---|
| `INITIALIZED` | Repository scaffolding exists; no architecture, no implementation. |
| `ARCHITECTURE FOUNDATION` | All documents/ADRs in [`docs/README.md`](README.md) exist, are internally consistent (see the cross-document consistency review performed for this phase), define every system boundary listed in [`docs/architecture.md`](architecture.md), and no database implementation has begun. |
| `SINGLE-NODE DURABLE ENGINE` | Phase 1 complete: `docs/scenario-corpus.md` §Local Durability scenarios pass, reproducibly, in CI. `internal/storage`/`internal/wal` implement and test LD-1 through LD-6 (see test files under those packages); a GitHub Actions workflow (`.github/workflows/ci.yml`) runs `go test -race ./...` on every push. |
| `TRANSACTIONAL ENGINE` | Phases 2-3 complete: §Transactions and §Idempotency (immediate/after-restart) scenarios pass. |
| `REPLICATED PROTOTYPE` | Phases 4-5 complete: §Raft/Replication scenarios pass in the deterministic simulator and in a real multi-process three-node deployment. See [`docs/scenario-corpus.md`](scenario-corpus.md)'s Phase 5 note for the specific per-scenario accounting: RF-1, RF-3 (log-catch-up leg), RF-4, RF-5, RF-6, RF-9 through RF-13 are proven against real disk/network/processes; RF-2, RF-7, RF-8, RF-14, and RF-15 remain proven only in the deterministic simulator, by deliberate, documented scope decisions ([`docs/raft.md`](raft.md) §10.2), not gaps in the wiring this gate is actually about. |
| `STRONG DISTRIBUTED V1` | Phases 6-7 complete: §Snapshots scenarios pass; chaos/combined fault schedules run for a meaningful duration without an invariant violation. **This repository's current state** — see [`docs/scenario-corpus.md`](scenario-corpus.md)'s Phase 7 note and [`docs/testing-strategy.md`](testing-strategy.md) §6-7 for the specific evidence: seeded randomized chaos suites at `internal/fault` (raft-core layer, tens of thousands of seeds run clean locally during this phase), real-disk/real-TCP chaos at `internal/node`, and genuine real-process SIGKILL chaos at `cmd/chronicledb-node`, plus the two genuine bugs and one data race this work found and fixed, each with a deterministic regression test. |
| `PORTFOLIO READY` | Phase 8-9 substantially complete: constrained SQL works end-to-end on the real engine; observability surfaces exist; auth/TLS gap from [`docs/non-goals.md`](non-goals.md) is resolved or explicitly, prominently documented as a deployment prerequisite. **This repository's current state** — Phase 8's real-cluster SQL evidence plus Phase 9's real, measured benchmarks ([`docs/benchmarks.md`](benchmarks.md)) and implemented/tested observability surfaces ([`docs/observability.md`](observability.md)); the Authentication/TLS gap remains explicitly, prominently documented ([`docs/non-goals.md`](non-goals.md) §Authentication and TLS) rather than resolved — this phase did not implement auth/TLS, which is out of Phase 9's own scope. |
| `OPEN-SOURCE READY` | Phase 11 packaging complete on top of `PORTFOLIO READY`: license, contribution docs, versioned release, no known unresolved correctness gaps. **This repository's current state** — `v0.1.0` is the actual published, tagged release (see [`CHANGELOG.md`](../CHANGELOG.md)); the Authentication/TLS gap remains explicitly, prominently documented as a deployment prerequisite, per the same gate wording that already permitted this at `PORTFOLIO READY`. |
| `EXTERNAL-REVIEW READY` | `OPEN-SOURCE READY` plus a specific, documented invitation/process for Phase 12 review is in place. **This repository's current state** — [`docs/break-chronicledb.md`](break-chronicledb.md) (the reviewer guide, guarantees/non-guarantees, challenge matrix, reproduction commands) and [`docs/external-review-findings.md`](external-review-findings.md) (the evidence ledger, currently zero entries) are published; the correctness-reporting workflow is extended accordingly. No external review has actually occurred yet — see `STAFF/PRINCIPAL DISCUSSION READY` below for what that would additionally require. |
| `STAFF/PRINCIPAL DISCUSSION READY` | Phase 12 has actually occurred and its actual findings (not a prediction of findings) are documented and, where applicable, resolved. **Not yet reached** — zero reports have been received as of this writing ([`docs/external-review-findings.md`](external-review-findings.md)); this level requires real findings or a real, honestly-documented null result over a stated review window, not merely the process existing. |

Advancing a maturity claim without its evidence gate is itself a
documentation defect and must be corrected on discovery — see
[`docs/vision.md`](vision.md) §Guiding principle.

## Observability (implemented in Phase 9)

Implemented — see [`docs/observability.md`](observability.md) for the
complete, current catalog. Diagnostic state ChronicleDB exposes for
operational visibility is never a correctness dependency (a correct
decision never depends on whether a metric was successfully recorded)
— summary of what Phase 9 actually built, against this section's
original (Phase 0) list:

- Node ID, role, current term, known leader — `Node.Status()`
  (unchanged since Phase 5), now also exposed via `/metrics` as gauges.
- `commitIndex`, `appliedIndex`, most recent snapshot index —
  `Node.Status()`. Durable log range: `LastIndex`/`SnapshotIndex`
  together give the retained entry count; exact on-disk WAL byte
  totals are not exposed (documented, deliberate — see
  `docs/observability.md` §2.1).
- Transaction conflict count — `Node.Metrics().ProposalsAbortedTotal`
  (replicated) / `txn.Manager.Metrics.TxnConflictsTotal` (standalone).
  Active transaction count is not exposed: ChronicleDB has no
  server-side open-transaction registry to count from (a `Txn`/SQL
  `Session` is owned entirely by its caller) — not built speculatively.
- `RequestID` duplicate-request count —
  `Node.Metrics().RequestIDDuplicatesTotal` /
  `txn.Manager.Metrics.RequestIDDuplicatesTotal`.
- Replication lag per follower — deferred; see
  `docs/observability.md` §2.1 for why and the concrete follow-up hook
  already available (`raft.Core.MatchIndexOf`) if a future phase needs
  it.
- Election count, leader-change history —
  `Node.Metrics().ElectionsTotal`/`LeaderChangesTotal`.
- Commit latency (observed) — `docs/benchmarks.md` §7's real, measured
  end-to-end latency distributions (p50/p95/p99/max), not a live
  per-request histogram in the running process (this phase judged a
  bounded histogram unnecessary beyond what benchmark-time measurement
  already provides at V1's scale — see `docs/observability.md` §9).

## Performance Targets (measured in Phase 9)

Measured — see [`docs/benchmarks.md`](benchmarks.md) for every actual
number, environment, and reproduction command. Summary against this
section's original (Phase 0) list:

- Standalone/replicated write latency, and the gap between them —
  `docs/benchmarks.md` §7.
- Read latency (leader-local strong read via `ReadIndex`) —
  `docs/benchmarks.md` §7.
- WAL append latency with and without the fsync durability boundary —
  `docs/benchmarks.md` §6.1.
- Recovery time as a function of log length, and from a snapshot plus
  retained suffix — `docs/benchmarks.md` §7, including the honest
  finding that the snapshot-plus-suffix path was not meaningfully
  faster than full-log replay at the measured (1,000-entry) scale.
- Snapshot creation/installation cost — `docs/benchmarks.md` §6.5
  (encode/decode) and §7 (`TestSnapshotLatencyImpact`'s measured
  latency-spike evidence for the documented synchronous-fsync
  limitation).
- CPU and allocation profiles for the hot commit path —
  `docs/benchmarks.md` §8, including the one genuine hotspot found and
  fixed (WAL replay, §8.1) and the profiling pass that correctly found
  no further code-level optimization justified (§8.2).
- **Not measured, honestly**: throughput under sustained concurrent
  multi-client load, and leader-failover-time-specifically-as-a-latency-
  number (Phase 7's chaos suites prove failover *works*; Phase 9 did
  not add a dedicated timer on top of that) — see
  `docs/benchmarks.md` §1 for the complete "not measured" list, each
  with its reason.

Any performance optimization proposed in or after Phase 9 must be
checked against [`docs/invariants.md`](invariants.md) before adoption;
an optimization that weakens a documented invariant requires a new ADR
explicitly reconsidering that invariant, not a silent trade-off. Phase
9's one optimization (WAL replay's I/O access pattern,
`docs/benchmarks.md` §8.1) needed no such ADR, since it changes no
durability/ordering/corruption-detection semantics — only how many
bytes are read from disk before an unchanged decode decision runs.
