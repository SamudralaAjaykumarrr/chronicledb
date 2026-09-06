# Observability

Status: Phase 9. This document describes the diagnostic state
`docs/roadmap.md` §Observability specified in advance (Phase 0) and
this phase actually implements: metrics, node status, health, logging,
and the optional HTTP endpoint. Every mechanism here is read-only
diagnostic state — **nothing in ChronicleDB's commit, replication, or
recovery path ever depends on it** (docs/roadmap.md: "a correct
decision must never depend on whether a metric was recorded"). This is
enforced by construction, not just convention: every metric increment
in this phase's diff is a pure side effect added next to an existing,
already-decided outcome — never a value any `if` branch reads back.

## 1. Packages

- **`internal/metrics`** — two dependency-free, `sync/atomic`-backed
  primitives: `Counter` (monotonic) and `Gauge` (up/down). No external
  metrics library (docs/roadmap.md: "do not build Phase-9 observability
  around external SaaS products").
- **`internal/node`** (`metrics.go`) — a `Node`'s counters
  (`Metrics` struct) and a safe-to-read-from-any-goroutine snapshot
  accessor, `Node.Metrics() MetricsSnapshot`, mirroring `Node.Status()`'s
  existing pattern.
- **`internal/txn`** (`txn.go`) — a `Manager`'s counters (`Manager.Metrics`,
  a `txn.Metrics` value), for the standalone (pre-Raft, and SQL
  standalone-mode) commit path.
- **`internal/benchutil`** — a latency-percentile recorder used by
  benchmarks (`docs/benchmarks.md`), not by production code; listed
  here only because it is part of this phase's diagnostic tooling.
- **`cmd/chronicledb-node`** — `/metrics` (Prometheus text exposition
  format) and `/health` (JSON) HTTP endpoints, alongside the
  pre-existing `/status` (`docs/roadmap.md` §Optional metrics
  endpoint).

## 2. `internal/node.Node`'s counters

| Field (`MetricsSnapshot`) | Metric name (`/metrics`) | What it counts |
|---|---|---|
| `ElectionsTotal` | `chronicledb_raft_elections_total` | Election timeouts this node's core actually acted on (excludes a no-op timeout while already Leader). |
| `LeaderChangesTotal` | `chronicledb_raft_leader_changes_total` | Times this node became Leader. |
| `RaftMessagesSentTotal` | `chronicledb_raft_messages_sent_total` | Outbound Raft protocol messages. |
| `RaftMessagesReceivedTotal` | `chronicledb_raft_messages_received_total` | Inbound Raft protocol messages. |
| `ProposalsTotal` | `chronicledb_proposals_total` | Client mutations accepted as leader and handed to Raft (excludes upfront not-leader rejections). |
| `ProposalsRejectedTotal` | `chronicledb_proposals_rejected_total` | Proposals rejected outright because this node was not leader (before or after `raft.Core` itself confirms it). |
| `ProposalsCommittedTotal` | `chronicledb_proposals_committed_total` | Accepted proposals whose terminal outcome was `COMMITTED`. |
| `ProposalsAbortedTotal` | `chronicledb_proposals_aborted_total` | Accepted proposals whose terminal outcome was `ABORTED` (a Snapshot Isolation write-write conflict, `docs/mvcc.md` §4) — this is the replicated-mode transaction-conflict counter. |
| `ProposalsUnknownTotal` | `chronicledb_proposals_unknown_total` | Accepted proposals that never reached a terminal outcome on this node: leadership lost mid-flight, superseded by divergent-suffix repair, or the node stopped (`docs/transactions.md` §8's "uncertain outcome" case — resolved only by a client's own `RequestID` retry, never a false negative). |
| `RequestIDDuplicatesTotal` | `chronicledb_requestid_duplicates_total` | `Propose` calls resolved as a known-`RequestID` retry via `Precheck`, without a fresh Raft round (`docs/transactions.md` §6). |
| `SnapshotsCreatedTotal` | `chronicledb_snapshots_created_total` | Local snapshots this node has created (and compacted its log against). |
| `SnapshotsInstalledTotal` | `chronicledb_snapshots_installed_total` | Peer-provided snapshots this node has installed (and that actually advanced its state). |

`Status()`'s existing fields (`ID`, `Role`, `Term`, `Leader`,
`CommitIndex`, `AppliedIndex`, `LastIndex`, `SnapshotIndex`) already
cover `docs/roadmap.md` §Observability's node/Raft-health bullets and
are unchanged by this phase — `/metrics` re-exposes them as gauges
(`chronicledb_raft_role`, `chronicledb_raft_term`,
`chronicledb_raft_commit_index`, `chronicledb_raft_applied_index`,
`chronicledb_raft_last_log_index`, `chronicledb_raft_snapshot_index`)
purely for a single scrapable text endpoint, not a new source of truth.

### 2.1 What is intentionally not exposed

- **WAL bytes on disk.** `docs/roadmap.md` §Observability says "where
  practical" — computing an exact total requires re-stat-ing every
  retained segment file, which this phase judged not worth adding as a
  hot-path-adjacent operation. `LastIndex - SnapshotIndex` (both
  already in `Status()`) gives the retained *entry count*, a reasonable
  proxy, without that cost.
- **Replication lag per follower (`matchIndex` vs. leader's log
  length).** `raft.Core.MatchIndexOf` already exists (Phase 4) but is
  not currently surfaced through `Node`'s own API to an external
  caller; a future phase can add a `Node.MatchIndexOf` passthrough if a
  concrete operational need arises — deliberately deferred rather than
  added speculatively (`docs/roadmap.md`: "do not create vanity
  metrics").
- **A cluster-wide "quorum available" boolean.** See §4.

## 3. `internal/txn.Manager`'s counters

| Field | What it counts |
|---|---|
| `TxnConflictsTotal` | Commit attempts that resolved to `StatusAborted` via the write-write conflict rule — the standalone-mode transaction-conflict counter (mirrors `Node.ProposalsAbortedTotal` for replicated mode). |
| `RequestIDDuplicatesTotal` | Commit attempts whose `RequestID` was already known (resolved by `Precheck` without a fresh WAL append). |

Both are incremented exactly once per genuine event, not once per
retry of an already-resolved event — see
`internal/txn/metrics_test.go::TestMetricsTxnConflictsCounted` for the
proof: retrying an already-conflicted `RequestID` increments
`RequestIDDuplicatesTotal` again (it is a fresh `Precheck` hit) but
does **not** increment `TxnConflictsTotal` again (the conflict itself
was decided once, at the original attempt).

## 4. Health

`GET /health` (JSON) reports:

```json
{
  "alive": true,
  "nodeStarted": true,
  "raftInitialized": true,
  "storageOpened": true,
  "role": "Leader",
  "leaderKnown": true,
  "leader": "n1",
  "note": "quorum availability is not reported: ..."
}
```

`alive`/`nodeStarted`/`raftInitialized`/`storageOpened` are
unconditionally `true` once this handler is reachable at all — a
process that could not open its durable log or construct its
`raft.Core` never finishes `node.Open` and never starts the HTTP
server (`cmd/chronicledb-node/main.go`), so these four fields exist for
API completeness/documentation clarity, not because any of them can
ever independently be `false` while the endpoint responds. The value in
this endpoint is `role`/`leaderKnown`/`leader`.

**Deliberately absent: a `quorumAvailable` boolean.** A Follower or
Candidate cannot reliably know whether a majority of the cluster is
currently reachable — it only knows whether *it itself* can currently
reach a leader. A Leader knows it had a majority as of its own last
successful heartbeat round, which can already be stale by the time a
client reads this endpoint. Reporting either case as a flat
`true`/`false` would be exactly the overclaim `docs/roadmap.md`'s brief
prohibits ("do not claim quorum availability if the implementation
cannot reliably know it") — so this endpoint omits the field entirely
rather than publish a heuristic disguised as a fact. See
`cmd/chronicledb-node/control_test.go::TestControlServerHealthNeverClaimsQuorumForFollower`.

## 5. `/metrics` endpoint

`GET /metrics` returns Prometheus' text exposition format (`# HELP` /
`# TYPE` comments plus one `name value` line per metric) — chosen
because it costs nothing beyond `fmt.Fprintf` (no client library
dependency) while remaining scrapable by that ecosystem's tooling if a
deployment already has it (docs/roadmap.md: "a stable text/JSON
endpoint is acceptable unless docs explicitly require Prometheus
format" — Prometheus' *format* costs nothing extra here, so this phase
used it rather than inventing a bespoke text layout). See §2's table
for the full metric list; `cmd/chronicledb-node/main.go`'s
`handleMetrics` is the single source of truth for exact names/help
text.

## 6. Logging

Unchanged from Phase 5-8's existing `*log.Logger`-based diagnostics
(`Node.logf`, gated on `Config.Logger` being non-nil, never a
correctness dependency) — Phase 9 added no new logging mechanism. The
existing important events already logged (node start, leadership
transition, fatal local errors, snapshot creation) satisfy this phase's
brief's logging list; Phase 9 did not find a gap worth a new log line
that metrics/status did not already cover better (a counter is more
useful than a log line for "how many elections happened," for
example).

## 7. Restart semantics

**Every counter in this document is in-memory only and resets to zero
on every process restart.** Metrics are not part of the durable
recovery model (`docs/recovery.md`) and never will be — persisting them
would require either (a) writing to the WAL, which would pollute the
durable command history with non-command records solely for
observability (an explicit non-goal — `docs/architecture.md` §5's
"internal/wal ... must not know about ... Raft semantics" spirit
extended to "must not know about metrics" either), or (b) a second,
independent persistence mechanism, reintroducing exactly the "three
unrelated logs" failure mode `docs/architecture.md` §2 explains
ChronicleDB avoids by design. A dashboard/alerting system consuming
`/metrics` should treat a restart as a counter reset (rate-based
queries, e.g. Prometheus' own `rate()`, already handle this correctly;
a raw absolute-value comparison across a restart would not).

`Status()`'s fields (`Term`, `CommitIndex`, `AppliedIndex`,
`LastIndex`, `SnapshotIndex`) are the opposite: they are recomputed
from real durable/replicated state at every `Open` (`docs/recovery.md`)
and therefore *do* survive a restart with their correct value, not a
reset to zero — only the Phase 9 *counters* in §2-§3 reset.

## 8. Observability tests

Per `docs/roadmap.md` §Observability tests, the following are
implemented and passing (see each file for the exact assertions):

- `internal/metrics/metrics_test.go` — `Counter`/`Gauge` race-safety
  under concurrent writers and one concurrent reader.
- `internal/txn/metrics_test.go` — conflict/duplicate counters move on
  a real conflict/retry, do not double-count a retry of an
  already-resolved event, and are race-safe under concurrent distinct-
  key commits.
- `internal/node/metrics_test.go` —
  `TestMetricsLeaderChangeAndElectionsCounted` (elections/leader
  changes move on a real 3-node election),
  `TestMetricsProposalsCountedByOutcome` (committed/aborted/rejected/
  duplicate counters move on real, distinct events — not a no-op),
  `TestMetricsSnapshotCountersMoveOnRealSnapshot` (created/installed
  counters move on a real create-then-install cycle), and
  `TestMetricsRaceSafeConcurrentReadsAndProposals` (`-race`-clean
  concurrent `Metrics()`/`Status()` reads during live proposal
  traffic).
- `cmd/chronicledb-node/control_test.go` — `/metrics` exposes every
  documented metric name against a real (single-node) `node.Node`;
  `/health` never claims a fabricated quorum signal.

All of the above pass under `go test -race`.

## 9. Metric-design constraints honored

- No high-cardinality label ever appears: no `RequestID`, SQL text,
  arbitrary key name, or client identifier is ever used as a metric
  name or label (there are, in fact, no labels at all in this phase's
  design — every metric is a single scalar per node, which is
  sufficient at ChronicleDB V1's one-shard, static-three-node scope).
- No wall-clock value participates in any correctness decision;
  `internal/benchutil`'s use of `time.Now()` is confined to
  benchmark/test code, never production code.
- Every counter is a `metrics.Counter`/`metrics.Gauge` value embedded
  directly in its owner (`Node`, `Manager`) — never copied after first
  use, exactly like the `sync.Mutex` values already throughout this
  codebase.
