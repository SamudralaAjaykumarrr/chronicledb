# Benchmarks

Status: Phase 9. Every number in this document was actually measured
by running the commands shown, on the machine described in
§Environment, on the commit that introduces this document (`git log -1
-- docs/benchmarks.md` on this branch identifies it exactly). Per
[`docs/roadmap.md`](roadmap.md) §Maturity Model, a benchmark command
existing does not prove performance — only an actual measured, reported
number does, and even then it proves only what was measured, not a
general claim. No number here is invented, extrapolated, or copied from
any other system.

## 1. What is measured, and what is not

Measured:

- Each durable-log operation in isolation (`internal/wal`): append
  with and without crossing the fsync durability boundary, replay.
- MVCC read/write/conflict-check cost, independent of any I/O
  (`internal/mvcc`).
- The deterministic `Apply` boundary, including the RequestID
  duplicate short-circuit (`internal/fsm`).
- The full standalone transaction commit path — real WAL, real fsync
  (`internal/txn`).
- State-machine snapshot encode/decode cost (`internal/snapshot`).
- The Raft proposal/replication path in the deterministic simulator,
  isolated from real I/O (`internal/fault`).
- SQL lexer/parser cost, isolated from execution; binding/planning
  cost; and each DML statement executed end-to-end against a real
  standalone engine (`internal/sql`).
- End-to-end client-to-durable-acknowledgement latency for a
  single-node write, a three-node quorum-committed write, a
  ReadIndex-backed primary-key read, a documented mixed workload, and
  node restart/recovery time (`internal/node`).
- The measured impact of synchronous, in-event-loop snapshot creation
  on commit latency (§7).

Not measured (honest limitations, not oversights):

- Multi-machine network latency: every "real network" benchmark here
  runs over TCP on `127.0.0.1` on one machine (docs/testing-strategy.md
  §4's same "real disk/real TCP within a single test process" scope as
  `internal/node`'s own correctness tests) — this measures the
  production code path's own overhead, not wide-area network latency.
- Disk hardware variation: all durability numbers reflect this one
  machine's filesystem/storage stack (§2), which for this measurement
  run is a WSL2 virtual disk, not bare-metal Linux — see §2's explicit
  caveat.
- Throughput under sustained concurrent multi-client load (every
  benchmark here is single-client, sequential — docs/roadmap.md's
  brief did not scope a concurrent-client load generator for Phase 9,
  and `cmd/chronicledb-bench` was not built — see §9).
- Leader failover time (election-to-service-restored) end-to-end —
  Phase 7's chaos suites prove failover *works*
  (docs/testing-strategy.md §6-7); Phase 9 did not add a dedicated
  failover-latency timer on top of that.
- Scaling behavior beyond the dataset sizes in §4 (no run larger than
  10,000 keys/entries was performed, per this phase's brief: "do not
  create benchmarks so huge that CI becomes impractical").

## 2. Environment

All numbers in this document were measured on:

- **Machine**: a single developer laptop running under WSL2 (Windows
  Subsystem for Linux) — **not** bare-metal Linux and **not** a cloud
  instance. Disk/scheduler behavior under WSL2's virtualized I/O path
  can differ from bare-metal Linux; do not generalize absolute numbers
  (especially fsync latency, §3) to a different environment.
- **CPU**: Intel(R) Core(TM) Ultra 7 258V, 8 logical CPUs visible to
  the guest.
- **Memory**: 15 GiB visible to the guest.
- **OS**: Ubuntu 24.04.4 LTS (WSL2 guest kernel
  `6.18.33.2-microsoft-standard-WSL2`).
- **Go**: `go version go1.26.5 linux/amd64`.
- **Date measured**: 2026-09-06.

Every command below can be re-run on any machine; only the specific
numbers reported in §5-§8 are specific to the environment above.

## 3. Durability mode

Every benchmark that crosses the WAL's durability boundary
(`internal/wal.WAL.Sync`, `docs/wal.md` §4) does a real `fsync(2)` per
call — there is no "relaxed"/batched durability mode in ChronicleDB V1
to select between (`docs/wal.md`: "Phase 1 makes no attempt at a
group-commit optimization ... every Sync call issues its own fsync").
Every end-to-end write benchmark in this document therefore reflects
the one durability mode ChronicleDB actually has, never a
fsync-disabled or mocked-persistence variant (explicitly prohibited by
`docs/roadmap.md`'s "do not cheat benchmarks").

## 4. Dataset / workload definitions

Exact, named sizes used throughout this document — never open-ended
"large scale" wording:

- **Small**: 100 keys/entries.
- **Medium**: 1,000 keys/entries.
- **Larger (local-development-friendly)**: 10,000 keys/entries.
- **WAL replay sizes**: 100 / 1,000 / 10,000 log entries, 128-byte
  payloads.
- **MVCC version-chain depths**: 1 / 10 / 100 / 1,000 / 10,000 versions
  of one key.
- **Mixed workload ratio**: a fixed, explicitly named **80% read / 20%
  write** mix (`BenchmarkMixedWorkload80Read20Write`) — every 5th
  operation is a quorum-committed write to a shared hot key, the other
  4 are `ReadIndex`-backed reads of that same key's latest value. No
  other ratio is used or implied anywhere else in this document.
- **Node counts**: 1 node (`BenchmarkSingleNodeDurableWrite`,
  `BenchmarkNodeRestartRecovery`) or 3 nodes (every other end-to-end
  `internal/node`/`internal/fault` benchmark) — ChronicleDB V1 has no
  other supported cluster size (`docs/architecture.md` §1).
- **Snapshot threshold**: package default (4096 entries) unless a
  benchmark name says otherwise; the snapshot-latency experiment (§7)
  uses a small threshold (20) specifically to make crossing it
  affordable inside a test.

## 5. Reproducing these results

Microbenchmarks, grouped by subsystem:

```bash
go test ./internal/wal/...      -run '^$' -bench . -benchmem
go test ./internal/mvcc/...     -run '^$' -bench . -benchmem
go test ./internal/fsm/...      -run '^$' -bench . -benchmem
go test ./internal/txn/...      -run '^$' -bench . -benchmem
go test ./internal/snapshot/... -run '^$' -bench . -benchmem
go test ./internal/sql/...      -run '^$' -bench . -benchmem
go test ./internal/fault/...    -run '^$' -bench . -benchmem
```

End-to-end/macro benchmarks (real TCP + real fsync; slower):

```bash
go test ./internal/node/... -run '^$' -bench . -benchmem
```

The snapshot-latency experiment (§7) is a scripted `Test`, not a
`-bench` benchmark (it needs exact control over when the snapshot
threshold is crossed — see its own doc comment):

```bash
go test ./internal/node/... -run TestSnapshotLatencyImpact -v
```

Everything above can also be run together (slower; useful before/after
comparison with `benchstat` if installed — not required, per this
phase's brief, "do not block Phase 9 solely because `benchstat` is
unavailable"):

```bash
go test ./... -run '^$' -bench . -benchmem -timeout 600s
```

CPU/memory profiling (used to produce §8's findings):

```bash
go test ./internal/sql/...  -run '^$' -bench BenchmarkSQLInsert -benchtime=2000x \
  -cpuprofile=/tmp/cpu.prof -memprofile=/tmp/mem.prof
go tool pprof -top -nodecount=15 /tmp/cpu.prof
go tool pprof -top -nodecount=15 -alloc_space /tmp/mem.prof
```

## 6. Microbenchmark results

All results below use `-benchmem`, default `go test` benchtime (≈1s
per case unless noted), 8 logical CPUs. `ns/op` is a per-b.N-loop
*average*, not a percentile — see §7 for real percentile data on the
end-to-end write path.

### 6.1 WAL (`internal/wal`)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| AppendNoSync, 64B payload | 360.4 | 96 | 1 |
| AppendNoSync, 256B payload | 512.1 | 288 | 1 |
| AppendNoSync, 4096B payload | 3,726 | 4,864 | 1 |
| AppendSync (fsync boundary), 64B | 999,326 | 96 | 1 |
| AppendSync (fsync boundary), 256B | 1,099,494 | 288 | 1 |
| AppendSync (fsync boundary), 4096B | 1,091,483 | 4,864 | 1 |
| AppendSequential (no per-op sync) | 507.8 | 288 | 1 |
| Replay, 100 entries | 57,716 | 34,787 | 320 |
| Replay, 1,000 entries | 549,841 | 337,719 | 3,020 |
| Replay, 10,000 entries | 5,193,065 | 3,366,543 | 30,025 |

**Crossing the fsync boundary costs roughly 1,000x an in-process
append** on this machine (≈0.4-3.7μs unsynced vs. ≈1.0-1.1ms synced) —
this is the real, measured cost of ChronicleDB's durability contract on
a WSL2 virtual disk (§2's caveat applies directly here: bare-metal
NVMe fsync latency is typically far lower). See §8.1 for a WAL replay
finding and fix.

### 6.2 MVCC (`internal/mvcc`, no I/O)

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Visible, chain depth=1 | 16.46 | 0 |
| Visible, chain depth=10 | 16.63 | 0 |
| Visible, chain depth=100 | 17.45 | 0 |
| Visible, chain depth=1,000 | 18.61 | 0 |
| Visible, chain depth=10,000 | 19.03 | 0 |
| ApplyCommit, 1 key | 493.8 | 4 |
| ApplyCommit, 4 keys | 2,306 | 17 |
| ApplyCommit, 16 keys | 8,852 | 65 |
| ApplyCommit, 64 keys | 27,702 | 257 |
| CheckConflicts, no conflict | 16.98 | 0 |
| CheckConflicts, conflict | 16.84 | 0 |

`Visible`'s near-flat latency across a 10,000x depth range confirms its
binary-search implementation is genuinely O(log depth), not O(depth) —
useful as a standing regression check.

### 6.3 FSM (`internal/fsm`, no I/O)

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Apply, single key | 1,216 | 8 |
| Apply, 4 keys | 2,549 | 20 |
| Apply, 16 keys | 8,332 | 68 |
| Apply, 64 keys | 31,410 | 260 |
| Apply, duplicate RequestID | 122.8 | 1 |
| Precheck, duplicate RequestID | 123.6 | 1 |
| EncodeCommitTxn | 63.59 | 2 |
| DecodeCommitTxn | 114.9 | 7 |

The duplicate-RequestID path (idempotent retry) is roughly 10x cheaper
than a fresh single-key `Apply`, entirely in-memory — exactly the
`docs/transactions.md` §6 "need not go through the log again" property.

### 6.4 Transaction commit (`internal/txn`, real WAL)

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Commit, single key | 904,887 | 14 |
| Commit, multi-key (1) | 928,322 | 14 |
| Commit, multi-key (4) | 959,702 | 28 |
| Commit, multi-key (16) | 955,859 | 80 |
| Commit, conflict path | 934,809 | 11 |

All five cluster tightly around ~0.9-1.0ms — dominated by the same
fsync cost §6.1 measured directly, not by mutation-set size (§8.2
confirms this with a CPU profile). Notably, the conflict path costs
essentially the same as a successful commit: a conflicting command is
still durably appended before `internal/fsm.Apply` evaluates and
rejects it (`docs/transactions.md` §9) — conflict detection is not a
cheap pre-check that avoids the durability cost.

### 6.5 Snapshot encode/decode (`internal/snapshot`)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Encode, 100 keys | 25,389 | 38,000 | 109 |
| Encode, 1,000 keys | 347,870 | 367,665 | 1,009 |
| Encode, 10,000 keys | 4,753,968 | 3,650,482 | 10,009 |
| Decode, 100 keys | 13,445 | 33,672 | 411 |
| Decode, 1,000 keys | 156,812 | 472,955 | 4,015 |
| Decode, 10,000 keys | 1,949,487 | 4,135,650 | 40,071 |

Both scale linearly with key count, as expected for a full
serialize/deserialize of every version chain (`docs/snapshots.md` §2).

### 6.6 SQL (`internal/sql`)

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Parse CREATE TABLE | 646.7 | 20 |
| Parse INSERT | 573.5 | 15 |
| Parse SELECT (primary-key predicate) | 436.7 | 12 |
| Bind INSERT (planInsert only) | 80.16 | 1 |
| INSERT (end-to-end, real engine) | 953,373 | 43 |
| PRIMARY KEY SELECT (end-to-end) | 1,050 | 30 |
| UPDATE (end-to-end) | 1,025,585 | 43 |
| DELETE (end-to-end) | 1,042,791 | 35 |
| Full table scan, 10 rows | 3,491 | 71 |
| Full table scan, 100 rows | 35,133 | 437 |
| Full table scan, 1,000 rows | 445,898 | 4,048 |

Parsing/binding is 1,000x+ cheaper than executing (fsync-bound, like
§6.4) — confirming `docs/roadmap.md`'s "do not combine parser cost with
database execution" separation is not just methodological caution but
reflects a real, large cost gap. The full table scan's near-linear
growth with row count is the measured cost of `docs/sql.md` §5.2's
documented limitation (`mvcc.Store.Export` scans every key in the
store, not just the queried table, on every predicate-less SELECT) —
an accepted V1 boundary, not a new finding.

### 6.7 Raft proposal/replication (`internal/fault`, deterministic simulator)

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Propose -> quorum replicate -> commit (3-node simulator) | 5,006 | 54 |

This isolates Raft's own logical replication/commit-rule cost from
real I/O (§6.1/§6.4 already measured that separately) — three orders of
magnitude cheaper than the real three-node write in §7, confirming that
gap is disk/network, not Raft's own logic.

## 7. End-to-end / macro benchmarks (`internal/node`)

All results below are real: real temp-dir-backed WAL, real TCP
transport on localhost, real fsync — the same infrastructure
`internal/node`'s own correctness tests use.

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Single-node durable write | 1,024,388 | 26 |
| Three-node replicated write | 1,715,287 | 1,386 |
| Primary-key read (ReadIndex) | 82,540 | 1,141 |
| Mixed workload (80% read / 20% write) | 390,544 | 1,186 |
| Node restart recovery, from log only (1,000 entries) | 1,166,740 | 7,151 |
| Node restart recovery, from snapshot + 50-entry suffix (base 1,000) | 1,310,770 | 10,595 |

Latency distribution for the three-node replicated write
(`BenchmarkThreeNodeReplicatedWriteLatencyDistribution`,
`internal/benchutil`, 200 samples):

| Percentile | Latency |
|---|---:|
| p50 | 1.74ms |
| p95 | 2.22ms |
| p99 | 2.37ms |
| max | 2.63ms |

Observations, honestly reported:

- A three-node replicated write costs ~1.7x a single-node write on
  this machine — the added cost of a real network round trip plus two
  more fsyncs (followers), not a large multiplier, because the
  dominant cost in both cases is fsync latency itself (§8.2).
  Reads (via `ReadIndex`) are ~20x cheaper than a write, since a
  successful read needs no durable append at all, only a fresh
  heartbeat round-trip proof of leadership (`docs/replication.md` §4).
- The mixed 80/20 workload's average (390μs/op) sits well below a pure
  write's cost, exactly as expected for a mostly-read workload.
- **The "restart from snapshot + short suffix" case here was *not*
  meaningfully faster than "restart from the full log"** (1.31ms vs.
  1.17ms) at these sizes. This is reported honestly rather than
  omitted: with §8.1's WAL replay fix in place, replaying 1,000 WAL
  entries and decoding a 1,000-key snapshot cost roughly the same order
  of magnitude on this machine — the snapshot's own decode cost
  (§6.5) is not free, and at only 1,000 keys/entries the two paths
  have not yet diverged the way they would at a much larger log length
  (where replay's now-linear-but-still-per-entry cost eventually
  exceeds one decode pass over a bounded snapshot). This is not a
  regression or a bug: it is a real, measured data point that the
  snapshot mechanism's recovery-time benefit is scale-dependent, not
  automatic at every size.

## 8. Profiling findings

### 8.1 WAL replay was O(n²) before this phase — found and fixed

**Workload**: `BenchmarkWALReplay`, `entries=10,000`, 128-byte payloads.

**Before** (pre-Phase-9 `internal/wal.readFrame`):

```
BenchmarkWALReplay/entries=100-8         21535     56263 ns/op      34794 B/op    320 allocs/op
BenchmarkWALReplay/entries=1000-8         2280    515420 ns/op     337687 B/op   3020 allocs/op
BenchmarkWALReplay/entries=10000-8         234 1417347154 ns/op 7344264219 B/op  30250 allocs/op
```

**Observed hotspot**: `internal/wal.readFrame` read *every remaining
byte in the current segment* (bounded only by
`MaxRecordPayloadSize` = 64 MiB, not by the actual next frame's size)
before decoding a single record, on every call. Scanning a segment
record-by-record (`Open`'s recovery scan, `Replay`, `Truncate`'s
`locateLogIndex`) therefore cost O(remaining bytes) per record —
O(n²) total for n records. At 10,000 entries this meant ~7.3 GB
allocated and 1.4 seconds spent replaying a log that is, at 128 bytes/
entry, only ~1.3 MB of actual data. This directly inflates **recovery
time** — every node restart replays its WAL from the last snapshot
boundary (`docs/recovery.md` §1) — which is exactly the metric
`docs/roadmap.md` §Performance Targets names ("Recovery time as a
function of log length since last snapshot").

**Change**: `internal/wal/wal.go`'s `readFrame` now reads only the
fixed-size frame header first (14 bytes) to learn the frame's declared
length, then reads exactly that many more bytes — O(1) work per record
in the common case — falling back to the original
read-everything-remaining behavior whenever the header alone cannot
prove a small, fully-present frame is safe to trust (an oversized or
corrupt declared length, or a torn tail), so every torn-tail/corruption
decision `decodeFrameBytes` makes is byte-for-byte unchanged. See
`internal/wal/wal.go`'s `readFrame`/`readFrameFull` doc comments for
the full reasoning.

**After** (this document's own §6.1 numbers, for direct comparison
against the "before" figures above):

```
BenchmarkWALReplay/entries=100-8      57716 ns/op      34787 B/op    320 allocs/op
BenchmarkWALReplay/entries=1000-8    549841 ns/op     337719 B/op   3020 allocs/op
BenchmarkWALReplay/entries=10000-8  5193065 ns/op    3366543 B/op  30025 allocs/op
```

At 10,000 entries: **~273x faster** (1.42s -> 5.2ms) and **~2,180x less
memory** (7.34 GB -> 3.4 MB). At 100/1,000 entries the difference is
within noise on this machine — the O(n²) cost only dominates once a
segment holds enough entries for "bytes remaining" to be large relative
to one frame, which is exactly why it was not caught by the existing
100/1,000-entry-scale correctness tests before this phase measured it
directly.

**Correctness validation**: the full `internal/wal` test suite
(including every crash/corruption/torn-tail/truncation test and
`FuzzDecodeFrameBytes`, run for an extra 20s of fuzz time after this
change) passes unchanged, plus the full repository test suite
(`go test ./...`, `go test ./... -race`, `-tags=integration`) — see
§10.

### 8.2 fsync dominates commit-path CPU time — no further optimization justified

**Workload**: `BenchmarkSQLInsert` (2,000 iterations, CPU+memory
profile) and `BenchmarkThreeNodeReplicatedWrite` (300 iterations, CPU
profile).

**Command**:

```bash
go test ./internal/sql/... -run '^$' -bench BenchmarkSQLInsert -benchtime=2000x \
  -cpuprofile=/tmp/cpu.prof -memprofile=/tmp/mem.prof
go tool pprof -top -nodecount=15 /tmp/cpu.prof
```

**Observed**: for the standalone SQL INSERT path, **77.8%** of sampled
CPU time is `internal/runtime/syscall/linux.Syscall6` reached through
`internal/storage.(*Segment).Sync` — i.e., the fsync syscall itself,
not application code. For the three-node replicated write path,
syscalls again lead at **28.6%** of samples (lower proportion because
three nodes' worth of goroutine scheduling/GC overhead is also
sampled), with no single application function accounting for more than
a few percent. The memory profile for the SQL INSERT path shows
allocations spread evenly across parsing (`sql.Parse`,
~19% cumulative), WAL framing (`wal.encodeRecord`, ~10%), planning
(`sql.planInsert`, ~6%), and `fsm.Apply`/`mvcc.ApplyCommit`
(~14% + ~7%) — no single dominant allocation source.

**Conclusion**: per this phase's policy ("do not optimize code merely
because it looks slow... if no worthwhile optimization is justified,
benchmark and document honestly instead of forcing one"), no
code-level optimization is justified here. The dominant cost is the
fsync durability boundary itself — a correctness requirement
(`docs/wal.md` §4), not a bug — and the surrounding allocation profile
shows ordinary, evenly-distributed overhead rather than a hotspot. The
one genuine hotspot this phase's profiling did find (§8.1) has already
been fixed.

## 9. `cmd/chronicledb-bench`

Not built. Per this phase's own scoping guidance ("add a dedicated
command... but only if it materially improves reproducibility over `go
test -bench`"): every benchmark this phase needed was expressible as a
standard Go benchmark or a small scripted test against the existing
`internal/node`/`internal/sql` test infrastructure, so a separate
command would have duplicated that infrastructure without adding
reproducibility `go test -bench`'s own seeded, table-driven benchmarks
do not already provide. If a future phase needs a configurable,
multi-client concurrent-load generator (§1's "not measured" list), that
is the concrete trigger for revisiting this decision.

## 10. Correctness regression

Every benchmark/profiling change in this document was validated against
the full existing test suite, unchanged:

```bash
go build ./... && go build -tags integration ./...
go test ./...
go test ./... -race
go test -tags=integration ./cmd/chronicledb-node/... -race -count=1
go test ./internal/wal/... -fuzz FuzzDecodeFrameBytes -fuzztime 20s
```

All green — see the Phase 9 completion report for the exact run log.

## 11. Limitations of this document

- Every number here is single-machine, single-run-per-benchmark (Go's
  own benchmark harness already runs each case enough times internally
  to produce a stable `ns/op`, but this document does not additionally
  report run-to-run variance across separate invocations).
- No `benchstat` comparison is included (not installed in this
  environment); per this phase's brief, this does not block Phase 9.
- These numbers describe *this specific environment and commit* — see
  §2's WSL2 caveat specifically. Do not quote them as ChronicleDB's
  general performance.
