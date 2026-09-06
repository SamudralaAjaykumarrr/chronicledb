# Failure Model

Status: this document's required behavior is implemented and tested
through Phase 7 (see [`docs/roadmap.md`](roadmap.md)) — each failure
class below's "future tests" are, as of the phase named in each
section's own status notes, largely present; see
[`docs/testing-strategy.md`](testing-strategy.md) and
[`docs/scenario-corpus.md`](scenario-corpus.md) for exactly which. This
document remains the specification new tests are checked against, not
merely a historical one — a future test gap against any class below is
still a defect to close, not a reason to weaken the requirement here.

For each failure class: what may be lost, what must survive, what
must never happen, client-visible behavior, invariants protected, and
required future tests.

## 1. Local durability failures

### 1.1 Clean shutdown

- **May be lost**: nothing not already covered by the durability
  contract ([`docs/replication.md`](replication.md) §1).
- **Must survive**: everything **persisted** before shutdown.
- **Must never happen**: data loss of anything acknowledged.
- **Client-visible**: in-flight requests may see connection closed;
  client retries per [`docs/transactions.md`](transactions.md) §7.
- **Invariants**: `DURABILITY`.
- **Future tests**: restart-after-clean-shutdown replay test.

### 1.2 Process crash (ungraceful)

Same guarantees as clean shutdown for **persisted** data; anything only
**appended** (not yet `Sync()`-ed) may be lost. See §1.3-1.6 for the
precise crash-timing breakdown.

### 1.3 Crash during append

- **May be lost**: the in-progress record (torn tail).
- **Must survive**: every prior fully framed, checksummed record.
- **Must never happen**: the torn tail being mistaken for a valid
  record, or its presence causing rejection of the (undamaged) prior
  history.
- **Mechanism**: [`docs/wal.md`](wal.md) §6.1 torn-tail truncation.
- **Invariants**: `RECOVERY-NON-INVENTION`.
- **Future tests**: crash-injection at random byte offsets during
  append, verify clean truncation and full recovery of prior records.

### 1.4 Crash before fsync

- **May be lost**: any record appended since the last successful
  `Sync()` — by definition, never acknowledged as durable to any
  caller (see [`docs/replication.md`](replication.md) §1.1 step 2).
- **Must survive**: everything covered by a completed `Sync()`.
- **Must never happen**: acknowledging a commit before its `Sync()`
  completes (this is a code-path invariant, not a crash-recovery
  concern, but its violation is what would make this crash class
  dangerous).
- **Invariants**: `DURABILITY`.
- **Future tests**: fault-injected `Sync()` delay + crash before it
  returns; verify the client never received a success response for
  the lost record.

### 1.5 Crash after fsync, before apply

- **May be lost**: nothing — the record is persisted.
- **Must survive**: the record; recovery replays and applies it (see
  [`docs/recovery.md`](recovery.md) §1 step 11).
- **Must never happen**: the record being skipped because "apply
  hadn't happened yet."
- **Invariants**: `DURABILITY`, `RECOVERY-NON-INVENTION`.
- **Future tests**: crash injected precisely between `Sync()` return
  and `Apply()` call; verify recovery applies it exactly once.

### 1.6 Crash after apply, before response

- **May be lost**: only the in-flight response to the client (not any
  server-side state).
- **Must survive**: the applied state and the recorded `RequestID`
  outcome.
- **Must never happen**: re-applying the command on retry (idempotency
  — see [`docs/transactions.md`](transactions.md) §6).
- **Client-visible**: client sees `UNKNOWN`, retries with same
  `RequestID`, observes `COMMITTED` (or `ABORTED`, whichever the
  original apply produced) — see
  [`docs/transactions.md`](transactions.md) §7.
- **Invariants**: `IDEMPOTENCY`, `REQUEST-OUTCOME-STABILITY`.
- **Future tests**: crash injected after `Apply` returns, before
  response is sent; verify retry returns the identical outcome and no
  duplicate mutation occurred.

### 1.7 Torn final record / bad final checksum / mid-log corruption

Covered in full in [`docs/wal.md`](wal.md) §6. Summary:

| Case | May be lost | Must survive | Must never happen |
|---|---|---|---|
| Torn final record | The torn record only | Everything before it | Treating a mid-log bad checksum the same way |
| Bad checksum, final record, but fully framed | Nothing automatically — startup refused | N/A until operator resolves | Silently discarding a fully framed record |
| Bad checksum, mid-log | Nothing automatically — startup refused | N/A until operator resolves | Silently skipping past it, guessing content, or partial replay |

### 1.8 Disk write failure / fsync failure

- **May be lost**: the specific write/sync that failed; no
  acknowledgment is given for it.
- **Must survive**: everything already durably persisted before the
  failure.
- **Must never happen**: treating a failed `Sync()` as successful.
- **Client-visible**: the request fails explicitly (not silently
  timed out); client may retry (possibly against a different node, in
  replicated mode) with the same `RequestID`.
- **Invariants**: `DURABILITY`.
- **Future tests**: fault-injected write/fsync error return codes at
  each pipeline stage.

### 1.9 Out-of-space condition

- Treated the same as a disk write failure (§1.8) at the storage
  layer: the write fails explicitly. Graceful degradation (e.g.
  proactive space-based backpressure before the OS reports `ENOSPC`)
  is a possible future enhancement, not a V1 correctness requirement;
  V1's requirement is only that an out-of-space condition never be
  silently treated as success.

## 2. Raft / replication failures

### 2.1 Leader crash before replication

- **May be lost**: the proposed entry, if it existed only in the
  crashed leader's own (possibly not even locally persisted) state.
- **Must survive**: every previously committed entry.
- **Client-visible**: client retry (same `RequestID`) against new
  leader; original request effectively never happened.
- **Invariants**: `DURABILITY` is not violated because nothing was ever
  acknowledged.

### 2.2 Leader crash after partial replication (no quorum yet)

- Same as §2.1: an entry on fewer than a majority of nodes is not
  committed; it is safe for it to vanish from the eventual leader's
  history (via divergent-suffix repair, [`docs/raft.md`](raft.md) §3).

### 2.3 Leader crash after quorum, before reply

- **Must survive**: the entry — it **is** committed (majority
  persisted it) even though the leader crashed before applying/
  replying. See [`docs/raft.md`](raft.md) §7 and
  [`docs/replication.md`](replication.md) §5 step 2 (new leader's log
  is guaranteed to include it).
- **Client-visible**: `UNKNOWN` until retry-by-`RequestID` against the
  new leader confirms `COMMITTED`.
- **Invariants**: `LEADER-COMPLETENESS`, `REQUEST-OUTCOME-STABILITY`.
- **Future tests**: kill leader process immediately after majority
  `matchIndex` is observed, before its own apply/reply; verify new
  leader surfaces `COMMITTED` on retry.

### 2.4 Follower crash / restart

- **Must survive**: follower's own previously persisted entries.
- **Client-visible**: no direct effect if a majority remains available.
- Rejoins via normal replication or snapshot install (§3 below), per
  [`docs/raft.md`](raft.md) §7.

### 2.5 Stale metadata on restart

- A node whose persisted `currentTerm`/`votedFor` is somehow
  inconsistent with its log (a bug or corrupted disk state) fails
  startup per [`docs/recovery.md`](recovery.md) §4 rather than
  guessing a resolution.

### 2.6 Dropped / delayed / duplicated / reordered messages

- Raft is designed to tolerate all of these for **liveness** (the
  cluster may be temporarily slower to make progress) without
  affecting **safety** (committed history is never corrupted by
  message-level unreliability) — this is a core Raft correctness
  property, exercised deterministically by the simulator (see
  [`docs/testing-strategy.md`](testing-strategy.md)).
- Duplicated `AppendEntriesRPC`/`RequestVoteRPC` messages are handled
  idempotently by the receiver's normal term/log-matching checks — no
  separate message-level deduplication mechanism is required for
  correctness (only for efficiency, optionally, later).

### 2.7 Network partition / healed partition / stale leader / divergent suffix

Full worked scenario in [`docs/replication.md`](replication.md) §5.

### 2.8 Election storm (repeated elections without a stable leader)

- **Must never happen indefinitely** under normal network conditions,
  due to randomized election timeouts breaking symmetry; may happen
  transiently under pathological network conditions.
- **Client-visible**: unavailability for new commits until a stable
  leader emerges; no data corruption.
- **Future tests**: simulator scenario with adversarial timer
  scheduling to confirm eventual convergence, per
  [`docs/scenario-corpus.md`](scenario-corpus.md).
- **Status (Phase 7)**: `internal/fault/chaos_test.go::TestChaos_AsymmetricPartitionSafety`
  proves this — and, in the course of proving it, found and led to the
  fix of a genuine bug where "must never happen indefinitely" was
  actually violated under an asymmetric partition: see
  [`docs/testing-strategy.md`](testing-strategy.md) §7.1.

### 2.9 Slow follower

- Falls behind `matchIndex`-wise; leader continues sending; if it
  falls far enough behind that its needed log range has been
  compacted, it is caught up via snapshot install
  ([`docs/snapshots.md`](snapshots.md) §7) instead. Never blocks
  cluster-wide commit progress (commit only requires a majority, not
  all nodes).

## 3. Snapshot failures

### 3.1 Snapshot creation interrupted

- Covered by [`docs/snapshots.md`](snapshots.md) §4: orphaned temp
  file, never trusted, no special recovery needed.

### 3.2 Snapshot installation interrupted (follower)

- Covered by [`docs/snapshots.md`](snapshots.md) §7 step 6: restarts
  from scratch, no partial state ever installed.

### 3.3 Corrupted snapshot

- Covered by [`docs/snapshots.md`](snapshots.md) §6: never used;
  fallback or operator intervention per
  [`docs/recovery.md`](recovery.md) §4.

## 4. Request-level failures

### 4.1 Duplicate request (same `RequestID`)

- Handled by idempotency (§1.6, [`docs/transactions.md`](transactions.md)
  §6): identical outcome returned, no duplicate effect.

### 4.2 Lost response

- Handled by client retry-by-`RequestID` and/or `GetRequestOutcome`
  (see [`docs/transactions.md`](transactions.md) §7).

### 4.3 Transaction conflict

- Not a failure in the infrastructure sense — a correct, deterministic
  outcome (`ABORTED`, see [`docs/mvcc.md`](mvcc.md) §4). Client-visible
  as an explicit abort with a conflict reason; client may retry as a
  **new** transaction (new `TxnID`, new `StartSeq`) if it wishes to
  re-attempt the logical operation.

### 4.4 Retry of an uncertain request

- Covered fully in [`docs/transactions.md`](transactions.md) §7.

## 5. Explicitly outside V1 guarantee scope

- Byzantine faults (nodes that lie or behave arbitrarily maliciously,
  as opposed to crashing/partitioning) — Raft assumes non-Byzantine
  failures; ChronicleDB inherits that assumption.
- Silent, undetected bit-level data corruption that happens to produce
  a **valid** checksum (astronomically unlikely with a proper checksum
  but not information-theoretically impossible) — outside any
  practical system's guarantee scope.
- Simultaneous loss of a majority of nodes' persistent storage in
  replicated mode, or the single node's storage in standalone mode
  (see [`docs/replication.md`](replication.md) §2).
- Clock-skew-dependent optimizations (lease reads) — not used in V1
  (see [`docs/replication.md`](replication.md) §4.2), so clock skew
  across nodes is not a correctness dependency for V1's guarantees.
- WAN-scale network behavior (see [`docs/non-goals.md`](non-goals.md)).

## 6. Security and Safety Expectations

Authentication and TLS are **explicitly deferred** in V1 — ChronicleDB
V1 assumes a trusted network between nodes and between client and
cluster (e.g. a private network or an operator-managed tunnel). This
is a scope decision, not an oversight; it is tracked for a future
phase in [`docs/roadmap.md`](roadmap.md) and must be resolved before
any claim of production-readiness or external exposure.

Regardless of the auth deferral, the following are required from V1
onward, because they are correctness/robustness properties independent
of authentication:

- **Malformed input validation**: no component trusts a length,
  count, or type field from disk or network without bounding it
  against the actual remaining bytes available (see
  [`docs/storage.md`](storage.md) §7).
- **Bounded request sizes / bounded allocations**: request and record
  decoding must reject oversized inputs before attempting to allocate
  buffers sized directly from an attacker- or corruption-controlled
  field.
- **Versioned durable formats**: every durable format (WAL records,
  snapshots) carries an explicit version field, checked before
  interpretation (see [`docs/wal.md`](wal.md) §3, §6,
  [`docs/snapshots.md`](snapshots.md) §5).
- **Corruption detection**: checksums are mandatory, not optional, on
  every durable record and snapshot (§1.7, [`docs/snapshots.md`](snapshots.md) §5).
- **Safe path handling**: file paths derived from configuration or
  internal identifiers only, never from unsanitized external input, to
  avoid path traversal in a future networked deployment.
- **No panic from malformed disk/network data**: decoding malformed or
  corrupted bytes must return an error, never panic the process —
  this matters especially for the network-facing protocol layer once
  it exists, where an external actor controls the bytes.
- **Race-safe concurrency**: shared state (MVCC version chains, Raft
  core state, session state) must be protected by the concurrency
  model chosen in each package's implementation phase; the
  deterministic core (`internal/raft`, `internal/fsm`) is designed as
  single-threaded/serialized specifically to sidestep most of this
  risk by construction (see [`docs/raft.md`](raft.md) §1).
- **Explicit error semantics**: functions return errors rather than
  using sentinel values or silent no-ops for failure conditions.
- **No committed secrets**: no credentials, keys, or tokens are ever
  committed to this repository; this is a repository hygiene rule
  enforced by review, not a runtime property.
- **Context cancellation**: request-handling code paths that may block
  (network I/O, disk I/O) respect cancellation/timeouts where the Go
  standard library idioms for this apply, once such code exists.

These expectations govern implementation quality from Phase 1 onward;
none of them are implemented yet in this Architecture Foundation
phase, since no code exists.
