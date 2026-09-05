# Recovery Model

Status: Architecture Foundation. No recovery implementation exists
yet.

This document defines the exact restart/recovery sequence a
ChronicleDB node follows, so that a durable-but-uncommitted suffix,
corrupted history, or a stale snapshot can never be turned into
invented committed state.

## 1. Recovery sequence (binding)

```
 1. Inspect persistent metadata (internal/wal Metadata record,
    docs/wal.md §8) — node identity, format version, pointer to most
    recent candidate snapshot.
 2. Locate the newest snapshot referenced by that metadata (if any).
 3. Verify that snapshot's integrity (checksum over its full contents
    — see docs/snapshots.md §Validation).
 4. If valid, restore state-machine state from the snapshot:
      - MVCC version chains as of lastIncludedIndex
      - tombstones
      - RequestID outcome table
      - lastIncludedIndex / lastIncludedTerm
    If the newest snapshot fails validation, fall back to the next
    older valid snapshot (if any) — see docs/snapshots.md §Corrupted
    Snapshot Handling. If none validate, proceed from empty state
    (index 0) — correct only if the durable log still contains the
    entire history from the start (normal for a fresh cluster; for an
    established cluster whose log has been compacted, this case means
    startup must fail rather than silently lose history — see §4.
 5. Inspect the durable log (internal/wal) starting at
    lastIncludedIndex + 1 (or index 1, if no snapshot).
 6. Verify record framing/checksums while reading (docs/wal.md §6).
 7. If the last record is a torn tail, truncate it automatically
    (docs/wal.md §6.1) — this is the one form of automatic repair
    permitted.
 8. If any fully framed record fails its checksum anywhere in the
    range being read, refuse startup (docs/wal.md §6.2) — this is
    unconditional; recovery does not attempt to route around it.
 9. Restore Raft persistent metadata (currentTerm, votedFor) from the
    most recent HardState record (docs/raft.md §5).
10. Determine the committed boundary correctly (§2 below) — never by
    assuming every entry present in the log is committed.
11. Apply only legitimate committed history: replay LogEntry records
    from lastIncludedIndex + 1 up to the determined committed boundary
    (§2), running internal/fsm.Apply for each, in order, exactly as
    if they were being applied for the first time (Apply is
    deterministic and safe to re-run from a consistent starting
    state — docs/transactions.md §5).
12. Restore idempotency outcomes as a byproduct of step 11 (each
    replayed CommitTxn command re-populates/confirms its RequestID
    outcome — see docs/transactions.md §6) plus whatever the snapshot
    already contained (step 4).
13. Verify local invariants (e.g. appliedIndex <= known log length;
    snapshot lastIncludedIndex/Term consistent with the first replayed
    entry's expected prevLogTerm — a mismatch here is treated as
    corruption per step 8, not silently patched over).
14. Only after all of the above succeeds does the node become eligible
    to participate: cast/request votes, serve as leader, or accept
    client requests.
```

Any failure at steps 3-4 (unrecoverable snapshot state) or step 8
(mid-log corruption) or step 13 (invariant mismatch) causes the node
to refuse to start and requires operator intervention (§4). Steps 6-7
(torn tail) are the sole automatic-repair path.

## 2. Determining the committed boundary correctly

A durable log can contain entries beyond what was ever actually
committed — most commonly, a **durable but uncommitted Raft suffix**
left by a node that was leader (or a candidate that appended
speculative entries — in practice, only a leader appends to its own
log) at the time of a crash or partition, where those entries never
reached majority persistence before the disruption.

**Rule: the presence of a log entry on disk is never itself proof
that the entry was committed.** Recovery does not compute
`commitIndex` by scanning "how far the local log goes." Instead:

- On restart, this node's own recovered state advances only as far as
  its own snapshot (`lastIncludedIndex`) plus whatever it can prove
  was committed using **its own prior knowledge** persisted alongside
  `HardState`/log data (if V1 chooses to persist a `commitIndex`
  watermark as a non-authoritative hint — see §2.1) — but it does not
  *apply* anything beyond a snapshot until it either:
  - **Rejoins as a follower** and receives authoritative
    `commitIndex` information from a current, legitimate leader via
    `AppendEntriesRPC` (normal case), or
  - **Wins an election and becomes leader itself**, in which case it
    re-establishes `commitIndex` using the current-term commit rule
    (see [`docs/raft.md`](raft.md) §4) — it does not trust its own
    pre-crash idea of `commitIndex` blindly, because that idea might
    itself have predated an uncommitted suffix from a still-earlier
    incarnation.
- A durable log suffix beyond the true committed point is left
  in place (not deleted) until a legitimate leader's
  `AppendEntriesRPC` either confirms it (matches and is covered by an
  advancing `commitIndex`) or contradicts it (triggering divergent
  suffix truncation, see [`docs/raft.md`](raft.md) §3). It is simply
  never **applied** in the meantime.

### 2.1 Standalone mode (no Raft yet)

In standalone mode there is only one node, so "committed" and
"durable-and-`Sync()`-ed" coincide (see
[`docs/replication.md`](replication.md) §1.1, §3) — there is no
possibility of an uncommitted suffix from a *different* node's
perspective. The only uncommitted-suffix case in standalone mode is
the torn-tail case already covered by [`docs/wal.md`](wal.md) §6.1,
because nothing is ever "appended-but-not-yet-known-committed" for an
extended period the way an in-flight Raft proposal can be — a
standalone commit either fully durably completes (§1.1) or it does
not exist in recoverable history at all.

## 3. Invented committed state is never acceptable

This is the single most important rule in this document, restated
plainly: **recovery must never cause a node to believe something is
committed, applied, or acknowledged-worthy that was not legitimately
committed per the rules in [`docs/raft.md`](raft.md) §4 (replicated
mode) or [`docs/replication.md`](replication.md) §1.1 (standalone
mode).** This is codified as the `RECOVERY-NON-INVENTION` invariant
(see [`docs/invariants.md`](invariants.md)) and is the reason mid-log
corruption fails startup instead of being "best-effort" recovered
(§1 step 8, [`docs/wal.md`](wal.md) §6.2) — a best-effort guess at
what a corrupted record "probably" contained would be exactly this
kind of invention.

## 4. Cases requiring operator intervention (explicitly out of automatic recovery)

| Case | Why it cannot be automatic in V1 |
|---|---|
| Mid-log corruption (bad checksum on a non-final, fully framed record) | Cannot determine what was lost or whether it was already acknowledged; guessing risks inventing or discarding committed state. |
| All known snapshots fail validation, and the log does not cover full history from index 1 | No safe starting point exists locally; the node must be re-provisioned from a healthy peer (Raft mode) or from a known-good backup (standalone mode). |
| Unknown/unsupported WAL or snapshot format version | Version skew must be resolved explicitly (upgrade/downgrade procedure), not guessed at by attempting to parse an unrecognized format. |
| Raft persistent metadata (`HardState`) missing or inconsistent with log content in a way step 13 detects | Indicates a prior bug or unclean disk state; must not be papered over automatically. |

V1 provides no automated quorum-based self-repair for these cases (a
node in one of these states does not automatically ask its peers to
resend a full copy of history — see
[`docs/wal.md`](wal.md) §6.3). The documented operator procedure is:
stop the affected node, discard its local data directory, and rejoin
it to the cluster as if it were a brand-new node (full snapshot
install + log catch-up, see [`docs/snapshots.md`](snapshots.md)).
Automating this procedure is a plausible future enhancement, not a
V1 correctness requirement — see [`docs/roadmap.md`](roadmap.md).

## 5. Recovery and idempotency

Because `RequestID` outcomes are part of the deterministic state
produced by `Apply` (see [`docs/transactions.md`](transactions.md)
§6), replaying committed history during recovery reconstructs the
exact same outcome table a live node would have. A client retrying a
`RequestID` against a node that just recovered gets the identical
answer it would have gotten from the node that originally applied the
command — this is what `REQUEST-OUTCOME-STABILITY` (see
[`docs/invariants.md`](invariants.md)) requires across restarts, not
just across retries against a single continuously-running process.
