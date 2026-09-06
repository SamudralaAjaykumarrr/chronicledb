# Recovery Model

Status: Phase 1 (`internal/wal.Open`, §1 steps 1, 5-8), Phase 2
(`internal/txn.Manager.recover`, §1 step 11 for `CommitTxn` commands in
standalone mode), and Phase 3 (§1 step 12, `RequestID` outcome
restoration via `internal/fsm.Apply` replay) are implemented. Phase 4
implements step 9's *logic* — `raft.NewCore` reconstructing
currentTerm/votedFor/log from a `raft.Storage`, with commitIndex/
appliedIndex always starting at 0 per §2 below — and proves it against
`internal/fault`'s simulated (in-memory) durable store
(`internal/fault/cluster_test.go::TestVoteSafety_SurvivesRestart`,
`TestRestartSafety_LogAndCommitmentSurvive`); it does not yet wire this
against the real `internal/wal` (that remains `internal/node`'s job,
Phase 5 — see [`docs/raft.md`](raft.md) §9.4). Steps 2-4, 10, and 13-14
remain Raft/snapshot scope and are not implemented yet.

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

## 5. Recovery and idempotency (implemented, Phase 3)

Because `RequestID` outcomes are part of the deterministic state
produced by `Apply` (see [`docs/transactions.md`](transactions.md)
§6), replaying committed history during recovery reconstructs the
exact same outcome table a live node would have. A client retrying a
`RequestID` against a node that just recovered gets the identical
answer it would have gotten from the node that originally applied the
command — this is what `REQUEST-OUTCOME-STABILITY` (see
[`docs/invariants.md`](invariants.md)) requires across restarts, not
just across retries against a single continuously-running process.

`internal/txn.Manager.recover` (§6 below) decodes every durable
`CommitTxn` command — Phase 3 durably appends one for every fresh
`RequestID`'s first submission, whether it ultimately commits or
conflicts (see [`docs/transactions.md`](transactions.md) §10) — and
calls `internal/fsm.Apply` for it, in order, into a freshly constructed
`internal/fsm.FSM`. Because `Apply` is deterministic, this reconstructs
both the MVCC version chains and the `RequestID` outcome table
(including each command's fingerprint, needed to detect a mismatched
`RequestID` reuse post-restart) exactly as they stood before the
restart — no separate durability mechanism for the outcome table
exists or is needed (`CONSISTENT-LOG-RESPONSIBILITY`,
[`docs/invariants.md`](invariants.md)): it is derived state, rebuilt
the same way MVCC state itself is. A `RequestID` that was never
submitted remains unknown after recovery (`GetRequestOutcome` returns
`fsm.ErrRequestIDUnknown`) — recovery only ever repopulates outcomes
for commands that actually exist in the durable log, never invents
one (`RECOVERY-NON-INVENTION`). See
`TestID2_DuplicateRequestIDAfterRestart`,
`TestConflictOutcomeSurvivesRestartAndRetryRemainsConflict`,
`TestMismatchedRequestIDReuseRejectedAfterRestart`, and
`TestRecoveryNeverInventsRequestIDOutcomes` (`internal/txn`).

## 6. Standalone transactional recovery (Phase 2, extended in Phase 3)

`internal/txn.Manager.recover` implements this document's §1 step 11
for `CommitTxn` commands, in standalone mode: it calls
`internal/wal.WAL.Replay(1)`, decodes each `RecordTypeLogEntry` payload
as an `internal/fsm.CommitTxnCommand`, and calls `internal/fsm.Apply`
for it — the identical function `Manager.commit` calls for a live
commit — in order, into a freshly constructed `internal/fsm.FSM` over
an empty `internal/mvcc.Store`.

Phase 2's version of this section described every replayed record as
guaranteed, by construction, to re-apply as `COMMITTED` — because
Phase 2's live commit path only ever durably appended a `CommitTxn`
record *after* it had already passed its conflict check, so a
conflict found during replay could only mean corruption. **This is no
longer true as of Phase 3** (see docs/transactions.md §10): the live
commit path now durably appends a command *before* evaluating its
conflict outcome, specifically so a command that conflicts still gets
a durable, replay-reconstructible `RequestID` outcome. Recovery
therefore does *not* treat a replay-time conflict as corruption — it
is a normal, expected, deterministic result, identical to what the
live commit produced, because `Apply` is a pure function of
(index, command, prior state) and replay reconstructs the identical
prior state at every index by replaying the identical prior commands
in the identical order. What recovery *does* still fail closed on is a
genuine inconsistency `Apply` itself detects: the same `RequestID`
appearing twice in the durable log with two different command
fingerprints, reported as `fsm.ErrRequestIDPayloadMismatch` — this
cannot happen via `internal/txn.Manager`'s own single-writer path
today (it pre-checks via `fsm.Precheck` before ever appending a fresh
`RequestID`), but `recover` still propagates it as a startup-refusing
error rather than guessing which of the two to keep
(`RECOVERY-NON-INVENTION`).

This gives Phase 3 the properties this document requires without
Raft's committed-boundary machinery, still absent: standalone mode has
no possibility of a durable-but-uncommitted suffix from another node's
perspective (§2.1), and every `CommitTxn` record that exists in the
log was, by the live commit path's own construction, already durably
decided (either `COMMITTED` or `ABORTED`) before the process that
appended it could have acknowledged anything about it — so replay
never needs to distinguish "committed" from "merely present" the way
Raft-mode recovery eventually will; it only needs to reproduce the
same deterministic decision, which `Apply`'s determinism guarantees.
