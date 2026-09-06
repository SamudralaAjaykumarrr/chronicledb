# ADR-0014: Election No-Op Entry for `ReadIndex` Liveness

Status: Accepted

## Context

[ADR-0010](0010-read-consistency.md) and
[`docs/replication.md`](../replication.md) §4 establish `ReadIndex` as
V1's mechanism for a safe, quorum-proven `StartSeq`. Its footnote
already anticipated that "some Raft implementations use a no-op-entry
variant of `ReadIndex`... either satisfies this ADR's safety
requirement" — but Phase 5/6/7 shipped the pure heartbeat-quorum variant
without a no-op, and [`docs/raft.md`](../raft.md) §9.5/§10.3 explicitly
documented the resulting liveness gap as an accepted, deferred trade-off:
a newly elected leader cannot advance `commitIndex` past a *previous*
term's entries (Raft's current-term commit rule, `docs/raft.md` §4)
until some *new* proposal commits in its *own* term — closing that
window with an election no-op was named there as "`§9.5`-anticipated
future ADR territory... if a future phase's failover-latency goals
require it," not a Phase 5 correctness requirement.

Phase 8 hit exactly that condition in practice, not hypothetically:
`internal/sql`'s real-cluster distributed tests
(`internal/sql/distributed_test.go`) call
`internal/node.Node.BeginReadIndex` for *every* SQL statement's
`Session.Begin` — including the first statement executed against a
newly elected leader, with no intervening write in its new term. Under
that exact condition, `BeginReadIndex` hung indefinitely (bounded only
by the caller's own context timeout): `ReadIndex`'s wait condition
(`appliedIndex >= target`, where `target` already includes the
old-term entry) could never become true without some other, unrelated
write eventually arriving to unstick it. This is a genuine liveness
defect for any client of `BeginReadIndex` immediately after failover —
not a hypothetical scenario `docs/raft.md` merely anticipated in the
abstract, but one Phase 8's own required "leader failover and retry
with same RequestID" testing (`docs/roadmap.md` Phase 8 brief) could
not pass without addressing it, since that path also goes through
`Session.Begin` → `BeginReadIndex`.

## Decision

`internal/node.Node`, upon observing `raft.Output.BecameLeader`,
immediately proposes one synthetic `CommitTxn` command with empty
`Mutations` in its own new term (`Node.proposeElectionNoOp`,
`internal/node/node.go`). Once that no-op commits — an ordinary
majority-match commit, now possible because it *is* a current-term
entry — the current-term commit rule's existing backward-scan behavior
(`docs/raft.md` §4) immediately also recognizes every earlier entry as
committed, unblocking any pending `ReadIndex` with no client action
required.

This is implemented entirely in `internal/node` (the driver), not
`internal/raft.Core`:

- `Core.becomeLeader` is **unchanged** — it still does not invent or
  append anything on its own election, exactly as `docs/raft.md` §9.5
  documents. `internal/raft` remains a pure, deterministic component
  driven entirely by explicit `Input` values it is given (`docs/raft.md`
  §1) — deciding *to propose* a no-op is a driver policy choice, the
  same category of decision `internal/node` already makes for every
  other command in this system (docs/architecture.md §5's dependency
  rules: nothing here requires `internal/raft` to know why an entry
  was proposed).
- The no-op's `RequestID` is deterministically derived from a leading
  NUL byte (a byte no real client is expected to send), this node's ID,
  and its new term — guaranteed distinct per election, and, given V1's
  trusted-network assumption (`docs/non-goals.md` §Authentication and
  TLS), practically impossible for a genuine client `RequestID` to
  collide with. A `Precheck` guard makes a second call for the
  identical (node, term) pair — which should not happen given `Output.BecameLeader`
  fires once per election — a harmless no-op rather than a duplicate
  proposal.
- A rejected proposal (this node loses leadership again before the
  no-op is even accepted) is silently dropped: nothing is waiting on
  its outcome, and the next election will try again.

## Alternatives Considered

1. **Do nothing; document the stall as an accepted client-visible
   liveness limitation, resolved only when some other write happens to
   arrive.** Rejected: this is exactly the deferred state `docs/raft.md`
   §10.3 already described, but Phase 8 demonstrated it is not merely
   theoretical — a real client-facing API (`BeginReadIndex`, and now
   every SQL statement built on it) can hang indefinitely on it in an
   ordinary, non-adversarial failover, which is a worse experience than
   the one extra committed no-op entry per election costs.
2. **Fix it in `internal/sql` only** (e.g. have the SQL layer retry
   `BeginReadIndex` with a fallback that proposes something itself).
   Rejected: the underlying liveness gap is in `internal/node`, not
   `internal/sql`; a workaround confined to one caller would leave
   every other (present or future) `BeginReadIndex` caller — including
   the raw `internal/node` API itself — with the identical bug. Fixing
   the actual defect once, in the layer that owns it, is correct;
   papering over it in a single caller is not.
3. **Have `internal/raft.Core` append a no-op entry internally in
   `becomeLeader`.** Rejected: `internal/raft` is deliberately a pure,
   input-driven component with no policy of its own about *what* gets
   proposed (`docs/raft.md` §1, §9.5) — every other command
   `internal/raft.Core` ever sees arrives via an explicit
   `Input{Kind: InputPropose}` from its driver. Inventing an exception
   specifically for this case would blur that boundary for no
   structural benefit, since the driver can express the identical
   effect (`Core.Step(Input{Kind: InputPropose, ...})`) itself, exactly
   as it already does for every real client command.
4. **A lease-based fast path instead, avoiding the need for any
   current-term entry.** Rejected for the same reason ADR-0010
   rejected lease reads generally: it depends on a bounded clock-skew
   assumption across nodes that V1 has not modeled or proven.

## Consequences

- Every leader election now durably commits one extra, permanently
  retained entry (empty `Mutations`, a harmless `RequestID` outcome
  entry — `docs/transactions.md` §6's "V1 may retain outcomes
  indefinitely" already covers this). This is a small, fixed, one-time
  cost per election, not a per-request cost.
- `internal/node`/`internal/sql` tests that assumed a cluster's very
  first proposal would land at exactly log index 1 (or that a fixed
  number of test proposals would align exactly with a
  `SnapshotThreshold` boundary with nothing else in the log) needed
  updating to compute their expected boundary dynamically instead of
  hardcoding it — see `docs/replication.md` §4.3 for the specific tests
  and the general principle (wait for the boundary to cover the actual
  entries under test, rather than asserting a specific absolute index).
  This is a test-robustness improvement, not a behavior change any
  production caller depends on.
- `docs/raft.md` §10.3's "not a design gap this phase should close...
  remains future ADR territory" is now resolved by this ADR — that
  section is updated to point here rather than continuing to describe
  the gap as open.

## Correctness Implications

- Does not change `RAFT-ELECTION-SAFETY`, `RAFT-LOG-MATCHING`, or
  `QUORUM-SAFETY` (`docs/invariants.md`): the no-op is an ordinary
  command subject to the identical proposal/replication/commit path
  and the identical current-term commit rule as any other entry: no
  new mechanism, no relaxed check.
- Preserves `DETERMINISM-BOUNDARY`: `internal/raft.Core` and
  `internal/fsm.Apply` remain pure functions of their given inputs;
  choosing *to* propose a no-op is a driver-level decision made outside
  either, exactly like every other client-originated command.
- Directly restores the liveness `ReadIndex` is supposed to have per
  ADR-0010 immediately after a failover, closing the gap
  `docs/raft.md` §10.3 had left open.

## Testing and Proof Obligations

- `internal/sql/distributed_test.go::TestDistributedSQLLeaderFailoverRetry`
  is the deterministic regression test: a real three-node cluster, a
  real leader crash, and an immediate `BeginReadIndex`-driven retry
  against the new leader with no other intervening write — confirmed to
  hang indefinitely before this fix and to pass immediately (well under
  a second) after it.
- `internal/node`'s existing snapshot-boundary tests
  (`TestSN1_RestartRestoresFromSnapshotAndCompactsLog`,
  `TestSN5_FollowerCatchesUpViaSnapshotAfterLeaderCompaction`,
  `TestChaos_SnapshotFollowerCrashDuringCatchupResumesCleanly`) were
  updated to assert against a dynamically observed snapshot boundary
  rather than one hardcoded index, and re-verified green under `-race`
  across repeated runs.
