# ADR-0008: Raft Architecture and Persistent State

Status: Accepted

## Context

ChronicleDB's vision explicitly requires implementing and exposing
real Raft mechanics, not hiding consensus behind a finished, embedded
library. This requires deciding how the Raft core is structured (as
inspectable, testable architecture) and exactly which state must
survive a node restart to preserve Raft's safety properties.

## Decision

- Implement `internal/raft` as a **deterministic, input/output-driven
  core** (`Step(state, input) -> (newState, outputs)`), with no direct
  I/O — see [`docs/raft.md`](../raft.md) §1. Production adapters
  (`internal/transport`, `internal/wal`, a clock package) and test
  adapters (`internal/fault`) implement small interfaces the Raft core
  itself defines.
- Persist `currentTerm`, `votedFor`, and log entries durably via
  `internal/wal` before they can affect other nodes' state (granting a
  vote, acknowledging replication) — see
  [`docs/raft.md`](../raft.md) §5.
- Persist snapshot `lastIncludedIndex`/`lastIncludedTerm` as part of
  snapshot metadata (see [`docs/snapshots.md`](../snapshots.md)).
- **Do not** independently persist `commitIndex`/`appliedIndex` as
  trusted-on-their-own facts; reconstruct them at restart from the
  snapshot boundary plus legitimate leader-confirmed information (see
  [`docs/raft.md`](../raft.md) §5.1, [`docs/recovery.md`](../recovery.md) §2).

## Alternatives Considered

1. **Embed an existing, finished Raft library (e.g. `etcd/raft`,
   `hashicorp/raft`).** Rejected: directly conflicts with the
   project's stated purpose of implementing and exposing real Raft
   mechanics as inspectable architecture (see
   [`docs/vision.md`](../vision.md)); using a finished library would
   make ChronicleDB "a wrapper around a finished consensus system,"
   an explicit non-goal.
2. **Design the Raft core to directly perform its own I/O (sockets,
   disk) rather than as a pure input/output component.** Rejected:
   makes deterministic simulation testing (a core project goal, see
   [`docs/testing-strategy.md`](../testing-strategy.md) §3) impossible
   without heavy mocking of low-level I/O calls; the pure
   input/output design is what allows the *exact same* Raft core code
   to run in production and under the deterministic simulator.
3. **Persist `commitIndex` directly and trust it on restart.**
   Rejected: a node's last-known `commitIndex` before a crash could
   itself have been based on since-superseded information (e.g. this
   node was a leader whose "committed" understanding gets contradicted
   by a real current leader after restart); trusting it blindly risks
   violating `APPLIED-PREFIX-SAFETY` (see
   [`docs/invariants.md`](../invariants.md)). Reconstruction via
   legitimate leader contact or re-derivation via the current-term
   commit rule is required regardless of what was cached.

## Consequences

- The Raft core's interfaces (transport, persistent store, clock) are
  a stable contract that both production and test code must satisfy;
  changing them affects both surfaces simultaneously — a deliberate
  trade-off that keeps production and test code exercising identical
  Raft logic.
- Every restart pays the cost of re-establishing `commitIndex`
  (contacting a leader or winning an election) rather than resuming
  instantly from a cached value — an accepted cost for correctness.

## Correctness Implications

- Directly implements `RAFT-ELECTION-SAFETY`, `RAFT-LOG-MATCHING`,
  `LEADER-COMPLETENESS`, and `APPLIED-PREFIX-SAFETY`
  ([`docs/invariants.md`](../invariants.md)).

## Testing and Proof Obligations

- Simulator-based tests (RF-1 through RF-15 in
  [`docs/scenario-corpus.md`](../scenario-corpus.md) §Raft/Replication)
  covering election safety, log matching under divergence, leader
  completeness across failover, and restart-time `commitIndex`
  reconstruction specifically.
