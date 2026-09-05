# ADR-0007: Deterministic Replicated State-Machine Boundary

Status: Accepted

## Context

Raft (and any replicated-state-machine design) requires that every
replica, applying the same sequence of committed commands, reach
equivalent state. This is only possible if the function that applies
commands (`Apply`) is strictly deterministic — free of wall-clock,
randomness, network, filesystem, or other environment-dependent
inputs. ChronicleDB also wants the standalone (pre-Raft) engine's
design to transition into the replicated design additively, not via a
rewrite.

## Decision

- Define `internal/fsm.Apply(command) -> result` as the **sole**
  deterministic boundary between "ordered committed commands" and
  "database state." `Apply` must not depend on wall clock, random
  generation, environment variables, filesystem queries, network
  calls, external services, process-local timing, unordered map
  iteration, or uncontrolled global mutable state (see
  [`docs/raft.md`](../raft.md) §1, [`docs/invariants.md`](../invariants.md)
  `STATE MACHINE SAFETY` and `DETERMINISM BOUNDARY`).
- Any nondeterministic value a command legitimately needs (e.g. a
  randomized ID generated somewhere) must be produced **outside**
  `Apply` and encoded into the command itself before it is proposed —
  never generated fresh inside `Apply`.
- The **standalone engine** (Phases 1-3, before Raft exists) is
  designed from the start so its durable, ordered command history is
  exactly what `internal/fsm.Apply` will later consume when driven by
  Raft — i.e. the standalone engine is architecturally "a Raft group
  of one" (see [`docs/architecture.md`](../architecture.md) §1,
  [`docs/replication.md`](../replication.md) §3), not a separate
  prototype requiring a rewrite to become replicated.

## Alternatives Considered

1. **Allow `Apply` to read wall-clock time for convenience (e.g.
   timestamping records with real time).** Rejected: introduces
   nondeterminism directly into the state machine — two replicas
   applying the "same" command at different real times would diverge.
   ChronicleDB uses logical sequencing (`CommitSeq` from log index)
   instead (see [`docs/architecture.md`](../architecture.md) §3),
   precisely to avoid this.
2. **Build the standalone engine as a throwaway prototype, rewrite for
   Raft later.** Rejected: doubles the engineering effort and risks
   the rewrite introducing subtly different (and unproven)
   `Apply`/MVCC semantics than what was tested in the standalone
   phase — undermining confidence built up through Phases 1-3's
   testing.
3. **Let `internal/raft` call directly into `internal/mvcc` /
   `internal/txn`, skipping a distinct `internal/fsm` layer.**
   Rejected: collapses the deliberate package boundary
   ([`docs/architecture.md`](../architecture.md) §5) that keeps
   `internal/raft` fully decoupled from database semantics — needed so
   the Raft core can be tested and reasoned about purely as a
   consensus protocol, independent of what commands mean.

## Consequences

- Any future feature that appears to need clock/randomness/external
  I/O inside `Apply` (e.g. "expire this row after N seconds of wall
  time") must instead be modeled as an explicit, externally-triggered
  command (e.g. a periodic "advance logical time to T" command
  proposed through the normal path) — a real design constraint future
  contributors must respect, not a hypothetical one.
- `internal/fsm` and `internal/mvcc` can be fully unit- and
  property-tested without any networking, disk, or timing
  infrastructure, which is a significant testing-velocity benefit.

## Correctness Implications

- This ADR is the direct basis for the `DETERMINISM BOUNDARY` and
  `STATE MACHINE SAFETY` invariants
  ([`docs/invariants.md`](../invariants.md)).

## Testing and Proof Obligations

- Deterministic replay tests: feed an identical command sequence to
  two independently constructed `internal/fsm` instances and assert
  identical resulting state (see
  [`docs/testing-strategy.md`](../testing-strategy.md) §3.2).
- A static-analysis/lint rule (once code exists) forbidding imports of
  `time`, `math/rand`'s global source, network packages, or
  unsanitized filesystem access inside `internal/raft`'s core logic and
  `internal/fsm`/`internal/mvcc`.
