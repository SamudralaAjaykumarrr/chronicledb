# ADR-0009: Transport/Clock/Randomness Abstraction

Status: Accepted

## Context

Raft correctness bugs are notoriously hard to reproduce when tests
depend on real wall-clock timing, real networking, and real
scheduling nondeterminism. ChronicleDB's testing strategy requires
deterministic, reproducible distributed simulation
([`docs/testing-strategy.md`](../testing-strategy.md) §3), which is
only possible if the production dependencies on network, clock, and
randomness are behind interfaces that a test harness can replace with
controlled, deterministic implementations.

## Decision

`internal/raft` defines its own small interfaces for:

- **Transport** — sending/receiving `RequestVote`/`AppendEntries`
  messages.
- **Persistent store** — durably writing `HardState` and log entries
  (satisfied in production by `internal/wal`).
- **Clock** — scheduling timers (election timeout, heartbeat) and
  (where genuinely needed) reading logical or wall time.
- **Randomness** — used only for election timeout jitter; injected as
  a source the Raft core calls, not read from a global default.

Production code (`internal/transport`, `internal/wal`, a real clock
adapter) implements these against real sockets/files/OS timers.
`internal/fault` implements them against an in-memory, single-threaded,
deterministic scheduler for tests (see
[`docs/testing-strategy.md`](../testing-strategy.md) §3.1). The **same**
`internal/raft` code runs, unmodified, in both environments.

## Alternatives Considered

1. **Mock/stub individual function calls in tests via a mocking
   framework, without a first-class interface boundary.** Rejected:
   tends to produce brittle, implementation-detail-coupled tests, and
   doesn't naturally give a single, coherent, replayable "simulated
   world" the way a purpose-built deterministic scheduler does — the
   goal is a full, controllable cluster simulation, not isolated
   mocked calls.
2. **Use real time with generous timeouts in tests, accept some
   flakiness.** Rejected: directly conflicts with the project's
   requirement for reproducible distributed-system tests
   ([`docs/testing-strategy.md`](../testing-strategy.md) §3.3);
   flaky tests for consensus-safety-critical code are not an
   acceptable trade-off for a project whose stated purpose includes
   demonstrating real correctness engineering.
3. **Run production and simulated tests against genuinely different
   Raft implementations (a "test-only" simplified Raft, and a
   "production" full Raft).** Rejected: this would mean the tested
   code and the shipped code are not the same code, defeating the
   purpose of testing — any bug specific to the production
   implementation would never be caught.

## Consequences

- `internal/raft` cannot use any concrete package for networking,
  disk, or time directly — this is a standing architectural
  constraint enforced by the package's own interface definitions and
  by review (see [`docs/architecture.md`](../architecture.md) §5).
- The deterministic simulator becomes the primary environment for
  discovering and reproducing Raft-level bugs; real multi-process
  end-to-end tests remain necessary but serve a different purpose
  (validating the production adapters themselves, not Raft safety
  logic — see [`docs/testing-strategy.md`](../testing-strategy.md) §3.3).

## Correctness Implications

- This ADR is the direct enabling mechanism for testing (not
  producing on its own) `STATE MACHINE SAFETY`,
  `RAFT-ELECTION-SAFETY`, `RAFT-LOG-MATCHING`, `LEADER-COMPLETENESS`,
  and `QUORUM-SAFETY` reproducibly.

## Testing and Proof Obligations

- The deterministic simulator itself needs component-level tests
  proving its scheduler behaves as documented (correct message
  ordering control, correct crash/restart semantics, correct seeded
  reproducibility) before it can be trusted as an oracle for Raft
  correctness — a Phase 4 obligation.
- Every scenario in [`docs/scenario-corpus.md`](../scenario-corpus.md)
  §Raft/Replication is expected to run through this simulator.
