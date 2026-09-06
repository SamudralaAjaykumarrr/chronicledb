// Package raft implements ChronicleDB's Raft consensus core
// (docs/raft.md, ADR-0008, ADR-0009): a pure, deterministic,
// input/output-driven state machine — Step(input) -> output — with no
// direct network I/O, disk I/O, wall-clock reads, or global
// randomness.
//
// Every decision Core.Step makes is a function of the Core's own prior
// state plus the single Input it is given. Production behavior (real
// sockets, real timers, real fsync latency) and test/simulation
// behavior (docs/testing-strategy.md §3) are both provided by code
// outside this package that drives Step and supplies its declared
// interfaces (Storage, Rand) — internal/fault today (Phase 4);
// internal/transport, an internal/wal-backed Storage adapter, and a
// real clock adapter in internal/node (Phase 5). The same Core code
// runs, unmodified, in both environments (ADR-0009).
//
// internal/raft must not import internal/transport, internal/wal, or
// internal/fsm (docs/architecture.md §5): it depends only on the small
// interfaces it defines itself (Storage, Rand). It has no knowledge of
// TxnID, RequestID, MVCC, or SQL — it replicates opaque command bytes
// (Entry.Data) and hands committed entries back to its caller in
// order; turning a committed entry into database state is
// internal/fsm's job (docs/raft.md §6).
package raft
