// Package fault implements ChronicleDB's deterministic distributed
// test harness (docs/testing-strategy.md §3, docs/architecture.md §5):
// an in-memory transport with explicit, controllable message delivery
// (drop/duplicate/delay/partition/reorder), a logical clock, an
// in-memory Storage adapter satisfying internal/raft.Storage, and a
// Cluster type that wires real, unmodified internal/raft.Core
// instances together the same way production will (Phase 5), so Raft-
// and replication-level correctness can be proven without real time,
// real sockets, or real disks.
//
// Every run is fully determined by: the initial cluster configuration,
// the seed given to NewRand, and the explicit sequence of
// Cluster/Transport calls a test makes (docs/testing-strategy.md
// §3.2). There is no time.Sleep anywhere in this package; logical time
// only ever advances when a caller explicitly asks it to.
//
// internal/fault is test-only and is never imported by production code
// (docs/architecture.md §5's component map).
package fault
