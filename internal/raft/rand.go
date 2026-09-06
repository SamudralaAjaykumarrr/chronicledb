package raft

// Rand is the randomness source Core uses for election-timeout jitter
// (docs/raft.md §2, docs/adr/0009): "injected as a source the Raft
// core calls, not read from a global default." Unlike Storage, Core
// does call this interface directly — ADR-0009 explicitly scopes
// Randomness this way, since jitter selection is a pure, reproducible
// function of the sequence of Intn calls, not I/O.
//
// A production adapter wraps a real (non-cryptographic — jitter is not
// a security boundary) RNG. internal/fault's deterministic simulator
// wraps a seeded math/rand source so a run is fully reproducible from
// its seed (docs/testing-strategy.md §3.2).
type Rand interface {
	// Intn returns a pseudo-random number in [0, n). n is always > 0.
	Intn(n int) int
}
