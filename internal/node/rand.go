package node

import (
	"math/rand"
	"time"
)

// sysRand is the production raft.Rand adapter (ADR-0009): a real,
// non-cryptographic RNG seeded from wall-clock time at process start.
// Election-timeout jitter is not a security boundary (docs/raft.md
// §9.4's Rand doc comment), so math/rand is sufficient. Only ever
// called from Node's single event-loop goroutine, so it needs no
// internal locking of its own.
type sysRand struct {
	r *rand.Rand
}

func newSysRand() *sysRand {
	return &sysRand{r: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (s *sysRand) Intn(n int) int { return s.r.Intn(n) }
