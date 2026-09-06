package fault

import "math/rand"

// Rand is a seeded, reproducible raft.Rand adapter
// (docs/testing-strategy.md §3.2): the same seed produces the same
// sequence of election-timeout jitter choices, so a discovered bug's
// triggering run can always be replayed exactly. It wraps a
// non-global *rand.Rand — internal/raft never reads math/rand's
// package-level default source.
type Rand struct {
	r *rand.Rand
}

// NewRand returns a Rand seeded deterministically from seed.
func NewRand(seed int64) *Rand {
	return &Rand{r: rand.New(rand.NewSource(seed))}
}

func (r *Rand) Intn(n int) int { return r.r.Intn(n) }
