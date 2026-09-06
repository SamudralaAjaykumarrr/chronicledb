package fault

import (
	"fmt"
	"sync"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// MemoryStorage is an in-memory raft.Storage implementation
// (docs/testing-strategy.md §3.1's "simulated disk"). It models the
// same semantics a real internal/wal-backed adapter must (contiguous
// append, hard-state durability, truncation) without touching a real
// file system — appropriate for Phase 4's deterministic simulator,
// where the goal is proving Core's protocol logic, not exercising real
// disk I/O (that remains internal/wal's own, already-proven
// responsibility; wiring a real internal/wal-backed raft.Storage
// adapter, including the truncate-from-index support Raft's log
// matching repair requires that internal/wal does not yet expose, is
// Phase 5 scope — see the implementation note in docs/raft.md).
//
// A MemoryStorage instance is never discarded across a simulated node
// crash — only the surrounding Node's volatile Core is (see Node.Crash
// / Node.Restart) — so it plays the same durable-vs-volatile role a
// real disk does.
type MemoryStorage struct {
	mu      sync.Mutex
	hs      raft.HardState
	entries []raft.Entry // entries[i] holds log index i+1

	// failAppends/failSetHardState/failTruncate count down remaining
	// injected failures for their respective operation
	// (docs/failure-model.md §1.8 "disk write failure"; Phase 7's
	// deterministic disk-fault-injection capability
	// docs/testing-strategy.md §3.1 already documents but Phase 4 never
	// implemented). Each call while the counter is > 0 decrements it and
	// returns errInjectedFault instead of performing the write; once the
	// counter reaches 0, calls succeed normally again — so a test can
	// inject exactly N consecutive failures (typically 1) at a precise
	// moment without needing wall-clock or call-count coordination
	// elsewhere.
	failAppends      int
	failSetHardState int
	failTruncate     int
}

// errInjectedFault is returned by a MemoryStorage operation a test has
// deliberately configured to fail (FailNextAppends etc.), modeling a
// real disk write/fsync failure (docs/failure-model.md §1.8) without
// touching real disk I/O.
var errInjectedFault = fmt.Errorf("fault: injected storage failure")

// NewMemoryStorage returns an empty MemoryStorage, as a brand-new
// node's durable store would start.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// FailNextAppends configures the next n calls to Append to fail with
// errInjectedFault instead of persisting anything, deterministically
// modeling a disk write/fsync failure at an exact, reproducible point
// in a run (docs/failure-model.md §1.8).
func (s *MemoryStorage) FailNextAppends(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAppends = n
}

// FailNextSetHardState configures the next n calls to SetHardState to
// fail with errInjectedFault. See FailNextAppends.
func (s *MemoryStorage) FailNextSetHardState(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSetHardState = n
}

// FailNextTruncate configures the next n calls to Truncate to fail
// with errInjectedFault. See FailNextAppends.
func (s *MemoryStorage) FailNextTruncate(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failTruncate = n
}

func (s *MemoryStorage) InitialState() (raft.HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, nil
}

func (s *MemoryStorage) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSetHardState > 0 {
		s.failSetHardState--
		return errInjectedFault
	}
	s.hs = hs
	return nil
}

func (s *MemoryStorage) LastIndex() (raft.Index, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return raft.Index(len(s.entries)), nil
}

func (s *MemoryStorage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo < 1 {
		lo = 1
	}
	maxHi := raft.Index(len(s.entries) + 1)
	if hi > maxHi {
		hi = maxHi
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]raft.Entry, hi-lo)
	copy(out, s.entries[lo-1:hi-1])
	return out, nil
}

func (s *MemoryStorage) Truncate(fromIndex raft.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failTruncate > 0 {
		s.failTruncate--
		return errInjectedFault
	}
	if fromIndex < 1 {
		return fmt.Errorf("fault: MemoryStorage.Truncate: fromIndex must be >= 1, got %d", fromIndex)
	}
	if int(fromIndex-1) < len(s.entries) {
		s.entries = s.entries[:fromIndex-1]
	}
	return nil
}

func (s *MemoryStorage) Append(entries []raft.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failAppends > 0 {
		s.failAppends--
		return errInjectedFault
	}
	want := raft.Index(len(s.entries) + 1)
	for _, e := range entries {
		if e.Index != want {
			return fmt.Errorf("fault: MemoryStorage.Append: non-contiguous append (got index %d, want %d)", e.Index, want)
		}
		want++
	}
	s.entries = append(s.entries, entries...)
	return nil
}
