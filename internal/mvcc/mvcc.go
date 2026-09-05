// Package mvcc implements ChronicleDB's version chains, tombstones, and
// Snapshot Isolation visibility/conflict rules, as specified in
// docs/mvcc.md. Per docs/architecture.md §5, this package imports
// nothing beyond the standard library: it has no knowledge of
// networking, disk I/O, SQL, or Raft, so it can be fully unit- and
// property-tested in isolation.
package mvcc

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrNonMonotonicCommit indicates an internal caller attempted to apply
// a commit whose CommitSeq is not strictly greater than the latest
// existing version for one of its keys. This should be unreachable
// given correct callers (internal/txn serializes all commits through a
// single ordering point), so its presence indicates an internal
// invariant violation, not a legitimate runtime condition.
var ErrNonMonotonicCommit = errors.New("mvcc: non-monotonic commit sequence")

// Version is one committed version of a key: a value or a tombstone,
// produced by exactly one committed transaction's write, tagged with
// the CommitSeq of that transaction's commit (docs/mvcc.md §2).
type Version struct {
	CommitSeq uint64
	Value     []byte
	Tombstone bool
}

// Mutation describes one key's write within a transaction's mutation
// set (docs/transactions.md §3 Mutations). Tombstone=true represents a
// delete; Value is ignored (and should be nil) when Tombstone is true.
type Mutation struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// Store holds every key's version chain: the ordered (by CommitSeq,
// ascending) list of all versions ever committed for that key
// (docs/mvcc.md §2). Chains only grow by appending; existing versions
// are never mutated or removed (MVCC GC is explicitly not implemented
// in V1 — docs/mvcc.md §6, docs/non-goals.md). Store is safe for
// concurrent use by multiple goroutines.
type Store struct {
	mu     sync.RWMutex
	chains map[string][]Version
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{chains: make(map[string][]Version)}
}

// Visible implements the read half of the binding visibility rule
// (docs/mvcc.md §3, steps 2-3): the newest committed version of key
// with CommitSeq <= startSeq. It does not consult any transaction's
// local write set — callers (internal/txn) must check that first, since
// own writes always shadow committed data regardless of CommitSeq
// (docs/mvcc.md §3 step 1).
//
// found is false both when key has never been written and when the
// visible version (if any) is a tombstone — in the Snapshot Isolation
// visibility rule, both cases mean "does not exist as of this
// snapshot" to the caller.
func (s *Store) Visible(key string, startSeq uint64) (value []byte, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chain := s.chains[key]
	// chain is maintained sorted ascending by CommitSeq (ApplyCommit only
	// ever appends a strictly larger CommitSeq), so the newest version
	// with CommitSeq <= startSeq is found by locating the first entry
	// with CommitSeq > startSeq and stepping back one.
	idx := sort.Search(len(chain), func(i int) bool { return chain[i].CommitSeq > startSeq }) - 1
	if idx < 0 {
		return nil, false
	}
	v := chain[idx]
	if v.Tombstone {
		return nil, false
	}
	return v.Value, true
}

// LatestCommitSeq returns the CommitSeq of the newest committed version
// of key (whether a value or a tombstone — both participate in
// conflict detection identically, docs/mvcc.md §2). ok is false if key
// has no committed version at all.
func (s *Store) LatestCommitSeq(key string) (seq uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	chain := s.chains[key]
	if len(chain) == 0 {
		return 0, false
	}
	return chain[len(chain)-1].CommitSeq, true
}

// CheckConflicts implements the first-committer-wins write-write
// conflict rule (docs/mvcc.md §4, ADR-0005): for each mutation, if the
// key's current latest committed CommitSeq exceeds startSeq, the whole
// transaction conflicts. It returns the first conflicting key found (in
// mutation order), for deterministic, reproducible error reporting; ok
// is false if no mutation conflicts.
//
// The whole set is checked under a single read lock so the answer
// reflects one consistent instant of the store, not a torn view across
// separate calls.
func (s *Store) CheckConflicts(startSeq uint64, mutations []Mutation) (key string, latestSeq uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range mutations {
		chain := s.chains[m.Key]
		if len(chain) == 0 {
			continue
		}
		latest := chain[len(chain)-1].CommitSeq
		if latest > startSeq {
			return m.Key, latest, true
		}
	}
	return "", 0, false
}

// ApplyCommit atomically appends one new version per mutation, all
// sharing commitSeq, to their respective version chains
// (docs/mvcc.md §5, ATOMICITY invariant): no reader taking the store's
// read lock can ever observe some-but-not-all of the mutation set
// applied. Callers (internal/txn) are responsible for having already
// run CheckConflicts under the same higher-level serialization point
// that guards this call, so ApplyCommit itself performs no conflict
// re-check — it validates only its own monotonicity precondition, and
// validates it for every mutation before mutating any chain, so a
// violation here never produces a partial update.
func (s *Store) ApplyCommit(commitSeq uint64, mutations []Mutation) error {
	if len(mutations) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range mutations {
		if chain := s.chains[m.Key]; len(chain) > 0 && chain[len(chain)-1].CommitSeq >= commitSeq {
			return fmt.Errorf("%w: key %q latest=%d new=%d", ErrNonMonotonicCommit, m.Key, chain[len(chain)-1].CommitSeq, commitSeq)
		}
	}
	for _, m := range mutations {
		s.chains[m.Key] = append(s.chains[m.Key], Version{CommitSeq: commitSeq, Value: m.Value, Tombstone: m.Tombstone})
	}
	return nil
}
