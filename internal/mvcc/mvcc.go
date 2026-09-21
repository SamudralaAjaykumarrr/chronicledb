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
	"strings"
	"sync"
	"sync/atomic"
)

// ErrNonMonotonicCommit indicates an internal caller attempted to apply
// a commit whose CommitSeq is not strictly greater than the latest
// existing version for one of its keys. This should be unreachable
// given correct callers (internal/txn serializes all commits through a
// single ordering point), so its presence indicates an internal
// invariant violation, not a legitimate runtime condition.
var ErrNonMonotonicCommit = errors.New("mvcc: non-monotonic commit sequence")

// ErrSnapshotTooOld is returned by every committed-read entry point
// (Visible, ScanVisible) when startSeq is below the Store's applied GC
// watermark (docs/v0.6.0-plan.md §15.1/§15.2, GC SAFETY): the version
// such a snapshot would be entitled to see may already have been
// reclaimed. Refusing is a correctness requirement, not a convenience —
// silently answering from the surviving chain would be an undetectable
// Snapshot Isolation violation (§15.1's analysis). Both entry points
// check this under the same lock that reads the chain/watermark, so
// there is no window in which a concurrent watermark advance lets a
// stale read slip through.
var ErrSnapshotTooOld = errors.New("mvcc: snapshot older than the GC horizon")

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
// (docs/mvcc.md §2). Chains grow by appending; MVCC GC (docs/mvcc.md
// §6, docs/v0.6.0-plan.md Part B) additionally removes superseded
// versions from the front of a chain, deterministically and only
// inside internal/fsm.Apply — Store itself performs no GC decision-
// making of its own (ReclaimKey does exactly what its caller tells it
// to; see its own doc comment). Store is safe for concurrent use by
// multiple goroutines.
type Store struct {
	mu     sync.RWMutex
	chains map[string][]Version
	// orderedKeys is chains' key set maintained in ascending sorted
	// order via binary-search insertion (docs/v0.6.0-plan.md §14.4,
	// fact 8b): what makes KeysFrom's bounded traversal possible
	// without a per-call full sort. The key set never shrinks under GC
	// (§14.1 never removes a key's newest version), so this needs an
	// insertion path but no deletion path.
	orderedKeys []string
	// gcWatermark is the applied GC horizon (docs/v0.6.0-plan.md §15.2):
	// monotonically non-decreasing, read and written under mu alongside
	// the chains it guards so a horizon check and the chain read it
	// guards are one atomic instant.
	gcWatermark uint64

	// skipHorizonGuardForTest, when set via SetSkipHorizonGuardForTest,
	// disables the startSeq < gcWatermark check in Visible/ScanVisible —
	// SL-3's negative control (docs/v0.6.0-plan.md §29): with it set, a
	// property test driving GC SAFETY's positive property (SL-1) must
	// detect the resulting silent stale read. Never set in production.
	skipHorizonGuardForTest atomic.Bool
}

// SetSkipHorizonGuardForTest is SL-3's negative-control hook (see
// skipHorizonGuardForTest's doc comment). Test-only; production code
// never calls it.
func (s *Store) SetSkipHorizonGuardForTest(skip bool) {
	s.skipHorizonGuardForTest.Store(skip)
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{chains: make(map[string][]Version)}
}

// insertOrderedKeyLocked inserts key into s.orderedKeys, preserving
// sorted order, via binary search. Caller must hold s.mu (write lock).
// Must only be called for a key not already present — every call site
// already checks this as part of deciding a chain is new.
func (s *Store) insertOrderedKeyLocked(key string) {
	idx := sort.SearchStrings(s.orderedKeys, key)
	s.orderedKeys = append(s.orderedKeys, "")
	copy(s.orderedKeys[idx+1:], s.orderedKeys[idx:])
	s.orderedKeys[idx] = key
}

// KeysFrom returns at most limit keys in ascending order, strictly
// after cursor ("" means "from the beginning"), without sorting or
// scanning the whole key set (docs/v0.6.0-plan.md §14.4) — the
// traversal cost is a function of limit, not of total key count. Used
// by ApplyAdvanceGCWatermark's bounded, resumable keyspace walk.
func (s *Store) KeysFrom(cursor string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := sort.SearchStrings(s.orderedKeys, cursor)
	if start < len(s.orderedKeys) && s.orderedKeys[start] == cursor {
		start++
	}
	if start >= len(s.orderedKeys) {
		return nil
	}
	end := start + limit
	if end > len(s.orderedKeys) {
		end = len(s.orderedKeys)
	}
	out := make([]string, end-start)
	copy(out, s.orderedKeys[start:end])
	return out
}

// ReclaimKey removes every version of key superseded per docs/mvcc.md
// §6's rule, verbatim (docs/v0.6.0-plan.md §14.1): a version
// (CommitSeq=c) may be removed iff there exists a version (CommitSeq=
// c') of the same key with c < c' <= w. The newest version of a key is
// never removed (there is, by definition, no later c' to satisfy the
// predicate for it) — nothing else is ever removed, tombstone or not
// (S-3: a tombstone is a version, reclaimed under exactly this
// predicate, no special case).
//
// budget bounds how many versions this call removes; ReclaimKey removes
// the OLDEST superseded versions first (chain is sorted ascending by
// CommitSeq) up to budget, and returns the actual count removed. The
// caller (ApplyAdvanceGCWatermark) is responsible for the separate
// keys-examined bound (KeysFrom's limit) — this method's own cost is
// O(1) beyond the removed count itself, never a function of total chain
// length beyond what is actually being removed.
func (s *Store) ReclaimKey(key string, w uint64, budget int) int {
	if budget <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	chain := s.chains[key]
	if len(chain) < 2 {
		return 0 // a single (or absent) version is always the newest: never removed
	}
	// idx: the largest index with CommitSeq <= w (or -1 if none). Every
	// version strictly before idx has some later version (at least
	// chain[idx] itself) with CommitSeq <= w, so is reclaimable; idx
	// itself is never reclaimable (by construction, no later version
	// also has CommitSeq <= w — that is exactly what makes idx maximal).
	idx := sort.Search(len(chain), func(i int) bool { return chain[i].CommitSeq > w }) - 1
	if idx <= 0 {
		return 0
	}
	n := idx // versions [0, idx-1], i.e. idx many
	if n > budget {
		n = budget
	}
	// A fresh backing array (not chain[n:], which would alias and leak
	// the reclaimed versions' memory via the old array, and would let a
	// concurrent reader's already-taken slice header keep observing a
	// mutated array if this were ever changed in place).
	s.chains[key] = append([]Version(nil), chain[n:]...)
	return n
}

// SetGCWatermark sets the Store's applied horizon, monotonically (a
// lower value is a no-op) — matching docs/v0.6.0-plan.md §14.4's
// monotone max() rule at the single point that must agree with it: the
// value the read-side guard (Visible/ScanVisible) refuses below.
//
// Called from exactly two places (docs/v0.6.0-plan.md §15.2b): from
// fsm.Apply's ApplyAdvanceGCWatermark (the live, replicated path), and
// from RestoreStore's caller immediately after constructing a fresh
// Store from decoded snapshot state — a restored Store must never run
// with the guard silently disabled at watermark 0 while FSM.gcWatermark
// already reflects the source's real value. See RestoreStore's own doc
// comment for the exact restore-path sequencing this requires.
func (s *Store) SetGCWatermark(w uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w > s.gcWatermark {
		s.gcWatermark = w
	}
}

// GCWatermark returns the Store's currently applied GC horizon.
func (s *Store) GCWatermark() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gcWatermark
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
//
// err is ErrSnapshotTooOld when startSeq is below the applied GC
// horizon (docs/v0.6.0-plan.md §15.1/§15.2) — checked under the same
// lock as the chain read, so the check and the read are one atomic
// instant. This is SNAPSHOT HORIZON ENFORCEMENT's structural boundary:
// the one place every single-key committed read passes through.
func (s *Store) Visible(key string, startSeq uint64) (value []byte, found bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if startSeq < s.gcWatermark && !s.skipHorizonGuardForTest.Load() {
		return nil, false, ErrSnapshotTooOld
	}
	chain := s.chains[key]
	// chain is maintained sorted ascending by CommitSeq (ApplyCommit only
	// ever appends a strictly larger CommitSeq), so the newest version
	// with CommitSeq <= startSeq is found by locating the first entry
	// with CommitSeq > startSeq and stepping back one.
	idx := sort.Search(len(chain), func(i int) bool { return chain[i].CommitSeq > startSeq }) - 1
	if idx < 0 {
		return nil, false, nil
	}
	v := chain[idx]
	if v.Tombstone {
		return nil, false, nil
	}
	return v.Value, true, nil
}

// KV is one key/value pair returned by ScanVisible.
type KV struct {
	Key   string
	Value []byte
}

// ScanVisible returns every key with the given prefix currently
// committed-visible as of startSeq, sorted ascending by key
// (docs/v0.6.0-plan.md §15.2a): the prefix filter and the per-key
// visibility search both happen here, inside internal/mvcc, under the
// same RLock that checks startSeq against the GC horizon — exactly as
// Visible does, and for the identical reason (SNAPSHOT HORIZON
// ENFORCEMENT's structural boundary must be the same one place for
// every committed-read entry point, not duplicated in a caller
// package). Callers (internal/sql) merge this with a transaction's own
// local write set — that merge stays outside this package, where it
// belongs.
//
// This is a full scan of the entire store, filtered down to prefix,
// not an indexed range scan — docs/sql.md §5.2's already-accepted,
// documented limitation of the SQL subset built on it; ScanVisible does
// not change that complexity, only where the horizon check lives.
func (s *Store) ScanVisible(prefix string, startSeq uint64) ([]KV, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if startSeq < s.gcWatermark && !s.skipHorizonGuardForTest.Load() {
		return nil, ErrSnapshotTooOld
	}
	var out []KV
	for _, k := range s.orderedKeys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		chain := s.chains[k]
		idx := sort.Search(len(chain), func(i int) bool { return chain[i].CommitSeq > startSeq }) - 1
		if idx < 0 {
			continue
		}
		v := chain[idx]
		if v.Tombstone {
			continue
		}
		out = append(out, KV{Key: k, Value: v.Value})
	}
	return out, nil
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

// KeyChain pairs a key with its full version chain, as returned by
// Export (docs/snapshots.md §2's "committed MVCC data" content). It is
// the unit of transfer between a live Store and a serialized state-
// machine snapshot.
type KeyChain struct {
	Key      string
	Versions []Version
}

// Export returns a deep copy of every key's version chain, sorted
// ascending by Key so a caller serializing this into a deterministic
// byte stream (internal/fsm's state-machine snapshot encoding) never
// depends on Go's unordered map iteration (docs/invariants.md
// DETERMINISM BOUNDARY's spirit — snapshot bytes are not on Apply's own
// determinism-critical path, but reproducible encoding is required for
// two independently-constructed FSMs to produce byte-identical
// snapshots, a useful and tested property). Each chain's Versions slice
// remains sorted ascending by CommitSeq, exactly as Store maintains it
// internally.
//
// Export's only legitimate caller is internal/fsm's snapshot encoding,
// and it must return every version regardless of any horizon
// (docs/v0.6.0-plan.md §15.2a): unlike every other read path, it is not
// itself a committed-READ for some transaction's snapshot — it is the
// full durable state a future restore must reconstruct from, GC horizon
// and all. TestExportOnlyCalledFromSnapshotEncoding (an AST test,
// mirroring TestControlKindRangesNeverCollide's class) asserts no
// package other than internal/fsm references this method — the
// bypass that made SL-6's gap possible in the first place
// (internal/sql's mergeScan/visibleInChain, deleted) must not
// reappear.
func (s *Store) Export() []KeyChain {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]KeyChain, 0, len(s.chains))
	for k, v := range s.chains {
		out = append(out, KeyChain{Key: k, Versions: append([]Version(nil), v...)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// RestoreStore reconstructs a Store directly from previously-exported
// key chains (docs/recovery.md snapshot restore path), bypassing
// ApplyCommit's monotonicity/atomicity machinery entirely: a snapshot
// restore is not a sequence of individual commits, it is the
// installation of already-decided, already-checksum-verified state
// (docs/snapshots.md §5's validation is the caller's responsibility,
// performed before this is ever called — RestoreStore itself does not
// re-validate CommitSeq ordering within a chain, trusting the caller's
// own checksum-verified decode). Each chain's Versions must already be
// sorted ascending by CommitSeq, matching what Export produces —
// Visible's binary search depends on this invariant.
//
// gcWatermark is the restored Store's initial GC horizon, required —
// not optional or defaulted — as a parameter (docs/v0.6.0-plan.md
// §15.2b): it is impossible to construct a restored Store without one,
// which is what closes the hazard an earlier draft left open ("called
// only from fsm.Apply" as a mere doc comment would have silently left
// the read-side guard at watermark 0 after every restart/InstallSnapshot/
// backup-restore, since this function otherwise builds a fresh Store
// from nothing). The caller (fsm.DecodeState) passes the value decoded
// from the generation-3 trailing block, before the FSM carrying this
// Store is ever returned — the binding property, stated so it can be
// tested rather than reviewed: Store.GCWatermark() == FSM.gcWatermark
// at every instant, on every node, including immediately after
// DecodeState.
//
// The outer key order is re-sorted here unconditionally (a one-time,
// O(n log n) cost at restore time — never on the GC Apply hot path
// §14.4 bounds) rather than trusted from the caller, so orderedKeys'
// invariant holds regardless of the exact order a snapshot decoder
// happens to preserve.
func RestoreStore(chains []KeyChain, gcWatermark uint64) *Store {
	m := make(map[string][]Version, len(chains))
	keys := make([]string, 0, len(chains))
	for _, kc := range chains {
		m[kc.Key] = kc.Versions
		keys = append(keys, kc.Key)
	}
	sort.Strings(keys)
	return &Store{chains: m, orderedKeys: keys, gcWatermark: gcWatermark}
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
		if _, exists := s.chains[m.Key]; !exists {
			s.insertOrderedKeyLocked(m.Key)
		}
		s.chains[m.Key] = append(s.chains[m.Key], Version{CommitSeq: commitSeq, Value: m.Value, Tombstone: m.Tombstone})
	}
	return nil
}
