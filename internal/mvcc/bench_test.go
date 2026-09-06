// Microbenchmarks for internal/mvcc (docs/roadmap.md Phase 9 §MVCC):
// point read, write (ApplyCommit), conflict-check, and version-chain
// lookup at varying history depth. internal/mvcc has no I/O, so these
// are pure in-memory operations — the numbers here isolate MVCC's own
// cost from WAL/fsync latency, which internal/wal's and internal/node's
// benchmarks measure separately.
//
// Run: go test ./internal/mvcc/... -run '^$' -bench . -benchmem
package mvcc

import (
	"fmt"
	"testing"
)

// storeWithChainDepth builds a Store with one key ("k") whose version
// chain has exactly depth versions, for BenchmarkVisibleByChainDepth.
func storeWithChainDepth(depth int) *Store {
	s := NewStore()
	for i := 1; i <= depth; i++ {
		_ = s.ApplyCommit(uint64(i), []Mutation{{Key: "k", Value: []byte("v")}})
	}
	return s
}

// BenchmarkVisibleByChainDepth measures Store.Visible's binary search
// at varying version-chain depths (docs/roadmap.md §MVCC "version-chain
// lookup at varying history depth") — Visible is O(log depth), so this
// also serves as a regression check against an accidental O(depth)
// change.
func BenchmarkVisibleByChainDepth(b *testing.B) {
	for _, depth := range []int{1, 10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			s := storeWithChainDepth(depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := s.Visible("k", uint64(depth)); !ok {
					b.Fatalf("Visible: key unexpectedly not found")
				}
			}
		})
	}
}

// BenchmarkApplyCommitSingleKey measures the write path for one
// mutation per commit.
func BenchmarkApplyCommitSingleKey(b *testing.B) {
	s := NewStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.ApplyCommit(uint64(i+1), []Mutation{{Key: fmt.Sprintf("k%d", i), Value: []byte("v")}}); err != nil {
			b.Fatalf("ApplyCommit: %v", err)
		}
	}
}

// BenchmarkApplyCommitMultiKey measures a multi-key atomic commit
// (docs/mvcc.md §5 ATOMICITY), varying the mutation-set size.
func BenchmarkApplyCommitMultiKey(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			s := NewStore()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				muts := make([]Mutation, n)
				for k := range muts {
					muts[k] = Mutation{Key: fmt.Sprintf("k%d-%d", i, k), Value: []byte("v")}
				}
				if err := s.ApplyCommit(uint64(i+1), muts); err != nil {
					b.Fatalf("ApplyCommit: %v", err)
				}
			}
		})
	}
}

// BenchmarkCheckConflictsNoConflict measures the first-committer-wins
// conflict check (docs/mvcc.md §4) on the common, non-conflicting path.
func BenchmarkCheckConflictsNoConflict(b *testing.B) {
	s := NewStore()
	for i := 0; i < 1000; i++ {
		_ = s.ApplyCommit(uint64(i+1), []Mutation{{Key: fmt.Sprintf("k%d", i), Value: []byte("v")}})
	}
	muts := []Mutation{{Key: "unwritten-key", Value: []byte("v")}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, conflict := s.CheckConflicts(1_000_000, muts); conflict {
			b.Fatalf("unexpected conflict")
		}
	}
}

// BenchmarkCheckConflictsWithConflict measures the same check on the
// conflicting path (a stale StartSeq against an already-newer commit).
func BenchmarkCheckConflictsWithConflict(b *testing.B) {
	s := NewStore()
	_ = s.ApplyCommit(1, []Mutation{{Key: "k", Value: []byte("v")}})
	muts := []Mutation{{Key: "k", Value: []byte("v2")}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, conflict := s.CheckConflicts(0, muts); !conflict {
			b.Fatalf("expected conflict")
		}
	}
}
