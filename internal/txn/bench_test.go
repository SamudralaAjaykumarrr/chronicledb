// Microbenchmarks for internal/txn (docs/roadmap.md Phase 9
// §Transaction commit"): the full standalone commit path (Begin ->
// Write -> Commit, including the real WAL append+fsync boundary
// docs/transactions.md §3-6 require) and the conflict path. These are
// deliberately end-to-end within the package (real internal/wal, real
// internal/fsm/internal/mvcc) rather than mocking persistence — see
// docs/roadmap.md's "Do not cheat benchmarks: ... benchmarking mocked
// persistence while labeling it durable."
//
// Run: go test ./internal/txn/... -run '^$' -bench . -benchmem
package txn

import (
	"fmt"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

func newBenchManager(b *testing.B) *Manager {
	b.Helper()
	w, _, err := wal.Open(b.TempDir(), wal.Options{})
	if err != nil {
		b.Fatalf("wal.Open: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	mgr, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		b.Fatalf("NewManager: %v", err)
	}
	return mgr
}

// BenchmarkCommitSingleKey measures one full standalone transaction
// commit for a single-key write: Begin, Write, Commit — including the
// real durable WAL append+Sync (docs/transactions.md §3-6). Each
// iteration uses a distinct key and RequestID, so every commit is a
// genuine fresh append, never a duplicate short-circuit.
func BenchmarkCommitSingleKey(b *testing.B) {
	mgr := newBenchManager(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := mgr.Begin()
		if err := tx.Write(fmt.Sprintf("k%d", i), []byte("v")); err != nil {
			b.Fatalf("Write: %v", err)
		}
		if _, err := tx.Commit(fsm.RequestID(fmt.Sprintf("req-%d", i))); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

// BenchmarkCommitMultiKey measures a multi-key transaction commit,
// varying mutation-set size.
func BenchmarkCommitMultiKey(b *testing.B) {
	for _, n := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			mgr := newBenchManager(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx := mgr.Begin()
				for k := 0; k < n; k++ {
					if err := tx.Write(fmt.Sprintf("k%d-%d", i, k), []byte("v")); err != nil {
						b.Fatalf("Write: %v", err)
					}
				}
				if _, err := tx.Commit(fsm.RequestID(fmt.Sprintf("req-%d", i))); err != nil {
					b.Fatalf("Commit: %v", err)
				}
			}
		})
	}
}

// BenchmarkCommitConflictPath measures a commit that always conflicts
// (docs/mvcc.md §4): each iteration's transaction is Begun once before
// the timed loop (so StartSeq is fixed and stale), then repeatedly
// retried under a fresh RequestID against the single contended key,
// isolating the conflict-detection cost from the (never-taken) success
// path.
func BenchmarkCommitConflictPath(b *testing.B) {
	mgr := newBenchManager(b)
	// Seed one committed version so every subsequent StartSeq=0 attempt
	// against "k" conflicts.
	seed := mgr.Begin()
	if err := seed.Write("k", []byte("seed")); err != nil {
		b.Fatalf("seed Write: %v", err)
	}
	if _, err := seed.Commit("seed-req"); err != nil {
		b.Fatalf("seed Commit: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqID := fsm.RequestID(fmt.Sprintf("conflict-req-%d", i))
		_, err := mgr.Resubmit(reqID, TxnID(i+1000), 0, []mvcc.Mutation{{Key: "k", Value: []byte("v")}})
		if err == nil {
			b.Fatalf("expected conflict error, got nil")
		}
	}
}
