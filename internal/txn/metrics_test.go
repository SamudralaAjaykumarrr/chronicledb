package txn

import (
	"fmt"
	"sync"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

func openTestManager(t *testing.T) *Manager {
	t.Helper()
	w, _, err := wal.Open(t.TempDir(), wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	mgr, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

// TestMetricsTxnConflictsCounted proves TxnConflictsTotal increments
// exactly once per genuine conflict, not on a subsequent retry of the
// same (already-resolved) RequestID (docs/roadmap.md Phase 9
// §Observability tests).
func TestMetricsTxnConflictsCounted(t *testing.T) {
	mgr := openTestManager(t)

	t1 := mgr.Begin()
	if err := t1.Write("k", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("Commit r1: %v", err)
	}

	// A commit whose StartSeq (0) predates t1's already-committed write
	// to the same key conflicts under the write-write conflict rule
	// (docs/mvcc.md §4). Resubmit is used directly (rather than
	// Manager.Begin + Txn.Commit) to construct a StartSeq known to be
	// stale without depending on Begin's own timing.
	if _, err := mgr.Resubmit(fsm.RequestID("r2"), TxnID(999), 0, []mvcc.Mutation{{Key: "k", Value: []byte("v2")}}); err == nil {
		t.Fatalf("expected conflict error")
	}
	if got := mgr.Metrics.TxnConflictsTotal.Value(); got != 1 {
		t.Errorf("TxnConflictsTotal = %d, want 1", got)
	}

	// Retrying the identical RequestID must not double-count the
	// conflict — it is the same logical event, resolved via Precheck.
	if _, err := mgr.Resubmit(fsm.RequestID("r2"), TxnID(999), 0, []mvcc.Mutation{{Key: "k", Value: []byte("v2")}}); err == nil {
		t.Fatalf("expected conflict error on retry")
	}
	if got := mgr.Metrics.TxnConflictsTotal.Value(); got != 1 {
		t.Errorf("TxnConflictsTotal after retry = %d, want still 1", got)
	}
	if got := mgr.Metrics.RequestIDDuplicatesTotal.Value(); got != 1 {
		t.Errorf("RequestIDDuplicatesTotal = %d, want 1", got)
	}
}

// TestMetricsRequestIDDuplicatesCounted proves a genuine committed
// retry increments RequestIDDuplicatesTotal without re-appending to
// the WAL or double-counting a conflict.
func TestMetricsRequestIDDuplicatesCounted(t *testing.T) {
	mgr := openTestManager(t)
	t1 := mgr.Begin()
	if err := t1.Write("k", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	seq1, err := t1.Commit("r1")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	seq2, err := mgr.Resubmit(fsm.RequestID("r1"), t1.ID(), t1.StartSeq(), []mvcc.Mutation{{Key: "k", Value: []byte("v1")}})
	if err != nil {
		t.Fatalf("Resubmit retry: %v", err)
	}
	if seq2.CommitSeq != seq1 {
		t.Errorf("retry CommitSeq = %d, want %d", seq2.CommitSeq, seq1)
	}
	if got := mgr.Metrics.RequestIDDuplicatesTotal.Value(); got != 1 {
		t.Errorf("RequestIDDuplicatesTotal = %d, want 1", got)
	}
	if got := mgr.Metrics.TxnConflictsTotal.Value(); got != 0 {
		t.Errorf("TxnConflictsTotal = %d, want 0", got)
	}
}

// TestMetricsRaceSafeUnderConcurrentCommits runs many concurrent
// distinct-key commits while concurrently reading Metrics, under
// -race (docs/roadmap.md Phase 9 §Observability tests: "metrics/status
// reads are race-safe").
func TestMetricsRaceSafeUnderConcurrentCommits(t *testing.T) {
	mgr := openTestManager(t)
	const goroutines = 20

	stop := make(chan struct{})
	var readerDone sync.WaitGroup
	readerDone.Add(1)
	go func() {
		defer readerDone.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = mgr.Metrics.TxnConflictsTotal.Value()
				_ = mgr.Metrics.RequestIDDuplicatesTotal.Value()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			tx := mgr.Begin()
			key := fmt.Sprintf("key-%d", i) // distinct per goroutine: no genuine conflicts expected
			if err := tx.Write(key, []byte("v")); err != nil {
				t.Errorf("Write: %v", err)
				return
			}
			if _, err := tx.Commit(fsm.RequestID(fmt.Sprintf("req-%d", i))); err != nil {
				t.Errorf("unexpected commit error for distinct key %q: %v", key, err)
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	readerDone.Wait()
}
