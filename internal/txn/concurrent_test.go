package txn

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
)

// TestConcurrentNonConflictingWritersAllCommit runs many goroutines,
// each writing its own disjoint key, all beginning concurrently. Every
// one must commit successfully (CONFLICT-CORRECTNESS: non-overlapping
// writers are never rejected) and every write must be durably visible
// afterward. Run with -race to prove the concurrent Begin/Commit path
// is race-safe.
func TestConcurrentNonConflictingWritersAllCommit(t *testing.T) {
	m, _ := newTestManager(t)
	const n = 50

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := m.Begin()
			key := fmt.Sprintf("key-%d", i)
			if err := tx.Write(key, []byte(fmt.Sprintf("val-%d", i))); err != nil {
				errs[i] = err
				return
			}
			requestID := fsm.RequestID(fmt.Sprintf("req-%d", i))
			if _, err := tx.Commit(requestID); err != nil {
				errs[i] = err
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: unexpected error: %v", i, err)
		}
	}

	reader := m.Begin()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)
		v, found, err := reader.Read(key)
		if err != nil || !found || string(v) != want {
			t.Fatalf("Read(%s) = %q,%v,%v, want %q,true,nil", key, v, found, err, want)
		}
	}
}

// TestConcurrentConflictingWritersExactlyOneWins has many goroutines
// begin from the same StartSeq and all write the SAME key. Exactly one
// must commit (first-committer-wins); every other must fail with
// ErrConflict. Run with -race.
func TestConcurrentConflictingWritersExactlyOneWins(t *testing.T) {
	m, _ := newTestManager(t)
	const n = 50

	txns := make([]*Txn, n)
	for i := 0; i < n; i++ {
		txns[i] = m.Begin()
		if err := txns[i].Write("K", []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		committed int
		conflicts int
		otherErrs int
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			requestID := fsm.RequestID(fmt.Sprintf("req-%d", i))
			_, err := txns[i].Commit(requestID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				committed++
			case errors.Is(err, ErrConflict):
				conflicts++
			default:
				otherErrs++
			}
		}(i)
	}
	wg.Wait()

	if committed != 1 {
		t.Fatalf("committed = %d, want exactly 1 (first-committer-wins)", committed)
	}
	if otherErrs != 0 {
		t.Fatalf("otherErrs = %d, want 0", otherErrs)
	}
	if conflicts != n-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts, n-1)
	}

	reader := m.Begin()
	if _, found, _ := reader.Read("K"); !found {
		t.Fatal("Read(K) after concurrent conflicting commits: not found, want the single winner's value present")
	}
}

// TestConcurrentReadsDuringCommit exercises readers running
// concurrently with an in-flight writer (different keys), proving reads
// never block on or race with the commit pipeline. Run with -race.
func TestConcurrentReadsDuringCommit(t *testing.T) {
	m, _ := newTestManager(t)
	setup := m.Begin()
	if err := setup.Write("K", []byte("v0")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			tx := m.Begin()
			if err := tx.Write(fmt.Sprintf("writer-key-%d", i), []byte("x")); err != nil {
				return
			}
			requestID := fsm.RequestID(fmt.Sprintf("writer-req-%d", i))
			if _, err := tx.Commit(requestID); err != nil {
				return
			}
		}
	}()

	for i := 0; i < 100; i++ {
		reader := m.Begin()
		if _, _, err := reader.Read("K"); err != nil {
			t.Errorf("Read(K): %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
