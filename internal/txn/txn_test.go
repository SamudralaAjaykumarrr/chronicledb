package txn

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// newTestManager opens a fresh WAL-backed Manager in a temp directory.
func newTestManager(t *testing.T) (*Manager, *wal.WAL) {
	t.Helper()
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m, w
}

// TX-1: Begin/read/write/commit (docs/scenario-corpus.md §Transactions).
func TestTX1_BeginReadWriteCommit(t *testing.T) {
	m, _ := newTestManager(t)

	// Establish K's initial committed value v0.
	setup := m.Begin()
	if err := setup.Write("K", []byte("v0")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("Commit setup: %v", err)
	}

	tx := m.Begin()
	v, found, err := tx.Read("K")
	if err != nil || !found || string(v) != "v0" {
		t.Fatalf("Read(K) = %q,%v,%v, want v0,true,nil", v, found, err)
	}
	if err := tx.Write("K", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	seq, err := tx.Commit("r1")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if tx.State() != StateCommitted {
		t.Fatalf("State() = %v, want committed", tx.State())
	}

	// A new transaction with StartSeq >= CommitSeq sees v1.
	reader := m.Begin()
	if reader.StartSeq() < seq {
		t.Fatalf("reader StartSeq %d < CommitSeq %d", reader.StartSeq(), seq)
	}
	v, found, err = reader.Read("K")
	if err != nil || !found || string(v) != "v1" {
		t.Fatalf("Read(K) after commit = %q,%v,%v, want v1,true,nil", v, found, err)
	}
}

// TX-2: Aborted transaction leaves no trace.
func TestTX2_AbortedTransaction(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Write("K", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if tx.State() != StateAborted {
		t.Fatalf("State() = %v, want aborted", tx.State())
	}

	reader := m.Begin()
	if _, found, err := reader.Read("K"); err != nil || found {
		t.Fatalf("Read(K) after abort: found=%v err=%v, want false,nil", found, err)
	}
}

// TX-3: Multi-key atomic commit (no conflicts): all three keys gain a
// version at the same CommitSeq.
func TestTX3_MultiKeyAtomicCommit(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Write("K1", []byte("a")); err != nil {
		t.Fatalf("Write K1: %v", err)
	}
	if err := tx.Write("K2", []byte("b")); err != nil {
		t.Fatalf("Write K2: %v", err)
	}
	if err := tx.Delete("K3"); err != nil {
		t.Fatalf("Delete K3: %v", err)
	}
	seq, err := tx.Commit("r1")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reader := m.Begin()
	for _, tc := range []struct {
		key       string
		wantValue string
		wantFound bool
	}{
		{"K1", "a", true},
		{"K2", "b", true},
		{"K3", "", false},
	} {
		v, found, err := reader.Read(tc.key)
		if err != nil || found != tc.wantFound || (found && string(v) != tc.wantValue) {
			t.Fatalf("Read(%s) = %q,%v,%v, want %q,%v,nil", tc.key, v, found, err, tc.wantValue, tc.wantFound)
		}
	}
	if seq == 0 {
		t.Fatal("CommitSeq is zero for a successful commit")
	}
}

// TX-4: Concurrent (sequential, same StartSeq) non-conflicting
// transactions both commit successfully.
func TestTX4_ConcurrentNonConflictingTransactions(t *testing.T) {
	m, _ := newTestManager(t)

	t1 := m.Begin()
	t2 := m.Begin()
	if t1.StartSeq() != t2.StartSeq() {
		t.Fatalf("t1.StartSeq=%d != t2.StartSeq=%d", t1.StartSeq(), t2.StartSeq())
	}
	if err := t1.Write("A", []byte("1")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("B", []byte("2")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}
	if _, err := t2.Commit("r2"); err != nil {
		t.Fatalf("t2.Commit: %v", err)
	}

	reader := m.Begin()
	if v, found, _ := reader.Read("A"); !found || string(v) != "1" {
		t.Fatalf("Read(A) = %q,%v, want 1,true", v, found)
	}
	if v, found, _ := reader.Read("B"); !found || string(v) != "2" {
		t.Fatalf("Read(B) = %q,%v, want 2,true", v, found)
	}
}

// TX-5: Concurrent conflicting transactions — first-committer-wins
// (exact docs/mvcc.md §4 example).
func TestTX5_ConcurrentConflictingTransactions(t *testing.T) {
	m, _ := newTestManager(t)

	t1 := m.Begin()
	t2 := m.Begin()
	if err := t1.Write("K", []byte("from-t1")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("K", []byte("from-t2")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}

	c1, err := t1.Commit("r1")
	if err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}

	_, err = t2.Commit("r2")
	if err == nil {
		t.Fatal("t2.Commit: expected conflict error, got nil")
	}
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("t2.Commit err = %v, want ErrConflict", err)
	}
	if t2.State() != StateAborted {
		t.Fatalf("t2.State() = %v, want aborted", t2.State())
	}

	reader := m.Begin()
	v, found, _ := reader.Read("K")
	if !found || string(v) != "from-t1" {
		t.Fatalf("Read(K) = %q,%v, want from-t1,true (t1's write must stand)", v, found)
	}
	if c1 == 0 {
		t.Fatal("t1's CommitSeq is zero")
	}
}

// TX-6: Read snapshot remains stable across a concurrent commit.
func TestTX6_ReadSnapshotStable(t *testing.T) {
	m, _ := newTestManager(t)
	setup := m.Begin()
	if err := setup.Write("K", []byte("v0")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	t1 := m.Begin()
	v, found, _ := t1.Read("K")
	if !found || string(v) != "v0" {
		t.Fatalf("t1 first Read(K) = %q,%v, want v0,true", v, found)
	}

	t2 := m.Begin()
	if err := t2.Write("K", []byte("v1")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t2.Commit("r2"); err != nil {
		t.Fatalf("t2.Commit: %v", err)
	}

	v, found, _ = t1.Read("K")
	if !found || string(v) != "v0" {
		t.Fatalf("t1 second Read(K) after t2 committed = %q,%v, want v0,true (snapshot must not change)", v, found)
	}
}

// TX-7: Delete/tombstone visibility.
func TestTX7_DeleteTombstoneVisibility(t *testing.T) {
	m, _ := newTestManager(t)
	setup := m.Begin()
	if err := setup.Write("K", []byte("v0")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	del := m.Begin()
	if err := del.Delete("K"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := del.Commit("del"); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	reader := m.Begin()
	if _, found, err := reader.Read("K"); err != nil || found {
		t.Fatalf("Read(K) after committed delete: found=%v err=%v, want false,nil", found, err)
	}
}

// TX-8: Snapshot Isolation write-skew example (docs/mvcc.md §1.1),
// proving ChronicleDB is Snapshot Isolation and NOT Serializable
// (ISOLATION-TRUTHFULNESS, docs/invariants.md). Both transactions must
// commit successfully even though the resulting state violates the
// application-level invariant x + y >= 0 — this is documented, expected
// SI behavior, and this test exists specifically to keep it
// demonstrated. A "fix" that makes this test start failing (one commit
// starts getting rejected) would itself be an undocumented, unproven
// escalation to a stronger isolation level and must not be made without
// a new ADR (docs/mvcc.md §1.1, ADR-0004).
func TestTX8_SnapshotIsolationWriteSkew(t *testing.T) {
	m, _ := newTestManager(t)
	setup := m.Begin()
	if err := setup.Write("x", []byte("10")); err != nil {
		t.Fatalf("Write x: %v", err)
	}
	if err := setup.Write("y", []byte("10")); err != nil {
		t.Fatalf("Write y: %v", err)
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("Commit setup: %v", err)
	}

	t1 := m.Begin()
	t2 := m.Begin()

	xv, _, err := t1.Read("x")
	if err != nil {
		t.Fatalf("t1.Read(x): %v", err)
	}
	yv, _, err := t1.Read("y")
	if err != nil {
		t.Fatalf("t1.Read(y): %v", err)
	}
	if string(xv) != "10" || string(yv) != "10" {
		t.Fatalf("t1 initial read x=%q y=%q, want 10,10", xv, yv)
	}
	// invariant check: x + y >= 0 holds (10 + 10 = 20); T1 writes x -= 15.
	if err := t1.Write("x", []byte("-5")); err != nil {
		t.Fatalf("t1.Write(x): %v", err)
	}

	xv2, _, err := t2.Read("x")
	if err != nil {
		t.Fatalf("t2.Read(x): %v", err)
	}
	yv2, _, err := t2.Read("y")
	if err != nil {
		t.Fatalf("t2.Read(y): %v", err)
	}
	if string(xv2) != "10" || string(yv2) != "10" {
		t.Fatalf("t2 initial read x=%q y=%q, want 10,10", xv2, yv2)
	}
	// T2 independently also sees the invariant hold, and writes y -= 15.
	if err := t2.Write("y", []byte("-5")); err != nil {
		t.Fatalf("t2.Write(y): %v", err)
	}

	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1.Commit: expected COMMITTED (disjoint write sets), got error: %v", err)
	}
	if _, err := t2.Commit("r2"); err != nil {
		t.Fatalf("t2.Commit: expected COMMITTED (disjoint write sets), got error: %v", err)
	}

	reader := m.Begin()
	xf, _, _ := reader.Read("x")
	yf, _, _ := reader.Read("y")
	if string(xf) != "-5" || string(yf) != "-5" {
		t.Fatalf("final x=%q y=%q, want -5,-5", xf, yf)
	}
	// x + y = -10 < 0: the application invariant is violated, as
	// documented. This is the expected write-skew outcome, not a bug.
}

// --- Lifecycle / error-semantics tests ---

func TestLifecycle_WriteAfterCommitRejected(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Write("K", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := tx.Commit("r1"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tx.Write("K", []byte("v2")); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("Write after commit: err = %v, want ErrAlreadyCommitted", err)
	}
	if err := tx.Delete("K"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("Delete after commit: err = %v, want ErrAlreadyCommitted", err)
	}
	if _, _, err := tx.Read("K"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("Read after commit: err = %v, want ErrAlreadyCommitted", err)
	}
	if err := tx.Abort(); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("Abort after commit: err = %v, want ErrAlreadyCommitted", err)
	}
}

func TestLifecycle_CommitTwiceRejected(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Write("K", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := tx.Commit("r1"); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	// A second Commit call on the SAME Txn object (whether reusing the
	// same RequestID or a new one) is rejected at the session-lifecycle
	// level, before RequestID identity is even consulted: the Txn
	// itself is already terminal.
	if _, err := tx.Commit("r1"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("second Commit (same RequestID): err = %v, want ErrAlreadyCommitted", err)
	}
	if _, err := tx.Commit("r2"); !errors.Is(err, ErrAlreadyCommitted) {
		t.Fatalf("second Commit (different RequestID): err = %v, want ErrAlreadyCommitted", err)
	}
}

func TestLifecycle_CommitAfterAbortRejected(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Write("K", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := tx.Commit("r1"); !errors.Is(err, ErrAlreadyAborted) {
		t.Fatalf("Commit after abort: err = %v, want ErrAlreadyAborted", err)
	}
}

func TestLifecycle_AbortTwiceRejected(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Abort(); err != nil {
		t.Fatalf("first Abort: %v", err)
	}
	if err := tx.Abort(); !errors.Is(err, ErrAlreadyAborted) {
		t.Fatalf("second Abort: err = %v, want ErrAlreadyAborted", err)
	}
}

func TestLifecycle_OperationsAfterConflictAbortRejected(t *testing.T) {
	m, _ := newTestManager(t)
	t1 := m.Begin()
	t2 := m.Begin()
	if err := t1.Write("K", []byte("a")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("K", []byte("b")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}
	if _, err := t2.Commit("r2"); err == nil {
		t.Fatal("t2.Commit: expected conflict error")
	}
	// t2 is now implicitly terminal (aborted) after a failed commit.
	if err := t2.Write("M", []byte("x")); !errors.Is(err, ErrAlreadyAborted) {
		t.Fatalf("t2.Write after conflict-abort: err = %v, want ErrAlreadyAborted", err)
	}
}

func TestReadOnlyCommitSucceedsWithoutWALGrowth(t *testing.T) {
	m, w := newTestManager(t)
	before := w.NextIndex()
	tx := m.Begin()
	if _, _, err := tx.Read("nonexistent"); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := tx.Commit("r1"); err != nil {
		t.Fatalf("Commit (read-only): %v", err)
	}
	after := w.NextIndex()
	if after != before {
		t.Fatalf("NextIndex changed for a read-only commit: before=%d after=%d", before, after)
	}
}

// TestConflictingCommitAppendsToWALForDurableOutcome: Phase 3 changes
// this from Phase 2's behavior (see docs/transactions.md §9/§10). A
// FRESH (never-seen) RequestID that turns out to conflict must still
// be durably appended: this is what makes the RequestID's ABORTED
// outcome reconstructable by recovery after a restart
// (REQUEST-OUTCOME-STABILITY must hold for ABORTED exactly as much as
// for COMMITTED — docs/invariants.md). See
// TestRetryDoesNotAppendToWAL for the still-true optimization that a
// *retry* of an already-known RequestID never appends again.
func TestConflictingCommitAppendsToWALForDurableOutcome(t *testing.T) {
	m, w := newTestManager(t)
	t1 := m.Begin()
	t2 := m.Begin()
	if err := t1.Write("K", []byte("a")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("K", []byte("b")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("winner"); err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}
	before := w.NextIndex()
	if _, err := t2.Commit("loser"); err == nil {
		t.Fatal("t2.Commit: expected conflict")
	}
	after := w.NextIndex()
	if after != before+1 {
		t.Fatalf("a fresh conflicting commit did not append exactly one WAL record: before=%d after=%d", before, after)
	}
}

// TestRetryDoesNotAppendToWAL: retrying an already-known RequestID
// (whether its original outcome was COMMITTED or ABORTED) never
// appends to the WAL again (docs/transactions.md §6's documented
// optimization).
func TestRetryDoesNotAppendToWAL(t *testing.T) {
	m, w := newTestManager(t)

	committed := m.Begin()
	committedTxnID, committedStartSeq := committed.ID(), committed.StartSeq()
	committedMutations := []mvcc.Mutation{{Key: "A", Value: []byte("v")}}
	if err := committed.Write("A", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wantSeq, err := committed.Commit("committed-req")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	winner := m.Begin()
	loser := m.Begin()
	loserTxnID, loserStartSeq := loser.ID(), loser.StartSeq()
	loserMutations := []mvcc.Mutation{{Key: "B", Value: []byte("l")}}
	if err := winner.Write("B", []byte("w")); err != nil {
		t.Fatalf("winner.Write: %v", err)
	}
	if err := loser.Write("B", []byte("l")); err != nil {
		t.Fatalf("loser.Write: %v", err)
	}
	if _, err := winner.Commit("winner-req"); err != nil {
		t.Fatalf("winner.Commit: %v", err)
	}
	if _, err := loser.Commit("loser-req"); err == nil {
		t.Fatal("loser.Commit: expected conflict")
	}

	before := w.NextIndex()

	// Retry both RequestIDs by resubmitting their exact original
	// request (RequestID identity, not Txn identity, is what makes a
	// retry idempotent — docs/transactions.md §8).
	retryOutcome, err := m.Resubmit("committed-req", committedTxnID, committedStartSeq, committedMutations)
	if err != nil {
		t.Fatalf("retry Resubmit(committed-req): %v", err)
	}
	if retryOutcome.CommitSeq != wantSeq {
		t.Fatalf("retry CommitSeq = %d, want original %d", retryOutcome.CommitSeq, wantSeq)
	}

	if _, err := m.Resubmit("loser-req", loserTxnID, loserStartSeq, loserMutations); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry Resubmit(loser-req): err = %v, want ErrConflict (same original outcome)", err)
	}

	after := w.NextIndex()
	if after != before {
		t.Fatalf("retrying known RequestIDs advanced the WAL index: before=%d after=%d", before, after)
	}
}

// TestMismatchedRequestIDReuseRejected (safe default,
// docs/transactions.md §6): reusing a RequestID with a materially
// different mutation set is rejected, and the original RequestID's
// outcome is unaffected.
func TestMismatchedRequestIDReuseRejected(t *testing.T) {
	m, _ := newTestManager(t)
	first := m.Begin()
	if err := first.Write("K", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wantSeq, err := first.Commit("dup")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	second := m.Begin()
	if err := second.Write("K", []byte("DIFFERENT")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := second.Commit("dup"); !errors.Is(err, fsm.ErrRequestIDPayloadMismatch) {
		t.Fatalf("Commit with reused RequestID + different payload: err = %v, want ErrRequestIDPayloadMismatch", err)
	}

	outcome, err := m.GetRequestOutcome("dup")
	if err != nil || outcome.Status != fsm.StatusCommitted || outcome.CommitSeq != wantSeq {
		t.Fatalf("GetRequestOutcome(dup) after mismatched reuse = %+v,%v, want unchanged Committed,%d", outcome, err, wantSeq)
	}
}

// TestGetRequestOutcomeUnknown: querying a RequestID that was never
// submitted returns an explicit not-found error, never a guess.
func TestGetRequestOutcomeUnknown(t *testing.T) {
	m, _ := newTestManager(t)
	if _, err := m.GetRequestOutcome("never-submitted"); !errors.Is(err, fsm.ErrRequestIDUnknown) {
		t.Fatalf("GetRequestOutcome(never-submitted): err = %v, want ErrRequestIDUnknown", err)
	}
}

// TestGetRequestOutcomeCommitted (ID-3): a committed RequestID's
// outcome is queryable without resubmitting anything.
func TestGetRequestOutcomeCommitted(t *testing.T) {
	m, _ := newTestManager(t)
	tx := m.Begin()
	if err := tx.Write("K", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	seq, err := tx.Commit("r1")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	outcome, err := m.GetRequestOutcome("r1")
	if err != nil {
		t.Fatalf("GetRequestOutcome: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted || outcome.CommitSeq != seq {
		t.Fatalf("outcome = %+v, want Committed CommitSeq=%d", outcome, seq)
	}
}

// TestGetRequestOutcomeAborted: an aborted (conflicted) RequestID's
// outcome is queryable and reports the conflict, not success.
func TestGetRequestOutcomeAborted(t *testing.T) {
	m, _ := newTestManager(t)
	t1 := m.Begin()
	t2 := m.Begin()
	if err := t1.Write("K", []byte("a")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("K", []byte("b")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("winner"); err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}
	if _, err := t2.Commit("loser"); err == nil {
		t.Fatal("t2.Commit: expected conflict")
	}
	outcome, err := m.GetRequestOutcome("loser")
	if err != nil {
		t.Fatalf("GetRequestOutcome(loser): %v", err)
	}
	if outcome.Status != fsm.StatusAborted || outcome.ConflictKey != "K" {
		t.Fatalf("outcome = %+v, want Aborted ConflictKey=K", outcome)
	}
}
