package txn

import (
	"errors"
	"fmt"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// ID-1: duplicate RequestID before any restart — a same-process retry
// (resubmitting the exact original TxnID/StartSeq/Mutations, per
// docs/transactions.md §8: a retry need not come from the original Txn
// session) returns the identical outcome, and no second version is
// created.
func TestID1_DuplicateRequestIDBeforeRestart(t *testing.T) {
	m, w := newTestManager(t)

	tx := m.Begin()
	txnID, startSeq := tx.ID(), tx.StartSeq()
	mutations := []mvcc.Mutation{{Key: "K", Value: []byte("v1")}}
	if err := tx.Write("K", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wantSeq, err := tx.Commit("R")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	before := w.NextIndex()
	retryOutcome, err := m.Resubmit("R", txnID, startSeq, mutations)
	if err != nil {
		t.Fatalf("Resubmit(R) retry: %v", err)
	}
	if retryOutcome.CommitSeq != wantSeq {
		t.Fatalf("retry CommitSeq = %d, want %d", retryOutcome.CommitSeq, wantSeq)
	}
	if w.NextIndex() != before {
		t.Fatalf("retry advanced the WAL: before=%d after=%d", before, w.NextIndex())
	}
	if seq, ok := m.fsm.Store().LatestCommitSeq("K"); !ok || seq != wantSeq {
		t.Fatalf("LatestCommitSeq(K) = %d,%v, want %d,true (no duplicate version)", seq, ok, wantSeq)
	}
}

// ID-2 / ID-4 (immediate/after-restart): a committed RequestID's
// outcome, and no-reapply behavior, survive a real restart (WAL
// Close + reopen, running the real recovery path — no in-memory
// shortcut). The retry resubmits the original TxnID/StartSeq/Mutations
// directly: the original Txn's in-memory session is gone (a fresh
// process reopened the WAL), exactly the scenario
// docs/transactions.md §8 describes.
func TestID2_DuplicateRequestIDAfterRestart(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	tx := m.Begin()
	txnID, startSeq := tx.ID(), tx.StartSeq()
	mutations := []mvcc.Mutation{{Key: "K", Value: []byte("v1")}}
	if err := tx.Write("K", []byte("v1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wantSeq, err := tx.Commit("R")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	outcome, err := m2.GetRequestOutcome("R")
	if err != nil {
		t.Fatalf("GetRequestOutcome(R) after restart: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted || outcome.CommitSeq != wantSeq {
		t.Fatalf("outcome after restart = %+v, want Committed CommitSeq=%d", outcome, wantSeq)
	}

	before := w2.NextIndex()
	retryOutcome, err := m2.Resubmit("R", txnID, startSeq, mutations)
	if err != nil {
		t.Fatalf("post-restart Resubmit(R): %v", err)
	}
	if retryOutcome.CommitSeq != wantSeq {
		t.Fatalf("post-restart retry CommitSeq = %d, want %d", retryOutcome.CommitSeq, wantSeq)
	}
	if w2.NextIndex() != before {
		t.Fatalf("post-restart retry advanced the WAL: before=%d after=%d", before, w2.NextIndex())
	}
	if seq, ok := m2.fsm.Store().LatestCommitSeq("K"); !ok || seq != wantSeq {
		t.Fatalf("LatestCommitSeq(K) after restart+retry = %d,%v, want %d,true (no duplicate version)", seq, ok, wantSeq)
	}
}

// TestConflictOutcomeSurvivesRestartAndRetryRemainsConflict: a
// conflicted RequestID's ABORTED outcome must survive a restart, and a
// post-restart retry (resubmitting the original request) must return
// the SAME conflict outcome — even though, by the time of the retry,
// the state that originally caused the conflict is no longer "new" (it
// must not be re-evaluated fresh against current state, which could in
// principle look different).
func TestConflictOutcomeSurvivesRestartAndRetryRemainsConflict(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	winner := m.Begin()
	loser := m.Begin()
	loserTxnID, loserStartSeq := loser.ID(), loser.StartSeq()
	loserMutations := []mvcc.Mutation{{Key: "K", Value: []byte("loser-value")}}
	if err := winner.Write("K", []byte("winner-value")); err != nil {
		t.Fatalf("winner.Write: %v", err)
	}
	if err := loser.Write("K", []byte("loser-value")); err != nil {
		t.Fatalf("loser.Write: %v", err)
	}
	if _, err := winner.Commit("winner-req"); err != nil {
		t.Fatalf("winner.Commit: %v", err)
	}
	if _, err := loser.Commit("loser-req"); !errors.Is(err, ErrConflict) {
		t.Fatalf("loser.Commit: err = %v, want ErrConflict", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	outcome, err := m2.GetRequestOutcome("loser-req")
	if err != nil {
		t.Fatalf("GetRequestOutcome(loser-req) after restart: %v", err)
	}
	if outcome.Status != fsm.StatusAborted || outcome.ConflictKey != "K" {
		t.Fatalf("outcome after restart = %+v, want Aborted ConflictKey=K", outcome)
	}

	// Unrelated later state changes must not affect the retry's answer.
	setup := m2.Begin()
	if err := setup.Write("other-key", []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := setup.Commit("unrelated-req"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if _, err := m2.Resubmit("loser-req", loserTxnID, loserStartSeq, loserMutations); !errors.Is(err, ErrConflict) {
		t.Fatalf("post-restart Resubmit(loser-req): err = %v, want ErrConflict (stable original outcome)", err)
	}

	// K's committed value must still be the original winner's, never
	// the loser's, on either side of the restart.
	reader := m2.Begin()
	v, found, _ := reader.Read("K")
	if !found || string(v) != "winner-value" {
		t.Fatalf("Read(K) after restart = %q,%v, want winner-value,true", v, found)
	}
}

// TestMismatchedRequestIDReuseRejectedAfterRestart: the fingerprint
// needed to detect an inconsistent RequestID reuse is itself
// reconstructed by recovery, not lost across a restart.
func TestMismatchedRequestIDReuseRejectedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	tx := m.Begin()
	if err := tx.Write("K", []byte("original")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	wantSeq, err := tx.Commit("R")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	mismatched := m2.Begin()
	if err := mismatched.Write("K", []byte("different-value")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := mismatched.Commit("R"); !errors.Is(err, fsm.ErrRequestIDPayloadMismatch) {
		t.Fatalf("post-restart Commit(R, different payload): err = %v, want ErrRequestIDPayloadMismatch", err)
	}

	outcome, err := m2.GetRequestOutcome("R")
	if err != nil || outcome.Status != fsm.StatusCommitted || outcome.CommitSeq != wantSeq {
		t.Fatalf("GetRequestOutcome(R) after rejected mismatched reuse = %+v,%v, want unchanged Committed,%d", outcome, err, wantSeq)
	}
}

// TestRecoveryNeverInventsRequestIDOutcomes: after replaying a real
// history of committed and conflicted commands, recovery must not
// invent an outcome for any RequestID that was never actually
// submitted.
func TestRecoveryNeverInventsRequestIDOutcomes(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Begin all five transactions up front, from the SAME StartSeq, so
	// every one after the first genuinely conflicts on K (rather than
	// each observing the previous one's already-committed write).
	const n = 5
	txns := make([]*Txn, n)
	for i := 0; i < n; i++ {
		txns[i] = m.Begin()
		if err := txns[i].Write("K", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		requestID := fsm.RequestID(fmt.Sprintf("req-%d", i))
		_, err := txns[i].Commit(requestID)
		if i == 0 {
			if err != nil {
				t.Fatalf("Commit 0: %v", err)
			}
		} else if !errors.Is(err, ErrConflict) {
			t.Fatalf("Commit %d: err = %v, want ErrConflict", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	if _, err := m2.GetRequestOutcome("never-submitted"); !errors.Is(err, fsm.ErrRequestIDUnknown) {
		t.Fatalf("GetRequestOutcome(never-submitted) after restart: err = %v, want ErrRequestIDUnknown", err)
	}
	// The submitted RequestIDs must all still resolve to exactly their
	// original outcome, though — recovery must lose nothing either.
	for i := 0; i < n; i++ {
		requestID := fsm.RequestID(fmt.Sprintf("req-%d", i))
		outcome, err := m2.GetRequestOutcome(requestID)
		if err != nil {
			t.Fatalf("GetRequestOutcome(%s) after restart: %v", requestID, err)
		}
		wantStatus := fsm.StatusAborted
		if i == 0 {
			wantStatus = fsm.StatusCommitted
		}
		if outcome.Status != wantStatus {
			t.Fatalf("GetRequestOutcome(%s).Status = %v, want %v", requestID, outcome.Status, wantStatus)
		}
	}
}

// TestMultipleRequestIDsSameMutationsSurviveRestartAsDistinct (ID-5,
// carried through a restart): two different RequestIDs submitting the
// same mutation set, both begun from the same StartSeq, remain
// independently evaluated after recovery.
func TestMultipleRequestIDsSameMutationsSurviveRestartAsDistinct(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	m, err := NewManager(w, mvcc.NewStore())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	r1 := m.Begin()
	r2 := m.Begin()
	if err := r1.Write("K", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r2.Write("K", []byte("v")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	seq1, err := r1.Commit("R1")
	if err != nil {
		t.Fatalf("R1.Commit: %v", err)
	}
	if _, err := r2.Commit("R2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("R2.Commit: err = %v, want ErrConflict (independent evaluation, not deduped against R1)", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, w2 := reopen(t, dir)
	defer w2.Close()

	o1, err := m2.GetRequestOutcome("R1")
	if err != nil || o1.Status != fsm.StatusCommitted || o1.CommitSeq != seq1 {
		t.Fatalf("GetRequestOutcome(R1) after restart = %+v,%v, want Committed,%d", o1, err, seq1)
	}
	o2, err := m2.GetRequestOutcome("R2")
	if err != nil || o2.Status != fsm.StatusAborted {
		t.Fatalf("GetRequestOutcome(R2) after restart = %+v,%v, want Aborted", o2, err)
	}
}
