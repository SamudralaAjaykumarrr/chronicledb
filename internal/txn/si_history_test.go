package txn

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/oracle"
)

// This file is Phase 10's explicit Snapshot Isolation history testing
// (docs/roadmap.md Phase 10 "SNAPSHOT ISOLATION HISTORY TESTS"): it
// documents, with named tests, specific histories Snapshot Isolation
// allows and forbids (docs/mvcc.md §4), then generalizes the same idea
// into a randomized, model-checked history generator. TX-8
// (docs/scenario-corpus.md, txn_test.go::TestTX8_SnapshotIsolationWriteSkew)
// already proves the canonical two-transaction write-skew example —
// this file does not repeat it; it adds shapes TX-8 does not cover
// (a three-way write-skew ring, tombstone/reinsert under conflict, and
// retry-as-a-new-transaction-after-conflict) plus the randomized
// model-based suite. Every test here explicitly documents whether the
// asserted outcome is ALLOWED or FORBIDDEN under Snapshot Isolation —
// never SERIALIZABLE (ISOLATION-TRUTHFULNESS, docs/invariants.md).

// ALLOWED under SI: two transactions with disjoint write sets, both
// beginning at the same StartSeq, both commit — no first-committer-wins
// conflict is possible when write sets never overlap.
func TestSIHistory_DisjointConcurrentWritesBothCommit(t *testing.T) {
	m, _ := newTestManager(t)
	t1 := m.Begin()
	t2 := m.Begin()
	if t1.StartSeq() != t2.StartSeq() {
		t.Fatalf("t1/t2 must share a StartSeq to be genuinely concurrent: %d vs %d", t1.StartSeq(), t2.StartSeq())
	}
	if err := t1.Write("x", []byte("1")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("y", []byte("2")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1.Commit: %v (disjoint writes must never conflict)", err)
	}
	if _, err := t2.Commit("r2"); err != nil {
		t.Fatalf("t2.Commit: %v (disjoint writes must never conflict)", err)
	}
}

// FORBIDDEN under SI (and under Serializable): two transactions with
// overlapping write sets, both beginning at the same StartSeq — exactly
// one may commit (first-committer-wins), never both.
func TestSIHistory_OverlappingWriteFirstCommitterWins(t *testing.T) {
	m, _ := newTestManager(t)
	t1 := m.Begin()
	t2 := m.Begin()
	if err := t1.Write("k", []byte("from-t1")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("k", []byte("from-t2")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1 (first committer) must succeed: %v", err)
	}
	if _, err := t2.Commit("r2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("t2 (second committer, overlapping write) must be rejected with ErrConflict, got %v", err)
	}
}

// ALLOWED under SI, FORBIDDEN under Serializable: a three-way write-
// skew ring extending TX-8's two-transaction example
// (txn_test.go::TestTX8_SnapshotIsolationWriteSkew, docs/mvcc.md §1.1).
// Three transactions T1, T2, T3 each read two of three keys (a, b, c)
// and each write a *different* one of the three based on what they
// read, so no two write sets ever overlap — every commit succeeds even
// though no serial execution of these three transactions could produce
// the resulting state (each transaction's write decision was made
// looking at a value the *other* transactions concurrently changed).
func TestSIHistory_ThreeWayWriteSkewRing(t *testing.T) {
	m, _ := newTestManager(t)
	setup := m.Begin()
	for _, kv := range [][2]string{{"a", "10"}, {"b", "10"}, {"c", "10"}} {
		if err := setup.Write(kv[0], []byte(kv[1])); err != nil {
			t.Fatalf("setup write %s: %v", kv[0], err)
		}
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("setup commit: %v", err)
	}

	t1, t2, t3 := m.Begin(), m.Begin(), m.Begin()
	if t1.StartSeq() != t2.StartSeq() || t2.StartSeq() != t3.StartSeq() {
		t.Fatalf("all three must share one StartSeq to be genuinely concurrent")
	}
	// T1 reads b,c and writes a; T2 reads a,c and writes b; T3 reads a,b
	// and writes c — a write-skew ring: each transaction's write target
	// is untouched by the other two, so no overlap ever occurs, but each
	// transaction's own read of the other keys is now stale by the time
	// all three have committed.
	if _, _, err := t1.Read("b"); err != nil {
		t.Fatalf("t1 read b: %v", err)
	}
	if _, _, err := t1.Read("c"); err != nil {
		t.Fatalf("t1 read c: %v", err)
	}
	if err := t1.Write("a", []byte("20")); err != nil {
		t.Fatalf("t1 write a: %v", err)
	}

	if _, _, err := t2.Read("a"); err != nil {
		t.Fatalf("t2 read a: %v", err)
	}
	if err := t2.Write("b", []byte("20")); err != nil {
		t.Fatalf("t2 write b: %v", err)
	}

	if _, _, err := t3.Read("a"); err != nil {
		t.Fatalf("t3 read a: %v", err)
	}
	if err := t3.Write("c", []byte("20")); err != nil {
		t.Fatalf("t3 write c: %v", err)
	}

	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1 commit: %v (write-skew ring must be ALLOWED under Snapshot Isolation)", err)
	}
	if _, err := t2.Commit("r2"); err != nil {
		t.Fatalf("t2 commit: %v (write-skew ring must be ALLOWED under Snapshot Isolation)", err)
	}
	if _, err := t3.Commit("r3"); err != nil {
		t.Fatalf("t3 commit: %v (write-skew ring must be ALLOWED under Snapshot Isolation)", err)
	}
	// Every serial execution order of T1,T2,T3 would have had at least
	// one transaction observe an already-updated value — this
	// three-way-simultaneous result is exactly what SERIALIZABLE would
	// forbid and SI does not (ISOLATION-TRUTHFULNESS).
}

// ALLOWED under SI: a key tombstoned by a conflict loser's competing
// concurrent delete, then the winner's own write stands; the loser's
// delete never partially applies (ABORT-SAFETY's tombstone handling).
func TestSIHistory_ConflictingDeleteAndWriteFirstCommitterWins(t *testing.T) {
	m, _ := newTestManager(t)
	setup := m.Begin()
	if err := setup.Write("k", []byte("v0")); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := setup.Commit("setup"); err != nil {
		t.Fatalf("setup commit: %v", err)
	}

	deleter := m.Begin()
	writer := m.Begin()
	if err := deleter.Delete("k"); err != nil {
		t.Fatalf("deleter.Delete: %v", err)
	}
	if err := writer.Write("k", []byte("v1")); err != nil {
		t.Fatalf("writer.Write: %v", err)
	}
	if _, err := deleter.Commit("del"); err != nil {
		t.Fatalf("deleter (first committer) must succeed: %v", err)
	}
	if _, err := writer.Commit("wr"); !errors.Is(err, ErrConflict) {
		t.Fatalf("writer (second committer) must be rejected with ErrConflict, got %v", err)
	}
	reader := m.Begin()
	if _, found, _ := reader.Read("k"); found {
		t.Fatalf("k must be tombstoned (the delete won), not the writer's v1")
	}

	// A fresh transaction, beginning strictly after the conflict is
	// resolved, may legitimately reinsert the now-deleted key.
	reinsert := m.Begin()
	if err := reinsert.Write("k", []byte("v2")); err != nil {
		t.Fatalf("reinsert.Write: %v", err)
	}
	if _, err := reinsert.Commit("reins"); err != nil {
		t.Fatalf("reinsert after tombstone must succeed: %v", err)
	}
}

// ALLOWED under SI: after a conflict, the loser retries as a genuinely
// NEW transaction (new TxnID, fresh Begin -> fresh StartSeq reflecting
// the winner's now-committed state) and succeeds — matching
// docs/failure-model.md §4.3's documented client-retry contract
// exactly (no server-side automatic retry, no semantic deduplication
// beyond RequestID identity).
func TestSIHistory_RetryAfterConflictAsNewTransactionSucceeds(t *testing.T) {
	m, _ := newTestManager(t)
	t1 := m.Begin()
	t2 := m.Begin()
	if err := t1.Write("k", []byte("winner")); err != nil {
		t.Fatalf("t1.Write: %v", err)
	}
	if err := t2.Write("k", []byte("loser")); err != nil {
		t.Fatalf("t2.Write: %v", err)
	}
	if _, err := t1.Commit("r1"); err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}
	if _, err := t2.Commit("r2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("t2.Commit: err=%v, want ErrConflict", err)
	}

	retry := m.Begin()
	if retry.StartSeq() == t2.StartSeq() {
		t.Fatalf("a fresh Begin after a conflict must observe a newer StartSeq than the failed attempt's")
	}
	if err := retry.Write("k", []byte("retried")); err != nil {
		t.Fatalf("retry.Write: %v", err)
	}
	if _, err := retry.Commit("r2-retry"); err != nil {
		t.Fatalf("retry as a new transaction after a conflict must succeed: %v", err)
	}
	reader := m.Begin()
	v, found, _ := reader.Read("k")
	if !found || string(v) != "retried" {
		t.Fatalf("final value = %q,%v, want retried,true", v, found)
	}
}

func siAdversarialSeeds(defaultN int) int {
	if v := os.Getenv("CHRONICLEDB_ADVERSARIAL_SEEDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultN
}

// TestModel_SIHistoryRandomizedAgainstIndependentModel generalizes the
// named histories above into a randomized generator: each round begins
// 2-3 transactions at a shared StartSeq (genuinely concurrent), gives
// each a randomized read/write set over a small keyspace, commits them
// in a randomized order, and checks every commit/abort decision against
// oracle.KVModel's independently-coded first-committer-wins prediction.
func TestModel_SIHistoryRandomizedAgainstIndependentModel(t *testing.T) {
	seeds := siAdversarialSeeds(10)
	const rounds = 15
	const keyspace = 5

	for seedI := 0; seedI < seeds; seedI++ {
		seed := int64(seedI)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSIModelHistory(t, seed, rounds, keyspace)
		})
	}
}

func runSIModelHistory(t *testing.T, seed int64, rounds, keyspace int) {
	t.Helper()
	m, _ := newTestManager(t)
	model := oracle.NewKVModel()
	rec := oracle.NewRecorder(seed)
	sched := rand.New(rand.NewSource(seed ^ 0x51de))

	fail := func(format string, args ...interface{}) {
		t.Fatalf("%s\n\n%s", fmt.Sprintf(format, args...), rec.Tail(rounds*4))
	}

	reqSeq := 0
	for round := 0; round < rounds; round++ {
		n := 2 + sched.Intn(2) // 2 or 3 concurrent transactions per round
		txns := make([]*Txn, n)
		muts := make([][]oracle.KVMutation, n)
		startSeq := uint64(0)
		for i := 0; i < n; i++ {
			txns[i] = m.Begin()
			startSeq = txns[i].StartSeq()
			key := fmt.Sprintf("key-%d", sched.Intn(keyspace))
			val := fmt.Sprintf("r%d-t%d", round, i)
			if err := txns[i].Write(key, []byte(val)); err != nil {
				fail("seed %d round %d: txn %d Write: %v", seed, round, i, err)
			}
			muts[i] = []oracle.KVMutation{{Key: key, Value: []byte(val)}}
		}
		// Commit in a randomized order — first-committer-wins is
		// evaluated relative to *commit* order, not Begin order.
		order := sched.Perm(n)
		for _, idx := range order {
			reqSeq++
			reqID := fmt.Sprintf("si-seed%d-req%d", seed, reqSeq)
			wantCommit, conflictKey, _ := model.Predict(startSeq, muts[idx])
			seq, err := txns[idx].Commit(fsm.RequestID(reqID))
			committed := err == nil
			rec.Record(oracle.Step{
				Node: "standalone", Op: "commit", RequestID: reqID,
				Args:    fmt.Sprintf("startSeq=%d mut=%+v", startSeq, muts[idx]),
				Outcome: fmt.Sprintf("committed=%v err=%v seq=%d", committed, err, seq),
			})
			if err != nil && !errors.Is(err, ErrConflict) {
				fail("seed %d round %d: unexpected non-conflict error: %v", seed, round, err)
			}
			if wantCommit != committed {
				fail("seed %d round %d: real committed=%v (err=%v), oracle predicted committed=%v (conflictKey=%q) — MISMATCH",
					seed, round, committed, err, wantCommit, conflictKey)
			}
			if committed {
				model.Apply(seq, muts[idx])
			}
		}
	}

	// Final state check via the real MVCC store at the latest committed
	// seq vs. the independent model, using the shared canonical digest.
	final := m.Begin()
	keys := model.Keys()
	gotDigest := oracle.CanonicalKVDigest(keys, func(k string) ([]byte, bool) {
		v, found, err := final.Read(k)
		if err != nil {
			t.Fatalf("final Read(%q): %v", k, err)
		}
		return v, found
	})
	if gotDigest != model.Digest() {
		fail("seed %d: final MVCC state digest %s != independent SI model digest %s", seed, gotDigest, model.Digest())
	}
}
