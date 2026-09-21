package mvcc

import (
	"errors"
	"sort"
	"testing"
)

func TestVisibleRefusesBelowWatermark(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(5, []Mutation{{Key: "K", Value: []byte("a")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	s.SetGCWatermark(10)

	if _, _, err := s.Visible("K", 9); !errors.Is(err, ErrSnapshotTooOld) {
		t.Fatalf("Visible(K, 9) with watermark 10: err = %v, want ErrSnapshotTooOld", err)
	}
	if v, found, err := s.Visible("K", 10); err != nil || !found || string(v) != "a" {
		t.Fatalf("Visible(K, 10) with watermark 10 (== is not <): got %q,%v,%v, want a,true,nil", v, found, err)
	}
}

func TestScanVisibleRefusesBelowWatermark(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(1, []Mutation{{Key: "p/a", Value: []byte("1")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	s.SetGCWatermark(5)

	if _, err := s.ScanVisible("p/", 4); !errors.Is(err, ErrSnapshotTooOld) {
		t.Fatalf("ScanVisible below watermark: err = %v, want ErrSnapshotTooOld", err)
	}
	if _, err := s.ScanVisible("p/", 5); err != nil {
		t.Fatalf("ScanVisible at watermark: err = %v, want nil", err)
	}
}

func TestSetGCWatermarkIsMonotone(t *testing.T) {
	s := NewStore()
	s.SetGCWatermark(10)
	s.SetGCWatermark(5) // lower: must be a no-op
	if got := s.GCWatermark(); got != 10 {
		t.Fatalf("GCWatermark() = %d, want 10 (a lower SetGCWatermark call must not decrease it)", got)
	}
	s.SetGCWatermark(20)
	if got := s.GCWatermark(); got != 20 {
		t.Fatalf("GCWatermark() = %d, want 20", got)
	}
}

func TestSkipHorizonGuardForTestDisablesTheCheck(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(1, []Mutation{{Key: "K", Value: []byte("a")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	s.SetGCWatermark(100)
	s.SetSkipHorizonGuardForTest(true)
	defer s.SetSkipHorizonGuardForTest(false)

	if _, _, err := s.Visible("K", 0); err != nil {
		t.Fatalf("with the guard skipped, Visible below watermark returned err = %v, want nil", err)
	}
	if _, err := s.ScanVisible("K", 0); err != nil {
		t.Fatalf("with the guard skipped, ScanVisible below watermark returned err = %v, want nil", err)
	}
}

func TestKeysFromOrderedAndBounded(t *testing.T) {
	s := NewStore()
	keys := []string{"c", "a", "e", "b", "d"}
	for i, k := range keys {
		if err := s.ApplyCommit(uint64(i+1), []Mutation{{Key: k, Value: []byte("v")}}); err != nil {
			t.Fatalf("ApplyCommit: %v", err)
		}
	}
	got := s.KeysFrom("", 100)
	want := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("KeysFrom(\"\", 100) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KeysFrom(\"\", 100)[%d] = %q, want %q (full result %v)", i, got[i], want[i], got)
		}
	}

	// Bounded: limit caps the result.
	if got := s.KeysFrom("", 2); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("KeysFrom(\"\", 2) = %v, want [a b]", got)
	}

	// Strictly after cursor: "b" itself is excluded.
	if got := s.KeysFrom("b", 2); len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Fatalf("KeysFrom(\"b\", 2) = %v, want [c d]", got)
	}

	// Past the end: empty, not an error.
	if got := s.KeysFrom("e", 10); len(got) != 0 {
		t.Fatalf("KeysFrom(\"e\", 10) = %v, want empty (e is the last key)", got)
	}

	// limit <= 0: empty.
	if got := s.KeysFrom("", 0); len(got) != 0 {
		t.Fatalf("KeysFrom(\"\", 0) = %v, want empty", got)
	}
}

func TestKeysFromNewKeysInsertedInOrder(t *testing.T) {
	s := NewStore()
	// Interleave new-key writes with writes to an already-existing key,
	// proving insertOrderedKeyLocked only fires for genuinely new keys
	// and the ordering survives repeated writes to the same key.
	commits := []Mutation{{Key: "m"}}
	if err := s.ApplyCommit(1, commits); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(2, []Mutation{{Key: "m", Value: []byte("v2")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(3, []Mutation{{Key: "a", Value: []byte("v")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(4, []Mutation{{Key: "z", Value: []byte("v")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	got := s.KeysFrom("", 10)
	want := []string{"a", "m", "z"}
	if len(got) != len(want) {
		t.Fatalf("KeysFrom = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KeysFrom[%d] = %q, want %q (full %v)", i, got[i], want[i], got)
		}
	}
}

func TestReclaimKeyNeverRemovesNewestVersion(t *testing.T) {
	s := NewStore()
	for _, seq := range []uint64{100, 200, 300, 900} {
		if err := s.ApplyCommit(seq, []Mutation{{Key: "K", Value: []byte("v")}}); err != nil {
			t.Fatalf("ApplyCommit(%d): %v", seq, err)
		}
	}
	// W = 1000 (above every version): everything except the newest
	// (900) is reclaimable.
	n := s.ReclaimKey("K", 1000, 100)
	if n != 3 {
		t.Fatalf("ReclaimKey removed %d, want 3 (all but the newest)", n)
	}
	if seq, ok := s.LatestCommitSeq("K"); !ok || seq != 900 {
		t.Fatalf("LatestCommitSeq(K) after reclaim = %d,%v, want 900,true (newest survives)", seq, ok)
	}
	// A second call at the same W reclaims nothing further.
	if n := s.ReclaimKey("K", 1000, 100); n != 0 {
		t.Fatalf("second ReclaimKey call removed %d, want 0 (nothing left to reclaim below W)", n)
	}
}

func TestReclaimKeyRespectsExactPredicate(t *testing.T) {
	// §14.1 example from the plan: versions c=100 and c'=900; W=1000
	// reclaims c=100 (a later version, 900, is <= 1000).
	s := NewStore()
	if err := s.ApplyCommit(100, []Mutation{{Key: "K", Value: []byte("old")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(900, []Mutation{{Key: "K", Value: []byte("new")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if n := s.ReclaimKey("K", 1000, 100); n != 1 {
		t.Fatalf("ReclaimKey(K, 1000, 100) = %d, want 1", n)
	}
	// W == 100 exactly: c=100 has no LATER version <= 100, so it is not
	// reclaimable — this must be tested on a fresh store since the
	// above already reclaimed it.
	s2 := NewStore()
	if err := s2.ApplyCommit(100, []Mutation{{Key: "K", Value: []byte("old")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s2.ApplyCommit(900, []Mutation{{Key: "K", Value: []byte("new")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if n := s2.ReclaimKey("K", 100, 100); n != 0 {
		t.Fatalf("ReclaimKey(K, 100, 100) = %d, want 0 (W == the older version's own CommitSeq, no LATER c'<=W exists)", n)
	}
}

func TestReclaimKeyTombstoneNoSpecialCase(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(1, []Mutation{{Key: "K", Value: []byte("v")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(2, []Mutation{{Key: "K", Tombstone: true}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(3, []Mutation{{Key: "K", Value: []byte("v2")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	// The tombstone at seq=2 is reclaimable under exactly the same rule
	// as any other version once superseded (S-3: no special case).
	if n := s.ReclaimKey("K", 10, 100); n != 2 {
		t.Fatalf("ReclaimKey = %d, want 2 (versions at seq 1 and 2, both superseded by seq 3)", n)
	}
}

func TestReclaimKeyBudgetBounds(t *testing.T) {
	s := NewStore()
	for _, seq := range []uint64{1, 2, 3, 4, 5, 100} {
		if err := s.ApplyCommit(seq, []Mutation{{Key: "K", Value: []byte("v")}}); err != nil {
			t.Fatalf("ApplyCommit(%d): %v", seq, err)
		}
	}
	// 5 versions (1..5) are reclaimable below W=100; budget caps it at 2.
	n := s.ReclaimKey("K", 100, 2)
	if n != 2 {
		t.Fatalf("ReclaimKey with budget=2 removed %d, want 2", n)
	}
	// Oldest-first: versions 1 and 2 removed, 3/4/5/100 remain.
	if seq, ok := s.LatestCommitSeq("K"); !ok || seq != 100 {
		t.Fatalf("LatestCommitSeq(K) = %d,%v, want 100,true", seq, ok)
	}
}

func TestReclaimKeySingleVersionNeverRemoved(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(1, []Mutation{{Key: "K", Value: []byte("v")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if n := s.ReclaimKey("K", ^uint64(0), 100); n != 0 {
		t.Fatalf("ReclaimKey on a single-version chain removed %d, want 0", n)
	}
}

func TestCheckConflictsUnaffectedByReclaim(t *testing.T) {
	// SL-5's "CheckConflicts is unaffected" property: it reads only the
	// newest version, which GC never removes, so it must give the
	// identical answer before and after a reclaim.
	s := NewStore()
	for _, seq := range []uint64{1, 2, 3} {
		if err := s.ApplyCommit(seq, []Mutation{{Key: "K", Value: []byte("v")}}); err != nil {
			t.Fatalf("ApplyCommit(%d): %v", seq, err)
		}
	}
	_, latestBefore, _ := s.CheckConflicts(0, []Mutation{{Key: "K"}})
	s.ReclaimKey("K", 100, 100)
	_, latestAfter, _ := s.CheckConflicts(0, []Mutation{{Key: "K"}})
	if latestBefore != latestAfter {
		t.Fatalf("CheckConflicts latest changed after reclaim: before=%d after=%d, want unchanged", latestBefore, latestAfter)
	}
}

// TestScanVisible_SL6_AllCommittedReadEntryPointsRefuseBelowHorizon is
// SL-6 (docs/v0.6.0-plan.md §15.2a, §30.2): scoped to every exported
// mvcc.Store read method — the internal/sql/internal/txn call-site
// portion of SL-6 (replicatedTxn.Get/ScanPrefix, standaloneTxn.Get/
// ScanPrefix) lives in those packages' own test suites, since it
// requires their own Txn wiring; this asserts the mvcc-package half of
// SL-6's scope: every read method Store itself exports refuses.
func TestScanVisible_SL6_AllCommittedReadEntryPointsRefuseBelowHorizon(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(1, []Mutation{{Key: "p/a", Value: []byte("v")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	s.SetGCWatermark(50)

	if _, _, err := s.Visible("p/a", 49); !errors.Is(err, ErrSnapshotTooOld) {
		t.Errorf("Visible: err = %v, want ErrSnapshotTooOld", err)
	}
	if _, err := s.ScanVisible("p/", 49); !errors.Is(err, ErrSnapshotTooOld) {
		t.Errorf("ScanVisible: err = %v, want ErrSnapshotTooOld", err)
	}
}

func TestScanVisibleResultsSortedAndPrefixFiltered(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(1, []Mutation{
		{Key: "p/b", Value: []byte("2")},
		{Key: "p/a", Value: []byte("1")},
		{Key: "other", Value: []byte("x")},
	}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	got, err := s.ScanVisible("p/", 1)
	if err != nil {
		t.Fatalf("ScanVisible: %v", err)
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i].Key < got[j].Key }) {
		t.Fatalf("ScanVisible results not sorted: %v", got)
	}
	if len(got) != 2 || got[0].Key != "p/a" || got[1].Key != "p/b" {
		t.Fatalf("ScanVisible(\"p/\", 1) = %v, want [p/a p/b]", got)
	}
}
