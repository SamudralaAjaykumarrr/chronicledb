package mvcc

import (
	"errors"
	"math/rand"
	"sort"
	"testing"
)

func TestVisibleNonExistentKey(t *testing.T) {
	s := NewStore()
	if _, found := s.Visible("K", 100); found {
		t.Fatal("Visible on a never-written key: found = true, want false")
	}
}

// TestVisibleBasicOrdering exercises docs/mvcc.md §3.2's exact worked
// example: a key with one committed version, read by two transactions
// with the same StartSeq before and after a newer commit.
func TestVisibleBasicOrdering(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(5, []Mutation{{Key: "K", Value: []byte("a")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}

	v, found := s.Visible("K", 10)
	if !found || string(v) != "a" {
		t.Fatalf("Visible(K, 10) = %q, %v, want \"a\", true", v, found)
	}

	if err := s.ApplyCommit(11, []Mutation{{Key: "K", Value: []byte("b")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}

	// A transaction whose snapshot predates CommitSeq=11 must still see
	// "a" (docs/mvcc.md §3.2).
	v, found = s.Visible("K", 10)
	if !found || string(v) != "a" {
		t.Fatalf("Visible(K, 10) after newer commit = %q, %v, want \"a\", true (stable snapshot)", v, found)
	}

	// A new snapshot at/after CommitSeq=11 sees "b".
	v, found = s.Visible("K", 11)
	if !found || string(v) != "b" {
		t.Fatalf("Visible(K, 11) = %q, %v, want \"b\", true", v, found)
	}
}

func TestVisibleExactBoundary(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(5, []Mutation{{Key: "K", Value: []byte("a"), Tombstone: false}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if _, found := s.Visible("K", 4); found {
		t.Fatal("Visible(K, 4) with only CommitSeq=5 version: found = true, want false (4 < 5)")
	}
	v, found := s.Visible("K", 5)
	if !found || string(v) != "a" {
		t.Fatalf("Visible(K, 5) = %q, %v, want \"a\", true (CommitSeq<=StartSeq is inclusive)", v, found)
	}
}

func TestVisibleTombstone(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(5, []Mutation{{Key: "K", Value: []byte("a")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(9, []Mutation{{Key: "K", Tombstone: true}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}

	// Before the delete: visible.
	if v, found := s.Visible("K", 6); !found || string(v) != "a" {
		t.Fatalf("Visible(K, 6) = %q, %v, want \"a\", true", v, found)
	}
	// At/after the delete: not found.
	if _, found := s.Visible("K", 9); found {
		t.Fatal("Visible(K, 9) after tombstone at CommitSeq=9: found = true, want false")
	}
	if _, found := s.Visible("K", 100); found {
		t.Fatal("Visible(K, 100) after tombstone: found = true, want false")
	}
}

func TestCheckConflicts(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(15, []Mutation{{Key: "K", Value: []byte("x")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	if err := s.ApplyCommit(8, []Mutation{{Key: "M", Value: []byte("y")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}

	// docs/mvcc.md §4 worked example: StartSeq=10, writes K (conflict,
	// latest=15>10) and M (no conflict, latest=8<=10). The whole
	// transaction must be reported as conflicting on K.
	key, latest, conflict := s.CheckConflicts(10, []Mutation{{Key: "K"}, {Key: "M"}})
	if !conflict || key != "K" || latest != 15 {
		t.Fatalf("CheckConflicts = key=%q latest=%d conflict=%v, want key=K latest=15 conflict=true", key, latest, conflict)
	}

	// A transaction whose StartSeq is >= the latest CommitSeq does not
	// conflict.
	if _, _, conflict := s.CheckConflicts(15, []Mutation{{Key: "K"}}); conflict {
		t.Fatal("CheckConflicts at StartSeq==latest CommitSeq: conflict = true, want false")
	}

	// A key with no committed version never conflicts.
	if _, _, conflict := s.CheckConflicts(0, []Mutation{{Key: "never-written"}}); conflict {
		t.Fatal("CheckConflicts on a never-written key: conflict = true, want false")
	}
}

func TestApplyCommitAtomicOnMonotonicityViolation(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(10, []Mutation{{Key: "A", Value: []byte("1")}}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}

	// A batch where "A" would violate monotonicity (new CommitSeq=5 <
	// existing 10) must apply NEITHER key, not just skip the bad one —
	// atomicity (docs/mvcc.md §5) applies to internal safety violations
	// too, not just conflicts.
	err := s.ApplyCommit(5, []Mutation{{Key: "A", Value: []byte("2")}, {Key: "B", Value: []byte("3")}})
	if !errors.Is(err, ErrNonMonotonicCommit) {
		t.Fatalf("ApplyCommit with non-monotonic CommitSeq: err = %v, want ErrNonMonotonicCommit", err)
	}
	if _, found := s.Visible("B", 100); found {
		t.Fatal("B became visible despite the batch failing on A: atomicity violated")
	}
	v, _ := s.Visible("A", 100)
	if string(v) != "1" {
		t.Fatalf("A's value changed despite the batch failing: got %q, want \"1\"", v)
	}
}

func TestApplyCommitMultiKeyAtomicity(t *testing.T) {
	s := NewStore()
	if err := s.ApplyCommit(7, []Mutation{
		{Key: "A", Value: []byte("10")},
		{Key: "B", Value: []byte("20")},
		{Key: "C", Tombstone: true},
	}); err != nil {
		t.Fatalf("ApplyCommit: %v", err)
	}
	for _, tc := range []struct {
		key       string
		wantValue string
		wantFound bool
	}{
		{"A", "10", true},
		{"B", "20", true},
		{"C", "", false},
	} {
		v, found := s.Visible(tc.key, 7)
		if found != tc.wantFound || (found && string(v) != tc.wantValue) {
			t.Fatalf("Visible(%s, 7) = %q, %v, want %q, %v", tc.key, v, found, tc.wantValue, tc.wantFound)
		}
	}
}

// TestVisibilityPropertyAgainstReferenceModel is a property test
// (docs/testing-strategy.md §1, docs/invariants.md MVCC-VISIBILITY):
// for randomly generated version chains and StartSeq values, Store's
// binary-search-based Visible must agree with a trivial linear-scan
// reference model. Seeded for reproducibility.
func TestVisibilityPropertyAgainstReferenceModel(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))

	for trial := 0; trial < 200; trial++ {
		numVersions := rng.Intn(20)
		var seqs []uint64
		seq := uint64(0)
		for i := 0; i < numVersions; i++ {
			seq += uint64(1 + rng.Intn(5)) // strictly increasing, with gaps
			seqs = append(seqs, seq)
		}

		type refVersion struct {
			commitSeq uint64
			value     string
			tombstone bool
		}
		var ref []refVersion
		s := NewStore()
		for _, cs := range seqs {
			tombstone := rng.Intn(4) == 0 // 25% tombstones
			value := ""
			if !tombstone {
				value = randString(rng, 8)
			}
			var m Mutation
			if tombstone {
				m = Mutation{Key: "K", Tombstone: true}
			} else {
				m = Mutation{Key: "K", Value: []byte(value)}
			}
			if err := s.ApplyCommit(cs, []Mutation{m}); err != nil {
				t.Fatalf("ApplyCommit: %v", err)
			}
			ref = append(ref, refVersion{commitSeq: cs, value: value, tombstone: tombstone})
		}

		maxSeq := uint64(0)
		if len(seqs) > 0 {
			maxSeq = seqs[len(seqs)-1] + 5
		}
		for q := 0; q < 30; q++ {
			startSeq := uint64(rng.Intn(int(maxSeq) + 1))

			// Reference: linear scan for newest version with
			// commitSeq <= startSeq.
			var (
				wantFound bool
				wantValue string
			)
			idx := sort.Search(len(ref), func(i int) bool { return ref[i].commitSeq > startSeq }) - 1
			if idx >= 0 && !ref[idx].tombstone {
				wantFound = true
				wantValue = ref[idx].value
			}

			gotValue, gotFound := s.Visible("K", startSeq)
			if gotFound != wantFound || (gotFound && string(gotValue) != wantValue) {
				t.Fatalf("trial %d: Visible(K, %d) = %q,%v; reference model = %q,%v (chain=%v)",
					trial, startSeq, gotValue, gotFound, wantValue, wantFound, ref)
			}
		}
	}
}

func randString(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}
