package mvcc

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// gcSafetyTrial holds one randomized scenario shared by
// TestGCSafety_SL1_PropertyRandomizedChainsSnapshotsWatermarks and its
// SL-3 negative control below: several keys, each with a randomized
// version chain (increasing CommitSeq, gaps, tombstones), plus a
// randomized watermark and a set of randomized query points ("candidate
// snapshots") to check Visible against, both above and below that
// watermark.
type gcSafetyTrial struct {
	keys      []string
	chains    map[string][]refVersion
	watermark uint64
	maxSeq    uint64
	queryPts  []uint64
}

type refVersion struct {
	commitSeq uint64
	value     string
	tombstone bool
}

func buildGCSafetyTrial(rng *rand.Rand) gcSafetyTrial {
	numKeys := 1 + rng.Intn(6)
	tr := gcSafetyTrial{chains: make(map[string][]refVersion)}
	seq := uint64(0)
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("k%d", k)
		tr.keys = append(tr.keys, key)
		numVersions := 1 + rng.Intn(8)
		var chain []refVersion
		for v := 0; v < numVersions; v++ {
			seq += uint64(1 + rng.Intn(4)) // strictly increasing, with gaps
			tombstone := rng.Intn(4) == 0
			value := ""
			if !tombstone {
				value = randString(rng, 6)
			}
			chain = append(chain, refVersion{commitSeq: seq, value: value, tombstone: tombstone})
		}
		tr.chains[key] = chain
	}
	tr.maxSeq = seq + 5
	tr.watermark = uint64(rng.Intn(int(tr.maxSeq) + 1))
	numQueries := 10 + rng.Intn(20)
	for q := 0; q < numQueries; q++ {
		tr.queryPts = append(tr.queryPts, uint64(rng.Intn(int(tr.maxSeq)+1)))
	}
	return tr
}

// referenceVisible is the trivial linear-scan model of what Visible
// *would* return at startSeq against the full, un-reclaimed chain —
// the ground truth both SL-1 and SL-3 compare against.
func referenceVisible(chain []refVersion, startSeq uint64) (value string, found bool) {
	idx := sort.Search(len(chain), func(i int) bool { return chain[i].commitSeq > startSeq }) - 1
	if idx < 0 || chain[idx].tombstone {
		return "", false
	}
	return chain[idx].value, true
}

func newStoreFromTrial(t *testing.T, tr gcSafetyTrial) *Store {
	t.Helper()
	s := NewStore()
	for _, key := range tr.keys {
		for _, v := range tr.chains[key] {
			var m Mutation
			if v.tombstone {
				m = Mutation{Key: key, Tombstone: true}
			} else {
				m = Mutation{Key: key, Value: []byte(v.value)}
			}
			if err := s.ApplyCommit(v.commitSeq, []Mutation{m}); err != nil {
				t.Fatalf("ApplyCommit: %v", err)
			}
		}
	}
	return s
}

// TestGCSafety_SL1_PropertyRandomizedChainsSnapshotsWatermarks is SL-1
// (docs/v0.6.0-plan.md §30.2): for randomized version chains, randomized
// query points ("live snapshots"), and a randomized watermark, GC SAFETY
// holds at the raw mvcc.Store level, directly against Store.ReclaimKey —
// below internal/fsm's own SL-2 property test, which integration-tests
// ApplyAdvanceGCWatermark's cursor/continuation-pass machinery built on
// top of this same predicate:
//
//   - no surviving snapshot (a query at or above the watermark) ever
//     observes a different answer after reclamation than the
//     un-reclaimed reference model would have given it — the version it
//     needed was never removed;
//   - every below-horizon read (a query below the watermark, once
//     SetGCWatermark has been applied) is refused with ErrSnapshotTooOld,
//     never silently answered.
func TestGCSafety_SL1_PropertyRandomizedChainsSnapshotsWatermarks(t *testing.T) {
	const trials = 30
	for trial := 0; trial < trials; trial++ {
		seed := int64(trial)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			tr := buildGCSafetyTrial(rng)
			s := newStoreFromTrial(t, tr)

			for _, key := range tr.keys {
				chain := tr.chains[key]
				n := s.ReclaimKey(key, tr.watermark, len(chain))
				if n < 0 || n >= len(chain) {
					t.Fatalf("ReclaimKey(%s, w=%d) removed %d of %d versions, want the newest kept", key, tr.watermark, n, len(chain))
				}
			}
			s.SetGCWatermark(tr.watermark)

			for _, key := range tr.keys {
				chain := tr.chains[key]
				for _, q := range tr.queryPts {
					wantValue, wantFound := referenceVisible(chain, q)
					gotValue, gotFound, err := s.Visible(key, q)

					if q < tr.watermark {
						if !errors.Is(err, ErrSnapshotTooOld) {
							t.Fatalf("seed=%d key=%s q=%d watermark=%d: Visible = (%q,%v,%v), want ErrSnapshotTooOld (below-horizon read must be refused)",
								seed, key, q, tr.watermark, gotValue, gotFound, err)
						}
						continue
					}
					// q >= watermark: a surviving snapshot. GC must never
					// have changed this answer from what the un-reclaimed
					// reference model computes.
					if err != nil {
						t.Fatalf("seed=%d key=%s q=%d watermark=%d: Visible unexpectedly refused a surviving snapshot: %v", seed, key, q, tr.watermark, err)
					}
					if gotFound != wantFound || (gotFound && string(gotValue) != wantValue) {
						t.Fatalf("GC SAFETY violated: seed=%d key=%s q=%d watermark=%d: Visible = (%q,%v), reference model = (%q,%v)",
							seed, key, q, tr.watermark, gotValue, gotFound, wantValue, wantFound)
					}
				}
			}
		})
	}
}

// TestGCSafety_SL3_NegativeControl_HorizonGuardDisabledDetectsSilentStaleRead
// is SL-3 (docs/v0.6.0-plan.md §30.2, §31 gate 3): re-running SL-1's
// exact scenarios with Store.SetSkipHorizonGuardForTest(true) must
// produce at least one observable divergence from the reference model on
// a below-horizon query — proving the horizon guard is what makes SL-1's
// "every below-horizon read refused" result true, not an artifact of a
// generator that never actually reclaims anything a below-horizon query
// would have needed. Without this, SL-1 passing would be no evidence at
// all: a horizon guard that was silently deleted would still pass SL-1
// with the guard's own ErrSnapshotTooOld checks simply never firing... except
// SL-1 skips the below-horizon branch when nothing was actually asked
// for below the watermark, so a too-gentle generator could vacuously
// satisfy SL-1. This test asserts the divergence directly instead of
// relying on SL-1's absence of failure.
func TestGCSafety_SL3_NegativeControl_HorizonGuardDisabledDetectsSilentStaleRead(t *testing.T) {
	const trials = 30
	divergenceObserved := false
	for trial := 0; trial < trials; trial++ {
		seed := int64(trial)
		rng := rand.New(rand.NewSource(seed))
		tr := buildGCSafetyTrial(rng)
		s := newStoreFromTrial(t, tr)

		for _, key := range tr.keys {
			chain := tr.chains[key]
			s.ReclaimKey(key, tr.watermark, len(chain))
		}
		s.SetGCWatermark(tr.watermark)
		s.SetSkipHorizonGuardForTest(true)

		for _, key := range tr.keys {
			chain := tr.chains[key]
			for _, q := range tr.queryPts {
				if q >= tr.watermark {
					continue // only below-horizon queries are interesting here
				}
				wantValue, wantFound := referenceVisible(chain, q)
				gotValue, gotFound, err := s.Visible(key, q)
				if err != nil {
					t.Fatalf("seed=%d: Visible returned an error with the guard skipped: %v (SetSkipHorizonGuardForTest must disable the check entirely)", seed, err)
				}
				if gotFound != wantFound || (gotFound && string(gotValue) != wantValue) {
					// This is the silent stale read: with the guard
					// disabled, a query below the watermark that
					// ReclaimKey has already touched returns something
					// other than the true historical answer, with no
					// error at all — exactly the failure mode the guard
					// exists to prevent.
					divergenceObserved = true
				}
			}
		}
		s.SetSkipHorizonGuardForTest(false)
	}
	if !divergenceObserved {
		t.Fatal("SL-3 negative control: across 30 seeds, disabling the horizon guard never produced an observable silent stale read — SL-1's generator is too gentle to prove its own passing result means anything (§31 gate 3)")
	}
}
