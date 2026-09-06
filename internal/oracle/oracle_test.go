package oracle

import "testing"

func TestKVModelPredictConflictFollowsFirstCommitterWins(t *testing.T) {
	m := NewKVModel()
	// T1 and T2 both start at seq 0.
	commit, _, _ := m.Predict(0, []KVMutation{{Key: "k", Value: []byte("v1")}})
	if !commit {
		t.Fatalf("first writer to an untouched key must be predicted to commit")
	}
	m.Apply(1, []KVMutation{{Key: "k", Value: []byte("v1")}})

	// T2, still at StartSeq=0, now conflicts: k's lastSeq (1) > 0.
	commit, key, seq := m.Predict(0, []KVMutation{{Key: "k", Value: []byte("v2")}})
	if commit {
		t.Fatalf("second writer to the same key at a stale StartSeq must be predicted to conflict")
	}
	if key != "k" || seq != 1 {
		t.Fatalf("got conflictKey=%q conflictSeq=%d, want k/1", key, seq)
	}

	// A transaction starting after T1's commit sees no conflict.
	commit, _, _ = m.Predict(1, []KVMutation{{Key: "k", Value: []byte("v3")}})
	if !commit {
		t.Fatalf("a writer starting at/after the latest commit must not conflict")
	}
}

func TestKVModelApplyIsAtomicAcrossKeys(t *testing.T) {
	m := NewKVModel()
	m.Apply(5, []KVMutation{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	})
	va, ok := m.Get("a")
	if !ok || string(va) != "1" {
		t.Fatalf("a: got %q,%v want 1,true", va, ok)
	}
	vb, ok := m.Get("b")
	if !ok || string(vb) != "2" {
		t.Fatalf("b: got %q,%v want 2,true", vb, ok)
	}
}

func TestKVModelTombstoneHidesKey(t *testing.T) {
	m := NewKVModel()
	m.Apply(1, []KVMutation{{Key: "k", Value: []byte("v")}})
	m.Apply(2, []KVMutation{{Key: "k", Tombstone: true}})
	if _, ok := m.Get("k"); ok {
		t.Fatalf("tombstoned key must read as not-found")
	}
	// Reinsert after tombstone.
	m.Apply(3, []KVMutation{{Key: "k", Value: []byte("v2")}})
	v, ok := m.Get("k")
	if !ok || string(v) != "v2" {
		t.Fatalf("reinsert after tombstone: got %q,%v want v2,true", v, ok)
	}
}

func TestKVModelDigestDeterministicRegardlessOfInsertionOrder(t *testing.T) {
	m1 := NewKVModel()
	m1.Apply(1, []KVMutation{{Key: "a", Value: []byte("1")}})
	m1.Apply(2, []KVMutation{{Key: "b", Value: []byte("2")}})

	m2 := NewKVModel()
	m2.Apply(1, []KVMutation{{Key: "b", Value: []byte("2")}})
	m2.Apply(2, []KVMutation{{Key: "a", Value: []byte("1")}})

	if m1.Digest() != m2.Digest() {
		t.Fatalf("digest must not depend on insertion order: %s vs %s", m1.Digest(), m2.Digest())
	}

	m3 := NewKVModel()
	m3.Apply(1, []KVMutation{{Key: "a", Value: []byte("1")}})
	m3.Apply(2, []KVMutation{{Key: "b", Value: []byte("different")}})
	if m1.Digest() == m3.Digest() {
		t.Fatalf("digest must differ when content differs")
	}
}

func TestKVModelDigestExcludesTombstonedKeys(t *testing.T) {
	m1 := NewKVModel()
	m1.Apply(1, []KVMutation{{Key: "a", Value: []byte("1")}})

	m2 := NewKVModel()
	m2.Apply(1, []KVMutation{{Key: "a", Value: []byte("1")}})
	m2.Apply(2, []KVMutation{{Key: "ghost", Value: []byte("x")}})
	m2.Apply(3, []KVMutation{{Key: "ghost", Tombstone: true}})

	if m1.Digest() != m2.Digest() {
		t.Fatalf("a tombstoned-away key must not affect the digest: %s vs %s", m1.Digest(), m2.Digest())
	}
}

func TestOutcomeTrackerDetectsInstability(t *testing.T) {
	o := NewOutcomeTracker()
	fp := Fingerprint(1, 0, []KVMutation{{Key: "k", Value: []byte("v")}})
	if err := o.Observe("R1", fp, RecordedOutcome{Committed: true, CommitSeq: 7}, "step1"); err != nil {
		t.Fatalf("first observation must never error: %v", err)
	}
	if err := o.Observe("R1", fp, RecordedOutcome{Committed: true, CommitSeq: 7}, "step2"); err != nil {
		t.Fatalf("identical retry must not error: %v", err)
	}
	err := o.Observe("R1", fp, RecordedOutcome{Committed: false, ConflictKey: "k"}, "step3")
	if err == nil {
		t.Fatalf("a differing outcome for the same RequestID+fingerprint must be flagged")
	}
	t.Logf("expected instability error: %v", err)
}

func TestOutcomeTrackerIgnoresDifferentFingerprintUnderSameID(t *testing.T) {
	o := NewOutcomeTracker()
	fp1 := Fingerprint(1, 0, []KVMutation{{Key: "k", Value: []byte("v1")}})
	fp2 := Fingerprint(2, 0, []KVMutation{{Key: "k", Value: []byte("v2")}})
	if err := o.Observe("R1", fp1, RecordedOutcome{Committed: true, CommitSeq: 1}, "step1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// A different fingerprint under the same RequestID is ID-6's
	// mismatched-payload scenario, not this tracker's concern.
	if err := o.Observe("R1", fp2, RecordedOutcome{Committed: true, CommitSeq: 99}, "step2"); err != nil {
		t.Fatalf("a different fingerprint under the same RequestID must not be flagged by this tracker: %v", err)
	}
}

func TestOutcomeTrackerDigestDeterministic(t *testing.T) {
	build := func() *OutcomeTracker {
		o := NewOutcomeTracker()
		fp := Fingerprint(1, 0, nil)
		o.Observe("R1", fp, RecordedOutcome{Committed: true, CommitSeq: 1}, "s")
		o.Observe("R2", fp, RecordedOutcome{Committed: false, ConflictKey: "x"}, "s")
		return o
	}
	if build().Digest() != build().Digest() {
		t.Fatalf("digest must be reproducible for identical content")
	}
}

func TestRecorderDumpAndTail(t *testing.T) {
	r := NewRecorder(42)
	for i := 0; i < 5; i++ {
		r.Record(Step{Node: "n1", Op: "write", Outcome: "ok"})
	}
	if len(r.Steps()) != 5 {
		t.Fatalf("got %d steps, want 5", len(r.Steps()))
	}
	full := r.Dump()
	tail := r.Tail(2)
	if len(tail) >= len(full) {
		t.Fatalf("Tail(2) should be shorter than Dump() for a 5-step history")
	}
	for i, s := range r.Steps() {
		if s.Seed != 42 {
			t.Fatalf("step %d: Seed not stamped", i)
		}
		if s.Index != i+1 {
			t.Fatalf("step %d: Index=%d, want %d", i, s.Index, i+1)
		}
	}
}
