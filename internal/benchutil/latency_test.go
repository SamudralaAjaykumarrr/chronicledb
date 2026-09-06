package benchutil

import "testing"

func TestSummarizeEmpty(t *testing.T) {
	r := NewRecorder(0)
	s := r.Summarize()
	if s.Count != 0 {
		t.Errorf("Count = %d, want 0", s.Count)
	}
}

func TestSummarizeKnownDistribution(t *testing.T) {
	r := NewRecorder(100)
	for i := int64(1); i <= 100; i++ {
		r.Record(i)
	}
	s := r.Summarize()
	if s.Count != 100 {
		t.Fatalf("Count = %d, want 100", s.Count)
	}
	if s.Max != 100 {
		t.Errorf("Max = %d, want 100", s.Max)
	}
	if s.P50 != 50 {
		t.Errorf("P50 = %d, want 50", s.P50)
	}
	if s.P95 != 95 {
		t.Errorf("P95 = %d, want 95", s.P95)
	}
	if s.P99 != 99 {
		t.Errorf("P99 = %d, want 99", s.P99)
	}
}

func TestMerge(t *testing.T) {
	a := NewRecorder(2)
	a.Record(1)
	a.Record(2)
	b := NewRecorder(2)
	b.Record(3)
	b.Record(4)
	a.Merge(b)
	if a.Len() != 4 {
		t.Fatalf("Len() = %d, want 4", a.Len())
	}
	if got := a.Summarize().Max; got != 4 {
		t.Errorf("Max = %d, want 4", got)
	}
}
