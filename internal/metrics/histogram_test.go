package metrics

import (
	"bytes"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestNewHistogramRejectsEmptyOrUnsortedBounds(t *testing.T) {
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected panic, got none", name)
			}
		}()
		f()
	}
	mustPanic("empty", func() { NewHistogram() })
	mustPanic("unsorted", func() { NewHistogram(1, 0.5) })
	mustPanic("duplicate", func() { NewHistogram(1, 1) })
}

func TestHistogramBucketBoundaries(t *testing.T) {
	h := NewHistogram(1, 2, 5)
	// Exactly on a bound goes into that bound's (upper-inclusive)
	// bucket; below the smallest bound goes into bucket 0; above the
	// largest bound goes into the +Inf bucket.
	h.Observe(-1)   // bucket 0 (<=1)
	h.Observe(1)    // bucket 0 (<=1)
	h.Observe(1.5)  // bucket 1 (<=2)
	h.Observe(2)    // bucket 1 (<=2)
	h.Observe(4.99) // bucket 2 (<=5)
	h.Observe(5)    // bucket 2 (<=5)
	h.Observe(5.01) // +Inf bucket
	h.Observe(1000) // +Inf bucket

	snap := h.Snapshot()
	want := []uint64{2, 2, 2, 2} // bucket0, bucket1, bucket2, +Inf
	if len(snap.BucketCounts) != len(want) {
		t.Fatalf("BucketCounts len = %d, want %d", len(snap.BucketCounts), len(want))
	}
	for i, w := range want {
		if snap.BucketCounts[i] != w {
			t.Errorf("BucketCounts[%d] = %d, want %d", i, snap.BucketCounts[i], w)
		}
	}
	if snap.Count != 8 {
		t.Errorf("Count = %d, want 8", snap.Count)
	}
	wantSum := -1 + 1 + 1.5 + 2 + 4.99 + 5 + 5.01 + 1000.0
	if diff := snap.Sum - wantSum; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("Sum = %v, want %v", snap.Sum, wantSum)
	}
}

// TestHistogramConcurrentObserveRaceSafe exercises Observe/Snapshot
// under concurrent writers and a concurrent reader — run under `go test
// -race`, mirroring TestCounterConcurrentIncrementsRaceSafe's existing
// pattern in this package.
func TestHistogramConcurrentObserveRaceSafe(t *testing.T) {
	h := NewHistogram(DefaultLatencyBounds...)
	const goroutines = 50
	const perGoroutine = 200

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
				_ = h.Snapshot()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				h.Observe(float64(n%20) * 0.01)
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	readerDone.Wait()

	if got, want := h.Snapshot().Count, uint64(goroutines*perGoroutine); got != want {
		t.Errorf("Count = %d, want %d", got, want)
	}
}

// TestHistogramWritePromFormat validates the emitted text against
// Prometheus' text exposition format (docs/v0.6.0-plan.md §11.2): HELP/
// TYPE preamble, one cumulative _bucket line per bound in ascending
// order plus a final "+Inf" bucket, _sum, and _count — and that the
// bucket counts are actually cumulative and the +Inf bucket's count
// equals the total.
func TestHistogramWritePromFormat(t *testing.T) {
	h := NewHistogram(0.1, 0.5, 1)
	for _, v := range []float64{0.05, 0.2, 0.2, 0.9, 5} {
		h.Observe(v)
	}

	var buf bytes.Buffer
	h.Snapshot().WriteProm(&buf, "chronicledb_test_seconds", "a test histogram")
	out := buf.String()

	if !strings.HasPrefix(out, "# HELP chronicledb_test_seconds a test histogram\n# TYPE chronicledb_test_seconds histogram\n") {
		t.Fatalf("missing/incorrect HELP+TYPE preamble:\n%s", out)
	}

	bucketRe := regexp.MustCompile(`(?m)^chronicledb_test_seconds_bucket\{le="([^"]+)"\} (\d+)$`)
	matches := bucketRe.FindAllStringSubmatch(out, -1)
	if len(matches) != 4 { // 3 bounds + Inf
		t.Fatalf("got %d _bucket lines, want 4:\n%s", len(matches), out)
	}
	wantLE := []string{"0.1", "0.5", "1", "+Inf"}
	wantCumulative := []uint64{1, 3, 4, 5}
	var prev uint64
	for i, m := range matches {
		if m[1] != wantLE[i] {
			t.Errorf("bucket %d le = %q, want %q", i, m[1], wantLE[i])
		}
		got, err := strconv.ParseUint(m[2], 10, 64)
		if err != nil {
			t.Fatalf("bucket %d count %q not an integer: %v", i, m[2], err)
		}
		if got != wantCumulative[i] {
			t.Errorf("bucket %d cumulative count = %d, want %d", i, got, wantCumulative[i])
		}
		if got < prev {
			t.Errorf("bucket %d count %d is less than previous bucket's %d — not cumulative", i, got, prev)
		}
		prev = got
	}

	if !strings.Contains(out, "chronicledb_test_seconds_sum ") {
		t.Errorf("missing _sum line:\n%s", out)
	}
	if !strings.Contains(out, "chronicledb_test_seconds_count 5\n") {
		t.Errorf("missing or incorrect _count line:\n%s", out)
	}
	// The +Inf bucket's cumulative count must equal _count exactly —
	// otherwise some sample was lost or double-counted.
	if matches[len(matches)-1][2] != "5" {
		t.Errorf("+Inf bucket count = %s, want 5 (must equal _count)", matches[len(matches)-1][2])
	}
}
