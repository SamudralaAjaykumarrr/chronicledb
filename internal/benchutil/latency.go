// Package benchutil provides a small, dependency-free latency
// recorder for ChronicleDB's macro/end-to-end benchmarks
// (docs/roadmap.md Phase 9 §Latency distributions). It exists only to
// turn a benchmark's per-operation durations into p50/p95/p99/max —
// Go's own `testing.B` already gives ns/op and, with -benchmem,
// allocation counts, so this package deliberately does nothing else
// (docs/roadmap.md's brief: "use a simple defensible implementation...
// do not add a heavy metrics dependency solely for percentile
// calculation").
//
// Recorder is not safe for concurrent use by multiple goroutines; a
// benchmark that measures concurrent operations should give each
// goroutine its own Recorder and merge them (see Merge) after all
// goroutines have finished, exactly as *testing.B itself expects
// timing-affecting work to stay outside any data race.
package benchutil

import "sort"

// Recorder collects individual operation latencies and computes
// order statistics over them. The zero value is ready to use.
type Recorder struct {
	samples []int64 // nanoseconds, insertion order
}

// NewRecorder returns a Recorder pre-sized for n samples (a hint, not
// a limit — Record still works past capacity).
func NewRecorder(n int) *Recorder {
	return &Recorder{samples: make([]int64, 0, n)}
}

// Record appends one observed operation latency, in nanoseconds.
func (r *Recorder) Record(nanos int64) {
	r.samples = append(r.samples, nanos)
}

// Len returns the number of recorded samples.
func (r *Recorder) Len() int { return len(r.samples) }

// Merge appends other's samples into r, for combining several
// goroutines' per-goroutine Recorders into one after a concurrent
// benchmark's timed section has finished.
func (r *Recorder) Merge(other *Recorder) {
	r.samples = append(r.samples, other.samples...)
}

// Summary is the set of order statistics Report computes over a
// Recorder's samples, all in nanoseconds.
type Summary struct {
	Count int
	P50   int64
	P95   int64
	P99   int64
	Max   int64
}

// Summarize sorts a copy of the recorded samples and returns their
// order statistics. Percentiles use the nearest-rank method (no
// interpolation) — adequate for a benchmark's own local
// before/after comparison, not a claim of statistical rigor beyond
// that (docs/roadmap.md: "a simple defensible implementation").
// Returns the zero Summary if no samples were recorded.
func (r *Recorder) Summarize() Summary {
	n := len(r.samples)
	if n == 0 {
		return Summary{}
	}
	sorted := append([]int64(nil), r.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rank := func(pct float64) int64 {
		idx := int(pct*float64(n)) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return sorted[idx]
	}
	return Summary{
		Count: n,
		P50:   rank(0.50),
		P95:   rank(0.95),
		P99:   rank(0.99),
		Max:   sorted[n-1],
	}
}

// ReportMetrics is implemented by *testing.B (and *testing.PB's parent
// benchmark) — declared narrowly here so this package does not import
// "testing" itself, keeping it usable from any caller that wants
// percentile output without pulling in the testing package's full
// surface.
type ReportMetrics interface {
	ReportMetric(n float64, unit string)
}

// Report publishes a Summary via b.ReportMetric under
// "<prefix>-p50-ns", "<prefix>-p95-ns", "<prefix>-p99-ns", and
// "<prefix>-max-ns" — alongside *testing.B's own standard ns/op and
// (with -benchmem) allocation metrics, never replacing them
// (docs/roadmap.md: "Do not pretend ns/op is p99 latency").
func (s Summary) Report(b ReportMetrics, prefix string) {
	b.ReportMetric(float64(s.P50), prefix+"-p50-ns")
	b.ReportMetric(float64(s.P95), prefix+"-p95-ns")
	b.ReportMetric(float64(s.P99), prefix+"-p99-ns")
	b.ReportMetric(float64(s.Max), prefix+"-max-ns")
}
