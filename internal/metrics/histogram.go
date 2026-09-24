package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
)

// DefaultLatencyBounds is the shared bucket set docs/v0.6.0-plan.md
// §11.1 fixes for every new latency histogram in this release, in
// seconds: "0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
// 2.5, 5, 10". A shared, documented set — rather than each call site
// picking its own — is what keeps histograms across the catalog
// comparable.
var DefaultLatencyBounds = []float64{
	0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// Histogram is ChronicleDB's first histogram primitive
// (docs/v0.6.0-plan.md §11.1, docs/observability.md §9, deferred since
// Phase 9 and revisited in v0.6.0): a fixed, immutable set of
// upper-inclusive ("le") bucket bounds, with no allocation on Observe —
// a bucket index is chosen by binary search over the fixed bounds and
// recorded with a single atomic increment, so it is cheap enough to sit
// on hot paths like Raft message processing
// (chronicledb_raft_message_process_seconds). Like Counter/Gauge, a
// Histogram must not be copied after first use; unlike them, its zero
// value is not valid — bounds must be supplied via NewHistogram.
type Histogram struct {
	bounds []float64 // immutable, strictly ascending, set at construction
	// buckets has len(bounds)+1 slots. buckets[i] (i < len(bounds))
	// counts samples v with v <= bounds[i] and (i == 0 || v >
	// bounds[i-1]); buckets[len(bounds)] is the "+Inf" bucket, counting
	// every sample greater than the largest bound. Snapshot turns these
	// per-bucket counts into Prometheus' required cumulative form.
	buckets []atomic.Uint64
	sumBits atomic.Uint64 // math.Float64bits of the running sum
	count   atomic.Uint64
}

// NewHistogram returns a Histogram with the given strictly ascending
// bucket upper bounds. Panics if bounds is empty or not strictly
// ascending — a construction-time programmer error to catch
// immediately, not a runtime condition a caller should have to handle.
func NewHistogram(bounds ...float64) *Histogram {
	if len(bounds) == 0 {
		panic("metrics: NewHistogram requires at least one bound")
	}
	for i := 1; i < len(bounds); i++ {
		if bounds[i] <= bounds[i-1] {
			panic("metrics: NewHistogram bounds must be strictly ascending")
		}
	}
	b := make([]float64, len(bounds))
	copy(b, bounds)
	return &Histogram{bounds: b, buckets: make([]atomic.Uint64, len(bounds)+1)}
}

// Observe records one sample. Never allocates. Safe for concurrent use
// from any goroutine, including concurrently with itself and with
// Snapshot.
func (h *Histogram) Observe(v float64) {
	// sort.SearchFloat64s returns the smallest index i such that
	// h.bounds[i] >= v (h.bounds is sorted ascending) — exactly the
	// upper-inclusive bucket v belongs in; len(h.bounds) when v exceeds
	// every bound, which is the +Inf bucket's index by construction.
	i := sort.SearchFloat64s(h.bounds, v)
	h.buckets[i].Add(1)
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		newSum := math.Float64frombits(old) + v
		if h.sumBits.CompareAndSwap(old, math.Float64bits(newSum)) {
			return
		}
	}
}

// HistogramSnapshot is a point-in-time, race-free copy of a Histogram's
// state, safe to read after the Histogram it came from has moved on.
type HistogramSnapshot struct {
	// Bounds is the histogram's immutable bucket upper bounds.
	Bounds []float64
	// BucketCounts holds len(Bounds)+1 per-bucket (NOT cumulative)
	// counts; BucketCounts[len(Bounds)] is the +Inf bucket.
	BucketCounts []uint64
	Sum          float64
	Count        uint64
}

// Snapshot returns a race-free copy of h's current state.
func (h *Histogram) Snapshot() HistogramSnapshot {
	counts := make([]uint64, len(h.buckets))
	for i := range h.buckets {
		counts[i] = h.buckets[i].Load()
	}
	return HistogramSnapshot{
		Bounds:       append([]float64(nil), h.bounds...),
		BucketCounts: counts,
		Sum:          math.Float64frombits(h.sumBits.Load()),
		Count:        h.count.Load(),
	}
}

// WriteProm writes snap in Prometheus text exposition format
// (docs/v0.6.0-plan.md §11.2: "_bucket{le="…"}, _sum, and _count",
// matching cmd/chronicledb-node's existing text/plain; version=0.0.4
// output for Counter/Gauge lines): one HELP/TYPE preamble, one
// cumulative _bucket line per bound plus the +Inf bucket, then _sum and
// _count. le is the only label on this line, and its value set is fixed
// at construction (docs/v0.6.0-plan.md §11.1's amended label-policy
// rule: only compile-time-bounded label values are ever permitted).
func (snap HistogramSnapshot) WriteProm(w io.Writer, name, help string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	var cumulative uint64
	for i, bound := range snap.Bounds {
		cumulative += snap.BucketCounts[i]
		fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", name, formatBound(bound), cumulative)
	}
	cumulative += snap.BucketCounts[len(snap.Bounds)]
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	fmt.Fprintf(w, "%s_sum %v\n", name, snap.Sum)
	fmt.Fprintf(w, "%s_count %d\n", name, snap.Count)
}

func formatBound(b float64) string {
	return strconv.FormatFloat(b, 'g', -1, 64)
}
