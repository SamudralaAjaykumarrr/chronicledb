package node

import (
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/metrics"
)

// testAdmissionGates constructs a fully-configured admissionGates using
// package defaults, for hand-constructed *Node test fixtures that
// intentionally bypass Open to get a precise, deterministic starting
// state (dm17_test.go, readindex_seq_test.go, and similar). Open itself
// always builds one, and every gated entry point now requires
// n.admission to be non-nil: ADMISSION FAILS CLOSED
// (docs/v0.6.0-plan.md §27.4) means a missing admission mechanism must
// reject, never silently bypass, so nil is deliberately not special-
// cased as "unbounded" — these hand-built fixtures must supply a real
// one, exactly as Open does.
func testAdmissionGates(t *testing.T) *admissionGates {
	t.Helper()
	cfg := Config{}
	cfg.setDefaults()
	g, err := newAdmissionGates(cfg)
	if err != nil {
		t.Fatalf("newAdmissionGates: %v", err)
	}
	return g
}

// testMetrics returns a Metrics value with every field Open would
// otherwise initialize explicitly (currently just
// RaftMessageProcessSeconds — metrics.Histogram's zero value is not
// valid, unlike Counter/Gauge) — for the same hand-built *Node test
// fixtures testAdmissionGates serves.
func testMetrics() Metrics {
	return Metrics{RaftMessageProcessSeconds: metrics.NewHistogram(metrics.DefaultLatencyBounds...)}
}
