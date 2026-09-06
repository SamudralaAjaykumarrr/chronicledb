// Package metrics implements ChronicleDB's minimal diagnostic-counter
// primitives (docs/roadmap.md Phase 9 §Observability). It intentionally
// does not depend on any external metrics/monitoring library: a
// counter and a gauge, both backed by sync/atomic, are all the
// observability surfaces documented in docs/observability.md actually
// need.
//
// Every type here is read-only from the perspective of correctness:
// nothing in ChronicleDB's commit, replication, or recovery path ever
// branches on a metric's value (docs/roadmap.md §Observability: "never
// as a correctness dependency — a correct decision must never depend
// on whether a metric was successfully recorded"). Callers are free to
// increment/set these from any goroutine without additional
// synchronization.
package metrics

import "sync/atomic"

// Counter is a monotonically increasing count (docs/roadmap.md
// §Metric design: "prefer counters"). The zero value is a valid,
// zeroed Counter. A Counter must not be copied after first use (it
// embeds an atomic value) — every owner holds it by reference or as a
// non-copied struct field, exactly like sync.Mutex.
type Counter struct {
	v atomic.Uint64
}

// Inc increments the counter by 1.
func (c *Counter) Inc() { c.v.Add(1) }

// Add increments the counter by n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value returns the counter's current value. Safe to call from any
// goroutine, concurrently with Inc/Add.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a value that can move up or down (docs/roadmap.md §Metric
// design). The zero value is a valid, zeroed Gauge. Like Counter, a
// Gauge must not be copied after first use.
type Gauge struct {
	v atomic.Int64
}

// Set stores n as the gauge's current value.
func (g *Gauge) Set(n int64) { g.v.Store(n) }

// Add adds delta to the gauge's current value.
func (g *Gauge) Add(delta int64) { g.v.Add(delta) }

// Value returns the gauge's current value. Safe to call from any
// goroutine, concurrently with Set/Add.
func (g *Gauge) Value() int64 { return g.v.Load() }
