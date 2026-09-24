package admission

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Pressure is one point-in-time sample of node-local resource headroom
// (docs/v0.6.0-plan.md §6.1). A single admission.PressureMonitor
// goroutine (wired up in internal/node, later slices) samples a
// PressureSource on a fixed interval and stores an immutable Pressure
// snapshot; nothing on a request path or the event loop ever makes a
// syscall or calls runtime/metrics directly.
type Pressure struct {
	DiskFreeBytes  uint64
	DiskTotalBytes uint64
	HeapBytes      uint64
	SampledAt      time.Time
	// Err is non-nil iff this sample failed (e.g. the platform does not
	// support disk-usage probing, or a runtime Statfs call itself
	// failed) — docs/v0.6.0-plan.md §6.2's fail-safe direction: a
	// failed sample must never be silently treated as "healthy".
	Err error
}

// PressureSource is implemented by whatever actually samples resource
// headroom (internal/storage.DiskUsage plus runtime/metrics heap
// sampling, in production; an injected fake in every deterministic
// test — docs/v0.6.0-plan.md §6.1: "specifically so deterministic tests
// inject a fake and never touch a real filesystem or sleep", C2).
type PressureSource interface {
	Sample() Pressure
}

// PressureMonitor samples a PressureSource on a fixed interval and
// stores an immutable Pressure snapshot in an atomic.Pointer
// (docs/v0.6.0-plan.md §6.1): gates and the disk-full state machine
// read Current() with no syscall, no lock, and no blocking — only this
// one goroutine ever calls Sample(). An initial sample is taken
// synchronously at construction, so Current() never returns a zero
// Pressure before the first tick.
type PressureMonitor struct {
	source   PressureSource
	interval time.Duration
	current  atomic.Pointer[Pressure]

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

// NewPressureMonitor returns a PressureMonitor that will sample source
// every interval once Start is called. interval must be > 0.
func NewPressureMonitor(source PressureSource, interval time.Duration) *PressureMonitor {
	if interval <= 0 {
		panic("admission: NewPressureMonitor requires interval > 0")
	}
	m := &PressureMonitor{
		source:   source,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	initial := source.Sample()
	m.current.Store(&initial)
	return m
}

// Start begins periodic sampling on a new goroutine. Call at most once.
func (m *PressureMonitor) Start() { go m.run() }

func (m *PressureMonitor) run() {
	defer close(m.doneCh)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p := m.source.Sample()
			m.current.Store(&p)
		case <-m.stopCh:
			return
		}
	}
}

// Current returns the most recently sampled Pressure. Safe to call from
// any goroutine, including before Start (returns the constructor's
// synchronous initial sample).
func (m *PressureMonitor) Current() Pressure { return *m.current.Load() }

// Stop halts sampling and waits for the sampling goroutine to exit.
// Idempotent.
func (m *PressureMonitor) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	<-m.doneCh
}

// Threshold is a parsed -disk-pressure-threshold / -disk-critical-
// threshold value (docs/v0.6.0-plan.md §6.4): either an absolute byte
// count ("2GiB") or a percentage of total disk capacity ("10%").
type Threshold struct {
	absoluteBytes uint64
	percent       float64 // 0-100; valid iff isPercent
	isPercent     bool
	raw           string
}

// IsZero reports whether t is the zero Threshold (unparsed / "off").
func (t Threshold) IsZero() bool { return !t.isPercent && t.absoluteBytes == 0 && t.raw == "" }

// Bytes resolves t against totalBytes (the disk's total capacity,
// meaningless for an absolute Threshold but required to resolve a
// percentage one).
func (t Threshold) Bytes(totalBytes uint64) uint64 {
	if t.isPercent {
		return uint64(t.percent / 100 * float64(totalBytes))
	}
	return t.absoluteBytes
}

// String renders t back in a form ParseThreshold accepts, round-
// tripping to an equal Threshold (docs/v0.6.0-plan.md AC-17).
func (t Threshold) String() string {
	if t.raw != "" {
		return t.raw
	}
	if t.isPercent {
		return strconv.FormatFloat(t.percent, 'g', -1, 64) + "%"
	}
	return strconv.FormatUint(t.absoluteBytes, 10) + "B"
}

var byteSizeSuffixes = []struct {
	suffix string
	mult   float64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
	{"B", 1},
}

// ParseThreshold parses a -disk-pressure-threshold / -disk-critical-
// threshold flag value: an absolute size ("2GiB", "512MiB", "1048576",
// meaning bytes with no suffix) or a percentage of total disk capacity
// ("10%"). Never panics on any input, including malformed, huge, or
// non-UTF8-adjacent unicode strings — required for AC-17's fuzz
// obligation — and every value it does accept round-trips through
// String/ParseThreshold to an equal Threshold.
func ParseThreshold(s string) (Threshold, error) {
	raw := s
	s = strings.TrimSpace(s)
	if s == "" {
		return Threshold{}, fmt.Errorf("admission: empty threshold")
	}

	if strings.HasSuffix(s, "%") {
		numStr := strings.TrimSuffix(s, "%")
		pct, err := strconv.ParseFloat(numStr, 64)
		if err != nil || math.IsNaN(pct) || math.IsInf(pct, 0) || pct < 0 || pct > 100 {
			return Threshold{}, fmt.Errorf("admission: invalid percentage threshold %q: must be a number in [0,100] followed by %%", raw)
		}
		return Threshold{percent: pct, isPercent: true, raw: raw}, nil
	}

	n, err := parseByteSize(s)
	if err != nil {
		return Threshold{}, fmt.Errorf("admission: invalid threshold %q: %w", raw, err)
	}
	return Threshold{absoluteBytes: n, raw: raw}, nil
}

func parseByteSize(s string) (uint64, error) {
	for _, sfx := range byteSizeSuffixes {
		if len(s) <= len(sfx.suffix) || !strings.EqualFold(s[len(s)-len(sfx.suffix):], sfx.suffix) {
			continue
		}
		numStr := s[:len(s)-len(sfx.suffix)]
		if numStr == "" {
			return 0, fmt.Errorf("missing numeric part before %q", sfx.suffix)
		}
		n, err := strconv.ParseFloat(numStr, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
			return 0, fmt.Errorf("invalid numeric part %q", numStr)
		}
		product := n * sfx.mult
		if product > float64(math.MaxUint64) {
			return 0, fmt.Errorf("size %q out of range", s)
		}
		return uint64(product), nil
	}
	// Bare integer: bytes.
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unrecognized size %q (want a plain byte count or a KiB/MiB/GiB/TiB-suffixed size)", s)
	}
	return n, nil
}
