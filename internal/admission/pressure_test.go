package admission

import (
	"sync/atomic"
	"testing"
	"time"
)

// fakePressureSource lets a test inject Pressure values deterministically
// (docs/v0.6.0-plan.md §6.1: "specifically so deterministic tests inject
// a fake and never touch a real filesystem or sleep") and counts calls.
type fakePressureSource struct {
	calls atomic.Int64
	next  atomic.Pointer[Pressure]
}

func newFakePressureSource(initial Pressure) *fakePressureSource {
	s := &fakePressureSource{}
	s.next.Store(&initial)
	return s
}

func (s *fakePressureSource) Sample() Pressure {
	s.calls.Add(1)
	return *s.next.Load()
}

func (s *fakePressureSource) set(p Pressure) { s.next.Store(&p) }

// TestPressureMonitorInjectedSourceWorks is docs/v0.6.0-plan.md §33
// slice 3's "an injected PressureSource works" exit criterion: an
// injected fake source drives Current() with no real filesystem or
// runtime/metrics call.
func TestPressureMonitorInjectedSourceWorks(t *testing.T) {
	src := newFakePressureSource(Pressure{DiskFreeBytes: 100, DiskTotalBytes: 1000})
	mon := NewPressureMonitor(src, 5*time.Millisecond)

	if got := mon.Current().DiskFreeBytes; got != 100 {
		t.Fatalf("Current() before Start() = %d, want the constructor's synchronous initial sample (100)", got)
	}
	if src.calls.Load() != 1 {
		t.Fatalf("Sample() call count before Start() = %d, want exactly 1 (the synchronous initial sample)", src.calls.Load())
	}

	mon.Start()
	defer mon.Stop()

	src.set(Pressure{DiskFreeBytes: 42, DiskTotalBytes: 1000})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mon.Current().DiskFreeBytes == 42 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Current().DiskFreeBytes never observed the updated sample (42); got %d", mon.Current().DiskFreeBytes)
}

func TestPressureMonitorSurfacesSampleError(t *testing.T) {
	wantErr := errStubSample
	src := newFakePressureSource(Pressure{Err: wantErr})
	mon := NewPressureMonitor(src, 5*time.Millisecond)
	if got := mon.Current().Err; got != wantErr {
		t.Fatalf("Current().Err = %v, want %v", got, wantErr)
	}
}

func TestPressureMonitorStopIsIdempotent(t *testing.T) {
	src := newFakePressureSource(Pressure{})
	mon := NewPressureMonitor(src, 5*time.Millisecond)
	mon.Start()
	mon.Stop()
	mon.Stop() // must not panic or deadlock
}

var errStubSample = &stubErr{"fake sample failure"}

type stubErr struct{ msg string }

func (e *stubErr) Error() string { return e.msg }
