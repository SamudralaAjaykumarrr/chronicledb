package metrics

import (
	"sync"
	"testing"
)

// TestCounterConcurrentIncrementsRaceSafe exercises Counter under
// concurrent writers and one concurrent reader — run under `go test
// -race` (docs/roadmap.md Phase 9 §Observability tests: "metrics/status
// reads are race-safe").
func TestCounterConcurrentIncrementsRaceSafe(t *testing.T) {
	var c Counter
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
				_ = c.Value()
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	close(stop)
	readerDone.Wait()

	if got, want := c.Value(), uint64(goroutines*perGoroutine); got != want {
		t.Errorf("Value() = %d, want %d", got, want)
	}
}

func TestGaugeSetAndAdd(t *testing.T) {
	var g Gauge
	g.Set(10)
	if got := g.Value(); got != 10 {
		t.Errorf("Value() = %d, want 10", got)
	}
	g.Add(-3)
	if got := g.Value(); got != 7 {
		t.Errorf("Value() = %d, want 7", got)
	}
}
