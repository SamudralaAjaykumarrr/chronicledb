package admission

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestGateExactBoundary is AC-1 (docs/v0.6.0-plan.md §30.1): with
// MaxQueueDepth=0, exactly MaxConcurrent Acquire calls succeed; the
// next is rejected queue_full; releasing one lets the next succeed.
func TestGateExactBoundary(t *testing.T) {
	const n = 4
	g, err := NewGate("test", Limits{MaxConcurrent: n, MaxQueueDepth: 0})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	ctx := context.Background()

	var releases []func()
	for i := 0; i < n; i++ {
		release, err := g.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d/%d: unexpected error: %v", i+1, n, err)
		}
		releases = append(releases, release)
	}

	if _, err := g.Acquire(ctx); err == nil {
		t.Fatal("acquire n+1: expected rejection, got success")
	} else {
		var rej *RejectedError
		if !errors.As(err, &rej) {
			t.Fatalf("acquire n+1: error is not *RejectedError: %v", err)
		}
		if rej.Reason != ReasonQueueFull {
			t.Errorf("acquire n+1: Reason = %q, want %q", rej.Reason, ReasonQueueFull)
		}
		if !errors.Is(err, ErrOverloaded) {
			t.Errorf("acquire n+1: errors.Is(err, ErrOverloaded) = false, want true")
		}
	}

	releases[0]()
	if release, err := g.Acquire(ctx); err != nil {
		t.Fatalf("acquire after release: unexpected error: %v", err)
	} else {
		release()
	}

	for _, r := range releases[1:] {
		r()
	}
}

// TestGateAcquireNeverSucceedsWithoutAHeldSlot is AC-2
// (docs/v0.6.0-plan.md §5.6 item 3, §30.1): MaxConcurrent+1 concurrent
// Acquire calls against a gate whose successful releases are withheld
// within each iteration; exactly MaxConcurrent succeed and exactly one
// is rejected, at 1000 iterations, under -race.
func TestGateAcquireNeverSucceedsWithoutAHeldSlot(t *testing.T) {
	const n = 5
	const iterations = 1000
	ctx := context.Background()

	for iter := 0; iter < iterations; iter++ {
		g, err := NewGate("test", Limits{MaxConcurrent: n, MaxQueueDepth: 0})
		if err != nil {
			t.Fatalf("NewGate: %v", err)
		}

		var wg sync.WaitGroup
		results := make([]error, n+1)
		releases := make([]func(), n+1)
		wg.Add(n + 1)
		for i := 0; i < n+1; i++ {
			go func(i int) {
				defer wg.Done()
				release, err := g.Acquire(ctx)
				results[i] = err
				releases[i] = release
			}(i)
		}
		wg.Wait()

		successes, rejections := 0, 0
		for i, err := range results {
			if err == nil {
				successes++
				if releases[i] == nil {
					t.Fatalf("iteration %d: nil error but nil release function — Acquire returned success without a held slot", iter)
				}
			} else {
				rejections++
				if releases[i] != nil {
					t.Fatalf("iteration %d: non-nil release alongside a non-nil error", iter)
				}
				var rej *RejectedError
				if !errors.As(err, &rej) || rej.Reason != ReasonQueueFull {
					t.Fatalf("iteration %d: unexpected error: %v", iter, err)
				}
			}
		}
		if successes != n {
			t.Fatalf("iteration %d: successes = %d, want %d", iter, successes, n)
		}
		if rejections != 1 {
			t.Fatalf("iteration %d: rejections = %d, want 1", iter, rejections)
		}
		if g.InFlight() != n {
			t.Fatalf("iteration %d: InFlight() = %d, want %d", iter, g.InFlight(), n)
		}

		for _, r := range releases {
			if r != nil {
				r()
			}
		}
		if g.InFlight() != 0 {
			t.Fatalf("iteration %d: InFlight() after releasing everything = %d, want 0", iter, g.InFlight())
		}
	}
}

// TestGateQueueingAndTimeout exercises MaxQueueDepth>0 and MaxWait: a
// queued caller succeeds once a slot frees, and a queued caller past
// MaxWait is rejected queue_timeout rather than blocking forever.
func TestGateQueueingAndTimeout(t *testing.T) {
	g, err := NewGate("test", Limits{MaxConcurrent: 1, MaxQueueDepth: 1, MaxWait: 30 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	ctx := context.Background()

	release1, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Second caller queues (MaxQueueDepth=1) and, since nothing frees
	// the slot within MaxWait, times out.
	start := time.Now()
	_, err = g.Acquire(ctx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected queue_timeout rejection, got success")
	}
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Reason != ReasonQueueTimeout {
		t.Fatalf("expected ReasonQueueTimeout, got %v", err)
	}
	if elapsed < 30*time.Millisecond {
		t.Errorf("timed out after %v, want >= MaxWait (30ms)", elapsed)
	}

	// A third caller now queues successfully and is admitted once the
	// first releases.
	done := make(chan error, 1)
	go func() {
		_, err := g.Acquire(ctx)
		done <- err
	}()
	time.Sleep(10 * time.Millisecond) // let it enter the waiting room
	release1()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("queued caller after release: unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued caller never resolved after the slot freed")
	}
}

// TestGateContextCancellationReleasesQueueSlot asserts a canceled
// caller frees its admission ticket for the next caller — the gate-
// level half of fact 8d's fix (the event-loop ceilings, not this gate,
// are what bound waiters/pendingReads across a canceled caller; this
// test only asserts the gate itself never leaks).
func TestGateContextCancellationReleasesQueueSlot(t *testing.T) {
	g, err := NewGate("test", Limits{MaxConcurrent: 1, MaxQueueDepth: 1})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	release1, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer release1()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Acquire(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// The canceled caller's admission ticket must have been released —
	// a fresh caller with an immediate deadline should be able to
	// queue (not reject queue_full) even though a slot is still held.
	shortCtx, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	_, err = g.Acquire(shortCtx)
	var rej *RejectedError
	if errors.As(err, &rej) && rej.Reason == ReasonQueueFull {
		t.Fatalf("expected to be able to enter the queue (not queue_full) — the canceled caller's admission ticket leaked")
	}
}

// TestNewGateRejectsInvalidConfig is AC-4's Gate-level portion
// (docs/v0.6.0-plan.md §30.1): MaxConcurrent<=0 refuses construction
// with a message naming the flag/field, never silently substituting
// "unlimited" (ADMISSION FAILS CLOSED). The remaining AC-4 sub-cases
// (threshold ordering: internal/node/ac13_test.go/pressure_test.go;
// platform support: internal/node/pressure_test.go) are covered where
// those flags are wired up. The originally-planned fourth sub-case
// (flag mutual exclusivity between -max-concurrent-transactions and
// -max-concurrent-sql-statements) does not apply: neither is a
// cmd/chronicledb-node flag in this release — see
// docs/v0.6.0-plan.md §26/D3.
func TestNewGateRejectsInvalidConfig(t *testing.T) {
	cases := []Limits{
		{MaxConcurrent: 0},
		{MaxConcurrent: -1},
		{MaxConcurrent: 1, MaxQueueDepth: -1},
		{MaxConcurrent: 1, MaxWait: -time.Second},
	}
	for _, lim := range cases {
		if _, err := NewGate("test", lim); err == nil {
			t.Errorf("NewGate(%+v): expected error, got none", lim)
		}
	}
}

// TestGateEffectiveConcurrentTightening exercises SetEffectiveConcurrent
// (docs/v0.6.0-plan.md §6.4 pressure tightening): once the ceiling is
// lowered below current in-flight count, a subsequent Acquire that
// would otherwise succeed (a real slot is free) is rejected with the
// configured pressure reason instead, and the ceiling can never exceed
// MaxConcurrent regardless of what it is set to.
func TestGateEffectiveConcurrentTightening(t *testing.T) {
	g, err := NewGate("test", Limits{MaxConcurrent: 4, MaxQueueDepth: 4})
	if err != nil {
		t.Fatalf("NewGate: %v", err)
	}
	ctx := context.Background()

	r1, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	r2, err := g.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}

	g.SetPressureReason(ReasonMemoryPressure)
	g.SetEffectiveConcurrent(2) // == current InFlight; a 3rd must now be refused

	if _, err := g.Acquire(ctx); err == nil {
		t.Fatal("expected rejection once effective ceiling reached, got success")
	} else {
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Reason != ReasonMemoryPressure {
			t.Fatalf("expected ReasonMemoryPressure, got %v", err)
		}
	}

	// The ceiling can never exceed MaxConcurrent.
	g.SetEffectiveConcurrent(1000)
	if got := g.Stats().EffectiveConcurrent; got != 4 {
		t.Errorf("EffectiveConcurrent after SetEffectiveConcurrent(1000) = %d, want clamped to MaxConcurrent=4", got)
	}

	r1()
	r2()
}
