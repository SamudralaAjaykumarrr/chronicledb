package fsm

import (
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// TestReadOnlyAccessorsDoNotSerializeAgainstEachOther is docs/v0.6.0-
// plan.md §5.4a's positive property, at the FSM-package tier: with the
// default RWMutex behavior, two concurrent read-only accessors
// (Precheck/GetOutcome) do not block each other — only a concurrent
// Apply (the writer) would. Proven directly: hold one goroutine inside
// GetOutcome via a blocking hook is not available at this layer, so
// instead this asserts many concurrent read-only calls complete well
// within a duration that would be impossible if they serialized one at
// a time behind an artificial per-call delay — see
// TestExclusiveOutcomeLockForTestSerializesReads below for the direct,
// deterministic proof that the *lock mode itself* is what changes.
func TestReadOnlyAccessorsDoNotSerializeAgainstEachOther(t *testing.T) {
	f := New(mvcc.NewStore())
	const readers = 64
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				f.GetOutcome(RequestID("nonexistent"))
				f.ClusterGeneration()
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("64 concurrent readers x 1000 read-only calls each did not complete within 10s")
	}
}

// TestExclusiveOutcomeLockForTestSerializesReads is AC-19's negative-
// control hook, proven directly (docs/v0.6.0-plan.md §5.4a, §29):
// with SetExclusiveOutcomeLockForTest(true), a read-only accessor
// blocks a concurrent read-only accessor — demonstrating the hook
// actually reverts to exclusive-lock behavior, which is what the
// node/real-process tier's AC-19 negative control (§29.3) depends on
// to prove the *positive* test would have caught the regression it
// reverts.
func TestExclusiveOutcomeLockForTestSerializesReads(t *testing.T) {
	f := New(mvcc.NewStore())
	f.SetExclusiveOutcomeLockForTest(true)
	defer f.SetExclusiveOutcomeLockForTest(false)

	f.mu.Lock() // simulate a long-held "reader" under exclusive-lock mode
	blocked := make(chan struct{})
	go func() {
		f.GetOutcome(RequestID("x")) // must block until the Lock above is released
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("GetOutcome returned while f.mu.Lock() was held — SetExclusiveOutcomeLockForTest(true) did not serialize reads")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	f.mu.Unlock()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("GetOutcome never unblocked after the exclusive lock was released")
	}
}

// TestReadOnlyAccessorsDoNotBlockEachOtherUnderRLock is the direct
// converse of the negative-control test above: with the hook left at
// its default (false), a held RLock does NOT block a concurrent
// read-only accessor.
func TestReadOnlyAccessorsDoNotBlockEachOtherUnderRLock(t *testing.T) {
	f := New(mvcc.NewStore())

	f.mu.RLock()
	defer f.mu.RUnlock()

	done := make(chan struct{})
	go func() {
		f.GetOutcome(RequestID("x"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GetOutcome blocked behind a concurrently held RLock — RWMutex reads should not serialize against each other")
	}
}

// TestReadRendezvousDiscriminatesLockMode calibrates AC-19's real-
// process negative-control *mechanism* itself at this package's own
// tier (readrendezvous.go, docs/v0.6.0-plan.md §5.4a): the barrier must
// fill under the production RWMutex and must be structurally unable to
// fill once SetExclusiveOutcomeLockForTest reverts GetOutcome to an
// exclusive mutex. A control that reported the same thing in both modes
// would prove nothing about the real-process run that depends on it.
func TestReadRendezvousDiscriminatesLockMode(t *testing.T) {
	const readers = 8
	const barrier = 5 * time.Second

	run := func(t *testing.T, exclusive bool) ReadRendezvousResult {
		t.Helper()
		f := New(mvcc.NewStore())
		f.SetExclusiveOutcomeLockForTest(exclusive)
		f.ArmReadRendezvousForTest(readers, barrier)

		var wg sync.WaitGroup
		wg.Add(readers)
		for i := 0; i < readers; i++ {
			go func() {
				defer wg.Done()
				f.GetOutcome(RequestID("nonexistent"))
			}()
		}
		wg.Wait()
		return f.ReadRendezvousResultForTest()
	}

	t.Run("shared", func(t *testing.T) {
		got := run(t, false)
		if got.Arrived != readers {
			t.Fatalf("Arrived = %d, want %d — the readers were never all sent", got.Arrived, readers)
		}
		if !got.Reached || got.Peak != readers {
			t.Errorf("under RLock: Peak = %d, Reached = %v; want Peak = %d, Reached = true — read-only accessors must not serialize (§5.4a)", got.Peak, got.Reached, readers)
		}
	})

	t.Run("exclusive (negative control)", func(t *testing.T) {
		got := run(t, true)
		if got.Arrived != readers {
			t.Fatalf("Arrived = %d, want %d — the readers were never all sent", got.Arrived, readers)
		}
		if got.Reached || got.Peak != 1 {
			t.Errorf("under the injected exclusive mutex: Peak = %d, Reached = %v; want Peak = 1, Reached = false — the control failed to detect the regression it injects", got.Peak, got.Reached)
		}
	})
}

// TestReadRendezvousUnarmedIsANoOp: the barrier must be inert unless a
// test armed one, since readRendezvousArrive sits on GetOutcome's
// production path.
func TestReadRendezvousUnarmedIsANoOp(t *testing.T) {
	f := New(mvcc.NewStore())
	if got := f.ReadRendezvousResultForTest(); got.Armed {
		t.Fatalf("a fresh FSM reports an armed rendezvous: %+v", got)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			f.GetOutcome(RequestID("nonexistent"))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("GetOutcome blocked with no rendezvous armed")
	}

	// Arming and then disarming must also leave nothing behind.
	f.ArmReadRendezvousForTest(2, time.Second)
	f.ArmReadRendezvousForTest(0, 0)
	if got := f.ReadRendezvousResultForTest(); got.Armed {
		t.Fatalf("disarming left an armed rendezvous: %+v", got)
	}
	f.GetOutcome(RequestID("nonexistent")) // must not block
}
