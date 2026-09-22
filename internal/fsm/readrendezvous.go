package fsm

import (
	"sync"
	"sync/atomic"
	"time"
)

// This file is AC-19's real-process negative-control mechanism
// (docs/v0.6.0-plan.md §5.4a, §30.1, §31 gate 3).
//
// Why it exists. AC-19's claim is that `/outcome` is safe to leave
// ungated because GetOutcome takes FSM.mu in *shared* mode: an
// arbitrary number of concurrent client lookups run at once and none of
// them holds the mutex the event loop's Apply needs. The negative
// control must show that a real regression — FSM.mu reverted to an
// exclusive mutex, which SetExclusiveOutcomeLockForTest does — is
// actually *detected* at the real-process tier, not merely at
// internal/fsm's own unit tier.
//
// Why the obvious observable does not work. The first attempt used
// latency (chronicledb_raft_message_process_seconds p99, and then
// client-observed /outcome round-trip p99) as the oracle. That cannot
// discriminate, for a reason that is a property of the code and not of
// the machine: GetOutcome's critical section is one map lookup, on the
// order of tens of nanoseconds, so serializing it across even hundreds
// of concurrent callers adds only microseconds of aggregate queuing
// delay — orders of magnitude under any real host's scheduling and
// network noise floor. No amount of load makes a nanosecond-scale
// effect visible in a millisecond-scale measurement.
//
// What this mechanism observes instead. The difference between an
// RWMutex and a Mutex is not latency, it is *how many readers can be
// inside the critical section at once*: unbounded versus exactly one.
// That is a counting property with an infinite effect size, and it is
// exact rather than statistical. A one-shot rendezvous barrier, armed
// only by a test and entered only from inside GetOutcome's critical
// section, measures it directly:
//
//   - shared mode (production): all `want` concurrent /outcome requests
//     get inside together, the barrier's arrival count reaches `want`,
//     it releases immediately, Peak == want and Reached is true;
//   - exclusive mode (the injected regression): the first reader holds
//     the whole mutex and the other `want-1` cannot enter at all, so
//     the barrier can never fill. It resolves on its bounded deadline,
//     the remaining readers then pass through one at a time, and the
//     result is Peak == 1 with Reached false.
//
// Both outcomes are reached without a sleep, without retries and
// without any load: the positive case completes the instant the last
// reader arrives, and the deadline exists only as the negative case's
// bounded detection bound (docs/testing-strategy.md §11's "poll a
// condition with a generous deadline", not a timing measurement — the
// assertion is on Peak and Reached, never on how long anything took).
//
// Cost when unarmed, which is always in production, is a single
// atomic pointer load per GetOutcome — the same class of cost as
// exclusiveOutcomeLockForTest's own load in rLock, and no lock, no
// allocation and no branch beyond a nil check.

// ReadRendezvousResult reports what a rendezvous armed by
// ArmReadRendezvousForTest observed. Armed is false when none was ever
// armed on this FSM; Peak is the high-water mark of readers
// simultaneously inside GetOutcome's critical section, which is the
// whole point of the mechanism (see this file's header).
type ReadRendezvousResult struct {
	Armed   bool
	Want    int
	Arrived int
	Peak    int
	Reached bool
}

// readRendezvous is one armed barrier. It is replaced wholesale by each
// ArmReadRendezvousForTest call rather than reset, so a stale reader
// still inside an earlier barrier can never touch a later one's counts.
type readRendezvous struct {
	want    int
	timeout time.Duration

	// release is closed exactly once, by resolve, when either `want`
	// readers are simultaneously inside (the barrier filled) or the
	// first waiter's deadline expires (it cannot fill).
	release  chan struct{}
	once     sync.Once
	resolved atomic.Bool

	mu      sync.Mutex
	inside  int
	arrived int
	peak    int
	reached bool
}

func (rz *readRendezvous) resolve() {
	rz.once.Do(func() {
		rz.resolved.Store(true)
		close(rz.release)
	})
}

// ArmReadRendezvousForTest arms a one-shot rendezvous barrier inside
// GetOutcome's critical section (see this file's header). want <= 0
// disarms instead. Test-only; production code never calls it, and the
// only way to reach it against a real OS process is the /fault
// endpoint's armoutcomereadrendezvous action, which is registered only
// under -enable-fault-endpoint.
func (f *FSM) ArmReadRendezvousForTest(want int, timeout time.Duration) {
	if want <= 0 {
		f.readRendezvous.Store(nil)
		return
	}
	f.readRendezvous.Store(&readRendezvous{
		want:    want,
		timeout: timeout,
		release: make(chan struct{}),
	})
}

// ReadRendezvousResultForTest reports the currently armed barrier's
// observations. Callers read it after every request they issued has
// returned; until then Arrived is still climbing.
func (f *FSM) ReadRendezvousResultForTest() ReadRendezvousResult {
	rz := f.readRendezvous.Load()
	if rz == nil {
		return ReadRendezvousResult{}
	}
	rz.mu.Lock()
	defer rz.mu.Unlock()
	return ReadRendezvousResult{
		Armed:   true,
		Want:    rz.want,
		Arrived: rz.arrived,
		Peak:    rz.peak,
		Reached: rz.reached,
	}
}

// readRendezvousArrive is called by GetOutcome with f.mu already held
// in whatever mode rLock chose — which is exactly what makes it a
// discriminating control: under RLock the callers accumulate, under
// Lock they cannot.
//
// Arrivals are counted whether or not the barrier has already resolved,
// so that Arrived is `want` in both the shared and exclusive cases and
// the test can distinguish "the readers did not overlap" from "the
// readers were never sent" — only the *waiting* is skipped once
// resolved.
func (f *FSM) readRendezvousArrive() {
	rz := f.readRendezvous.Load()
	if rz == nil {
		return
	}

	rz.mu.Lock()
	rz.arrived++
	rz.inside++
	if rz.inside > rz.peak {
		rz.peak = rz.inside
	}
	full := rz.inside >= rz.want
	if full {
		rz.reached = true
	}
	rz.mu.Unlock()

	if full || rz.resolved.Load() {
		rz.resolve()
	} else {
		timer := time.NewTimer(rz.timeout)
		select {
		case <-rz.release:
		case <-timer.C:
			rz.resolve()
		}
		timer.Stop()
	}

	rz.mu.Lock()
	rz.inside--
	rz.mu.Unlock()
}
