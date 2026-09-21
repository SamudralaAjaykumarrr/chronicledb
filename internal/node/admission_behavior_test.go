package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
)

// smallAdmissionOverride configures a testCluster with deliberately
// small admission limits so a handful of goroutines can saturate a gate
// or an event-loop ceiling deterministically, without needing hundreds
// of concurrent callers.
func smallAdmissionOverride(maxInflight, maxReads, queueDepth int) func(*Config) {
	return func(cfg *Config) {
		cfg.MaxInflightProposals = maxInflight
		cfg.MaxConcurrentReads = maxReads
		cfg.AdmissionQueueDepth = queueDepth
		cfg.AdmissionMaxWait = 50 * time.Millisecond
	}
}

// awaitCondition2 polls cond until it returns true or timeout elapses,
// failing the test otherwise — the bounded-polling discipline this
// package's own awaitCondition already establishes, duplicated here
// (rather than reused) only because it is defined with a different
// receiver elsewhere in the package; kept trivial on purpose.
func pollUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition did not become true within %s", timeout)
	}
}

// TestAC1through3_BoundedAdmittedWork is AC-1 (restated at the node
// tier, beyond admission's own package-level AC-1)/AC-3
// (docs/v0.6.0-plan.md §30.1): under a saturating write load against a
// real testCluster, len(n.waiters) — the authoritative BOUNDED ADMITTED
// WORK ceiling — never exceeds -max-inflight-proposals, and (AC-14)
// neither admission-defense counter moves, because no client context is
// ever canceled in this scenario.
func TestAC3_WaitersNeverExceedsCeilingUnderSaturation(t *testing.T) {
	const maxInflight = 4
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 4))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var maxObserved int64
	var mu sync.Mutex
	stop := make(chan struct{})
	var monitor sync.WaitGroup
	monitor.Add(1)
	go func() {
		defer monitor.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			g := leader.Metrics().WaitersGauge
			mu.Lock()
			if g > maxObserved {
				maxObserved = g
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	const workers = 20
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 15; j++ {
				reqID := fmt.Sprintf("sat-%d-%d", i, j)
				_, _ = propose(t, leader, cmd(reqID, uint64(i*1000+j), 0, fmt.Sprintf("k-%d-%d", i, j), "v"), 5*time.Second)
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	monitor.Wait()

	mu.Lock()
	got := maxObserved
	mu.Unlock()
	if got > int64(maxInflight) {
		t.Fatalf("observed len(n.waiters) = %d, want never to exceed -max-inflight-proposals (%d)", got, maxInflight)
	}

	// AC-14: no client context was ever canceled in this scenario, so
	// the defense-rejection counters must stay exactly zero.
	m := leader.Metrics()
	if m.AdmissionDefenseRejectionsWaitersTotal != 0 {
		t.Errorf("AdmissionDefenseRejectionsWaitersTotal = %d, want 0 (no cancellation occurred in this test)", m.AdmissionDefenseRejectionsWaitersTotal)
	}
	if m.AdmissionDefenseRejectionsPendingReadsTotal != 0 {
		t.Errorf("AdmissionDefenseRejectionsPendingReadsTotal = %d, want 0", m.AdmissionDefenseRejectionsPendingReadsTotal)
	}
}

// TestAC5_NoopGateStillBoundedByEventLoopCeiling is AC-5's negative
// control (docs/v0.6.0-plan.md §30.1, §27.4 ADMISSION FAILS CLOSED):
// with the write gate replaced by a no-op, the event-loop
// len(n.waiters) ceiling still holds the bound, and the defense counter
// rises to prove it actually fired.
func TestAC5_NoopGateStillBoundedByEventLoopCeiling(t *testing.T) {
	const maxInflight = 3
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)
	leader.SetNoopAdmissionGateForTest(true)
	defer leader.SetNoopAdmissionGateForTest(false)

	// Isolate the leader so proposals are accepted into its local log
	// (registering a waiter) but never commit — every caller's short
	// ctx times out, freeing the (now no-op, so irrelevant) gate slot
	// while the waiter entry survives, exactly fact 8d's shape. With no
	// gate to bound admission at all, only the event-loop ceiling can
	// prevent len(n.waiters) from growing without bound.
	tc.isolate(leaderID)

	var wg sync.WaitGroup
	const flood = 40
	wg.Add(flood)
	for i := 0; i < flood; i++ {
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			_, _ = leader.Propose(ctx, cmd(fmt.Sprintf("ac5-%d", i), uint64(i), 0, fmt.Sprintf("k%d", i), "v"))
		}(i)
	}
	wg.Wait()

	if got := leader.Metrics().WaitersGauge; got > int64(maxInflight) {
		t.Fatalf("with a no-op gate, len(n.waiters) = %d, want never to exceed the event-loop ceiling (%d)", got, maxInflight)
	}
	if got := leader.Metrics().AdmissionDefenseRejectionsWaitersTotal; got == 0 {
		t.Fatal("AdmissionDefenseRejectionsWaitersTotal = 0, want > 0 — the event-loop ceiling should have fired under this flood")
	}
}

// TestAC6_ControlPlaneNonStarvationUnderSaturatedClientLoad is AC-6
// (docs/v0.6.0-plan.md §30.1, §29.3's testCluster-tier substitution for
// internal/fault's Core-only limitation): a permanently saturated
// client write gate must not produce elections — heartbeat/AppendEntries
// processing must continue on schedule regardless of client admission
// pressure.
func TestAC6_ControlPlaneNonStarvationUnderSaturatedClientLoad(t *testing.T) {
	const maxInflight = 2
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	electionsAt := func() uint64 {
		var total uint64
		for _, n := range tc.nodes {
			total += n.Metrics().ElectionsTotal
		}
		return total
	}
	before := electionsAt()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	const saturators = 8
	wg.Add(saturators)
	for i := 0; i < saturators; i++ {
		go func(i int) {
			defer wg.Done()
			j := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				j++
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, _ = leader.Propose(ctx, cmd(fmt.Sprintf("ac6-%d-%d", i, j), uint64(i*100000+j), 0, fmt.Sprintf("k-%d-%d", i, j), "v"))
				cancel()
			}
		}(i)
	}

	// Sustain saturation across several real heartbeat intervals
	// (10ms tick, 1 heartbeat tick per configFor) so a genuine
	// starvation-induced election would have every opportunity to fire.
	time.Sleep(1 * time.Second)
	close(stop)
	wg.Wait()

	after := electionsAt()
	if after != before {
		t.Fatalf("elections occurred under sustained client-write saturation (before=%d after=%d) — CONTROL-PLANE NON-STARVATION requires zero", before, after)
	}
	// Leadership must still be stable post-saturation; awaitLeader fails
	// the test itself if no single leader is present.
	tc.awaitLeader(2 * time.Second)
}

// TestAC7_NegativeControl_LaneSeparationDisabledProducesElections is
// AC-7 (docs/v0.6.0-plan.md §30.1): with lane separation deliberately
// disabled via the test hook, the identical saturation scenario AC-6
// ran DOES produce elections — proving AC-6's positive test would
// actually have caught the regression this hook reintroduces (§31
// gate 3: a test that cannot fail when its mechanism is removed does
// not count).
func TestAC7_NegativeControl_LaneSeparationDisabledProducesElections(t *testing.T) {
	const maxInflight = 2
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)
	leader.SetSkipAdmissionLaneSeparationForTest(true)
	defer leader.SetSkipAdmissionLaneSeparationForTest(false)

	electionsAt := func() uint64 {
		var total uint64
		for _, n := range tc.nodes {
			total += n.Metrics().ElectionsTotal
		}
		return total
	}
	before := electionsAt()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	const saturators = 8
	wg.Add(saturators)
	for i := 0; i < saturators; i++ {
		go func(i int) {
			defer wg.Done()
			j := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				j++
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, _ = leader.Propose(ctx, cmd(fmt.Sprintf("ac7-%d-%d", i, j), uint64(i*100000+j), 0, fmt.Sprintf("k-%d-%d", i, j), "v"))
				cancel()
			}
		}(i)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && electionsAt() == before {
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if after := electionsAt(); after == before {
		t.Fatal("negative control: with lane separation disabled, expected at least one election to occur under saturation, but none did — the positive AC-6 test would not have caught a real regression")
	}
}

// TestAC8_RejectionSafety_NoDuplicateEffectsUnderRecycledRetries is
// AC-8 (docs/v0.6.0-plan.md §30.1): under a stress workload where
// proposals are frequently rejected for capacity and retried with the
// same RequestID, every successful outcome ever observed for a given
// RequestID is identical — a rejection never partially took effect.
func TestAC8_RejectionSafety_NoDuplicateEffectsUnderRecycledRetries(t *testing.T) {
	const maxInflight = 2
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	const distinctRequests = 6
	const attemptsPerRequest = 30
	type observed struct {
		status    fsm.Status
		commitSeq uint64
	}
	results := make([]map[observed]bool, distinctRequests)
	var mus [distinctRequests]sync.Mutex
	for i := range results {
		results[i] = map[observed]bool{}
	}

	var wg sync.WaitGroup
	for r := 0; r < distinctRequests; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			reqID := fmt.Sprintf("ac8-%d", r)
			for a := 0; a < attemptsPerRequest; a++ {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				outcome, err := leader.Propose(ctx, cmd(reqID, uint64(r), 0, fmt.Sprintf("ac8key-%d", r), "v"))
				cancel()
				if err != nil {
					continue // rejected or otherwise unresolved; retry
				}
				mus[r].Lock()
				results[r][observed{status: outcome.Status, commitSeq: outcome.CommitSeq}] = true
				mus[r].Unlock()
			}
		}(r)
	}
	wg.Wait()

	for r, set := range results {
		if len(set) > 1 {
			t.Errorf("RequestID ac8-%d observed %d distinct outcomes across retries, want at most 1: %+v", r, len(set), set)
		}
	}
}

// TestAC9_RejectedProposalRecordsNoOutcome is AC-9
// (docs/v0.6.0-plan.md §30.1): a proposal rejected for capacity never
// reaches GetOutcome (the /outcome equivalent) — nothing was recorded.
func TestAC9_RejectedProposalRecordsNoOutcome(t *testing.T) {
	const maxInflight = 1
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	// Hold the single slot with a long-running proposal by isolating
	// the leader first (so the holder never resolves and releases).
	tc.isolate(leaderID)
	defer tc.heal(leaderID)

	holderCtx, holderCancel := context.WithCancel(context.Background())
	defer holderCancel()
	holderStarted := make(chan struct{})
	go func() {
		go func() { close(holderStarted) }()
		_, _ = leader.Propose(holderCtx, cmd("ac9-holder", 1, 0, "hk", "v"))
	}()
	<-holderStarted
	pollUntil(t, 2*time.Second, func() bool { return leader.admission.write.InFlight() >= maxInflight })

	rejectedID := fsm.RequestID("ac9-rejected")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := leader.Propose(ctx, fsm.CommitTxnCommand{RequestID: rejectedID, TxnID: 99, StartSeq: 0,
		Mutations: nil})
	if err == nil {
		t.Fatal("expected a rejection while the single write slot is held, got success")
	}
	var rej *admission.RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("expected *admission.RejectedError, got %v", err)
	}

	if _, ok := leader.FSM().GetOutcome(rejectedID); ok {
		t.Fatalf("GetOutcome(%q) found a recorded outcome for a request that was rejected before ever being proposed", rejectedID)
	}
}

// TestAC20_CancelHeavyWorkloadBoundedAndNoLeakAfterQuiescence is AC-20
// (docs/v0.6.0-plan.md §30.1) — the central proof of fact 8d's fix:
// every client abandons its Propose call well before the (partitioned,
// non-committing) leader could ever resolve it. len(n.waiters) never
// exceeds the ceiling, the defense counter DOES rise (this is normal
// operation under cancellation, not a bug — §5.3), and after the
// partition heals and the cluster quiesces, no waiter or gate slot has
// leaked.
func TestAC20_CancelHeavyWorkloadBoundedAndNoLeakAfterQuiescence(t *testing.T) {
	const maxInflight = 4
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(maxInflight, 512, 4))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	tc.isolate(leaderID) // no quorum reachable: nothing this leader accepts will ever commit

	var maxWaiters int64
	var mu sync.Mutex
	stopMonitor := make(chan struct{})
	var monWG sync.WaitGroup
	monWG.Add(1)
	go func() {
		defer monWG.Done()
		for {
			select {
			case <-stopMonitor:
				return
			default:
			}
			g := leader.Metrics().WaitersGauge
			mu.Lock()
			if g > maxWaiters {
				maxWaiters = g
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	const flood = 60
	wg.Add(flood)
	for i := 0; i < flood; i++ {
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			_, _ = leader.Propose(ctx, cmd(fmt.Sprintf("ac20-%d", i), uint64(i), 0, fmt.Sprintf("k%d", i), "v"))
		}(i)
	}
	wg.Wait()
	close(stopMonitor)
	monWG.Wait()

	mu.Lock()
	got := maxWaiters
	mu.Unlock()
	if got > int64(maxInflight) {
		t.Fatalf("under a cancel-heavy workload, len(n.waiters) peaked at %d, want never to exceed -max-inflight-proposals (%d)", got, maxInflight)
	}
	if defenseCount := leader.Metrics().AdmissionDefenseRejectionsWaitersTotal; defenseCount == 0 {
		t.Fatal("AdmissionDefenseRejectionsWaitersTotal = 0, want > 0 — a cancel-heavy workload should trip the event-loop ceiling")
	}

	// Heal and let the cluster quiesce; a former leader isolated this
	// long has likely been superseded, so its stuck waiters resolve via
	// the SteppedDown/ErrLeadershipLost path (§9.7) once it rejoins.
	tc.heal(leaderID)
	pollUntil(t, 10*time.Second, func() bool {
		return leader.Metrics().WaitersGauge == 0
	})
	if got := leader.admission.write.InFlight(); got != 0 {
		t.Fatalf("write gate InFlight() = %d after quiescence, want 0 (no leaked slot)", got)
	}
	if got := leader.admission.write.Queued(); got != 0 {
		t.Fatalf("write gate Queued() = %d after quiescence, want 0", got)
	}
}

// TestAC21Partial_PendingReadsCeilingUnderMinorityPartitionFlood is
// AC-21's pendingReads-ceiling portion (docs/v0.6.0-plan.md §30.1): a
// leader isolated into a minority resolves no reads; a flood of
// BeginReadIndex calls with short timeouts must never push
// len(n.pendingReads) past -max-concurrent-reads, and the defense
// counter fires. The live-read-lease ceiling and O(1) minLease portions
// of AC-21 are proven once the read-lease registry itself lands
// (docs/v0.6.0-plan.md §33 slice 9) — there is no lease to bound yet.
func TestAC21Partial_PendingReadsCeilingUnderMinorityPartitionFlood(t *testing.T) {
	const maxReads = 2
	tc := newTestClusterWithAdmissionOverride(t, 3, smallAdmissionOverride(256, maxReads, 2))
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	tc.isolate(leaderID)
	defer tc.heal(leaderID)

	var maxPending int64
	var mu sync.Mutex
	stopMonitor := make(chan struct{})
	var monWG sync.WaitGroup
	monWG.Add(1)
	go func() {
		defer monWG.Done()
		for {
			select {
			case <-stopMonitor:
				return
			default:
			}
			g := leader.Metrics().PendingReadsGauge
			mu.Lock()
			if g > maxPending {
				maxPending = g
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	// A sustained, time-based flood, not a single fixed batch: the
	// event-loop ceiling only diverges from the gate's own snapshot
	// concurrency across REPEATED waves (fact 8c) — a caller's ctx
	// expires, freeing its readGate slot (the caller-side defer) while
	// its pendingReads entry survives, so the next wave's caller is
	// admitted through the now-free gate slot and registers ANOTHER
	// entry alongside the stale one. Running many short-lived waves
	// over a real time window (rather than one synchronized batch) is
	// what reliably reproduces that pile-up regardless of scheduling
	// jitter under -race.
	stopFlood := make(chan struct{})
	var wg sync.WaitGroup
	const workers = 12
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopFlood:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
				_, _ = leader.BeginReadIndex(ctx)
				cancel()
			}
		}()
	}
	time.Sleep(1500 * time.Millisecond)
	close(stopFlood)
	wg.Wait()
	close(stopMonitor)
	monWG.Wait()

	mu.Lock()
	got := maxPending
	mu.Unlock()
	if got > int64(maxReads) {
		t.Fatalf("under a never-resolving read flood, len(n.pendingReads) peaked at %d, want never to exceed -max-concurrent-reads (%d)", got, maxReads)
	}
	if c := leader.Metrics().AdmissionDefenseRejectionsPendingReadsTotal; c == 0 {
		t.Fatal("AdmissionDefenseRejectionsPendingReadsTotal = 0, want > 0 under this flood")
	}
}

// TestAC22Partial_BackupDoesNotBlockMembershipChange is AC-22's
// Backup-vs-membership portion (docs/v0.6.0-plan.md §30.1, Rule CP-3):
// a running backup (Lane A2) must never delay or fail a concurrent
// membership change (Lane A1) — the two lanes are structurally
// disjoint. The scrub half of AC-22 lands once Scrub exists
// (docs/v0.6.0-plan.md §33 slice 12).
func TestAC22Partial_BackupDoesNotBlockMembershipChange(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	// Membership changes require generation >= 2 (§8.2).
	mustFinalizeToMax(t, tc, leader)

	// A spontaneous re-election anywhere in this test is
	// indistinguishable from a real defect from the test's own point of
	// view (see mustFinalizeToMax's identical remedy, and its doc
	// comment on why this is not rare under `go test -race`): freeze
	// the election clock for the duration. Heartbeat ticks keep
	// running, so replication/backup/membership are unaffected.
	tc.pauseTicking()
	defer tc.resumeTicking()

	// Drive some load so the backup has real work to do and genuinely
	// overlaps the membership call below, not a same-tick coincidence.
	for i := 0; i < 20; i++ {
		_, _ = propose(t, leader, cmd(fmt.Sprintf("ac22-%d", i), uint64(i), 0, fmt.Sprintf("k%d", i), "v"), 2*time.Second)
	}

	outDir := t.TempDir() + "/backup"
	backupDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := leader.Backup(ctx, outDir, false, "ac22")
		backupDone <- err
	}()

	// Give the backup a moment to actually be in flight before issuing
	// the membership call, so Lane A2 saturation is genuinely exercised
	// (not a race that happens to pass regardless).
	pollUntil(t, 2*time.Second, func() bool { return leader.admission.maintenance.InFlight() > 0 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome, err := leader.AddLearner(ctx, fsm.RequestID("ac22-learner"), "n4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("AddLearner concurrent with a running backup: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("AddLearner outcome = %+v, want StatusCommitted", outcome)
	}

	if err := <-backupDone; err != nil {
		t.Fatalf("Backup: %v", err)
	}
}
