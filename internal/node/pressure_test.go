package node

import (
	"errors"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
)

// diskPressureOverride configures a testCluster's single node with fixed
// absolute-byte disk thresholds — deliberately small, arbitrary numbers
// (never compared against the real host filesystem's actual free space,
// since this test drives PressureMonitor via SetPressureSourceForTest
// instead of real sampling) purely so Open's own critical<pressure
// syntax/ordering validation has something concrete to check.
func diskPressureOverride(pressure, critical string, maxHeapBytes uint64) func(*Config) {
	return func(cfg *Config) {
		cfg.DiskPressureThreshold = pressure
		cfg.DiskCriticalThreshold = critical
		cfg.MaxHeapBytes = maxHeapBytes
		cfg.MaxInflightProposals = 8
		// A huge poll interval keeps tick()'s own periodic
		// checkResourcePressure call from ever firing during the test,
		// so this test's direct same-goroutine-only calls to it (an
		// event-loop-only method, by its own doc comment) never race
		// against run()'s goroutine.
		cfg.ResourcePollInterval = time.Hour
	}
}

// TestPressureHysteresis_StateMachine is the deterministic unit-level
// half of AC-13/§19.1/§6.4: drives Node.checkResourcePressure through
// Healthy -> LowSpace -> Critical -> LowSpace -> Healthy using injected
// Pressure samples (SetPressureSourceForTest), asserting both the
// PressureState transitions and the resulting write-gate tightening at
// every step, including the 10% hysteresis band on de-escalation.
func TestPressureHysteresis_StateMachine(t *testing.T) {
	tc := newTestClusterWithAdmissionOverride(t, 1, diskPressureOverride("1000B", "200B", 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	n := tc.node(leaderID)

	set := func(freeBytes uint64) {
		n.SetPressureSourceForTest(&fakePressureSource{p: admission.Pressure{
			DiskFreeBytes: freeBytes, DiskTotalBytes: 1_000_000, SampledAt: time.Now(),
		}})
		n.checkResourcePressure()
	}

	if PressureState(n.pressureState.Load()) != PressureNormal {
		t.Fatalf("initial state = %v, want Normal", PressureState(n.pressureState.Load()))
	}

	// Healthy -> LowSpace: free (500) < pressure threshold (1000).
	set(500)
	if PressureState(n.pressureState.Load()) != PressureLowSpace {
		t.Fatalf("after free=500: state = %v, want LowSpace", PressureState(n.pressureState.Load()))
	}
	if eff := n.admission.write.Stats().EffectiveConcurrent; eff != 2 { // max(1, 8/4)
		t.Fatalf("LowSpace effective concurrency = %d, want 2 (MaxInflightProposals/4)", eff)
	}

	// LowSpace -> Critical: free (100) < critical threshold (200).
	set(100)
	if PressureState(n.pressureState.Load()) != PressureCritical {
		t.Fatalf("after free=100: state = %v, want Critical", PressureState(n.pressureState.Load()))
	}
	if eff := n.admission.write.Stats().EffectiveConcurrent; eff != 0 {
		t.Fatalf("Critical effective concurrency = %d, want 0", eff)
	}

	// Critical does NOT clear at exactly the critical threshold (no
	// hysteresis margin yet): free=200 is not >= 200*1.1=220.
	set(200)
	if PressureState(n.pressureState.Load()) != PressureLowSpace {
		// Per this file's own state machine, leaving Critical the moment
		// diskCriticalClear holds moves to LowSpace on THIS tick (one
		// transition per tick) — 200 >= 200 is not >= 220, so it should
		// NOT have cleared yet; if it did, that's the hysteresis bug this
		// assertion exists to catch. Re-assert explicitly below.
	}
	if PressureState(n.pressureState.Load()) != PressureCritical {
		t.Fatalf("after free=200 (no 10%% margin yet): state = %v, want still Critical", PressureState(n.pressureState.Load()))
	}

	// Critical -> LowSpace: free (250) >= 200*1.1=220.
	set(250)
	if PressureState(n.pressureState.Load()) != PressureLowSpace {
		t.Fatalf("after free=250 (past 10%% margin): state = %v, want LowSpace", PressureState(n.pressureState.Load()))
	}

	// LowSpace does NOT clear to Healthy without its own 10% margin:
	// free=1000 is not >= 1000*1.1=1100.
	set(1000)
	if PressureState(n.pressureState.Load()) != PressureLowSpace {
		t.Fatalf("after free=1000 (no 10%% margin yet): state = %v, want still LowSpace", PressureState(n.pressureState.Load()))
	}

	// LowSpace -> Healthy: free (1200) >= 1000*1.1=1100.
	set(1200)
	if PressureState(n.pressureState.Load()) != PressureNormal {
		t.Fatalf("after free=1200 (past 10%% margin): state = %v, want Normal", PressureState(n.pressureState.Load()))
	}
	if eff := n.admission.write.Stats().EffectiveConcurrent; eff != 8 {
		t.Fatalf("Normal effective concurrency = %d, want 8 (MaxInflightProposals, fully restored)", eff)
	}
}

// TestPressureHysteresis_HeapAlsoDrivesLowSpaceNotCritical is §6.4's
// table: heap pressure alone can enter LowSpace but never Critical
// (Critical is disk-only).
func TestPressureHysteresis_HeapAlsoDrivesLowSpaceNotCritical(t *testing.T) {
	tc := newTestClusterWithAdmissionOverride(t, 1, diskPressureOverride("", "", 1000))
	leaderID := tc.awaitLeader(5 * time.Second)
	n := tc.node(leaderID)

	n.SetPressureSourceForTest(&fakePressureSource{p: admission.Pressure{HeapBytes: 2000, SampledAt: time.Now()}})
	n.checkResourcePressure()
	if PressureState(n.pressureState.Load()) != PressureLowSpace {
		t.Fatalf("heap over threshold: state = %v, want LowSpace", PressureState(n.pressureState.Load()))
	}

	n.SetPressureSourceForTest(&fakePressureSource{p: admission.Pressure{HeapBytes: 850, SampledAt: time.Now()}})
	n.checkResourcePressure()
	if PressureState(n.pressureState.Load()) != PressureNormal {
		t.Fatalf("heap back under 0.9x threshold: state = %v, want Normal", PressureState(n.pressureState.Load()))
	}
}

// TestPressureProbeFailure_FailSafeDirection is §6.2's fail-safe
// direction: a sampling error is never silently treated as healthy — it
// drives LowSpace, exactly like a real low-headroom reading would.
func TestPressureProbeFailure_FailSafeDirection(t *testing.T) {
	tc := newTestClusterWithAdmissionOverride(t, 1, diskPressureOverride("1000B", "200B", 0))
	leaderID := tc.awaitLeader(5 * time.Second)
	n := tc.node(leaderID)

	before := n.Metrics().DiskProbeFailuresTotal
	n.SetPressureSourceForTest(&fakePressureSource{p: admission.Pressure{Err: errors.New("injected probe failure"), SampledAt: time.Now()}})
	n.checkResourcePressure()
	if PressureState(n.pressureState.Load()) != PressureLowSpace {
		t.Fatalf("probe error: state = %v, want LowSpace (fail-safe direction)", PressureState(n.pressureState.Load()))
	}
	if got := n.diskPressureStatusString(); got != "probe_failed" {
		t.Fatalf("diskPressureStatusString() = %q, want %q", got, "probe_failed")
	}
	if after := n.Metrics().DiskProbeFailuresTotal; after != before+1 {
		t.Fatalf("DiskProbeFailuresTotal = %d, want %d", after, before+1)
	}
}
