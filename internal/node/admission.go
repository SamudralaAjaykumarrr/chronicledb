package node

import (
	"context"
	"errors"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
)

// admissionGates bundles every internal/admission.Gate a Node owns
// (docs/v0.6.0-plan.md §3.2's four lanes). Lane K (consensus) has no
// gate at all, structurally (§4.2 Rule CP-1): there is deliberately no
// field here for it, and TestAdmissionNeverReachableFromEventLoop
// (admission_ast_test.go) asserts no function reachable from run()
// references anything in this file or in internal/admission.
type admissionGates struct {
	write       *admission.Gate // Lane B: Propose
	read        *admission.Gate // Lane B: BeginReadIndex
	control     *admission.Gate // Lane A1: membership, upgrade precheck/finalize, TLS reload
	maintenance *admission.Gate // Lane A2: backup, scrub (§3.2a)
	// backupSlot/scrubSlot: the single-slot-per-kind on top of the
	// shared maintenance gate (§3.2a): at most one backup and at most
	// one scrub at a time, and the two may run concurrently with each
	// other (they only share the maintenance gate's capacity, never a
	// dedicated slot).
	backupSlot *admission.Gate
	scrubSlot  *admission.Gate
}

// newAdmissionGates constructs every gate a Node owns from cfg. Returns
// an error — never a gate with a silently-adjusted limit — for any
// invalid configuration (ADMISSION FAILS CLOSED); Open treats this as
// fatal, exactly like an invalid TLS/RBAC configuration.
func newAdmissionGates(cfg Config) (*admissionGates, error) {
	laneB := func(name string, maxConcurrent int) (*admission.Gate, error) {
		return admission.NewGate(name, admission.Limits{
			MaxConcurrent: maxConcurrent,
			MaxQueueDepth: cfg.AdmissionQueueDepth,
			MaxWait:       cfg.AdmissionMaxWait,
		})
	}
	// Lane A (control/maintenance) never queues: it is deliberately
	// immediate-reject-or-admit (docs/v0.6.0-plan.md §3.2a) — a busy
	// admin/maintenance slot means "come back shortly"
	// (admin_operation_in_progress), not "wait here."
	laneA := func(name string, maxConcurrent int) (*admission.Gate, error) {
		return admission.NewGate(name, admission.Limits{MaxConcurrent: maxConcurrent})
	}

	write, err := laneB("write", cfg.MaxInflightProposals)
	if err != nil {
		return nil, err
	}
	read, err := laneB("read", cfg.MaxConcurrentReads)
	if err != nil {
		return nil, err
	}
	control, err := laneA("control", cfg.MaxAdminConcurrency)
	if err != nil {
		return nil, err
	}
	maintenance, err := laneA("maintenance", cfg.MaxMaintenanceConcurrency)
	if err != nil {
		return nil, err
	}
	backupSlot, err := laneA("maintenance_backup", 1)
	if err != nil {
		return nil, err
	}
	scrubSlot, err := laneA("maintenance_scrub", 1)
	if err != nil {
		return nil, err
	}
	return &admissionGates{
		write: write, read: read,
		control: control, maintenance: maintenance,
		backupSlot: backupSlot, scrubSlot: scrubSlot,
	}, nil
}

// acquireSingleSlot acquires gate and translates a queue_full rejection
// into ReasonAdminOperationInProgress (docs/v0.6.0-plan.md §8.2): gate
// is one of the capacity-1 single-slot gates (backupSlot/scrubSlot),
// where "the waiting room was full" always means, semantically, "this
// specific Lane A2 operation kind is already running" — a distinct,
// documented rejection reason from generic queue_full.
func acquireSingleSlot(ctx context.Context, gate *admission.Gate) (func(), error) {
	release, err := gate.Acquire(ctx)
	if err == nil {
		return release, nil
	}
	var rej *admission.RejectedError
	if errors.As(err, &rej) && rej.Reason == admission.ReasonQueueFull {
		return nil, &admission.RejectedError{
			Reason:     admission.ReasonAdminOperationInProgress,
			RetryAfter: admission.ReasonAdminOperationInProgress.DefaultRetryAfter(),
		}
	}
	return nil, err
}
