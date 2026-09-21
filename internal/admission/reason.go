// Package admission implements ChronicleDB's bounded-capacity admission
// control (docs/v0.6.0-plan.md §5.1, part of the v0.6.0 Admission
// Control / Resource Protection release). It is a leaf package: it
// imports nothing beyond the standard library plus internal/metrics,
// and internal/metrics imports nothing at all — so this package can
// never introduce a dependency cycle with internal/raft, internal/fsm,
// internal/wal, internal/storage, internal/txn, or internal/node
// (docs/architecture.md §5's acyclic dependency direction).
//
// internal/admission never touches internal/raft, internal/node, or
// internal/fsm: it knows nothing about proposals, leadership, or the
// state machine. It is a generic bounded-concurrency primitive (Gate)
// plus the stable rejection vocabulary (Reason) and resource-pressure
// types (Pressure/PressureSource) that node.go's event loop and
// internal/sql wire up against it (docs/v0.6.0-plan.md §4.2 Rule CP-1:
// every gate acquisition happens in a method that runs on a caller's
// goroutine, never inside internal/node's event loop itself).
package admission

import "time"

// Reason is admission's stable, documented rejection-reason vocabulary
// (docs/v0.6.0-plan.md §8.2), following the precedent docs/membership.md
// already set with its own fixed "reason" strings. New reasons may be
// added in a MINOR release; existing ones never change meaning — a
// client must treat an unknown reason as retryable if 503 was returned.
type Reason string

const (
	// ReasonQueueFull: the gate's waiting room was full.
	ReasonQueueFull Reason = "queue_full"
	// ReasonQueueTimeout: waited MaxWait without a slot.
	ReasonQueueTimeout Reason = "queue_timeout"
	// ReasonConcurrencyLimit: an event-loop ceiling fired — len(n.waiters)
	// or len(n.pendingReads) (docs/v0.6.0-plan.md §5.3, §9.1a).
	ReasonConcurrencyLimit Reason = "concurrency_limit"
	// ReasonReadLeaseLimit: the live read-lease ceiling
	// -max-live-read-leases was reached (docs/v0.6.0-plan.md §9.1, §15.3).
	ReasonReadLeaseLimit Reason = "read_lease_limit"
	// ReasonDiskPressure: LowSpace; the tightened ceiling was reached.
	ReasonDiskPressure Reason = "disk_pressure"
	// ReasonDiskCritical: Critical; all client writes refused.
	ReasonDiskCritical Reason = "disk_critical"
	// ReasonMemoryPressure: -max-heap-bytes exceeded.
	ReasonMemoryPressure Reason = "memory_pressure"
	// ReasonAdminOperationInProgress: a Lane A single-slot operation is
	// busy (docs/v0.6.0-plan.md §3.2a).
	ReasonAdminOperationInProgress Reason = "admin_operation_in_progress"
	// ReasonShuttingDown: the node is stopping. Do not retry here.
	ReasonShuttingDown Reason = "shutting_down"
)

// KnownReasons lists every Reason this build understands, in the order
// docs/v0.6.0-plan.md §8.2 defines them — used to pre-build the fixed
// per-reason metric label set at construction time
// (docs/v0.6.0-plan.md §11.1's amended label-policy rule: a label's
// value set must be fixed at compile time and bounded) and by tests
// that must exercise every reason.
var KnownReasons = []Reason{
	ReasonQueueFull,
	ReasonQueueTimeout,
	ReasonConcurrencyLimit,
	ReasonReadLeaseLimit,
	ReasonDiskPressure,
	ReasonDiskCritical,
	ReasonMemoryPressure,
	ReasonAdminOperationInProgress,
	ReasonShuttingDown,
}

// DefaultRetryAfter returns the docs/v0.6.0-plan.md §8.2 Retry-After
// value for r. Zero means "do not retry" (ReasonShuttingDown only). A
// Reason this build does not recognize (e.g. one a future MINOR release
// added) gets a conservative 1s default — matching the client-side rule
// that an unknown reason must still be treated as retryable.
func (r Reason) DefaultRetryAfter() time.Duration {
	switch r {
	case ReasonQueueFull, ReasonQueueTimeout, ReasonConcurrencyLimit, ReasonReadLeaseLimit:
		return time.Second
	case ReasonDiskPressure, ReasonMemoryPressure:
		return 5 * time.Second
	case ReasonDiskCritical:
		return 30 * time.Second
	case ReasonAdminOperationInProgress:
		return 10 * time.Second
	case ReasonShuttingDown:
		return 0
	default:
		return time.Second
	}
}
