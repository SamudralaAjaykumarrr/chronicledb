package node

import "sync/atomic"

// FaultPoint names one of maybeSnapshot's six ordering points
// (docs/v0.6.0-plan.md §17.2, C4) or handleInstallSnapshot's three
// (§22) — each a deterministic, index-selected place this node's own
// production code can be made to simulate an ungraceful crash right
// there, never by timing (§33 slice 10a). SL-8 and SL-26 need this
// because internal/fault's deterministic simulator imports only
// internal/raft and has no snapshot manager, no internal/wal and no
// internal/storage of its own (§29.3/D10) — it cannot inject a failure
// at any of these steps.
type FaultPoint int

const (
	FaultPointNone FaultPoint = iota

	// maybeSnapshot's six ordering points (§17.2), one immediately
	// after each lettered step completes — matching exactly the six
	// crash-point rows in §22's table.
	FaultAfterSnapshotCreate         // after (a) snapMgr.Create
	FaultAfterAppendMetadataSnapshot // after (b) walog.AppendMetadataSnapshot
	FaultAfterCoreCompact            // after (c) core.Compact
	FaultAfterStorageCompact         // after (d) storage.Compact
	FaultAfterReaffirm               // after (e) storage.Reaffirm
	FaultAfterCompactBefore          // after (f) walog.CompactBefore

	// handleInstallSnapshot's three points (§22's three new rows).
	FaultBeforeInstallSnapshotStorage      // before n.storage.InstallSnapshot
	FaultAfterInstallSnapshotBeforeFSMSwap // after storage.InstallSnapshot, before n.fsmachine.Store
	FaultAfterFSMSwapBeforeGenerationAdopt // after the FSM swap, before adoptClusterGeneration
)

// faultHook, when armed, is consulted at every named FaultPoint. It is
// armed only by SetFaultPointForTest, which exists only in a binary
// built with -tags faulttest (faultpoint_hook.go): no ordinary build —
// including every production binary cmd/chronicledb-node ever ships —
// links that file, so no ordinary build contains any code path that
// could ever populate this variable. triggerFaultPoint is therefore an
// unconditional, permanently-cheap no-op (one atomic load of a nil
// pointer) in every build except a deliberately-constructed faulttest
// one, and is safe to call unconditionally from maybeSnapshot/
// handleInstallSnapshot's own production code.
var faultHook atomic.Pointer[func(FaultPoint) bool]

// faultPointCrash is the sentinel panic value triggerFaultPoint raises
// to simulate a crash. It is recovered exactly once, in the goroutine
// wrapper Open launches run() from (node.go), which lets run()'s own
// defer chain (ticker.Stop, shutdown) unwind and run first — the same
// fidelity testCluster.crash's own doc comment already establishes as
// an acceptable "ungraceful kill" simulation: shutdown adds no flush
// beyond what each durable write already fsync'd, so its running does
// not undo or mask anything the crash-safety proof cares about. A
// recover of any OTHER panic value is not ours to swallow and is
// re-panicked immediately, so a genuine bug is never mistaken for a
// simulated crash.
type faultPointCrash struct{ point FaultPoint }

// triggerFaultPoint calls the currently-armed hook, if any, and panics
// with faultPointCrash if it returns true for p. Called unconditionally
// from each of maybeSnapshot's six ordering points and
// handleInstallSnapshot's three (see the FaultPoint constants above).
func triggerFaultPoint(p FaultPoint) {
	h := faultHook.Load()
	if h == nil {
		return
	}
	if (*h)(p) {
		panic(faultPointCrash{point: p})
	}
}
