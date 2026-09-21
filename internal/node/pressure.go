package node

import (
	"errors"
	"fmt"
	rtmetrics "runtime/metrics"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/metrics"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// classifyErrKind names err's DISK-FULL EXPLICITNESS classification
// (§27.9, §19.3) for an audit/log message: "out_of_space" iff it is (or
// wraps) wal.ErrOutOfSpace/storage.ErrOutOfSpace, "io_error" otherwise.
func classifyErrKind(err error) string {
	if errors.Is(err, wal.ErrOutOfSpace) || errors.Is(err, storage.ErrOutOfSpace) {
		return "out_of_space"
	}
	return "io_error"
}

// PressureState is the admission-affecting half of docs/v0.6.0-plan.md
// §19.1's disk-full state machine (§19.1 calls the first state "Healthy";
// §6.4 calls the identical concept "Normal" — this type uses §19.1's
// name since §19 is the section that actually defines the transition
// diagram). §19.1's fourth state, "Failing", is not a PressureState value
// at all: on the Raft durable path it is Node.fail's existing unconditional
// halt (§20.1), and on every other path it is the separate
// Node.storageUnhealthy flag (§20.2), which can coexist with any
// PressureState.
type PressureState int

const (
	PressureNormal PressureState = iota
	PressureLowSpace
	PressureCritical
)

func (s PressureState) String() string {
	switch s {
	case PressureLowSpace:
		return "low"
	case PressureCritical:
		return "critical"
	default:
		return "normal"
	}
}

// FsyncPath labels a durable-write path for
// chronicledb_fsync_failures_total{path=...} (docs/v0.6.0-plan.md §20,
// §25) and for Node.noteFsyncResult's threshold accounting. FsyncPathRaft
// is tracked only for the metric — it never participates in the
// consecutive-failure/threshold logic, because a Raft-path failure halts
// the node immediately and unconditionally (§20.1), before any counter
// would matter.
type FsyncPath string

const (
	FsyncPathRaft     FsyncPath = "raft"
	FsyncPathSnapshot FsyncPath = "snapshot"
	FsyncPathBackup   FsyncPath = "backup"
	FsyncPathAudit    FsyncPath = "audit"
)

var allFsyncPaths = []FsyncPath{FsyncPathRaft, FsyncPathSnapshot, FsyncPathBackup, FsyncPathAudit}

// newFsyncFailuresTotal pre-builds the fixed per-path counter set
// (docs/v0.6.0-plan.md §11.1's amended label-policy rule: a label's
// value set must be fixed at compile time and bounded), mirroring
// admission.Gate's own rejectedTotal construction.
func newFsyncFailuresTotal() map[FsyncPath]*metrics.Counter {
	m := make(map[FsyncPath]*metrics.Counter, len(allFsyncPaths))
	for _, p := range allFsyncPaths {
		m[p] = &metrics.Counter{}
	}
	return m
}

// nodePressureSource is the production admission.PressureSource
// (docs/v0.6.0-plan.md §6.2/§6.3): internal/admission itself stays a leaf
// package (no internal/storage, no runtime/metrics), so the concrete
// combination lives here, the first layer up that already imports
// internal/storage.
type nodePressureSource struct {
	dataDir   string
	probeDisk bool // false iff neither disk threshold is configured; skips the syscall entirely rather than reporting a spurious probe error nobody asked about
}

func (s nodePressureSource) Sample() admission.Pressure {
	p := admission.Pressure{HeapBytes: sampleHeapBytes(), SampledAt: time.Now()}
	if s.probeDisk {
		p.DiskFreeBytes, p.DiskTotalBytes, p.Err = storage.DiskUsage(s.dataDir)
	}
	return p
}

// sampleHeapBytes reads /memory/classes/heap/objects:bytes via
// runtime/metrics (docs/v0.6.0-plan.md §6.3) — deliberately not
// runtime.ReadMemStats, which can stop the world.
func sampleHeapBytes() uint64 {
	samples := []rtmetrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	rtmetrics.Read(samples)
	if samples[0].Value.Kind() == rtmetrics.KindUint64 {
		return samples[0].Value.Uint64()
	}
	return 0
}

// diskPressureSetup is Open's resolved disk/heap-pressure configuration,
// computed once (docs/v0.6.0-plan.md §6.2's "Resolved: what an
// unsupported platform does" — checked at configuration time, not
// request time).
type diskPressureSetup struct {
	mon            *admission.PressureMonitor
	pressureSet    bool
	criticalSet    bool
	pressureThresh admission.Threshold
	criticalThresh admission.Threshold
}

// setupDiskPressure resolves and validates cfg's disk/heap pressure
// configuration against cfg.DataDir's actual filesystem (which must
// already exist — Open calls this after wal.Open). Returns a
// startup-fatal error for exactly the two cases §6.2 names: a threshold
// flag set on a platform DiskUsage cannot probe, or
// -disk-critical-threshold not being strictly less than
// -disk-pressure-threshold.
func setupDiskPressure(cfg Config) (*diskPressureSetup, error) {
	pressureSet := cfg.DiskPressureThreshold != ""
	criticalSet := cfg.DiskCriticalThreshold != ""
	if criticalSet && !pressureSet {
		return nil, fmt.Errorf("node: Config.DiskCriticalThreshold is set but Config.DiskPressureThreshold is not — a critical floor requires a pressure floor above it")
	}

	var pressureThresh, criticalThresh admission.Threshold
	var err error
	if pressureSet {
		if pressureThresh, err = admission.ParseThreshold(cfg.DiskPressureThreshold); err != nil {
			return nil, fmt.Errorf("node: Config.DiskPressureThreshold: %w", err)
		}
	}
	if criticalSet {
		if criticalThresh, err = admission.ParseThreshold(cfg.DiskCriticalThreshold); err != nil {
			return nil, fmt.Errorf("node: Config.DiskCriticalThreshold: %w", err)
		}
	}

	probeDisk := pressureSet || criticalSet
	_, totalBytes, probeErr := storage.DiskUsage(cfg.DataDir)
	if probeDisk && errors.Is(probeErr, storage.ErrDiskUsageUnsupported) {
		return nil, fmt.Errorf("node: Config.DiskPressureThreshold/DiskCriticalThreshold require disk-usage probing, which is not supported on this platform: %w", probeErr)
	}
	if criticalSet && criticalThresh.Bytes(totalBytes) >= pressureThresh.Bytes(totalBytes) {
		return nil, fmt.Errorf("node: Config.DiskCriticalThreshold (%s = %d bytes) must be strictly less than Config.DiskPressureThreshold (%s = %d bytes)",
			cfg.DiskCriticalThreshold, criticalThresh.Bytes(totalBytes), cfg.DiskPressureThreshold, pressureThresh.Bytes(totalBytes))
	}

	// Always constructed, even with nothing configured (§6.1): cheap,
	// stdlib-only sampling that /status can report diagnostically
	// regardless of whether it ever affects admission.
	source := nodePressureSource{dataDir: cfg.DataDir, probeDisk: !errors.Is(probeErr, storage.ErrDiskUsageUnsupported)}
	mon := admission.NewPressureMonitor(source, cfg.ResourcePollInterval)
	return &diskPressureSetup{
		mon: mon, pressureSet: pressureSet, criticalSet: criticalSet,
		pressureThresh: pressureThresh, criticalThresh: criticalThresh,
	}, nil
}

// diskPressureStatusString computes /status's diskPressure field
// (docs/v0.6.0-plan.md §11.3's fixed vocabulary). Safe from any
// goroutine: reads only atomics/immutable config plus the monitor's own
// safe Current().
func (n *Node) diskPressureStatusString() string {
	p := n.pressureMon.Load().Current()
	if (n.diskPressureSet || n.diskCriticalSet) && errors.Is(p.Err, storage.ErrDiskUsageUnsupported) {
		return "unsupported"
	}
	if p.Err != nil {
		return "probe_failed"
	}
	if !n.diskPressureSet && !n.diskCriticalSet {
		return "normal"
	}
	return PressureState(n.pressureState.Load()).String()
}

// checkResourcePressure re-evaluates n.pressureState against the
// PressureMonitor's latest sample and, on a transition, tightens or
// restores the write gate's effective ceiling (§6.4), triggers an
// immediate GC pass on entering LowSpace/Critical (§14.5), and records an
// audit entry (§20.3). Called only from tick(), on run's own goroutine —
// never a syscall, never a lock beyond the monitor's own atomic load
// (same-package tests also call it directly, from a test goroutine, with
// pressureMon replaced by SetPressureSourceForTest first).
func (n *Node) checkResourcePressure() {
	p := n.pressureMon.Load().Current()
	if p.Err != nil {
		n.metrics.DiskProbeFailuresTotal.Inc()
	}

	diskLow := n.diskPressureSet && p.Err == nil && p.DiskFreeBytes < n.diskPressureThresh.Bytes(p.DiskTotalBytes)
	diskCritical := n.diskCriticalSet && p.Err == nil && p.DiskFreeBytes < n.diskCriticalThresh.Bytes(p.DiskTotalBytes)
	heapLow := n.cfg.MaxHeapBytes > 0 && p.HeapBytes > n.cfg.MaxHeapBytes
	// De-escalation requires 10% hysteresis (§6.4/§19.1); heap has no
	// documented multiplier of its own, so a symmetric 10% band is
	// applied here to avoid the identical flapping-on-every-poll problem
	// disk's own hysteresis exists to prevent.
	diskCriticalClear := !n.diskCriticalSet || p.Err == nil && p.DiskFreeBytes >= scaleTenths(n.diskCriticalThresh.Bytes(p.DiskTotalBytes), 11)
	diskLowClear := !n.diskPressureSet || p.Err == nil && p.DiskFreeBytes >= scaleTenths(n.diskPressureThresh.Bytes(p.DiskTotalBytes), 11)
	heapLowClear := n.cfg.MaxHeapBytes == 0 || p.HeapBytes <= scaleTenths(n.cfg.MaxHeapBytes, 9)

	// A probe error (p.Err != nil) can only ever push the state AT MOST
	// to LowSpace (§6.2's fail-safe direction: "a storage layer that has
	// stopped answering questions about itself is not one to accept
	// more writes against" — not one to be presumed critically full,
	// either, absent an actual measurement). diskCritical/diskLow/
	// heapLow are already false whenever p.Err != nil (see their
	// definitions above), so every switch below only needs its own
	// explicit p.Err arm to get this right; a probe error already in
	// Critical simply never clears (diskCriticalClear requires
	// p.Err == nil), which is the correct conservative default.
	old := PressureState(n.pressureState.Load())
	next := old
	switch old {
	case PressureCritical:
		switch {
		case diskCritical || p.Err != nil:
			next = PressureCritical
		case diskCriticalClear:
			next = PressureLowSpace // re-evaluated for a further drop to Normal below only on a LATER tick — one transition per tick, matching the diagram's own single-edge-at-a-time shape
		default:
			next = PressureCritical
		}
	case PressureLowSpace:
		switch {
		case diskCritical:
			next = PressureCritical
		case p.Err != nil || diskLow || heapLow:
			next = PressureLowSpace
		case diskLowClear && heapLowClear:
			next = PressureNormal
		default:
			next = PressureLowSpace
		}
	default: // PressureNormal
		switch {
		case diskCritical:
			// A single poll can land below both thresholds at once (a
			// sudden large write); §19.1's diagram describes the
			// ordinary decay path, and this is still, correctly, at
			// least Critical — level-based, not strictly edge-sequenced.
			next = PressureCritical
		case p.Err != nil || diskLow || heapLow:
			next = PressureLowSpace
		default:
			next = PressureNormal
		}
	}

	if next == old {
		return
	}
	n.pressureState.Store(int32(next))

	reason := admission.ReasonMemoryPressure
	if p.Err != nil || diskLow || diskCritical {
		reason = admission.ReasonDiskPressure
	}
	n.applyPressureGate(next, reason)

	n.logf("node %s: disk/heap pressure state %s -> %s (freeBytes=%d totalBytes=%d heapBytes=%d probeErr=%v)",
		n.cfg.ID, old, next, p.DiskFreeBytes, p.DiskTotalBytes, p.HeapBytes, p.Err)
	n.auditHealth("node.pressure_state", fmt.Sprintf("%s -> %s (freeBytes=%d totalBytes=%d heapBytes=%d probeErr=%v)",
		old, next, p.DiskFreeBytes, p.DiskTotalBytes, p.HeapBytes, p.Err))

	// §14.5: entering LowSpace or Critical triggers an immediate GC pass
	// proposal if GC is enabled — edge-triggered (old was Normal), not
	// level-triggered, so a node sitting in LowSpace does not re-propose
	// on every subsequent poll; maybeProposeGC's own tick-driven
	// evaluation already covers ongoing pressure.
	if old == PressureNormal && (next == PressureLowSpace || next == PressureCritical) && n.gcIntervalTicks > 0 {
		n.maybeProposeGC()
	}
}

// scaleTenths returns v*tenths/10 without float rounding surprises at
// the boundary (docs/v0.6.0-plan.md §6.4/§19.1's *1.1 de-escalation
// margin, tenths=11; the symmetric *0.9 heap margin this file applies,
// tenths=9).
func scaleTenths(v uint64, tenths uint64) uint64 {
	return v/10*tenths + v%10*tenths/10
}

// applyPressureGate tightens or restores the write gate's effective
// ceiling for state s (docs/v0.6.0-plan.md §6.4's table): only Lane B
// writes are ever affected — Lane B reads, Lane A (control/maintenance),
// and Lane K (consensus, which has no gate at all) are never touched by
// any pressure state.
func (n *Node) applyPressureGate(s PressureState, reason admission.Reason) {
	switch s {
	case PressureCritical:
		n.admission.write.SetPressureReason(admission.ReasonDiskCritical)
		n.admission.write.SetEffectiveConcurrent(0)
	case PressureLowSpace:
		n.admission.write.SetPressureReason(reason)
		eff := n.cfg.MaxInflightProposals / 4
		if eff < 1 {
			eff = 1
		}
		n.admission.write.SetEffectiveConcurrent(eff)
	default:
		n.admission.write.SetEffectiveConcurrent(n.cfg.MaxInflightProposals)
	}
}

// noteFsyncResult is §20.2's consecutive-failure/threshold machinery:
// called for every non-Raft-path durable write this process makes
// (maybeSnapshot's four durable steps via FsyncPathSnapshot, Node.Backup
// via FsyncPathBackup, and every internal/audit.Log.Append via
// NoteAuditWriteResult/FsyncPathAudit). err==nil resets the consecutive
// counter and, if this node was storage-unhealthy, restores it — a
// single success is evidence the storage layer is answering again,
// mirroring §19.3's disk-pressure "no stuck forever state, recovery
// requires no operator action" position; there is no documented reason
// storage-unhealthy should be a one-way latch a healthy node can never
// leave without a restart, unlike the Raft path's unconditional halt.
// Safe to call from any goroutine (cmd/chronicledb-node's HTTP
// request-handling goroutines call it via NoteAuditWriteResult, not just
// run's event-loop goroutine).
func (n *Node) noteFsyncResult(path FsyncPath, err error) {
	if err == nil {
		if n.consecutiveNonRaftFsyncFailures.Swap(0) > 0 || n.storageUnhealthy.Load() {
			if n.storageUnhealthy.CompareAndSwap(true, false) {
				n.logf("node %s: storage healthy again (path=%s)", n.cfg.ID, path)
				n.auditHealth("node.storage_healthy", fmt.Sprintf("path=%s", path))
			}
		}
		return
	}

	if c, ok := n.fsyncFailuresTotal[path]; ok {
		c.Inc()
	}
	if path == FsyncPathRaft {
		return // §20.1: the Raft path halts unconditionally; it never participates in this threshold.
	}

	count := n.consecutiveNonRaftFsyncFailures.Add(1)
	if int(count) < n.cfg.FsyncFailureThreshold {
		return
	}
	if n.storageUnhealthy.CompareAndSwap(false, true) {
		n.logf("node %s: storage unhealthy: %d consecutive non-Raft fsync failures (path=%s, threshold=%d): %v",
			n.cfg.ID, count, path, n.cfg.FsyncFailureThreshold, err)
		n.auditHealth("node.storage_unhealthy", fmt.Sprintf("path=%s consecutiveFailures=%d threshold=%d err=%v", path, count, n.cfg.FsyncFailureThreshold, err))
	}
}

// NoteAuditWriteResult feeds an internal/audit.Log.Append outcome into
// this node's §20.2 storage-health tracking (FsyncPathAudit) —
// cmd/chronicledb-node's request-auditing middleware calls this after
// every Append, alongside this node's own health-transition audit calls
// (noteFsyncResult -> auditHealth), since both write through the same
// on-disk audit chain (Config.AuditLog's doc comment). Safe to call from
// any goroutine.
func (n *Node) NoteAuditWriteResult(err error) {
	n.noteFsyncResult(FsyncPathAudit, err)
}

// auditHealth best-effort appends one audit.Entry for a §19.1/§20.2
// state transition (§20.3). Never itself fatal, and never retried: if
// Config.AuditLog is nil (no Go-API caller supplied one) or the append
// itself fails (e.g. the audit log is on the same exhausted filesystem),
// this logs and moves on rather than looping — an audit-write failure
// about the audit log's own health is not a case this method recurses
// into (it does not call noteFsyncResult(FsyncPathAudit, ...) on its own
// failure; the caller that Appends application audit records
// (cmd/chronicledb-node) is what feeds that path).
func (n *Node) auditHealth(action, detail string) {
	if n.cfg.AuditLog == nil {
		return
	}
	entry := audit.Entry{
		Principal: "node",
		Role:      "system",
		Action:    action,
		Endpoint:  "",
		Result:    "allow",
		Detail:    detail,
	}
	if err := n.cfg.AuditLog.Append(entry); err != nil {
		n.logf("node %s: recording health-transition audit entry (%s): %v", n.cfg.ID, action, err)
	}
}

// SetPressureSourceForTest replaces n's PressureMonitor with a fresh one
// sampling source exactly once, synchronously, at call time
// (admission.NewPressureMonitor's own documented construction-time
// sample) — a deterministic seam for exercising the §19.1/§6.4
// hysteresis state machine against injected Pressure values instead of
// real disk/heap sampling, mirroring every other "...ForTest" seam this
// package already exposes (e.g. SetSkipAdmissionLaneSeparationForTest).
// The caller drives the transition itself by calling checkResourcePressure
// (same-package tests only) after each Set. Production code never calls
// this.
func (n *Node) SetPressureSourceForTest(source admission.PressureSource) {
	mon := admission.NewPressureMonitor(source, time.Hour)
	mon.Start()
	old := n.pressureMon.Swap(mon)
	old.Stop()
}

// fakePressureSource is a settable admission.PressureSource for tests
// (SetPressureSourceForTest).
type fakePressureSource struct{ p admission.Pressure }

func (s *fakePressureSource) Sample() admission.Pressure { return s.p }

// SetStorageUnhealthyForTest directly sets n's §20.2 storage-unhealthy
// flag, for cmd/chronicledb-node's fast in-process /health readiness
// tests that need this state without driving a real repeated fsync
// failure end to end. Status() and /health pick it up on run's own next
// refreshStatusLocked pass (every tick) — never call
// refreshStatusLocked directly from a test goroutine, which owns no
// other event-loop-confined field this method could safely touch).
// Production code never calls this.
func (n *Node) SetStorageUnhealthyForTest(v bool) {
	n.storageUnhealthy.Store(v)
}
