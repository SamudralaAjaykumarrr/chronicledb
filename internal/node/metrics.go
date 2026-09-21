package node

import "github.com/SamudralaAjaykumarrr/chronicledb/internal/metrics"

// Metrics is a Node's diagnostic counters (docs/roadmap.md Phase 9
// §Observability). Every counter is safe to read from any goroutine at
// any time via Node.Metrics(), and none of them is ever consulted by
// Node's own event loop for a correctness decision — reading or
// failing to increment one never changes what the node does
// (docs/roadmap.md: "a correct decision must never depend on whether a
// metric was recorded"). Counters are in-memory only and reset to zero
// on every process restart (see docs/observability.md).
type Metrics struct {
	// ElectionsTotal counts every InputElectionTimeout this node's core
	// actually acted on (i.e. it was not already Leader — raft.Core's
	// own handleElectionTimeout no-ops for a Leader).
	ElectionsTotal metrics.Counter
	// LeaderChangesTotal counts every Step call in which this node
	// became Leader (raft.Output.BecameLeader).
	LeaderChangesTotal metrics.Counter

	// ProposalsTotal counts every client mutation this node's event
	// loop accepted as leader and handed to raft.Core (i.e. not
	// rejected for not being leader).
	ProposalsTotal metrics.Counter
	// ProposalsRejectedTotal counts every Propose call rejected
	// immediately because this node was not leader at the time.
	ProposalsRejectedTotal metrics.Counter
	// ProposalsCommittedTotal counts every accepted proposal whose
	// terminal outcome was fsm.StatusCommitted.
	ProposalsCommittedTotal metrics.Counter
	// ProposalsAbortedTotal counts every accepted proposal whose
	// terminal outcome was fsm.StatusAborted (a Snapshot Isolation
	// write-write conflict, docs/mvcc.md §4) — ChronicleDB's
	// replicated-mode transaction-conflict counter.
	ProposalsAbortedTotal metrics.Counter
	// ProposalsUnknownTotal counts every accepted proposal that never
	// reached a terminal outcome from this node's own point of view
	// (leadership lost, superseded by divergent-suffix repair, or the
	// node stopped before the entry committed) — docs/transactions.md
	// §8's "uncertain outcome, resolve by RequestID retry" case, not a
	// false negative.
	ProposalsUnknownTotal metrics.Counter

	// RequestIDDuplicatesTotal counts every Propose call whose
	// RequestID was already known (resolved by Precheck without a
	// fresh Raft round), per docs/transactions.md §6.
	RequestIDDuplicatesTotal metrics.Counter

	// SnapshotsCreatedTotal counts every successful local snapshot
	// creation+compaction cycle (maybeSnapshot).
	SnapshotsCreatedTotal metrics.Counter
	// SnapshotsInstalledTotal counts every successful installation of a
	// peer-provided snapshot that actually advanced this node's state
	// (handleInstallSnapshot).
	SnapshotsInstalledTotal metrics.Counter
	// SnapshotServeMissTotal counts every time this node, as leader, was
	// asked by processOutput to fill a MsgInstallSnapshotRequest's bytes
	// for an index its own snapMgr no longer retains (docs/v0.6.0-plan.md
	// §18.1's self-healing race: a snapshot created between Core
	// deciding a follower needs index X and the message actually being
	// filled can prune X away first). Nonzero is an expected, bounded
	// operating signal — Core re-derives a request for the newer index
	// on the next heartbeat — not a bug signal by itself; SL-11 is what
	// proves the follower still converges via retry.
	SnapshotServeMissTotal metrics.Counter

	// RaftMessagesSentTotal/RaftMessagesReceivedTotal count outbound
	// and inbound internal/raft protocol messages processed by this
	// node's event loop.
	RaftMessagesSentTotal     metrics.Counter
	RaftMessagesReceivedTotal metrics.Counter

	// BackupsTotal/BackupsFailedTotal count every Node.Backup call this
	// node has completed/failed (docs/enterprise-v1-plan.md §6
	// Observability: "duration, size, and success/failure counters").
	BackupsTotal       metrics.Counter
	BackupsFailedTotal metrics.Counter

	// UpgradePrecheckTotal/UpgradeFinalizeTotal/UpgradeFinalizeFailedTotal
	// count every UpgradePrecheck call and every FinalizeUpgrade
	// attempt/failure this node has made (docs/enterprise-v1-plan.md §7
	// Observability: "precheck pass/fail history, finalize event...as a
	// distinct metric").
	UpgradePrecheckTotal       metrics.Counter
	UpgradeFinalizeTotal       metrics.Counter
	UpgradeFinalizeFailedTotal metrics.Counter

	// AdmissionDefenseRejectionsWaitersTotal/
	// AdmissionDefenseRejectionsPendingReadsTotal count every rejection
	// from the two event-loop ceilings docs/v0.6.0-plan.md §5.3/§9.1a
	// add — chronicledb_admission_defense_rejections_total{ceiling=
	// "waiters"|"pending_reads"}. Nonzero is a **normal operating
	// signal under client cancellation**, not a bug signal (§5.3): a
	// caller that cancels its context frees its admission.Gate slot
	// while its waiter/pendingRead entry survives until it resolves,
	// so a new caller is admitted through the gate while the ceiling
	// is what actually bounds BOUNDED ADMITTED WORK.
	AdmissionDefenseRejectionsWaitersTotal      metrics.Counter
	AdmissionDefenseRejectionsPendingReadsTotal metrics.Counter

	// WaitersGauge/PendingReadsGauge mirror len(n.waiters)/
	// len(n.pendingReads) (docs/v0.6.0-plan.md §5.3/§9.1a's authoritative
	// BOUNDED ADMITTED WORK ceilings), updated on run()'s own goroutine
	// at every mutation site so a concurrent reader never races the map/
	// slice itself — chronicledb_node_waiters / chronicledb_node_pending_reads.
	WaitersGauge      metrics.Gauge
	PendingReadsGauge metrics.Gauge

	// ReadLeasesActiveGauge mirrors n.leases.Len() (docs/v0.6.0-plan.md
	// §9.1a/§15.3/§25's chronicledb_read_leases_active), refreshed
	// alongside WaitersGauge/PendingReadsGauge.
	ReadLeasesActiveGauge metrics.Gauge

	// GCProposalsTotal/GCProposalsFailedTotal count every leader-side
	// AdvanceGCWatermark proposal attempt (docs/v0.6.0-plan.md §25's
	// chronicledb_mvcc_gc_proposals_total/_failed_total): Total on
	// InputPropose acceptance, FailedTotal when Core rejected it
	// outright (e.g. a leadership change raced the proposal).
	GCProposalsTotal       metrics.Counter
	GCProposalsFailedTotal metrics.Counter

	// RaftMessageProcessSeconds is Lane K's own service-time histogram
	// (docs/v0.6.0-plan.md §4.3, §11.2's chronicledb_raft_message_
	// process_seconds — the A-8 CONTROL-PLANE NON-STARVATION proof
	// metric): one observation per inbound raft.Message processed by
	// step(), regardless of client admission-queue depth, concurrency,
	// or rejection rate. Not a Counter/Gauge, so it is a pointer,
	// explicitly constructed by Open (metrics.Histogram's zero value is
	// not valid — see its own doc comment).
	RaftMessageProcessSeconds *metrics.Histogram
}

// MetricsSnapshot is a point-in-time, safe-to-read-anywhere copy of a
// Node's counters (mirroring Status's own snapshot pattern).
type MetricsSnapshot struct {
	ElectionsTotal             uint64
	LeaderChangesTotal         uint64
	ProposalsTotal             uint64
	ProposalsRejectedTotal     uint64
	ProposalsCommittedTotal    uint64
	ProposalsAbortedTotal      uint64
	ProposalsUnknownTotal      uint64
	RequestIDDuplicatesTotal   uint64
	SnapshotsCreatedTotal      uint64
	SnapshotsInstalledTotal    uint64
	SnapshotServeMissTotal     uint64
	RaftMessagesSentTotal      uint64
	RaftMessagesReceivedTotal  uint64
	BackupsTotal               uint64
	BackupsFailedTotal         uint64
	UpgradePrecheckTotal       uint64
	UpgradeFinalizeTotal       uint64
	UpgradeFinalizeFailedTotal uint64

	AdmissionDefenseRejectionsWaitersTotal      uint64
	AdmissionDefenseRejectionsPendingReadsTotal uint64

	WaitersGauge      int64
	PendingReadsGauge int64

	ReadLeasesActiveGauge int64

	GCProposalsTotal       uint64
	GCProposalsFailedTotal uint64

	RaftMessageProcessSeconds metrics.HistogramSnapshot
}

// Metrics returns a snapshot of this node's current diagnostic
// counters. Safe to call from any goroutine.
func (n *Node) Metrics() MetricsSnapshot {
	m := &n.metrics
	return MetricsSnapshot{
		ElectionsTotal:             m.ElectionsTotal.Value(),
		LeaderChangesTotal:         m.LeaderChangesTotal.Value(),
		ProposalsTotal:             m.ProposalsTotal.Value(),
		ProposalsRejectedTotal:     m.ProposalsRejectedTotal.Value(),
		ProposalsCommittedTotal:    m.ProposalsCommittedTotal.Value(),
		ProposalsAbortedTotal:      m.ProposalsAbortedTotal.Value(),
		ProposalsUnknownTotal:      m.ProposalsUnknownTotal.Value(),
		RequestIDDuplicatesTotal:   m.RequestIDDuplicatesTotal.Value(),
		SnapshotsCreatedTotal:      m.SnapshotsCreatedTotal.Value(),
		SnapshotsInstalledTotal:    m.SnapshotsInstalledTotal.Value(),
		SnapshotServeMissTotal:     m.SnapshotServeMissTotal.Value(),
		RaftMessagesSentTotal:      m.RaftMessagesSentTotal.Value(),
		RaftMessagesReceivedTotal:  m.RaftMessagesReceivedTotal.Value(),
		BackupsTotal:               m.BackupsTotal.Value(),
		BackupsFailedTotal:         m.BackupsFailedTotal.Value(),
		UpgradePrecheckTotal:       m.UpgradePrecheckTotal.Value(),
		UpgradeFinalizeTotal:       m.UpgradeFinalizeTotal.Value(),
		UpgradeFinalizeFailedTotal: m.UpgradeFinalizeFailedTotal.Value(),

		AdmissionDefenseRejectionsWaitersTotal:      m.AdmissionDefenseRejectionsWaitersTotal.Value(),
		AdmissionDefenseRejectionsPendingReadsTotal: m.AdmissionDefenseRejectionsPendingReadsTotal.Value(),

		WaitersGauge:      m.WaitersGauge.Value(),
		PendingReadsGauge: m.PendingReadsGauge.Value(),

		ReadLeasesActiveGauge: m.ReadLeasesActiveGauge.Value(),

		GCProposalsTotal:       m.GCProposalsTotal.Value(),
		GCProposalsFailedTotal: m.GCProposalsFailedTotal.Value(),

		RaftMessageProcessSeconds: m.RaftMessageProcessSeconds.Snapshot(),
	}
}
