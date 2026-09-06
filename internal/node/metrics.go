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

	// RaftMessagesSentTotal/RaftMessagesReceivedTotal count outbound
	// and inbound internal/raft protocol messages processed by this
	// node's event loop.
	RaftMessagesSentTotal     metrics.Counter
	RaftMessagesReceivedTotal metrics.Counter
}

// MetricsSnapshot is a point-in-time, safe-to-read-anywhere copy of a
// Node's counters (mirroring Status's own snapshot pattern).
type MetricsSnapshot struct {
	ElectionsTotal            uint64
	LeaderChangesTotal        uint64
	ProposalsTotal            uint64
	ProposalsRejectedTotal    uint64
	ProposalsCommittedTotal   uint64
	ProposalsAbortedTotal     uint64
	ProposalsUnknownTotal     uint64
	RequestIDDuplicatesTotal  uint64
	SnapshotsCreatedTotal     uint64
	SnapshotsInstalledTotal   uint64
	RaftMessagesSentTotal     uint64
	RaftMessagesReceivedTotal uint64
}

// Metrics returns a snapshot of this node's current diagnostic
// counters. Safe to call from any goroutine.
func (n *Node) Metrics() MetricsSnapshot {
	m := &n.metrics
	return MetricsSnapshot{
		ElectionsTotal:            m.ElectionsTotal.Value(),
		LeaderChangesTotal:        m.LeaderChangesTotal.Value(),
		ProposalsTotal:            m.ProposalsTotal.Value(),
		ProposalsRejectedTotal:    m.ProposalsRejectedTotal.Value(),
		ProposalsCommittedTotal:   m.ProposalsCommittedTotal.Value(),
		ProposalsAbortedTotal:     m.ProposalsAbortedTotal.Value(),
		ProposalsUnknownTotal:     m.ProposalsUnknownTotal.Value(),
		RequestIDDuplicatesTotal:  m.RequestIDDuplicatesTotal.Value(),
		SnapshotsCreatedTotal:     m.SnapshotsCreatedTotal.Value(),
		SnapshotsInstalledTotal:   m.SnapshotsInstalledTotal.Value(),
		RaftMessagesSentTotal:     m.RaftMessagesSentTotal.Value(),
		RaftMessagesReceivedTotal: m.RaftMessagesReceivedTotal.Value(),
	}
}
