// Package scrub holds the fixed vocabulary docs/v0.6.0-plan.md §21.1
// defines for ChronicleDB's storage-integrity verification: the finding
// kinds and shape every subsystem's Scrub function (internal/wal,
// internal/snapshot, internal/audit) reports through, and
// internal/node.Scrub assembles into one ScrubReport. A leaf package —
// it imports nothing — so any of internal/wal/snapshot/audit/node can
// depend on it without creating a cycle among themselves
// (docs/architecture.md §5's acyclic dependency direction).
package scrub

// FindingKind is scrub's fixed, cross-subsystem vocabulary
// (docs/v0.6.0-plan.md §21.1). New kinds may be added in a MINOR
// release; existing ones never change meaning.
type FindingKind string

const (
	FindingBadChecksum          FindingKind = "bad_checksum"
	FindingBadFraming           FindingKind = "bad_framing"
	FindingUnsupportedVersion   FindingKind = "unsupported_version"
	FindingIndexOutOfOrder      FindingKind = "index_out_of_order"
	FindingUnreadable           FindingKind = "unreadable"
	FindingSnapshotDecodeFailed FindingKind = "snapshot_decode_failed"
	FindingAuditChainBroken     FindingKind = "audit_chain_broken"
)

// Finding is one scrub-detected problem, at a specific file and byte
// offset (docs/v0.6.0-plan.md §21.1's report shape).
type Finding struct {
	File   string      `json:"file"`
	Offset int64       `json:"offset"`
	Kind   FindingKind `json:"kind"`
}
