package backup

import "github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"

// This file implements the restore-side half of the dynamic-membership
// plan's §7.6 rule: "-restore-from re-bootstraps Configuration from the
// operator's -cluster/-peers flags, and never restores the source
// cluster's runtime membership." Part 1 (stripping the staged
// snapshot's Configuration) lives in buildStaging (restore.go). Part 2
// — voiding any EntryConfig entry in the staged WAL suffix — lives here.
//
// internal/backup deliberately does not import internal/raft or
// internal/node (docs/architecture.md §5's dependency layering: this
// package operates purely on snapshot.Meta/fsm.FSM/*wal.WAL). It
// matches on the documented payload byte layout instead:
//
//   - internal/node/storage.go's entry-payload framing (§6.1a):
//     untyped (Type == EntryNormal): term(8B) || data
//     typed   (Type != EntryNormal): term(8B) || 0xFF || entryType(1B) || data
//   - internal/raft's EntryConfig kind == 1, and its EntryConfig.Data
//     framing (§2.5): 0xF0 || kind(1B) || four length-prefixed fields.
//
// Both layouts are pinned against drift by cross-package tests:
// internal/raft's TestEncodeVoidedEntryConfigPayloadLayout and this
// package's TestVoidConfigEntryPayloadMatchesRaftFraming.
const (
	entryPayloadTypeSentinelByte = 0xFF // mirrors internal/node's entryPayloadTypeSentinel
	entryTypeConfigByte          = 1    // mirrors raft.EntryConfig's numeric value
	entryPayloadTypedHeaderLen   = 8 + 1 + 1
)

// voidedConfigChangePayload is this package's own independent encoding
// of raft.EncodeVoidedEntryConfigPayload()'s exact byte layout: the
// 0xF0 marker, membership-change kind 19 (Voided), and four
// zero-length fields (requestID/targetID/targetAddr/fullConfig).
var voidedConfigChangePayload = []byte{
	0xF0, 19,
	0, 0, 0, 0,
	0, 0, 0, 0,
	0, 0, 0, 0,
	0, 0, 0, 0,
}

// voidRestoredMembershipEntries gates part 2 of §7.6's restore
// transform. Always true in production; a test in this package's own
// suite may set it false to exercise DM-21's negative control (a
// restore that carries no configuration in its staged snapshot but
// still carries the source's live EntryConfig entries in its staged
// WAL suffix must break the restored cluster — proving this transform
// is load-bearing, not decorative).
var voidRestoredMembershipEntries = true

// stripSnapshotConfiguration implements §7.6 part 1: a staged snapshot
// carries no configuration at all, regardless of what the source
// carried. A v1 source snapshot already decodes with
// Meta.HasConfiguration == false and is returned unchanged (needs no
// transform); a v2 source snapshot carrying a configuration is
// re-encoded with Meta.HasConfiguration cleared and Meta.Configuration
// zeroed, preserving its FSM state and consensus boundary exactly —
// re-encoding a v2 frame rather than copying its bytes through, so the
// cleared fields are actually reflected in what gets installed.
func stripSnapshotConfiguration(snapBytes []byte) ([]byte, error) {
	snap, err := snapshot.Decode(snapBytes)
	if err != nil {
		return nil, err
	}
	if !snap.Meta.HasConfiguration {
		return snapBytes, nil
	}
	meta := snap.Meta
	meta.HasConfiguration = false
	meta.Configuration = snapshot.Configuration{}
	return snapshot.Encode(meta, snap.FSM, snapshot.FormatVersion), nil
}

// voidConfigEntryPayload rewrites a WAL entry payload that carries an
// EntryConfig entry into the "Voided" form (dynamic-membership plan
// §7.6): the term and entry-type header bytes are preserved exactly
// (the entry keeps its index and term, so every log-matching
// relationship and the snapshot-boundary relationship are unchanged) —
// only the embedded Configuration is discarded. Every other payload
// (an EntryNormal entry, or one already Voided) passes through
// unchanged, byte for byte.
func voidConfigEntryPayload(payload []byte) []byte {
	if len(payload) < entryPayloadTypedHeaderLen {
		return payload // too short to carry a typed-entry header at all
	}
	if payload[8] != entryPayloadTypeSentinelByte || payload[9] != entryTypeConfigByte {
		return payload // untyped (EntryNormal), or a typed entry that is not EntryConfig
	}
	out := make([]byte, entryPayloadTypedHeaderLen+len(voidedConfigChangePayload))
	copy(out, payload[:entryPayloadTypedHeaderLen])
	copy(out[entryPayloadTypedHeaderLen:], voidedConfigChangePayload)
	return out
}
