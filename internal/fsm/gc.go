package fsm

import (
	"encoding/binary"
	"fmt"
)

// controlKindAdvanceGCWatermark identifies an AdvanceGCWatermarkCommand
// payload (docs/v0.6.0-plan.md §16.1) — the second control-command
// kind, after controlKindSetClusterVersion = 1 (clusterversion.go).
// Collision with the mirrored raft membership-kind range (16-19) is
// guarded structurally by membership.go's init(), extended to cover
// this value, and by TestControlKindRangesNeverCollide.
const controlKindAdvanceGCWatermark byte = 2

// AdvanceGCWatermarkCommand is the single replicated command through
// which MVCC GC ever mutates anything (docs/v0.6.0-plan.md §13.2: "GC
// is replicated state... all reclamation happens inside fsm.Apply as a
// pure, deterministic function of that entry and prior FSM state").
// Proposed only by the leader (internal/node's GC proposer, a later
// slice), on a fixed cadence and in reaction to disk pressure — never
// by a follower, and never outside the ordinary Raft log.
type AdvanceGCWatermarkCommand struct {
	// RequestID is this command's idempotency token
	// (docs/v0.6.0-plan.md §13.4/§13.4a): deterministically derived by
	// the leader from (Watermark, gcPassSeq) so a retry after a leader
	// crash mid-propose is idempotent through the existing
	// controlOutcomes table, while successive continuation passes at an
	// unchanged watermark remain distinct commands.
	RequestID RequestID
	// Watermark is the watermark this command proposes advancing to.
	// ApplyAdvanceGCWatermark takes max(f.gcWatermark, Watermark) —
	// monotone, so a replayed or out-of-order lower value is a
	// deterministic no-op (§14.4).
	Watermark uint64
	// MaxVersions bounds versions REMOVED by this one Apply call
	// (docs/v0.6.0-plan.md §14.4) — carried in the command, not read
	// from local config, so every replica performs byte-identical work
	// regardless of its own -gc-max-versions-per-pass flag.
	MaxVersions uint32
	// MaxKeys bounds keys EXAMINED by this one Apply call — the
	// companion bound that keeps the keyspace walk itself independent
	// of total key count, not only of versions actually removed
	// (fact 8b/§14.4: without it, an Apply that reclaims nothing still
	// costs O(total keys)).
	MaxKeys uint32
}

// EncodeAdvanceGCWatermark serializes cmd as an FSM control-command
// payload.
//
// Layout: marker(1B)=ControlCommandMarker kind(1B)=controlKindAdvanceGCWatermark
//
//	requestIDLen(4B) requestID(requestIDLen) watermark(8B)
//	maxVersions(4B) maxKeys(4B)
func EncodeAdvanceGCWatermark(cmd AdvanceGCWatermarkCommand) []byte {
	buf := make([]byte, 1+1+4+len(cmd.RequestID)+8+4+4)
	off := 0
	buf[off] = ControlCommandMarker
	off++
	buf[off] = controlKindAdvanceGCWatermark
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(cmd.RequestID)))
	off += 4
	off += copy(buf[off:], cmd.RequestID)
	binary.BigEndian.PutUint64(buf[off:], cmd.Watermark)
	off += 8
	binary.BigEndian.PutUint32(buf[off:], cmd.MaxVersions)
	off += 4
	binary.BigEndian.PutUint32(buf[off:], cmd.MaxKeys)
	return buf
}

// DecodeAdvanceGCWatermark parses a control-command payload previously
// produced by EncodeAdvanceGCWatermark, following
// DecodeSetClusterVersion's bounded-decoding discipline exactly: never
// trusts a length field beyond the bytes actually present, never
// allocates from an untrusted length, never panics on malformed input
// (docs/failure-model.md §6).
func DecodeAdvanceGCWatermark(b []byte) (AdvanceGCWatermarkCommand, error) {
	var cmd AdvanceGCWatermarkCommand
	const fixedHeaderSize = 1 + 1 + 4
	if len(b) < fixedHeaderSize {
		return cmd, fmt.Errorf("%w: control command payload too short (%d bytes, need at least %d)", ErrMalformedCommand, len(b), fixedHeaderSize)
	}
	if b[0] != ControlCommandMarker {
		return cmd, fmt.Errorf("%w: payload is not a control command", ErrMalformedCommand)
	}
	kind := b[1]
	if kind != controlKindAdvanceGCWatermark {
		return cmd, fmt.Errorf("%w: control command kind %d", ErrUnknownControlCommand, kind)
	}
	off := 2
	reqIDLen := binary.BigEndian.Uint32(b[off:])
	off += 4
	if int64(reqIDLen) > int64(len(b)-off) {
		return cmd, fmt.Errorf("%w: truncated RequestID (declared %d bytes, %d remain)", ErrMalformedCommand, reqIDLen, len(b)-off)
	}
	cmd.RequestID = RequestID(b[off : off+int(reqIDLen)])
	off += int(reqIDLen)
	if len(b)-off != 8+4+4 {
		return cmd, fmt.Errorf("%w: %d trailing/missing bytes after RequestID, expected exactly 16 (watermark+maxVersions+maxKeys)", ErrMalformedCommand, len(b)-off)
	}
	cmd.Watermark = binary.BigEndian.Uint64(b[off:])
	off += 8
	cmd.MaxVersions = binary.BigEndian.Uint32(b[off:])
	off += 4
	cmd.MaxKeys = binary.BigEndian.Uint32(b[off:])
	return cmd, nil
}

// ApplyAdvanceGCWatermark is the deterministic Apply boundary for an
// AdvanceGCWatermarkCommand (docs/v0.6.0-plan.md §14.4), mirroring
// ApplySetClusterVersion's role for SetClusterVersionCommand: a pure
// function of cmd and the FSM's own prior state, called once, in
// order, for every committed AdvanceGCWatermark index (live or
// replayed).
//
// Steps, exactly as specified:
//
//  0. Idempotency: a previously recorded outcome for cmd.RequestID is
//     returned unchanged — a retry of the same pass (leader crash
//     mid-propose, or replayed log entry) is idempotent.
//  1. f.gcWatermark = max(f.gcWatermark, cmd.Watermark) — monotone; a
//     lower cmd.Watermark is a deterministic no-op.
//  2. W := f.gcWatermark (the AUTHORITATIVE, post-max() horizon — never
//     cmd.Watermark directly, so there is exactly one horizon governing
//     both reclamation and the read-side guard — §14.4/SL-4a). Walk up
//     to cmd.MaxKeys keys from f.gcCursor via Store.KeysFrom, reclaiming
//     up to cmd.MaxVersions total versions via Store.ReclaimKey.
//  3. f.gcCursor advances to resume after the last key examined, or ""
//     (with f.gcPasses++) if the walk reached the end of the key set.
//  4. f.gcPassSeq++.
//  5. The outcome is recorded and f.store.SetGCWatermark(W) is called
//     — keeping Store.GCWatermark() == FSM.gcWatermark, the invariant
//     the read-side guard depends on (§15.2b).
func (f *FSM) ApplyAdvanceGCWatermark(index uint64, cmd AdvanceGCWatermarkCommand) (Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if entry, ok := f.controlOutcomes[cmd.RequestID]; ok {
		return entry, nil
	}

	if cmd.Watermark > f.gcWatermark {
		f.gcWatermark = cmd.Watermark
	}
	w := f.gcWatermark // authoritative — see doc comment

	if cmd.MaxKeys > 0 {
		keys := f.store.KeysFrom(f.gcCursor, int(cmd.MaxKeys))
		removed := 0
		lastExamined := ""
		fullyConsumedBatch := true
		for _, k := range keys {
			if uint32(removed) >= cmd.MaxVersions {
				fullyConsumedBatch = false
				break
			}
			budget := int(cmd.MaxVersions) - removed
			removed += f.store.ReclaimKey(k, w, budget)
			lastExamined = k
		}
		if fullyConsumedBatch && len(keys) < int(cmd.MaxKeys) {
			// The walk reached the end of the key set: fewer keys were
			// returned than requested, and every one of them was
			// examined (no early break for the version budget).
			f.gcCursor = ""
			f.gcPasses++
		} else if lastExamined != "" {
			f.gcCursor = lastExamined
		}
		// else: nothing was examined at all (an empty batch, or the
		// version budget was already exhausted before the first key) —
		// f.gcCursor is left unchanged, so the next pass resumes from
		// exactly where this one started.
	}

	f.gcPassSeq++

	outcome := Outcome{RequestID: cmd.RequestID, Status: StatusCommitted, CommitSeq: index}
	f.controlOutcomes[cmd.RequestID] = outcome
	f.store.SetGCWatermark(w)
	return outcome, nil
}

// GCWatermark returns the FSM's currently applied GC watermark. Safe
// for concurrent use.
func (f *FSM) GCWatermark() uint64 {
	f.rLock()
	defer f.rUnlock()
	return f.gcWatermark
}

// GCCursor/GCPasses/GCPassSeq expose the remaining replicated GC state
// for diagnostics and for the leader's own proposer (a later slice) to
// read after applying its own proposal. Safe for concurrent use.
func (f *FSM) GCCursor() string {
	f.rLock()
	defer f.rUnlock()
	return f.gcCursor
}

func (f *FSM) GCPasses() uint64 {
	f.rLock()
	defer f.rUnlock()
	return f.gcPasses
}

func (f *FSM) GCPassSeq() uint64 {
	f.rLock()
	defer f.rUnlock()
	return f.gcPassSeq
}
