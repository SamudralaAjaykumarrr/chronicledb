package fsm

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ControlCommandMarker is the first byte of an FSM control-command
// payload (docs/enterprise-v1-plan.md §7's "FSM command format
// version" surface): a raft.Entry.Data whose first byte equals this
// value is never a CommitTxnCommand, and vice versa.
//
// This is the versioning discriminator both node.applyCommitted's
// dispatch (which of the two decoders to try) and, for free,
// backward-compatibility with a pre-v0.4.0 binary rely on:
// commitTxnCommandVersion (command.go) is a small, sequentially
// incrementing integer starting at 1 and currently at 2 — realistically
// never reaching this byte's value within CommitTxn's own version
// lineage — so a pre-v0.4.0 node's unmodified
// applyCommitted/DecodeCommitTxn, which unconditionally attempts to
// decode every committed entry as a CommitTxn command, already fails
// closed (ErrUnsupportedCommandVersion -> Node.fail, halting that
// node) on any entry whose first byte is ControlCommandMarker, with no
// code change required on that old binary at all. This is exactly the
// NO SILENT FORMAT MISINTERPRETATION / fail-closed behavior
// docs/enterprise-v1-plan.md §7 requires for "an N-version node
// applying an N+1-version command it does not understand."
//
// Because a SetClusterVersion command is only ever proposed after
// Node.UpgradePrecheck confirms every live cluster member already
// reports MaxSupportedGeneration >= the target generation (see
// Node.FinalizeUpgrade), an old node hitting this fail-closed path
// during ordinary operation should never actually happen — it exists
// as defense in depth, not as the primary safety mechanism.
const ControlCommandMarker byte = 0xF0

// Control-command kinds, the second byte of a control-command payload
// (after ControlCommandMarker). Currently exactly one exists.
const (
	// controlKindSetClusterVersion identifies a SetClusterVersionCommand
	// payload.
	controlKindSetClusterVersion byte = 1
)

// SetClusterVersionCommand is the single new FSM command this phase
// introduces (docs/enterprise-v1-plan.md §7 "Cluster version /
// finalize"): a Raft-replicated, deterministically-applied change to
// the cluster's durably agreed "minimum understood generation."
// Proposing and committing this command through the ordinary Raft log
// (exactly like a CommitTxnCommand — see internal/node.ProposeControl)
// is what "finalize" actually is: there is no separate finalize
// mechanism outside the normal deterministic Apply boundary.
type SetClusterVersionCommand struct {
	// RequestID is this command's idempotency token, in exactly the same
	// role as CommitTxnCommand.RequestID (docs/transactions.md §6) —
	// FSM.ApplySetClusterVersion tracks it in a dedicated outcome table
	// so a retried finalize proposal (e.g. after a leader crash mid-call,
	// docs/enterprise-v1-plan.md §7 Failure semantics) is idempotent
	// rather than re-evaluated.
	RequestID RequestID
	// TargetGeneration is the generation the cluster should durably
	// agree it now requires (docs/upgrades.md). Apply accepts only
	// TargetGeneration == current generation + 1 (single-step, matching
	// the N/N+1-only support policy; docs/enterprise-v1-plan.md §7's
	// explicit non-goal "N/N+2 (skip-version) upgrade support") and
	// TargetGeneration > current generation (ROLLBACK BOUNDARY HONESTY:
	// the agreed generation only ever moves forward) — any other value
	// is a deterministic StatusAborted outcome, not a Go error, so every
	// replica reaches the identical decision independent of which node
	// proposed it.
	TargetGeneration uint32
}

// EncodeSetClusterVersion serializes cmd as an FSM control-command
// payload.
//
// Layout: marker(1B)=ControlCommandMarker kind(1B)=controlKindSetClusterVersion
//
//	requestIDLen(4B) requestID(requestIDLen) targetGeneration(4B)
func EncodeSetClusterVersion(cmd SetClusterVersionCommand) []byte {
	buf := make([]byte, 1+1+4+len(cmd.RequestID)+4)
	off := 0
	buf[off] = ControlCommandMarker
	off++
	buf[off] = controlKindSetClusterVersion
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(cmd.RequestID)))
	off += 4
	off += copy(buf[off:], cmd.RequestID)
	binary.BigEndian.PutUint32(buf[off:], cmd.TargetGeneration)
	return buf
}

// ErrUnknownControlCommand indicates a payload's ControlCommandMarker
// byte was recognized but its control-kind byte was not — a control
// command kind newer than this build understands. Fails closed exactly
// like ErrUnsupportedCommandVersion, never guessing at the layout.
var ErrUnknownControlCommand = errors.New("fsm: unknown control command kind")

// IsControlCommand reports whether payload's first byte marks it as an
// FSM control command (see ControlCommandMarker) rather than a
// CommitTxnCommand. Safe to call on an empty payload.
func IsControlCommand(payload []byte) bool {
	return len(payload) > 0 && payload[0] == ControlCommandMarker
}

// DecodeSetClusterVersion parses a control-command payload previously
// produced by EncodeSetClusterVersion. It never trusts a length field
// beyond the bytes actually present and never panics on malformed
// input, mirroring DecodeCommitTxn's bounded-decoding discipline
// (docs/failure-model.md §6).
func DecodeSetClusterVersion(b []byte) (SetClusterVersionCommand, error) {
	var cmd SetClusterVersionCommand
	const fixedHeaderSize = 1 + 1 + 4
	if len(b) < fixedHeaderSize {
		return cmd, fmt.Errorf("%w: control command payload too short (%d bytes, need at least %d)", ErrMalformedCommand, len(b), fixedHeaderSize)
	}
	if b[0] != ControlCommandMarker {
		return cmd, fmt.Errorf("%w: payload is not a control command", ErrMalformedCommand)
	}
	kind := b[1]
	if kind != controlKindSetClusterVersion {
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
	if len(b)-off != 4 {
		return cmd, fmt.Errorf("%w: %d trailing/missing bytes after RequestID, expected exactly 4 (targetGeneration)", ErrMalformedCommand, len(b)-off)
	}
	cmd.TargetGeneration = binary.BigEndian.Uint32(b[off:])
	return cmd, nil
}

// ApplySetClusterVersion is the deterministic Apply boundary for a
// SetClusterVersionCommand, in exactly the same role FSM.Apply plays
// for a CommitTxnCommand (docs/architecture.md §5, ADR-0007): a pure
// function of cmd and the FSM's own prior state, called once, in
// order, for every committed control-command index (live or replayed
// — see Node.applyCommitted/Node.applyControlEntry).
//
// Steps:
//  1. Idempotency: a previously recorded outcome for cmd.RequestID is
//     returned unchanged (no fingerprint-mismatch detection, unlike
//     CommitTxn's Precheck/Apply — a SetClusterVersionCommand has no
//     business reason to be legitimately resubmitted under the same
//     RequestID with different fields, so simple RequestID-keyed
//     idempotency is sufficient here).
//  2. Validation (ROLLBACK BOUNDARY HONESTY / N-N+1-only): if
//     cmd.TargetGeneration is not exactly f.clusterGeneration+1, the
//     outcome is a deterministic StatusAborted — every replica
//     evaluating the identical command against the identical prior
//     state reaches the identical rejection, so this is a safe,
//     replicated business decision, never a Go error.
//  3. Otherwise: f.clusterGeneration advances to cmd.TargetGeneration
//     and the outcome is StatusCommitted with CommitSeq=index, exactly
//     mirroring CommitTxnCommand's outcome shape (Outcome is reused
//     unchanged — Node's waiter-resolution path does not need to know
//     which command kind produced it).
func (f *FSM) ApplySetClusterVersion(index uint64, cmd SetClusterVersionCommand) (Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if entry, ok := f.controlOutcomes[cmd.RequestID]; ok {
		return entry, nil
	}

	var outcome Outcome
	if cmd.TargetGeneration != f.clusterGeneration+1 {
		outcome = Outcome{RequestID: cmd.RequestID, Status: StatusAborted}
	} else {
		f.clusterGeneration = cmd.TargetGeneration
		outcome = Outcome{RequestID: cmd.RequestID, Status: StatusCommitted, CommitSeq: index}
	}
	f.controlOutcomes[cmd.RequestID] = outcome
	return outcome, nil
}

// ClusterGeneration returns the FSM's currently agreed cluster
// generation (0 until the first successful ApplySetClusterVersion, or
// after restoring generation-0 state — docs/upgrades.md). Safe for
// concurrent use.
func (f *FSM) ClusterGeneration() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clusterGeneration
}
