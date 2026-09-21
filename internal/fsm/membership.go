package fsm

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// membershipChangeKindMirror mirrors internal/raft's own EntryConfig
// membership-change-kind byte range (dynamic-membership plan §2.5).
// internal/fsm never decodes EntryConfig payloads itself — Core owns
// membership decoding entirely, and the quorum effect of a membership
// change happens at append time, inside Core, never via internal/fsm
// (see that plan's §2.4). This mirror exists solely so
// TestControlKindRangesNeverCollide can assert the two packages'
// reserved control-kind ranges never collide, without introducing an
// import dependency in either direction (internal/fsm does not import
// internal/raft, and internal/raft does not import internal/fsm).
type membershipChangeKindMirror byte

const (
	membershipKindAddLearnerMirror   membershipChangeKindMirror = 16
	membershipKindPromoteVoterMirror membershipChangeKindMirror = 17
	membershipKindRemoveServerMirror membershipChangeKindMirror = 18
	membershipKindVoidedMirror       membershipChangeKindMirror = 19
)

// init structurally guarantees this package's control-kind range never
// collides with internal/raft's reserved membership-change-kind range,
// mirroring ControlCommandMarker's own existing non-collision guard
// (clusterversion.go).
func init() {
	for _, k := range []membershipChangeKindMirror{
		membershipKindAddLearnerMirror, membershipKindPromoteVoterMirror,
		membershipKindRemoveServerMirror, membershipKindVoidedMirror,
	} {
		if byte(k) == controlKindSetClusterVersion {
			panic(fmt.Sprintf("fsm: membership control-kind mirror (%d) collides with controlKindSetClusterVersion (%d)", k, controlKindSetClusterVersion))
		}
	}
}

// MembershipKind identifies which of the three membership-changing
// operations a recorded outcome belongs to, for fingerprinting only
// (dynamic-membership plan §10) — never encoded onto the wire by this
// package.
type MembershipKind byte

const (
	MembershipAddLearner   MembershipKind = MembershipKind(membershipKindAddLearnerMirror)
	MembershipPromoteVoter MembershipKind = MembershipKind(membershipKindPromoteVoterMirror)
	MembershipRemoveServer MembershipKind = MembershipKind(membershipKindRemoveServerMirror)
)

// ErrRequestIDConflict is returned when a membership RequestID already
// has a recorded outcome, but the request now presented under that same
// RequestID has a different {kind, nodeId, address} fingerprint
// (dynamic-membership plan §10) — deliberately excluding
// confirmVoterCount, which is an authorization gesture about one
// submission, not part of the operation's identity (§12.2).
var ErrRequestIDConflict = errors.New("fsm: RequestID reused for a conflicting membership request")

func membershipFingerprintOf(kind MembershipKind, nodeID, address string) fingerprint {
	buf := make([]byte, 0, 1+4+len(nodeID)+4+len(address))
	buf = append(buf, byte(kind))
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(len(nodeID)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, nodeID...)
	binary.BigEndian.PutUint32(tmp[:], uint32(len(address)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, address...)
	return sha256.Sum256(buf)
}

// membershipOutcomeEntry mirrors outcomeEntry for the membership
// outcome table: the terminal outcome plus the fingerprint of the
// request it was originally recorded from.
type membershipOutcomeEntry struct {
	outcome     Outcome
	fingerprint fingerprint
}

// RecordMembershipOutcome durably records outcome for id the first
// time it is seen, or returns the previously recorded outcome for an
// identical {kind, nodeId, address} retry (dynamic-membership plan
// §2.6, §10) — called by internal/node.applyCommitted for every
// committed EntryConfig entry that establishes a configuration (never
// for a Voided entry, which records nothing at all, §7.6), exactly
// once, deterministically, at the same committed index on every
// replica. This is Apply-adjacent but is not Apply itself: Core has
// already decided the entry's quorum effect at append time (§2.4) —
// this call's only job is the RequestID -> Outcome idempotency record,
// the same role FSM already plays for every other command kind.
func (f *FSM) RecordMembershipOutcome(id RequestID, kind MembershipKind, nodeID, address string, outcome Outcome) (Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fp := membershipFingerprintOf(kind, nodeID, address)
	if e, ok := f.membershipOutcomes[id]; ok {
		if e.fingerprint != fp {
			return Outcome{}, fmt.Errorf("%w: RequestID %q", ErrRequestIDConflict, id)
		}
		return e.outcome, nil
	}
	f.membershipOutcomes[id] = membershipOutcomeEntry{outcome: outcome, fingerprint: fp}
	return outcome, nil
}

// GetMembershipOutcome is the idempotency pre-check read path (§2.6a),
// mirroring Precheck: ErrRequestIDUnknown when id has never been
// recorded (the caller must proceed to propose), the recorded outcome
// when the fingerprint matches (the caller must not re-propose), or
// ErrRequestIDConflict on a fingerprint mismatch (the caller must
// reject the request outright without touching the original
// RequestID's recorded outcome).
func (f *FSM) GetMembershipOutcome(id RequestID, kind MembershipKind, nodeID, address string) (Outcome, error) {
	f.rLock()
	defer f.rUnlock()
	e, ok := f.membershipOutcomes[id]
	if !ok {
		return Outcome{}, ErrRequestIDUnknown
	}
	if e.fingerprint != membershipFingerprintOf(kind, nodeID, address) {
		return Outcome{}, ErrRequestIDConflict
	}
	return e.outcome, nil
}
