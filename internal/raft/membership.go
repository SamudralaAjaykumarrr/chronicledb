package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// controlCommandMarkerMirror deliberately mirrors internal/fsm's
// ControlCommandMarker (0xF0) so that, on the wire, a pre-v0.5.0 binary
// decoding an EntryConfig entry as an ordinary opaque command (its
// Type field silently dropped by gob) routes it into
// fsm.IsControlCommand/DecodeSetClusterVersion and fails closed there
// (dynamic-membership plan §2.5). internal/raft does not import
// internal/fsm (that would invert docs/architecture.md §5's dependency
// direction) — this is a deliberate, documented, independently-defined
// mirror, guarded by TestControlKindRangesNeverCollide (in
// internal/fsm, which mirrors this package's reserved range the same
// way) against ever accidentally diverging.
const controlCommandMarkerMirror byte = 0xF0

// Reserved EntryConfig membership-change-kind byte range (dynamic-
// membership plan §2.5). Starts at 16 — a generously-spaced offset
// past internal/fsm's own control-kind range (currently just
// controlKindSetClusterVersion == 1) — so each package can grow its
// own range in future phases without approaching the other's.
const (
	membershipKindAddLearner   byte = 16
	membershipKindPromoteVoter byte = 17
	membershipKindRemoveServer byte = 18
	// membershipKindVoided marks an EntryConfig entry that establishes
	// no configuration at all (dynamic-membership plan §7.6): produced
	// only by internal/backup's offline restore transform, never by
	// ProposeConfigChange, but accepted unconditionally on the ordinary
	// replication path like any other entry.
	membershipKindVoided byte = 19
)

// MembershipChangeKind identifies which of the three operator-facing
// membership operations a ProposeConfigChange call requests.
type MembershipChangeKind uint8

const (
	AddLearnerChange MembershipChangeKind = iota + 1
	PromoteToVoterChange
	RemoveServerChange
)

var (
	// ErrNotLeader is returned by ProposeConfigChange when this Core is
	// not currently Leader (dynamic-membership plan §2.6a check 1). Use
	// LeaderID() for the current best-known leader hint.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrConfigChangeInheritedSuffixUncommitted is §2.6a check 2 (P2):
	// this leader's inherited log tail (as of its own election) has not
	// yet committed. Automatically retryable with the same RequestID.
	ErrConfigChangeInheritedSuffixUncommitted = errors.New("raft: configuration change refused: inherited log suffix not yet committed")
	// ErrConfigChangeNoCurrentTermCommit is §2.6a check 3 (P1): this
	// leader has not yet committed an entry of its own current term.
	// Automatically retryable with the same RequestID.
	ErrConfigChangeNoCurrentTermCommit = errors.New("raft: configuration change refused: no current-term commit yet")
	// ErrConfigChangeInProgress is §2.6a check 4 (P3): a change this
	// leader itself appended has not yet committed. Operator-visible,
	// not auto-retried.
	ErrConfigChangeInProgress = errors.New("raft: configuration change already in progress")
	// ErrInvalidTransition is §2.6a check 5: the requested transition is
	// not exactly one of the four legal single-server shapes.
	ErrInvalidTransition = errors.New("raft: proposed configuration is not a valid single-server transition")
	// ErrLastVoterRemoval is §2.6a check 6 (MINIMUM VOTER INVARIANT):
	// refusing to reduce the voter count to zero. Never
	// operator-overridable.
	ErrLastVoterRemoval = errors.New("raft: cannot remove the last voter")
	// ErrUnknownMember is returned when a promote/remove target is not a
	// current member (promote) or not a member at all (remove).
	ErrUnknownMember = errors.New("raft: target is not a current member")
	// ErrMemberAlreadyExists is returned when an add target's ID is
	// already present anywhere in the current configuration.
	ErrMemberAlreadyExists = errors.New("raft: target is already a member")

	// ErrMalformedConfigChange indicates an EntryConfig payload could
	// not be decoded — truncated, or a length/count field inconsistent
	// with the bytes actually present. Never produced by a panic;
	// decoding always fails closed with this error instead.
	ErrMalformedConfigChange = errors.New("raft: malformed EntryConfig payload")
	// ErrUnknownConfigChangeKind indicates a recognized marker byte but
	// an unrecognized membership-change-kind byte — a kind newer than
	// this build understands.
	ErrUnknownConfigChangeKind = errors.New("raft: unknown EntryConfig membership-change kind")
)

// --- EntryConfig payload framing (dynamic-membership plan §2.5) ---
//
//	byte[0]      = 0xF0 (controlCommandMarkerMirror)
//	byte[1]      = membership-change kind (16/17/18/19)
//	requestIDLen(4B)  requestID(...)
//	targetIDLen(4B)   targetID(...)
//	targetAddrLen(4B) targetAddr(...)
//	fullConfigLen(4B) fullConfig(...)  (empty for a Voided entry)

func putUint32At(buf []byte, off int, v uint32) int {
	binary.BigEndian.PutUint32(buf[off:], v)
	return off + 4
}

func putLenPrefixed(buf []byte, off int, b []byte) int {
	off = putUint32At(buf, off, uint32(len(b)))
	off += copy(buf[off:], b)
	return off
}

func getLenPrefixed(b []byte, off int) ([]byte, int, error) {
	if len(b)-off < 4 {
		return nil, off, fmt.Errorf("%w: truncated length prefix at offset %d", ErrMalformedConfigChange, off)
	}
	n := binary.BigEndian.Uint32(b[off:])
	off += 4
	if int64(n) > int64(len(b)-off) {
		return nil, off, fmt.Errorf("%w: truncated field (declared %d bytes, %d remain)", ErrMalformedConfigChange, n, len(b)-off)
	}
	out := b[off : off+int(n)]
	off += int(n)
	return out, off, nil
}

// encodeConfiguration serializes cfg per the fullConfig section of
// encodeConfigChange's layout.
func encodeConfiguration(cfg Configuration) []byte {
	size := 4
	for _, m := range cfg.Voters {
		size += 4 + len(m.ID) + 4 + len(m.Address)
	}
	size += 4
	for _, m := range cfg.Learners {
		size += 4 + len(m.ID) + 4 + len(m.Address)
	}
	buf := make([]byte, size)
	off := 0
	off = putUint32At(buf, off, uint32(len(cfg.Voters)))
	for _, m := range cfg.Voters {
		off = putLenPrefixed(buf, off, []byte(m.ID))
		off = putLenPrefixed(buf, off, []byte(m.Address))
	}
	off = putUint32At(buf, off, uint32(len(cfg.Learners)))
	for _, m := range cfg.Learners {
		off = putLenPrefixed(buf, off, []byte(m.ID))
		off = putLenPrefixed(buf, off, []byte(m.Address))
	}
	return buf[:off]
}

const minEncodedMemberSize = 4 + 4 // shortest possible idLen+addrLen pair

func decodeConfiguration(b []byte) (Configuration, error) {
	if len(b) < 4 {
		return Configuration{}, fmt.Errorf("%w: truncated voter count", ErrMalformedConfigChange)
	}
	off := 0
	voterCount := binary.BigEndian.Uint32(b[off:])
	off += 4
	if remaining := len(b) - off; voterCount > uint32(remaining/minEncodedMemberSize) {
		return Configuration{}, fmt.Errorf("%w: declares %d voters but only %d bytes remain", ErrMalformedConfigChange, voterCount, remaining)
	}
	voters := make([]Member, 0, voterCount)
	for i := uint32(0); i < voterCount; i++ {
		idB, o, err := getLenPrefixed(b, off)
		if err != nil {
			return Configuration{}, err
		}
		off = o
		addrB, o2, err := getLenPrefixed(b, off)
		if err != nil {
			return Configuration{}, err
		}
		off = o2
		voters = append(voters, Member{ID: NodeID(idB), Address: string(addrB)})
	}
	if len(b)-off < 4 {
		return Configuration{}, fmt.Errorf("%w: truncated learner count", ErrMalformedConfigChange)
	}
	learnerCount := binary.BigEndian.Uint32(b[off:])
	off += 4
	if remaining := len(b) - off; learnerCount > uint32(remaining/minEncodedMemberSize) {
		return Configuration{}, fmt.Errorf("%w: declares %d learners but only %d bytes remain", ErrMalformedConfigChange, learnerCount, remaining)
	}
	learners := make([]Member, 0, learnerCount)
	for i := uint32(0); i < learnerCount; i++ {
		idB, o, err := getLenPrefixed(b, off)
		if err != nil {
			return Configuration{}, err
		}
		off = o
		addrB, o2, err := getLenPrefixed(b, off)
		if err != nil {
			return Configuration{}, err
		}
		off = o2
		learners = append(learners, Member{ID: NodeID(idB), Address: string(addrB)})
	}
	if off != len(b) {
		return Configuration{}, fmt.Errorf("%w: %d trailing bytes", ErrMalformedConfigChange, len(b)-off)
	}
	return Configuration{Voters: voters, Learners: learners}, nil
}

func encodeConfigChange(kind byte, requestID, targetID, targetAddr string, cfg Configuration) []byte {
	cfgBytes := encodeConfiguration(cfg)
	size := 2 + 4 + len(requestID) + 4 + len(targetID) + 4 + len(targetAddr) + 4 + len(cfgBytes)
	buf := make([]byte, size)
	off := 0
	buf[off] = controlCommandMarkerMirror
	off++
	buf[off] = kind
	off++
	off = putLenPrefixed(buf, off, []byte(requestID))
	off = putLenPrefixed(buf, off, []byte(targetID))
	off = putLenPrefixed(buf, off, []byte(targetAddr))
	off = putLenPrefixed(buf, off, cfgBytes)
	return buf[:off]
}

// EncodeVoidedEntryConfigPayload returns the EntryConfig.Data payload
// for a "voided" entry (dynamic-membership plan §7.6): an entry that
// establishes no configuration at all. Only internal/backup's offline
// restore transform ever constructs one — exported so a pinning test
// (in both packages) asserts internal/backup's independent
// implementation of this exact byte layout never drifts from this
// package's own definition; internal/backup does not import
// internal/raft (dynamic-membership plan §6.3's call-site table).
func EncodeVoidedEntryConfigPayload() []byte {
	return []byte{
		controlCommandMarkerMirror, membershipKindVoided,
		0, 0, 0, 0, // requestIDLen = 0
		0, 0, 0, 0, // targetIDLen = 0
		0, 0, 0, 0, // targetAddrLen = 0
		0, 0, 0, 0, // fullConfigLen = 0
	}
}

// decodeConfigChange parses an EntryConfig.Data payload. establishes is
// false only for a Voided entry, which carries no Configuration to
// validate at all — §2.6's carve-out.
func decodeConfigChange(b []byte) (kind byte, requestID, targetID, targetAddr string, cfg Configuration, establishes bool, err error) {
	if len(b) < 2 {
		err = fmt.Errorf("%w: payload too short (%d bytes)", ErrMalformedConfigChange, len(b))
		return
	}
	if b[0] != controlCommandMarkerMirror {
		err = fmt.Errorf("%w: payload does not carry the membership marker byte", ErrMalformedConfigChange)
		return
	}
	kind = b[1]
	off := 2
	var reqIDB, targetIDB, targetAddrB, cfgB []byte
	if reqIDB, off, err = getLenPrefixed(b, off); err != nil {
		return
	}
	if targetIDB, off, err = getLenPrefixed(b, off); err != nil {
		return
	}
	if targetAddrB, off, err = getLenPrefixed(b, off); err != nil {
		return
	}
	if cfgB, off, err = getLenPrefixed(b, off); err != nil {
		return
	}
	if off != len(b) {
		err = fmt.Errorf("%w: %d trailing bytes", ErrMalformedConfigChange, len(b)-off)
		return
	}
	requestID = string(reqIDB)
	targetID = string(targetIDB)
	targetAddr = string(targetAddrB)
	switch kind {
	case membershipKindAddLearner, membershipKindPromoteVoter, membershipKindRemoveServer:
		cfg, err = decodeConfiguration(cfgB)
		if err != nil {
			return
		}
		establishes = true
	case membershipKindVoided:
		if len(cfgB) != 0 || len(reqIDB) != 0 || len(targetIDB) != 0 || len(targetAddrB) != 0 {
			err = fmt.Errorf("%w: voided entry must carry no fields", ErrMalformedConfigChange)
			return
		}
		establishes = false
	default:
		err = fmt.Errorf("%w: kind %d", ErrUnknownConfigChangeKind, kind)
		return
	}
	return
}

// --- §2.6 four-shape structural validation ---

func toMemberSet(members []Member) map[NodeID]Member {
	s := make(map[NodeID]Member, len(members))
	for _, m := range members {
		s[m.ID] = m
	}
	return s
}

func setsEqualMembers(a, b map[NodeID]Member) bool {
	if len(a) != len(b) {
		return false
	}
	for id, m := range a {
		if bm, ok := b[id]; !ok || bm != m {
			return false
		}
	}
	return true
}

// oneAdded returns the single member present in newSet but not oldSet,
// when newSet has exactly one more element than oldSet and that is the
// only difference in cardinality; ok is false otherwise (the caller
// still separately verifies the remainder is identical).
func oneAdded(oldSet, newSet map[NodeID]Member) (Member, bool) {
	if len(newSet) != len(oldSet)+1 {
		return Member{}, false
	}
	for id, m := range newSet {
		if _, ok := oldSet[id]; !ok {
			return m, true
		}
	}
	return Member{}, false
}

func oneRemoved(oldSet, newSet map[NodeID]Member) (Member, bool) {
	return oneAdded(newSet, oldSet)
}

// classifyTransition validates that new is exactly one of §2.6's four
// legal single-server transitions relative to old. Returns nil iff
// valid. Used both by ProposeConfigChange (leader side) and by the
// accept-time defense-in-depth re-check every replica performs.
func classifyTransition(old, new Configuration) error {
	oldV, newV := toMemberSet(old.Voters), toMemberSet(new.Voters)
	oldL, newL := toMemberSet(old.Learners), toMemberSet(new.Learners)
	for id := range newV {
		if _, ok := newL[id]; ok {
			return ErrInvalidTransition
		}
	}

	switch {
	case len(newV) == len(oldV) && len(newL) == len(oldL)+1:
		// AddLearner
		if !setsEqualMembers(oldV, newV) {
			return ErrInvalidTransition
		}
		added, ok := oneAdded(oldL, newL)
		if !ok {
			return ErrInvalidTransition
		}
		if _, exists := oldV[added.ID]; exists {
			return ErrInvalidTransition
		}
		return nil
	case len(newV) == len(oldV)+1 && len(newL)+1 == len(oldL):
		// PromoteToVoter
		addedV, ok := oneAdded(oldV, newV)
		if !ok {
			return ErrInvalidTransition
		}
		removedL, ok := oneRemoved(oldL, newL)
		if !ok {
			return ErrInvalidTransition
		}
		if addedV != removedL {
			return ErrInvalidTransition
		}
		for id, m := range oldV {
			if nm, ok := newV[id]; !ok || nm != m {
				return ErrInvalidTransition
			}
		}
		return nil
	case len(newV)+1 == len(oldV) && len(newL) == len(oldL):
		// RemoveServer (voter)
		if len(newV) < 1 {
			return ErrInvalidTransition
		}
		if !setsEqualMembers(oldL, newL) {
			return ErrInvalidTransition
		}
		if _, ok := oneRemoved(oldV, newV); !ok {
			return ErrInvalidTransition
		}
		return nil
	case len(newV) == len(oldV) && len(newL)+1 == len(oldL):
		// RemoveServer (learner)
		if !setsEqualMembers(oldV, newV) {
			return ErrInvalidTransition
		}
		if _, ok := oneRemoved(oldL, newL); !ok {
			return ErrInvalidTransition
		}
		return nil
	default:
		return ErrInvalidTransition
	}
}

// --- Building the candidate Configuration for a proposed change ---

func removeMemberByID(members []Member, id NodeID) []Member {
	out := make([]Member, 0, len(members))
	for _, m := range members {
		if m.ID != id {
			out = append(out, m)
		}
	}
	return out
}

func appendMember(members []Member, m Member) []Member {
	out := make([]Member, len(members), len(members)+1)
	copy(out, members)
	return append(out, m)
}

func findMemberByID(members []Member, id NodeID) (Member, bool) {
	for _, m := range members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

func (c *Core) buildNewConfiguration(kind MembershipChangeKind, targetID NodeID, targetAddr string) (Configuration, error) {
	cur := c.activeConfig
	switch kind {
	case AddLearnerChange:
		if cur.IsMember(targetID) {
			return Configuration{}, ErrMemberAlreadyExists
		}
		return Configuration{
			Voters:   cloneMembers(cur.Voters),
			Learners: appendMember(cur.Learners, Member{ID: targetID, Address: targetAddr}),
		}, nil
	case PromoteToVoterChange:
		m, ok := findMemberByID(cur.Learners, targetID)
		if !ok {
			return Configuration{}, ErrUnknownMember
		}
		return Configuration{
			Voters:   appendMember(cur.Voters, m),
			Learners: removeMemberByID(cur.Learners, targetID),
		}, nil
	case RemoveServerChange:
		if cur.IsVoter(targetID) {
			return Configuration{Voters: removeMemberByID(cur.Voters, targetID), Learners: cloneMembers(cur.Learners)}, nil
		}
		if cur.IsLearner(targetID) {
			return Configuration{Voters: cloneMembers(cur.Voters), Learners: removeMemberByID(cur.Learners, targetID)}, nil
		}
		return Configuration{}, ErrUnknownMember
	default:
		return Configuration{}, fmt.Errorf("%w: unknown MembershipChangeKind %d", ErrInvalidTransition, kind)
	}
}

func wireKindFor(kind MembershipChangeKind) byte {
	switch kind {
	case AddLearnerChange:
		return membershipKindAddLearner
	case PromoteToVoterChange:
		return membershipKindPromoteVoter
	case RemoveServerChange:
		return membershipKindRemoveServer
	default:
		return 0
	}
}

// ProposeConfigChange is the leader-only membership-change entry point
// (dynamic-membership plan §2.6a): every check below is synchronous and
// returns before any entry is created, in the exact order specified —
// a refused membership change never occupies a log index, never has an
// ambiguous outcome, and never records anything in the idempotency
// table, so the caller's RequestID remains entirely unused and safe to
// reuse verbatim. requestID/targetAddr are opaque strings Core never
// interprets beyond carrying them in the resulting Configuration.
func (c *Core) ProposeConfigChange(kind MembershipChangeKind, requestID string, targetID NodeID, targetAddr string) (Output, error) {
	// Check 1: leadership.
	if c.role != Leader {
		return Output{}, ErrNotLeader
	}
	// Check 2 (P2): inherited-suffix floor.
	if c.commitIndex < c.pendingConfIndex {
		return Output{}, ErrConfigChangeInheritedSuffixUncommitted
	}
	// Check 3 (P1): leader-term commit gate.
	if !c.skipP1ForTest && c.termAt(c.commitIndex) != c.currentTerm {
		return Output{}, ErrConfigChangeNoCurrentTermCommit
	}
	// Check 4 (P3): local serialization.
	if c.activeConfigIndex > c.commitIndex {
		return Output{}, ErrConfigChangeInProgress
	}

	newCfg, err := c.buildNewConfiguration(kind, targetID, targetAddr)
	if err != nil {
		return Output{}, err
	}
	// Check 6 (MINIMUM VOTER INVARIANT), checked ahead of the general
	// shape check so its own distinct, never-overridable error surfaces.
	if len(newCfg.Voters) == 0 {
		return Output{}, ErrLastVoterRemoval
	}
	// Check 5: exactly one of the four shapes (defense-in-depth re-run
	// of what buildNewConfiguration already guarantees by construction —
	// never expected to fail here, but never skipped either).
	if err := classifyTransition(c.activeConfig, newCfg); err != nil {
		return Output{}, err
	}

	wireKind := wireKindFor(kind)
	if wireKind == membershipKindVoided || wireKind == 0 {
		panic("raft: ProposeConfigChange constructed an invalid or voided wire kind — this must never happen")
	}
	payload := encodeConfigChange(wireKind, requestID, string(targetID), targetAddr, newCfg)
	return c.appendLeaderEntry(EntryConfig, payload), nil
}

// --- ConfigAt: the single configuration-reconstruction algorithm (§6.3) ---

// ConfigAt returns the Configuration effective at log index i, and the
// index of the EntryConfig entry that established it (0 when it came
// from the snapshot boundary or the bootstrap seed). Pure: reads only
// c.log, c.snapshotIndex, c.snapshotConfig, c.snapshotHasConfig, and
// c.bootstrapConfig — no I/O, no mutation, deterministic. This is the
// entirety of MEMBERSHIP RECOVERY DETERMINISM's mechanism, and it is
// the sole way any configuration is ever derived anywhere in this
// package (dynamic-membership plan §6.3).
//
// Precondition: c.snapshotIndex <= i <= c.lastIndex().
func (c *Core) ConfigAt(i Index) (Configuration, Index) {
	for idx := i; idx > c.snapshotIndex; idx-- {
		p := c.pos(idx)
		if p < 0 || p >= len(c.log) {
			continue
		}
		e := c.log[p]
		if e.Type != EntryConfig {
			continue
		}
		_, _, _, _, cfg, establishes, err := decodeConfigChange(e.Data)
		if err != nil || !establishes {
			continue // a Voided entry, or (should never happen) undecodable
		}
		return cfg.clone(), idx
	}
	if c.snapshotHasConfig {
		return c.snapshotConfig.clone(), 0
	}
	// bootstrapConfig's zero value already equals the zero
	// Configuration, so this single return also covers step 4's
	// "never joined" fallback.
	return c.bootstrapConfig.clone(), 0
}

// ActiveConfig returns a deep copy of the currently live configuration
// (dynamic-membership plan §6.3a) — never a slice aliasing Core's own
// backing arrays.
func (c *Core) ActiveConfig() Configuration { return c.activeConfig.clone() }

// ActiveConfigIndex returns the log index of the EntryConfig entry that
// produced ActiveConfig(), or 0 when it came from the snapshot boundary
// or Config.Bootstrap.
func (c *Core) ActiveConfigIndex() Index { return c.activeConfigIndex }

// ConfigChangeReady reports whether ProposeConfigChange's P1/P2 gates
// (§2.2a) currently hold on this Core, and if not, which one is
// outstanding — for /admin/membership/status's changesReady/
// notReadyReason fields (§9, §11) and for the membership_changes_ready
// metric (§14). Always false with reason "not-leader" on a non-leader.
// Read-only: performs no I/O and mutates nothing.
func (c *Core) ConfigChangeReady() (ready bool, reason string) {
	if c.role != Leader {
		return false, "not-leader"
	}
	if c.commitIndex < c.pendingConfIndex {
		return false, "inherited-suffix-uncommitted"
	}
	if c.termAt(c.commitIndex) != c.currentTerm {
		return false, "no-current-term-commit"
	}
	if c.activeConfigIndex > c.commitIndex {
		return false, "change-in-progress"
	}
	return true, ""
}

// neverJoined reports whether this node has never observed any
// configuration from any source (dynamic-membership plan §3.1).
func (c *Core) neverJoined() bool { return c.activeConfig.IsZero() }

// selfRemoved reports whether this node has observed a real
// configuration that excludes its own ID (dynamic-membership plan
// §3.1) — distinct from neverJoined, which this package never conflates
// with it.
func (c *Core) selfRemoved() bool {
	return !c.activeConfig.IsZero() && !c.activeConfig.IsMember(c.cfg.ID)
}

// initReplicationStateFor lazily initializes leader-only volatile
// replication state for id the first time it is ever seen, using
// prevLast (this leader's lastIndex() immediately before the entry that
// introduced id) rather than Go's zero value — a zero nextIndex would
// be clamped to 1 by appendEntriesMessage and would then copy this
// leader's entire retained log tail into a single message (dynamic-
// membership plan §2.2).
func (c *Core) initReplicationStateFor(id NodeID, prevLast Index) {
	if _, ok := c.nextIndex[id]; !ok {
		c.nextIndex[id] = prevLast + 1
	}
	if _, ok := c.matchIndex[id]; !ok {
		c.matchIndex[id] = 0
	}
}

// activateFromAppendedEntries is the follower/replica-side fast-path
// activation used when handleAppendEntriesRequest appends a batch of
// entries with no accompanying truncation: since ConfigAt's step-1 scan
// only ever matches EntryConfig entries, and none existed above the
// old lastIndex() before this batch, the newest configuration-
// establishing entry can only be found within the batch itself (or is
// unchanged, if none of the batch qualifies) — never requiring a full
// backward scan.
//
// This is also where a replica's defense-in-depth four-shape re-check
// runs (§2.6) — but only once this replica has an independent basis to
// evaluate it against. A node whose own prior configuration is still
// the zero value (neverJoined(), §3.1) has, by construction, no
// locally-derivable "before" state to compare against: its own
// Config.Bootstrap is empty (it is joining, not founding), so its
// first-ever configuration-establishing entry legitimately arrives as
// part of a bulk catch-up batch that also replicates everything the
// cluster did before this node existed — a real, reachable production
// path (a brand-new learner's very first AppendEntries batch), not a
// corner case. Validating that entry against a zero prior would always
// fail (no shape has zero voters on one side and a real voter set on
// the other), which would make catching up a new learner
// indistinguishable, from this check's point of view, from a
// corrupted message — a false positive this check must not produce.
// Once this node's own prior becomes real (from that same entry
// onward), every SUBSEQUENT EntryConfig entry in this or a later batch
// is validated normally: this node now has a genuine local basis, and
// a structurally invalid entry at that point can only mean a
// leader-side bug or a corrupted/adversarial message, never a state a
// correct leader would produce, so it fails closed exactly like the
// existing "refusing to truncate a committed entry" panic just above in
// this file's sibling core.go. Decode failures fail closed
// unconditionally, in both cases — a malformed payload is never
// legitimate regardless of this node's own prior state.
func (c *Core) activateFromAppendedEntries(entries []Entry) {
	prior := c.activeConfig
	for _, e := range entries {
		if e.Type != EntryConfig {
			continue
		}
		_, _, _, _, cfg, establishes, err := decodeConfigChange(e.Data)
		if err != nil {
			panic(fmt.Sprintf("raft: entry %d: malformed EntryConfig payload: %v — this indicates a leader-side bug or a corrupted/adversarial message, not a recoverable local condition", e.Index, err))
		}
		if !establishes {
			continue // Voided: accepted unconditionally, establishes nothing (§7.6)
		}
		if !prior.IsZero() {
			if err := classifyTransition(prior, cfg); err != nil {
				panic(fmt.Sprintf("raft: entry %d: EntryConfig is not a valid single-server transition relative to the preceding configuration: %v", e.Index, err))
			}
		}
		prior = cfg
		c.activeConfig = cfg
		c.activeConfigIndex = e.Index
	}
}

func membershipChangeKindFromWire(kind byte) (MembershipChangeKind, bool) {
	switch kind {
	case membershipKindAddLearner:
		return AddLearnerChange, true
	case membershipKindPromoteVoter:
		return PromoteToVoterChange, true
	case membershipKindRemoveServer:
		return RemoveServerChange, true
	default:
		return 0, false
	}
}

// DecodeConfigEntry decodes a committed EntryConfig entry's payload for
// internal/node.applyCommitted (dynamic-membership plan §2.6/§7.6): the
// membership operation kind, the request's opaque identifiers, the
// resulting Configuration, and establishes (false only for a Voided
// entry, which carries none of the above and requires no further
// action beyond advancing appliedIndex). e.Type must be EntryConfig.
//
// This is the only way internal/node ever learns a committed
// configuration's requestID/targetID/targetAddr — internal/fsm never
// decodes EntryConfig payloads itself (§2.4).
func DecodeConfigEntry(e Entry) (kind MembershipChangeKind, requestID, targetID, targetAddr string, cfg Configuration, establishes bool, err error) {
	if e.Type != EntryConfig {
		err = fmt.Errorf("raft: DecodeConfigEntry: entry %d has Type %d, not EntryConfig", e.Index, e.Type)
		return
	}
	wireKind, requestID, targetID, targetAddr, cfg, establishes, err := decodeConfigChange(e.Data)
	if err != nil {
		return
	}
	if !establishes {
		return 0, requestID, targetID, targetAddr, cfg, false, nil
	}
	k, ok := membershipChangeKindFromWire(wireKind)
	if !ok {
		err = fmt.Errorf("%w: %d", ErrUnknownConfigChangeKind, wireKind)
		return
	}
	kind = k
	return
}
