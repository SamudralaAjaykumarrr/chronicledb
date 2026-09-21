package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
)

// --- Boundary conversions (raft.Configuration <-> snapshot.Configuration) ---
//
// internal/raft and internal/snapshot each define their own local
// Member/Configuration types (dynamic-membership plan §7.1) so neither
// package depends on the other; internal/node is the boundary that
// converts between them, exactly as it already does for
// LastIncludedIndex/Term's raft.Index/raft.Term <-> uint64 conversion.

func toSnapshotConfiguration(c raft.Configuration) snapshot.Configuration {
	return snapshot.Configuration{
		Voters:   toSnapshotMembers(c.Voters),
		Learners: toSnapshotMembers(c.Learners),
	}
}

func toSnapshotMembers(members []raft.Member) []snapshot.Member {
	if len(members) == 0 {
		return nil
	}
	out := make([]snapshot.Member, len(members))
	for i, m := range members {
		out[i] = snapshot.Member{ID: string(m.ID), Address: m.Address}
	}
	return out
}

func fromSnapshotConfiguration(c snapshot.Configuration) raft.Configuration {
	return raft.Configuration{
		Voters:   fromSnapshotMembers(c.Voters),
		Learners: fromSnapshotMembers(c.Learners),
	}
}

func fromSnapshotMembers(members []snapshot.Member) []raft.Member {
	if len(members) == 0 {
		return nil
	}
	out := make([]raft.Member, len(members))
	for i, m := range members {
		out[i] = raft.Member{ID: raft.NodeID(m.ID), Address: m.Address}
	}
	return out
}

// bootstrapConfiguration builds the Configuration seeding
// raft.Config.Bootstrap from cfg.Peers/cfg.PeerAddrs/cfg.ListenAddr
// (dynamic-membership plan §1.8): consulted by raft.Core's ConfigAt
// ONLY when this node's own durable log/snapshot carries no
// configuration at all (a truly fresh data directory, or one restored
// via -restore-from — §7.6) — every later restart derives the active
// Configuration from durable state instead, so passing this
// unconditionally on every Open call is safe and requires no
// "is this a fresh directory" branch here at all.
//
// An operator starting a brand-new node that is meant to join an
// existing cluster as a learner (§3.1) passes an empty cfg.Peers,
// which correctly yields the zero Configuration here — raft.Core's own
// bootstrap-seed validation (Config.validate) treats a wholly empty
// Bootstrap as legitimate for exactly this reason.
func bootstrapConfiguration(cfg Config) raft.Configuration {
	if len(cfg.Peers) == 0 {
		return raft.Configuration{}
	}
	voters := make([]raft.Member, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		addr := cfg.ListenAddr
		if p != cfg.ID {
			addr = cfg.PeerAddrs[p]
		}
		voters = append(voters, raft.Member{ID: p, Address: addr})
	}
	return raft.Configuration{Voters: voters}
}

// snapshotWriteVersion is the generation gate shared by maybeSnapshot
// and handleBackup (dynamic-membership plan §7.1): a node never writes
// a FormatVersion 2 snapshot until the cluster has finalized to
// generation >= 2, so a pre-finalize snapshot/backup stays
// byte-identical to what v0.4.0 itself would produce.
func (n *Node) snapshotWriteVersion() uint8 {
	if n.clusterGeneration >= 2 {
		return snapshot.FormatVersion
	}
	return 1
}

// --- Membership request plumbing (dynamic-membership plan §9) ---

// membershipReq is the event-loop-dispatched request every mutating
// admin membership endpoint funnels through, mirroring
// controlProposeReq's shape.
type membershipReq struct {
	kind              raft.MembershipChangeKind
	requestID         fsm.RequestID
	targetID          raft.NodeID
	targetAddr        string
	confirmVoterCount int
	resultCh          chan proposeResult
}

func fsmMembershipKind(kind raft.MembershipChangeKind) fsm.MembershipKind {
	switch kind {
	case raft.AddLearnerChange:
		return fsm.MembershipAddLearner
	case raft.PromoteToVoterChange:
		return fsm.MembershipPromoteVoter
	default:
		return fsm.MembershipRemoveServer
	}
}

// ErrMembershipNotPermitted is §2.6a check 7 / §8.2's leader-side
// generation gate: no membership change may be proposed until the
// cluster has finalized to generation >= 2.
var ErrMembershipNotPermitted = errors.New("node: membership changes are not permitted until the cluster finalizes to generation 2 (run /admin/upgrade/precheck and /admin/upgrade/finalize first)")

// ErrPeerGenerationTooOld is §8.2a's promotion precondition: the target
// learner's last-known compatibility generation is below the cluster's
// committed generation (or entirely unknown), so it may not become a
// voter.
var ErrPeerGenerationTooOld = errors.New("node: target peer's generation is older than the cluster's committed generation (or unknown)")

// ErrNodeRemoved is returned by Propose/BeginReadIndex/membership calls
// once this node has observed a real Configuration excluding its own
// ID (dynamic-membership plan §3.1/§4.6) — a category error distinct
// from NotLeaderError: this node is not merely "not currently leader,"
// it is not a cluster member at all and never will be again without an
// explicit operator re-add.
var ErrNodeRemoved = errors.New("node: this node has observed its own removal from the cluster and is no longer a member")

// ErrConfirmationRequired is §12.2's operator-policy guard: any
// membership operation whose resulting voter count would be fewer than
// three requires the caller to state that exact resulting count via
// confirmVoterCount.
type ErrConfirmationRequired struct {
	ResultingVoterCount int
}

func (e *ErrConfirmationRequired) Error() string {
	return fmt.Sprintf("node: resulting voter count %d requires explicit confirmVoterCount=%d", e.ResultingVoterCount, e.ResultingVoterCount)
}

// ErrLearnerNotCaughtUp is §3.3's promotion lag gate: the target
// learner's matchIndex is not within PromotionMaxLagEntries of this
// leader's own LastIndex(). A pre-proposal refusal — nothing is
// recorded and the caller's RequestID remains freshly usable — and
// explicitly retryable (dynamic-membership plan §2.6a).
type ErrLearnerNotCaughtUp struct {
	Lag uint64
}

func (e *ErrLearnerNotCaughtUp) Error() string {
	return fmt.Sprintf("node: learner not caught up (lag=%d entries)", e.Lag)
}

// selfRemoved reports whether this node's own currently active
// Configuration is real (non-zero) and excludes this node's own ID
// (dynamic-membership plan §3.1) — computed from the one exported
// Configuration accessor rather than duplicating Core's internal
// predicate. Call only from run's goroutine.
func (n *Node) selfRemoved() bool {
	cfg := n.core.ActiveConfig()
	return !cfg.IsZero() && !cfg.IsMember(n.cfg.ID)
}

func resultingVoterCount(cfg raft.Configuration, kind raft.MembershipChangeKind, targetID raft.NodeID) int {
	switch kind {
	case raft.PromoteToVoterChange:
		return len(cfg.Voters) + 1
	case raft.RemoveServerChange:
		if cfg.IsVoter(targetID) {
			return len(cfg.Voters) - 1
		}
		return len(cfg.Voters)
	default: // AddLearnerChange never changes the voter count
		return len(cfg.Voters)
	}
}

// AddLearner adds nodeID (already running, listening at address, with
// a valid mTLS identity — dynamic-membership plan §3) as a new
// non-voting member. Returns the committed outcome, or an error if the
// change was refused before ever being proposed, or if leadership was
// lost/the node stopped/ctx was canceled while it was outstanding.
func (n *Node) AddLearner(ctx context.Context, requestID fsm.RequestID, nodeID raft.NodeID, address string) (fsm.Outcome, error) {
	return n.membershipRequest(ctx, membershipReq{kind: raft.AddLearnerChange, requestID: requestID, targetID: nodeID, targetAddr: address})
}

// PromoteToVoter promotes an existing, sufficiently-caught-up learner
// to voter (dynamic-membership plan §3.3). confirmVoterCount is
// accepted for symmetry with RemoveServer but is never actually
// gating: a promotion can only ever raise the voter count.
func (n *Node) PromoteToVoter(ctx context.Context, requestID fsm.RequestID, nodeID raft.NodeID, confirmVoterCount int) (fsm.Outcome, error) {
	return n.membershipRequest(ctx, membershipReq{kind: raft.PromoteToVoterChange, requestID: requestID, targetID: nodeID, confirmVoterCount: confirmVoterCount})
}

// RemoveServer removes a voter or learner (dynamic-membership plan
// §4). confirmVoterCount must equal the resulting voter count whenever
// that count would drop below three (§12.2); pass 0 when the resulting
// count is (or is expected to be) three or more.
func (n *Node) RemoveServer(ctx context.Context, requestID fsm.RequestID, nodeID raft.NodeID, confirmVoterCount int) (fsm.Outcome, error) {
	return n.membershipRequest(ctx, membershipReq{kind: raft.RemoveServerChange, requestID: requestID, targetID: nodeID, confirmVoterCount: confirmVoterCount})
}

func (n *Node) membershipRequest(ctx context.Context, req membershipReq) (fsm.Outcome, error) {
	// Lane A1 (docs/v0.6.0-plan.md §3.2, §5.4): the single dispatch
	// point AddLearner/PromoteToVoter/RemoveServer all share, so every
	// mutating membership call is gated exactly once, in exactly one
	// place. A saturated client workload must never delay or fail a
	// membership change (A-14/AC-12) — Lane A1's capacity is disjoint
	// from Lane B's (writeGate/readGate) by construction, never a
	// shared structure.
	release, err := n.admission.control.Acquire(ctx)
	if err != nil {
		return fsm.Outcome{}, err
	}
	defer release()

	req.resultCh = make(chan proposeResult, 1)
	select {
	case n.membershipCh <- req:
	case <-ctx.Done():
		return fsm.Outcome{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Outcome{}, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res.outcome, res.err
	case <-ctx.Done():
		return fsm.Outcome{}, ctx.Err()
	case <-n.doneCh:
		return fsm.Outcome{}, ErrNodeStopped
	}
}

// handleMembership is the event-loop entry point for every mutating
// membership request, implementing §2.6a's complete, ordered
// precondition list (checks 1-6 inside Core.ProposeConfigChange itself;
// checks 7-8, plus §3.3's lag gate, here — all leader-only judgments
// that are deliberately not Core's business, §2.6).
func (n *Node) handleMembership(req membershipReq) {
	fsmKind := fsmMembershipKind(req.kind)

	// Idempotency pre-check (§10): before ever touching Core. A refused
	// request below this point has recorded nothing, so this pre-check
	// never itself needs to distinguish "never proposed" from "refused."
	if outcome, err := n.fsmachine.Load().GetMembershipOutcome(req.requestID, fsmKind, string(req.targetID), req.targetAddr); err == nil {
		n.metrics.RequestIDDuplicatesTotal.Inc()
		req.resultCh <- proposeResult{outcome: outcome}
		return
	} else if errors.Is(err, fsm.ErrRequestIDConflict) {
		req.resultCh <- proposeResult{err: err}
		return
	}

	if n.selfRemoved() {
		req.resultCh <- proposeResult{err: ErrNodeRemoved}
		return
	}
	if n.core.Role() != raft.Leader {
		req.resultCh <- proposeResult{err: &NotLeaderError{Leader: n.core.LeaderID()}}
		return
	}
	// Check 7 (§8.2): cluster generation >= 2.
	if n.clusterGeneration < 2 {
		req.resultCh <- proposeResult{err: ErrMembershipNotPermitted}
		return
	}

	cfg := n.core.ActiveConfig()
	// Check 8 (§12.2): sub-three-voter operator confirmation. Deliberately
	// not applied to AddLearner, which never changes the voter count and
	// has no confirmVoterCount field in its own request shape (§9).
	if req.kind != raft.AddLearnerChange {
		resulting := resultingVoterCount(cfg, req.kind, req.targetID)
		if resulting < 3 && resulting != req.confirmVoterCount {
			req.resultCh <- proposeResult{err: &ErrConfirmationRequired{ResultingVoterCount: resulting}}
			return
		}
	}
	// §3.3 + §8.2a: promotion-only leader-only, non-replicated gates,
	// evaluated before ever proposing so a refusal here also records
	// nothing.
	if req.kind == raft.PromoteToVoterChange && cfg.IsLearner(req.targetID) {
		last := n.core.LastIndex()
		match := n.core.MatchIndexOf(req.targetID)
		var lag uint64
		if last > match {
			lag = uint64(last - match)
		}
		if lag > n.cfg.PromotionMaxLagEntries {
			req.resultCh <- proposeResult{err: &ErrLearnerNotCaughtUp{Lag: lag}}
			return
		}
		gen, known := n.peerGenerations[req.targetID]
		if !known || gen < n.clusterGeneration {
			req.resultCh <- proposeResult{err: ErrPeerGenerationTooOld}
			return
		}
	}

	out, err := n.core.ProposeConfigChange(req.kind, string(req.requestID), req.targetID, req.targetAddr)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			req.resultCh <- proposeResult{err: &NotLeaderError{Leader: n.core.LeaderID()}}
			return
		}
		req.resultCh <- proposeResult{err: err}
		return
	}
	if out.PersistRequest == nil || len(out.PersistRequest.Entries) != 1 {
		n.fail(fmt.Errorf("node: unexpected ProposeConfigChange output shape: %+v", out))
		req.resultCh <- proposeResult{err: ErrNodeStopped}
		return
	}
	idx := out.PersistRequest.Entries[0].Index
	n.waiters[idx] = waiter{requestID: req.requestID, resultCh: req.resultCh}
	n.processOutput(out)
}

// applyConfigEntry applies one committed EntryConfig entry (dynamic-
// membership plan §2.6/§7.6/§8.2): the follower-side generation gate,
// the membership RequestID outcome record, the live dial-table update,
// and waiter resolution. Returns false if it called n.fail, mirroring
// applyCommitted's own control flow.
func (n *Node) applyConfigEntry(e raft.Entry) bool {
	kind, requestID, targetID, targetAddr, cfg, establishes, err := raft.DecodeConfigEntry(e)
	if err != nil {
		n.fail(fmt.Errorf("node: decoding committed EntryConfig %d: %w", e.Index, err))
		return false
	}
	n.appliedIndex = uint64(e.Index)
	n.core.SetApplied(e.Index)

	if !establishes {
		// Voided (§7.6): a committed index with no effect. No outcome
		// record, no dial-table change, no waiter resolution, no
		// generation check beyond what already applied below it.
		return true
	}

	// §8.2's follower-side acceptance gate: an EntryConfig entry can
	// never legitimately reach this point on a node below generation 2
	// (a v0.5.0 leader only proposes once its own committed generation
	// is >= 2, and the generation-2 finalize necessarily precedes any
	// EntryConfig entry in the same log) — defense in depth, exactly
	// the posture ADR-0017 already documents for its own equivalents.
	if n.clusterGeneration < 2 {
		n.fail(fmt.Errorf("node: committed EntryConfig at index %d requires cluster generation >= 2, this node is at %d", e.Index, n.clusterGeneration))
		return false
	}

	outcome := fsm.Outcome{RequestID: fsm.RequestID(requestID), Status: fsm.StatusCommitted, CommitSeq: uint64(e.Index)}
	recorded, err := n.fsmachine.Load().RecordMembershipOutcome(fsm.RequestID(requestID), fsmMembershipKind(kind), targetID, targetAddr, outcome)
	if err != nil {
		// A fingerprint conflict on an already-committed entry means the
		// same RequestID was used for two structurally different
		// membership operations that both reached the log — deterministic
		// given identical committed history on every replica, but not a
		// state this node can safely continue past.
		n.fail(fmt.Errorf("node: recording membership outcome at index %d: %w", e.Index, err))
		return false
	}

	// Dial-table update (§1.8): every member's address, so a newly added
	// learner is dialable immediately.
	for _, m := range cfg.Voters {
		n.tr.SetPeerAddr(m.ID, m.Address)
	}
	for _, m := range cfg.Learners {
		n.tr.SetPeerAddr(m.ID, m.Address)
	}

	n.resolveWaiter(e.Index, fsm.RequestID(requestID), recorded, nil)
	return true
}

// --- /admin/membership/status (§9, §14) ---

// MemberStatus is one voter or learner's detail in a
// MembershipStatusResult.
type MemberStatus struct {
	ID              raft.NodeID
	Address         string
	MatchIndex      raft.Index
	Lag             uint64 // learners only; always 0 for a voter
	Generation      uint32
	GenerationKnown bool
}

// MembershipStatusResult is GET /admin/membership/status's answer.
type MembershipStatusResult struct {
	ConfigIndex          raft.Index
	CommittedConfigIndex raft.Index
	Voters               []MemberStatus
	Learners             []MemberStatus
	InProgress           bool
	ChangesReady         bool
	NotReadyReason       string
}

type membershipStatusReq struct {
	resultCh chan MembershipStatusResult
}

// MembershipStatus reports this node's current view of cluster
// membership. Read-only; never mutates anything. Safe to call from any
// goroutine (dispatches through the event loop, mirroring
// UpgradePrecheck).
func (n *Node) MembershipStatus(ctx context.Context) (MembershipStatusResult, error) {
	req := membershipStatusReq{resultCh: make(chan MembershipStatusResult, 1)}
	select {
	case n.membershipStatusCh <- req:
	case <-ctx.Done():
		return MembershipStatusResult{}, ctx.Err()
	case <-n.doneCh:
		return MembershipStatusResult{}, ErrNodeStopped
	}
	select {
	case res := <-req.resultCh:
		return res, nil
	case <-ctx.Done():
		return MembershipStatusResult{}, ctx.Err()
	case <-n.doneCh:
		return MembershipStatusResult{}, ErrNodeStopped
	}
}

func (n *Node) computeMembershipStatus() MembershipStatusResult {
	cfg := n.core.ActiveConfig()
	_, committedIdx := n.core.ConfigAt(n.core.CommitIndex())
	ready, reason := n.core.ConfigChangeReady()
	last := n.core.LastIndex()

	voters := make([]MemberStatus, 0, len(cfg.Voters))
	for _, m := range cfg.Voters {
		voters = append(voters, MemberStatus{ID: m.ID, Address: m.Address, MatchIndex: n.core.MatchIndexOf(m.ID)})
	}
	learners := make([]MemberStatus, 0, len(cfg.Learners))
	for _, m := range cfg.Learners {
		mi := n.core.MatchIndexOf(m.ID)
		var lag uint64
		if last > mi {
			lag = uint64(last - mi)
		}
		gen, known := n.peerGenerations[m.ID]
		learners = append(learners, MemberStatus{ID: m.ID, Address: m.Address, MatchIndex: mi, Lag: lag, Generation: gen, GenerationKnown: known})
	}
	return MembershipStatusResult{
		ConfigIndex:          n.core.ActiveConfigIndex(),
		CommittedConfigIndex: committedIdx,
		Voters:               voters,
		Learners:             learners,
		InProgress:           n.core.ActiveConfigIndex() > n.core.CommitIndex(),
		ChangesReady:         ready,
		NotReadyReason:       reason,
	}
}
