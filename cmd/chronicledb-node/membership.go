// This file implements the control-plane's dynamic-membership admin
// surface (docs/dynamic-membership-plan.md §9): the three admin-gated,
// audited mutating endpoints (/admin/membership/add, /promote,
// /remove) and the read-only /admin/membership/status endpoint,
// mirroring upgrade.go's precheck/finalize request/response/
// error-handling conventions exactly.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// membershipMutateRequest is the shared JSON request shape for
// add/promote/remove (§9): Address is meaningful only for add;
// ConfirmVoterCount only for promote/remove (§12.2).
type membershipMutateRequest struct {
	RequestID         string `json:"requestId"`
	NodeID            string `json:"nodeId"`
	Address           string `json:"address,omitempty"`
	ConfirmVoterCount int    `json:"confirmVoterCount,omitempty"`
}

type membershipMutateResponse struct {
	Status              string `json:"status,omitempty"`
	Error               string `json:"error,omitempty"`
	Reason              string `json:"reason,omitempty"`
	LeaderHint          string `json:"leaderHint,omitempty"`
	Warning             string `json:"warning,omitempty"`
	ResultingVoterCount int    `json:"resultingVoterCount,omitempty"`
	Lag                 uint64 `json:"lag,omitempty"`
	RetryAfterSeconds   int    `json:"-"`
}

// membershipRetryAfterSeconds is the Retry-After value this
// implementation uses for the three admin-layer-retryable refusals
// (§2.6a, §9): P1/P2's ErrConfigChangeNotReady-equivalents and §3.3's
// ErrLearnerNotCaughtUp. The exact number is free (§22); the retry
// semantics it signals are not.
const membershipRetryAfterSeconds = 1

// membershipErrorResponse maps every §2.6a/§8.2/§8.2a/§12.2 refusal to
// its fixed HTTP status and machine-readable reason (§9's pinned
// table): 400 malformed/illegal-transition, 409 not-leader/leadership-
// lost/in-progress/confirmation-required, 412 capability-not-permitted,
// 425 learner-not-caught-up, 503 transiently-not-ready. reason is
// always populated so a client never has to distinguish conditions by
// status code alone — the same string is also what recordMembershipAudit
// logs (§13.4).
func membershipErrorResponse(err error) (status int, resp membershipMutateResponse) {
	resp = membershipMutateResponse{Status: "error", Error: err.Error()}
	// Overload/capacity (docs/v0.6.0-plan.md §8.3): checked first,
	// before every other case — a saturated Lane A1 control gate is a
	// distinct, always-503 outcome, never conflated with "not leader"
	// or any membership-specific refusal below.
	if rej, ok := asAdmissionRejection(err); ok {
		resp.Reason = string(rej.Reason)
		if rej.RetryAfter > 0 {
			resp.RetryAfterSeconds = int(rej.RetryAfter.Seconds())
			if resp.RetryAfterSeconds < 1 {
				resp.RetryAfterSeconds = 1
			}
		}
		return http.StatusServiceUnavailable, resp
	}
	var nle *node.NotLeaderError
	if errors.As(err, &nle) {
		resp.Reason = "not-leader"
		resp.LeaderHint = string(nle.Leader)
		return http.StatusConflict, resp
	}
	// ErrLeadershipLost (dynamic-membership plan §16's real-process
	// suite found this: a membership call proposed against a leader
	// that then loses leadership before the entry commits — a real,
	// if narrow, race an election can create around any proposal, not
	// specific to membership calls) is exactly as retryable as
	// NotLeaderError, by the same RequestID, against whoever is leader
	// next: its own message says as much ("retry by RequestID against
	// the current leader"), and §10's idempotency table is what makes
	// that safe regardless of whether the original attempt actually
	// committed. Falling through to the generic 500 below would
	// misreport an ordinary, well-understood consensus outcome as a
	// server bug, and would violate §16's own "never a 500" acceptance
	// criterion for a membership call immediately following a real
	// failover.
	if errors.Is(err, node.ErrLeadershipLost) {
		resp.Reason = "leadership-lost"
		resp.RetryAfterSeconds = membershipRetryAfterSeconds
		return http.StatusConflict, resp
	}
	switch {
	case errors.Is(err, raft.ErrConfigChangeInheritedSuffixUncommitted):
		resp.Reason = "not-ready-inherited-suffix"
		resp.RetryAfterSeconds = membershipRetryAfterSeconds
		return http.StatusServiceUnavailable, resp
	case errors.Is(err, raft.ErrConfigChangeNoCurrentTermCommit):
		resp.Reason = "not-ready-no-current-term-commit"
		resp.RetryAfterSeconds = membershipRetryAfterSeconds
		return http.StatusServiceUnavailable, resp
	case errors.Is(err, raft.ErrConfigChangeInProgress):
		resp.Reason = "change-in-progress"
		return http.StatusConflict, resp
	case errors.Is(err, raft.ErrInvalidTransition):
		resp.Reason = "invalid-transition"
		return http.StatusBadRequest, resp
	case errors.Is(err, raft.ErrLastVoterRemoval):
		resp.Reason = "last-voter-removal"
		return http.StatusBadRequest, resp
	case errors.Is(err, raft.ErrUnknownMember), errors.Is(err, raft.ErrMemberAlreadyExists):
		resp.Reason = "invalid-transition"
		return http.StatusBadRequest, resp
	case errors.Is(err, node.ErrMembershipNotPermitted):
		resp.Reason = "generation-too-low"
		return http.StatusPreconditionFailed, resp
	case errors.Is(err, node.ErrPeerGenerationTooOld):
		resp.Reason = "peer-generation-too-old"
		return http.StatusPreconditionFailed, resp
	case errors.Is(err, node.ErrNodeRemoved):
		resp.Reason = "node-removed"
		return http.StatusConflict, resp
	case errors.Is(err, fsm.ErrRequestIDConflict):
		resp.Reason = "request-id-conflict"
		return http.StatusConflict, resp
	}
	var confirmErr *node.ErrConfirmationRequired
	if errors.As(err, &confirmErr) {
		resp.Reason = "confirmation-required"
		resp.ResultingVoterCount = confirmErr.ResultingVoterCount
		return http.StatusConflict, resp
	}
	var lagErr *node.ErrLearnerNotCaughtUp
	if errors.As(err, &lagErr) {
		resp.Reason = "learner-not-caught-up"
		resp.Lag = lagErr.Lag
		resp.RetryAfterSeconds = membershipRetryAfterSeconds
		return http425TooEarly, resp
	}
	resp.Reason = "internal-error"
	return http.StatusInternalServerError, resp
}

// http425TooEarly is RFC 8470's status code; net/http does not export a
// named constant for it.
const http425TooEarly = 425

func writeMembershipMutateResponse(w http.ResponseWriter, status int, resp membershipMutateResponse) {
	if resp.RetryAfterSeconds > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", resp.RetryAfterSeconds))
	}
	writeJSON(w, status, resp)
}

// evenVoterCountWarning computes §12.1's non-blocking diagnostic: a
// resulting even voter count never provides more fault tolerance than
// the next smaller odd size while requiring a strictly larger quorum.
func evenVoterCountWarning(n *node.Node) string {
	if n.Status().VoterCount%2 == 0 && n.Status().VoterCount > 0 {
		return fmt.Sprintf("resulting voter count %d is even; an odd count provides the same fault tolerance at a smaller quorum", n.Status().VoterCount)
	}
	return ""
}

func decodeMembershipMutateRequest(r *http.Request) (membershipMutateRequest, error) {
	var req membershipMutateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, fmt.Errorf("decoding request body: %w", err)
	}
	if req.RequestID == "" {
		return req, errors.New("requestId must not be empty")
	}
	if req.NodeID == "" {
		return req, errors.New("nodeId must not be empty")
	}
	return req, nil
}

// recordMembershipAudit writes a supplementary, outcome-carrying audit
// record beyond security.wrap's own generic per-call allow/deny entry
// (dynamic-membership plan §13.4: "every mutating endpoint produces
// exactly one audit record per call, regardless of outcome" — wrap's
// own record already satisfies that baseline for every admin endpoint;
// this adds the actual resolved outcome and machine-readable reason,
// known only once the handler itself runs). A no-op when security is
// disabled (sec == nil / auth-mode none), mirroring wrap's own
// no-audit-without-auth posture.
func recordMembershipAudit(s *controlServer, r *http.Request, action, result, reason, detail string) {
	if s.sec == nil || s.sec.auditLog == nil {
		return
	}
	name, role := principalFromRequest(r)
	entry := audit.Entry{
		Timestamp: time.Now().UnixNano(),
		Principal: name,
		Role:      role,
		Action:    action,
		Endpoint:  r.URL.Path,
		Result:    result,
		Detail:    reason + " " + detail,
	}
	err := s.sec.auditLog.Append(entry)
	s.n.NoteAuditWriteResult(err)
	if err != nil && s.logger != nil {
		s.logger.Printf("membership audit write failed for %s: %v", action, err)
	}
}

func outcomeAuditResult(o fsm.Outcome) string {
	if o.Status == fsm.StatusCommitted {
		return "committed"
	}
	return "aborted"
}

// handleMembershipAdd implements POST /admin/membership/add (§3, §9).
func (s *controlServer) handleMembershipAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	req, err := decodeMembershipMutateRequest(r)
	if err != nil {
		writeMembershipMutateResponse(w, http.StatusBadRequest, membershipMutateResponse{Status: "error", Error: err.Error(), Reason: "invalid-transition"})
		return
	}
	if _, _, err := net.SplitHostPort(req.Address); err != nil {
		writeMembershipMutateResponse(w, http.StatusBadRequest, membershipMutateResponse{Status: "error", Error: "address must be a syntactically valid host:port", Reason: "invalid-transition"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	outcome, err := s.n.AddLearner(ctx, fsm.RequestID(req.RequestID), raft.NodeID(req.NodeID), req.Address)
	if err != nil {
		status, resp := membershipErrorResponse(err)
		recordMembershipAudit(s, r, "membership.add", "rejected", resp.Reason, req.NodeID)
		writeMembershipMutateResponse(w, status, resp)
		return
	}
	recordMembershipAudit(s, r, "membership.add", outcomeAuditResult(outcome), outcomeAuditResult(outcome), req.NodeID)
	writeMembershipMutateResponse(w, http.StatusOK, membershipMutateResponse{Status: outcome.Status.String()})
}

// handleMembershipPromote implements POST /admin/membership/promote
// (§3.3, §9).
func (s *controlServer) handleMembershipPromote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	req, err := decodeMembershipMutateRequest(r)
	if err != nil {
		writeMembershipMutateResponse(w, http.StatusBadRequest, membershipMutateResponse{Status: "error", Error: err.Error(), Reason: "invalid-transition"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	outcome, err := s.n.PromoteToVoter(ctx, fsm.RequestID(req.RequestID), raft.NodeID(req.NodeID), req.ConfirmVoterCount)
	if err != nil {
		status, resp := membershipErrorResponse(err)
		recordMembershipAudit(s, r, "membership.promote", "rejected", resp.Reason, req.NodeID)
		writeMembershipMutateResponse(w, status, resp)
		return
	}
	recordMembershipAudit(s, r, "membership.promote", outcomeAuditResult(outcome), outcomeAuditResult(outcome), req.NodeID)
	resp := membershipMutateResponse{Status: outcome.Status.String(), Warning: evenVoterCountWarning(s.n)}
	writeMembershipMutateResponse(w, http.StatusOK, resp)
}

// handleMembershipRemove implements POST /admin/membership/remove
// (§4, §9, §12.2).
func (s *controlServer) handleMembershipRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	req, err := decodeMembershipMutateRequest(r)
	if err != nil {
		writeMembershipMutateResponse(w, http.StatusBadRequest, membershipMutateResponse{Status: "error", Error: err.Error(), Reason: "invalid-transition"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	outcome, err := s.n.RemoveServer(ctx, fsm.RequestID(req.RequestID), raft.NodeID(req.NodeID), req.ConfirmVoterCount)
	if err != nil {
		status, resp := membershipErrorResponse(err)
		recordMembershipAudit(s, r, "membership.remove", "rejected", resp.Reason, req.NodeID)
		writeMembershipMutateResponse(w, status, resp)
		return
	}
	recordMembershipAudit(s, r, "membership.remove", outcomeAuditResult(outcome), outcomeAuditResult(outcome), req.NodeID)
	resp := membershipMutateResponse{Status: outcome.Status.String(), Warning: evenVoterCountWarning(s.n)}
	writeMembershipMutateResponse(w, http.StatusOK, resp)
}

type memberStatusJSON struct {
	ID              string `json:"id"`
	Address         string `json:"address"`
	MatchIndex      uint64 `json:"matchIndex"`
	Lag             uint64 `json:"lag,omitempty"`
	Generation      uint32 `json:"generation,omitempty"`
	GenerationKnown bool   `json:"generationKnown,omitempty"`
}

type membershipStatusResponse struct {
	ConfigIndex          uint64             `json:"configIndex"`
	CommittedConfigIndex uint64             `json:"committedConfigIndex"`
	Voters               []memberStatusJSON `json:"voters"`
	Learners             []memberStatusJSON `json:"learners"`
	InProgress           bool               `json:"inProgress"`
	ChangesReady         bool               `json:"changesReady"`
	NotReadyReason       string             `json:"notReadyReason,omitempty"`
	Error                string             `json:"error,omitempty"`
}

func toMemberStatusJSON(ms node.MemberStatus) memberStatusJSON {
	return memberStatusJSON{
		ID: string(ms.ID), Address: ms.Address, MatchIndex: uint64(ms.MatchIndex),
		Lag: ms.Lag, Generation: ms.Generation, GenerationKnown: ms.GenerationKnown,
	}
}

// handleMembershipStatus implements GET /admin/membership/status (§9,
// §14). Read-only: never proposes anything, never mutates anything.
func (s *controlServer) handleMembershipStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	res, err := s.n.MembershipStatus(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, membershipStatusResponse{Error: err.Error()})
		return
	}
	voters := make([]memberStatusJSON, 0, len(res.Voters))
	for _, m := range res.Voters {
		voters = append(voters, toMemberStatusJSON(m))
	}
	learners := make([]memberStatusJSON, 0, len(res.Learners))
	for _, m := range res.Learners {
		learners = append(learners, toMemberStatusJSON(m))
	}
	writeJSON(w, http.StatusOK, membershipStatusResponse{
		ConfigIndex:          uint64(res.ConfigIndex),
		CommittedConfigIndex: uint64(res.CommittedConfigIndex),
		Voters:               voters,
		Learners:             learners,
		InProgress:           res.InProgress,
		ChangesReady:         res.ChangesReady,
		NotReadyReason:       res.NotReadyReason,
	})
}
