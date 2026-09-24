// This file implements the control-plane's storage-integrity surface
// (docs/v0.6.0-plan.md §21.1): the admin-gated POST /admin/storage/scrub
// endpoint and the operator+ read-only GET /admin/storage/status
// endpoint reporting the last scrub's result.
package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/node"
)

type scrubResponse struct {
	Report *node.ScrubReport `json:"report,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// handleStorageScrub triggers a full scrub of this node's own retained
// WAL segments, snapshot files, and audit-log hash chain
// (docs/v0.6.0-plan.md §21.1). Admin role only (authz.EndpointStorageScrub),
// audited exactly like every other administrative action.
func (s *controlServer) handleStorageScrub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	report, err := s.n.Scrub(ctx, node.ScrubOptions{})
	if err != nil {
		if rej, ok := asAdmissionRejection(err); ok {
			writeAdmissionRejection(w, rej)
			return
		}
		recordStorageAudit(s, r, "admin.storage.scrub", "deny_unauthorized", "", err.Error())
		writeJSON(w, http.StatusInternalServerError, scrubResponse{Error: err.Error()})
		return
	}
	recordStorageAudit(s, r, "admin.storage.scrub", "allow", "", scrubAuditDetail(report))
	writeJSON(w, http.StatusOK, scrubResponse{Report: &report})
}

// handleStorageStatus reports the last scrub's result (or that none has
// ever run) — read-only, operator+ (authz.EndpointStorageStatus).
func (s *controlServer) handleStorageStatus(w http.ResponseWriter, r *http.Request) {
	report, ok := s.n.LastScrubReport()
	if !ok {
		writeJSON(w, http.StatusOK, scrubResponse{})
		return
	}
	writeJSON(w, http.StatusOK, scrubResponse{Report: &report})
}

func scrubAuditDetail(report node.ScrubReport) string {
	return fmt.Sprintf("%d findings", len(report.Findings))
}

// recordStorageAudit mirrors recordMembershipAudit's shape exactly
// (membership.go) — every administrative action gets one audit record
// regardless of outcome, and its Append result feeds this node's own
// §20.2 storage-health tracking, since both write through the same
// shared audit chain.
func recordStorageAudit(s *controlServer, r *http.Request, action, result, reason, detail string) {
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
		s.logger.Printf("storage audit write failed for %s: %v", action, err)
	}
}
