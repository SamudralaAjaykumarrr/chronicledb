// This file implements the control-plane's Backup / Disaster Recovery
// surface (docs/enterprise-v1-plan.md §6): the admin-gated /admin/backup
// HTTP endpoint that triggers a live backup of this running node's own
// currently-durable state, and the -restore-from/-restore-until/
// -force-overwrite startup flags main.go checks before ever calling
// node.Open — restore only ever targets a not-yet-opened, clean (or
// explicitly force-cleared) data directory, never a live node's own
// in-use one (docs/enterprise-v1-plan.md §6 "DESTRUCTIVE RESTORE
// ISOLATION").
package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
)

type backupResponse struct {
	Manifest backup.Manifest `json:"manifest,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// handleBackup triggers Node.Backup against this node's own currently-
// durable committed state (docs/enterprise-v1-plan.md §6). Query
// parameters: "dir" (required, an absolute local-filesystem path this
// process can write to — V1 ships local-filesystem-path backup/restore
// only, per that section's explicit non-goals) and "continuous"
// ("true" for continuous WAL archiving up to this moment, RPO near
// zero; anything else, including omitted, for a snapshot-only backup,
// RPO bounded by "time since this node's last snapshot boundary" — see
// Node.Backup's doc comment for the two schedules' tradeoff).
func (s *controlServer) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSON(w, http.StatusBadRequest, backupResponse{Error: "dir query parameter is required"})
		return
	}
	continuous := r.URL.Query().Get("continuous") == "true"

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	m, err := s.n.Backup(ctx, dir, continuous, s.clusterID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, backupResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, backupResponse{Manifest: m})
}

// runRestore implements the -restore-from preflight (main.go, before
// node.Open is ever called): validates and restores backupDir into
// dataDir, honoring -restore-until (a PITR boundary; "" means
// everything the backup includes) and -force-overwrite (required to
// restore over a non-clean dataDir — docs/enterprise-v1-plan.md §6
// "DESTRUCTIVE RESTORE ISOLATION"). Returns the restore.RestoreResult
// for the caller to log, or a non-nil error causing main to exit
// non-zero without ever starting a node against a possibly-incomplete
// data directory.
func runRestore(backupDir, dataDir, untilFlag string, force bool) (backup.RestoreResult, error) {
	until := backup.UntilLatest
	if untilFlag != "" {
		var parsed uint64
		if _, err := fmt.Sscanf(untilFlag, "%d", &parsed); err != nil {
			return backup.RestoreResult{}, fmt.Errorf("invalid -restore-until %q: must be a non-negative log index: %w", untilFlag, err)
		}
		until = parsed
	}
	return backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: until, Force: force})
}

// recordRestoreAudit appends one audit record for a completed
// -restore-from action, mirroring the HTTP admin middleware chain's
// exactly-one-record-per-administrative-action discipline (AUDIT
// COMPLETENESS) even though a CLI-triggered restore has no HTTP-
// authenticated principal to attribute it to: the operator's shell-level
// access to invoke the binary with these flags is itself the
// authorization boundary here (the same reasoning
// -enable-fault-endpoint already relies on), recorded as a fixed
// "cli-operator" principal so the audit trail is honest about what it
// actually knows rather than inventing a specific identity it cannot
// verify.
func recordRestoreAudit(auditDir, backupDir, dataDir string, force bool, res backup.RestoreResult) error {
	log, err := audit.Open(auditDir)
	if err != nil {
		return fmt.Errorf("opening audit log at %s: %w", auditDir, err)
	}
	defer log.Close()

	entry := audit.Entry{
		Timestamp: time.Now().UnixNano(),
		Principal: "cli-operator",
		Role:      "admin",
		Action:    "restore",
		Endpoint:  "cli:-restore-from",
		Result:    "allow",
		Detail: fmt.Sprintf("backupDir=%s dataDir=%s force=%v restoredUntilIndex=%d manifestRange=[%d,%d]",
			backupDir, dataDir, force, res.RestoredUntilIndex, res.Manifest.LastIncludedIndex, res.Manifest.WALUntilIndex),
	}
	if err := log.Append(entry); err != nil {
		return err
	}
	return nil
}
