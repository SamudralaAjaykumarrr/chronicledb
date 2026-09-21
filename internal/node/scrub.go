package node

import (
	"context"
	"path/filepath"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// ScrubOptions configures a Scrub call. Empty today — reserved because
// Scrub's own signature (docs/v0.6.0-plan.md §21.1) already takes one;
// no per-call knob is named anywhere else in that section.
type ScrubOptions struct{}

// ScrubReport is Node.Scrub's result (docs/v0.6.0-plan.md §21.1's exact
// shape).
type ScrubReport struct {
	StartedAt           time.Time       `json:"startedAt"`
	DurationMs          int64           `json:"durationMs"`
	BytesRead           int64           `json:"bytesRead"`
	WALSegmentsChecked  int             `json:"walSegmentsChecked"`
	WALFramesChecked    int             `json:"walFramesChecked"`
	SnapshotsChecked    int             `json:"snapshotsChecked"`
	AuditRecordsChecked int             `json:"auditRecordsChecked"`
	Findings            []scrub.Finding `json:"findings"`
	// Truncated is true iff ctx was canceled/expired before every
	// subsystem finished — the report above still reflects whatever was
	// actually checked before that point, never fabricated.
	Truncated bool `json:"truncated"`
}

// scrubPacer implements -scrub-bytes-per-sec (docs/v0.6.0-plan.md
// §21.4) as a simple proportional sleep after each file read: coarse
// (per-file, not sub-file-chunked, since internal/wal.Scrub/internal/
// snapshot.Scrub/internal/audit.Scrub each read one whole file at a
// time) but real — "the smallest technically real design"
// (docs/vision.md) for a background maintenance rate cap, not a
// request-latency-sensitive path.
type scrubPacer struct{ bytesPerSec int64 }

func (p scrubPacer) onBytesRead(n int) {
	if p.bytesPerSec <= 0 || n <= 0 {
		return
	}
	d := time.Duration(float64(n) / float64(p.bytesPerSec) * float64(time.Second))
	time.Sleep(d)
}

// Scrub verifies this node's own retained WAL segments, snapshot files,
// and audit-log hash chain, read-only, reporting every finding rather
// than repairing anything (docs/v0.6.0-plan.md §21). It runs entirely on
// the calling goroutine — never dispatched onto run's event-loop
// goroutine the way Propose/BeginReadIndex/Backup are (§21.4: "It never
// takes FSM.mu and never touches Core") — reading only the immutable
// directory paths in n.cfg, never n.walog/n.snapMgr/n.fsmachine
// themselves, so a scrub can run concurrently with ordinary traffic and
// with a concurrent snapshot/compaction cycle without any synchronization
// against it.
//
// Bounded by the shared Lane A2 maintenance gate plus a dedicated
// single-slot (§3.2a): a scrub and a backup may run concurrently with
// each other, but a second concurrent scrub is refused outright
// (admin_operation_in_progress) rather than queued.
func (n *Node) Scrub(ctx context.Context, opts ScrubOptions) (ScrubReport, error) {
	release, err := n.admission.maintenance.Acquire(ctx)
	if err != nil {
		return ScrubReport{}, err
	}
	defer release()
	releaseSlot, err := acquireSingleSlot(ctx, n.admission.scrubSlot)
	if err != nil {
		return ScrubReport{}, err
	}
	defer releaseSlot()

	report := ScrubReport{StartedAt: time.Now()}
	pacer := scrubPacer{bytesPerSec: n.cfg.ScrubBytesPerSec}

	walFindings, walStats, err := wal.Scrub(n.cfg.DataDir, pacer.onBytesRead)
	if err != nil {
		return report, err
	}
	report.Findings = append(report.Findings, walFindings...)
	report.WALSegmentsChecked = walStats.SegmentsChecked
	report.WALFramesChecked = walStats.FramesChecked
	report.BytesRead += walStats.BytesRead

	if ctx.Err() != nil {
		report.Truncated = true
		n.finishScrub(&report)
		return report, nil
	}

	snapDir := filepath.Join(n.cfg.DataDir, "snapshot")
	snapFindings, snapStats, err := snapshot.Scrub(snapDir, pacer.onBytesRead)
	if err != nil {
		n.finishScrub(&report)
		return report, err
	}
	report.Findings = append(report.Findings, snapFindings...)
	report.SnapshotsChecked = snapStats.SnapshotsChecked
	report.BytesRead += snapStats.BytesRead

	if ctx.Err() != nil {
		report.Truncated = true
		n.finishScrub(&report)
		return report, nil
	}

	if n.cfg.AuditLogDir != "" {
		auditFindings, auditStats, err := audit.Scrub(n.cfg.AuditLogDir, pacer.onBytesRead)
		if err != nil {
			n.finishScrub(&report)
			return report, err
		}
		report.Findings = append(report.Findings, auditFindings...)
		report.AuditRecordsChecked = auditStats.RecordsChecked
		report.BytesRead += auditStats.BytesRead
	}

	n.finishScrub(&report)
	return report, nil
}

// finishScrub finalizes report's duration, updates §25's scrub metrics,
// and caches the report for LastScrubReport/GET /admin/storage/status.
func (n *Node) finishScrub(report *ScrubReport) {
	report.DurationMs = time.Since(report.StartedAt).Milliseconds()
	n.metrics.ScrubRunsTotal.Inc()
	n.metrics.ScrubFindingsTotal.Add(uint64(len(report.Findings)))
	n.metrics.ScrubLastDurationMillis.Set(report.DurationMs)

	n.scrubMu.Lock()
	cp := *report
	n.lastScrubReport = &cp
	n.scrubMu.Unlock()
}

// LastScrubReport returns the most recent Scrub call's report and true,
// or the zero value and false if Scrub has never been called on this
// node (process lifetime only — never persisted, docs/v0.6.0-plan.md
// §21.1's GET /admin/storage/status). Safe from any goroutine.
func (n *Node) LastScrubReport() (ScrubReport, bool) {
	n.scrubMu.Lock()
	defer n.scrubMu.Unlock()
	if n.lastScrubReport == nil {
		return ScrubReport{}, false
	}
	return *n.lastScrubReport, true
}
