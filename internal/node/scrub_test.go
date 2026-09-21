package node

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/audit"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
)

// openScrubTestNode opens a single-node cluster with a real,
// independently-opened audit log wired in (Config.AuditLog/AuditLogDir),
// so a scrub exercises all three subsystems (WAL, snapshot, audit) end
// to end.
func openScrubTestNode(t *testing.T, snapshotThreshold uint64) (*Node, string) {
	t.Helper()
	auditDir := t.TempDir()
	auditLog, err := audit.Open(auditDir)
	if err != nil {
		t.Fatalf("audit.Open: %v", err)
	}
	t.Cleanup(func() { auditLog.Close() })

	addrs := freeAddrs(t, 1)
	n, err := Open(Config{
		ID:                         "n1",
		Peers:                      []raft.NodeID{"n1"},
		ListenAddr:                 addrs[0],
		DataDir:                    t.TempDir(),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
		SnapshotThreshold:          snapshotThreshold,
		AuditLog:                   auditLog,
		AuditLogDir:                auditDir,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(n.Stop)
	awaitCondition(t, 2*time.Second, "node became leader", func() bool {
		return n.Status().Role == raft.Leader
	})
	return n, auditDir
}

func TestNodeScrub_CleanNodeZeroFindings(t *testing.T) {
	n, _ := openScrubTestNode(t, 2)
	for i := 0; i < 5; i++ {
		if _, err := propose(t, n, cmd("scrub-clean-"+string(rune('a'+i)), 0, 0, "k", "v"), 5*time.Second); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	awaitCondition(t, 2*time.Second, "at least one snapshot created", func() bool {
		return n.Metrics().SnapshotsCreatedTotal > 0
	})

	report, err := n.Scrub(context.Background(), ScrubOptions{})
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(report.Findings) != 0 {
		t.Fatalf("clean node: findings = %+v, want none", report.Findings)
	}
	if report.WALSegmentsChecked == 0 || report.WALFramesChecked == 0 || report.SnapshotsChecked == 0 {
		t.Fatalf("report = %+v, want nonzero WAL/snapshot counts", report)
	}
	if report.DurationMs < 0 {
		t.Fatalf("DurationMs = %d, want >= 0", report.DurationMs)
	}

	cached, ok := n.LastScrubReport()
	if !ok {
		t.Fatal("LastScrubReport: ok = false after a completed Scrub call")
	}
	if len(cached.Findings) != len(report.Findings) {
		t.Fatalf("LastScrubReport findings = %+v, want it to match the just-returned report", cached.Findings)
	}
}

func TestNodeScrub_CorruptionDetectedAcrossSubsystems(t *testing.T) {
	n, _ := openScrubTestNode(t, 2)
	for i := 0; i < 5; i++ {
		if _, err := propose(t, n, cmd("scrub-corrupt-"+string(rune('a'+i)), 0, 0, "k", "v"), 5*time.Second); err != nil {
			t.Fatalf("propose: %v", err)
		}
	}
	awaitCondition(t, 2*time.Second, "at least one snapshot created", func() bool {
		return n.Metrics().SnapshotsCreatedTotal > 0
	})

	// Corrupt the WAL's own current segment content (not the tail, to
	// avoid it looking like a legal torn tail): flip a byte well inside
	// an early frame.
	segPath := onlyOneSegmentPath(t, n.cfg.DataDir)
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) < 40 {
		t.Fatalf("segment too small to safely corrupt mid-frame: %d bytes", len(data))
	}
	data[20] ^= 0xFF
	if err := os.WriteFile(segPath, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	report, err := n.Scrub(context.Background(), ScrubOptions{})
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(report.Findings) == 0 {
		t.Fatal("corrupted WAL segment: report has zero findings, want at least one")
	}
	found := false
	for _, f := range report.Findings {
		if f.Kind == scrub.FindingBadChecksum {
			found = true
		}
	}
	if !found {
		t.Fatalf("findings = %+v, want a bad_checksum finding", report.Findings)
	}
}

func TestNodeScrub_SecondConcurrentScrubRefused(t *testing.T) {
	n, _ := openScrubTestNode(t, 100000) // no snapshot needed for this test
	if _, err := propose(t, n, cmd("scrub-conc-1", 0, 0, "k", "v"), 5*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Hold the scrub slot directly (bypassing Node.Scrub) to deterministically
	// simulate "a scrub is already running" without a timing-dependent race
	// between two real goroutines.
	release, err := acquireSingleSlot(context.Background(), n.admission.scrubSlot)
	if err != nil {
		t.Fatalf("acquireSingleSlot: %v", err)
	}
	defer release()

	_, err = n.Scrub(context.Background(), ScrubOptions{})
	if err == nil {
		t.Fatal("expected the second concurrent Scrub to be rejected, it succeeded")
	}
	var rej *admission.RejectedError
	if !errors.As(err, &rej) || rej.Reason != admission.ReasonAdminOperationInProgress {
		t.Fatalf("got err=%v, want RejectedError{Reason: admin_operation_in_progress}", err)
	}
}

// onlyOneSegmentPath returns the path of the (test setup guarantees:
// exactly one, since no rotation was forced) WAL segment file in dir.
func onlyOneSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".seg" {
			return dir + "/" + e.Name()
		}
	}
	t.Fatalf("no .seg file found in %s", dir)
	return ""
}
