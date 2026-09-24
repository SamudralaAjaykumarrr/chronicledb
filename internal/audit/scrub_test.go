package audit

import (
	"os"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
)

func TestAuditScrub_CleanLogZeroFindings(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	for i := 0; i < 5; i++ {
		if err := l.Append(Entry{Principal: "alice", Action: "propose", Result: "allow"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	l.Close()

	findings, stats, err := Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("clean log: findings = %+v, want none", findings)
	}
	if stats.RecordsChecked != 5 {
		t.Fatalf("RecordsChecked = %d, want 5", stats.RecordsChecked)
	}
}

func TestAuditScrub_TamperedRecordDetected(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	for i := 0; i < 5; i++ {
		if err := l.Append(Entry{Principal: "alice", Action: "propose", Result: "allow"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	l.Close()

	path := onlySegmentFile(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[headerSize+hashSize+2] ^= 0xFF // inside the second record's payload
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	findings, _, err := Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) == 0 {
		t.Fatalf("tampered audit log: findings = %+v, want at least one audit_chain_broken", findings)
	}
	for _, f := range findings {
		if f.Kind != scrub.FindingAuditChainBroken {
			t.Errorf("unexpected finding kind %q, want audit_chain_broken", f.Kind)
		}
	}
}

func TestAuditScrub_TornTailInOpenSegmentIsNotAFinding(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir)
	for i := 0; i < 5; i++ {
		if err := l.Append(Entry{Principal: "alice", Action: "propose", Result: "allow"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	l.Close()

	path := onlySegmentFile(t, dir)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	findings, _, err := Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("torn tail in the only (current) segment: findings = %+v, want none", findings)
	}
}
