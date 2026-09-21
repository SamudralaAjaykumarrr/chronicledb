package snapshot

import (
	"os"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
)

func TestSnapshotScrub_CleanTreeZeroFindings(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f, FormatVersion); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := m.Create(Meta{LastIncludedIndex: 7, LastIncludedTerm: 2}, f, FormatVersion); err != nil {
		t.Fatalf("Create: %v", err)
	}

	findings, stats, err := Scrub(m.dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("clean tree: findings = %+v, want none", findings)
	}
	if stats.SnapshotsChecked != 2 {
		t.Fatalf("SnapshotsChecked = %d, want 2", stats.SnapshotsChecked)
	}
}

func TestSnapshotScrub_ChecksumCorruptionDetected(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f, FormatVersion); err != nil {
		t.Fatalf("Create: %v", err)
	}
	data, err := os.ReadFile(m.path(3))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[len(data)-1] ^= 0xFF // flip a byte inside the trailing checksum
	if err := os.WriteFile(m.path(3), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	findings, _, err := Scrub(m.dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 1 || findings[0].Kind != scrub.FindingSnapshotDecodeFailed {
		t.Fatalf("corrupted snapshot: findings = %+v, want exactly one snapshot_decode_failed", findings)
	}
}

func TestSnapshotScrub_FilenameIndexMismatchDetected(t *testing.T) {
	m := newManager(t)
	f := buildFSM(t)
	if _, err := m.Create(Meta{LastIncludedIndex: 3, LastIncludedTerm: 1}, f, FormatVersion); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Rename the file to a different index than what its own encoded
	// Meta.LastIncludedIndex says — Decode alone cannot catch this,
	// since it never sees the filename.
	if err := os.Rename(m.path(3), m.path(99)); err != nil {
		t.Fatalf("rename: %v", err)
	}

	findings, _, err := Scrub(m.dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 1 || findings[0].Kind != scrub.FindingSnapshotDecodeFailed {
		t.Fatalf("filename/content index mismatch: findings = %+v, want exactly one snapshot_decode_failed", findings)
	}
}
