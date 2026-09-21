//go:build unix

package storage

import "testing"

// TestDiskUsageRealFilesystem is docs/v0.6.0-plan.md §33 slice 3's
// "DiskUsage tested on Linux" exit criterion: a real syscall.Statfs
// call against a real temp directory, asserting sane, self-consistent
// values (never a fabricated or mocked filesystem).
func TestDiskUsageRealFilesystem(t *testing.T) {
	dir := t.TempDir()
	free, total, err := DiskUsage(dir)
	if err != nil {
		t.Fatalf("DiskUsage(%q): %v", dir, err)
	}
	if total == 0 {
		t.Errorf("DiskUsage(%q): totalBytes = 0, want > 0", dir)
	}
	if free > total {
		t.Errorf("DiskUsage(%q): freeBytes (%d) > totalBytes (%d)", dir, free, total)
	}
}

func TestDiskUsageNonexistentPath(t *testing.T) {
	if _, _, err := DiskUsage("/this/path/should/not/exist/chronicledb-test-xyz"); err == nil {
		t.Error("DiskUsage on a nonexistent path: expected an error, got none")
	}
}
