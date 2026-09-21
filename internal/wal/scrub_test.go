package wal_test

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// dirHash returns a stable hash of every regular file's path and content
// under dir, recursively — used to prove scrub never modifies anything
// it reads (SL-10, SCRUB NON-DESTRUCTIVE §27.10).
func dirHash(t *testing.T, dir string) string {
	t.Helper()
	var paths []string
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, _ := filepath.Rel(dir, p)
		fmt.Fprintf(h, "path=%s\x00", rel)
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("opening %s: %v", p, err)
		}
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			t.Fatalf("hashing %s: %v", p, err)
		}
		f.Close()
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// writeMultiSegmentWAL writes n small log entries with a tiny
// SegmentMaxSize, forcing several segment rotations, and returns the dir
// plus the ascending segment ids actually created.
func writeMultiSegmentWAL(t *testing.T, n int) (dir string, ids []uint64) {
	t.Helper()
	dir = t.TempDir()
	w, _, err := wal.Open(dir, wal.Options{SegmentMaxSize: 200})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := w.AppendLogEntry([]byte("payload-of-some-length-to-force-rotation")); err != nil {
			t.Fatalf("AppendLogEntry %d: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ids, err = storage.ListSegmentIDs(dir)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 3 {
		t.Fatalf("test setup needs >= 3 segments to exercise \"earlier vs current\", got %d", len(ids))
	}
	return dir, ids
}

func TestScrub_CleanTreeZeroFindings(t *testing.T) {
	dir, _ := writeMultiSegmentWAL(t, 30)
	findings, stats, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("clean tree: findings = %+v, want none", findings)
	}
	if stats.SegmentsChecked < 3 || stats.FramesChecked == 0 {
		t.Fatalf("stats = %+v, want several segments and frames checked", stats)
	}
}

func TestScrub_TornTailInCurrentSegmentIsNotAFinding(t *testing.T) {
	dir, ids := writeMultiSegmentWAL(t, 30)
	current := ids[len(ids)-1]
	path := storage.SegmentPath(dir, current)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil { // chop off the last few bytes: a torn final frame
		t.Fatalf("truncate: %v", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("torn tail in current segment: findings = %+v, want none (legal per docs/wal.md §6)", findings)
	}
}

func TestScrub_TornTailInEarlierSegmentIsAFinding(t *testing.T) {
	dir, ids := writeMultiSegmentWAL(t, 30)
	earlier := ids[0]
	path := storage.SegmentPath(dir, earlier)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	// A torn tail in a non-open segment is corruption in its own right
	// (bad_framing); the entries that truncation discarded also produce
	// a real, honest index gap once scanning resumes in the next
	// segment (index_out_of_order) — both are legitimate, independent
	// symptoms of the same injected corruption, so this only asserts
	// the primary one is present, not that it is the sole finding.
	found := false
	for _, f := range findings {
		if f.File == path && f.Kind == scrub.FindingBadFraming {
			found = true
		}
	}
	if !found {
		t.Fatalf("torn tail in an earlier (non-open) segment: findings = %+v, want a bad_framing finding for %s", findings, path)
	}
}

func TestScrub_ChecksumCorruptionDetected(t *testing.T) {
	dir, ids := writeMultiSegmentWAL(t, 30)
	earlier := ids[0]
	path := storage.SegmentPath(dir, earlier)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Flip a byte well inside the first frame's payload region (past the
	// 14-byte header), leaving the frame's own declared length/version
	// intact so this trips the checksum, not framing.
	data[20] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	findings, _, err := wal.Scrub(dir, nil)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if len(findings) != 1 || findings[0].Kind != scrub.FindingBadChecksum {
		t.Fatalf("checksum corruption: findings = %+v, want exactly one bad_checksum", findings)
	}
}

func TestScrub_DirectoryUnchangedBeforeAndAfter(t *testing.T) {
	dir, _ := writeMultiSegmentWAL(t, 30)
	before := dirHash(t, dir)
	if _, _, err := wal.Scrub(dir, nil); err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	after := dirHash(t, dir)
	if before != after {
		t.Fatalf("scrub modified the directory: before=%s after=%s", before, after)
	}
}
