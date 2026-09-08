// Microbenchmarks measuring Export/Restore cost for the two backup
// schedules docs/enterprise-v1-plan.md §6's acceptance criteria require
// published RPO/RTO numbers for: "snapshot-only" (ExportOptions.UntilIndex
// == the base boundary, no trailing WAL suffix) and "continuous WAL
// archiving" (ExportOptions.UntilIndex == UntilLatest, every currently-
// durable WAL entry beyond the base boundary included). The measured
// numbers here are transcribed into docs/backup.md's RPO/RTO section —
// see that doc for the published, human-readable figures and what they
// mean operationally.
//
// Run: go test ./internal/backup/... -run '^$' -bench . -benchmem
package backup_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// benchSource builds a source WAL with baseKeys committed entries, plus
// suffixKeys further committed entries beyond a base snapshot boundary
// at index baseKeys — i.e. a node that snapshotted once, then kept
// taking writes.
func benchSource(b *testing.B, dir string, baseKeys, suffixKeys int) backup.Source {
	b.Helper()
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		b.Fatalf("opening source WAL: %v", err)
	}
	b.Cleanup(func() { w.Close() })

	store := mvcc.NewStore()
	baseFSM := fsm.New(store)
	total := baseKeys + suffixKeys
	for i := 1; i <= total; i++ {
		cmd := fsm.CommitTxnCommand{
			RequestID: fsm.RequestID(fmt.Sprintf("r%d", i)),
			TxnID:     uint64(i),
			Mutations: []mvcc.Mutation{{Key: fmt.Sprintf("key-%08d", i), Value: []byte("some-representative-value-bytes")}},
		}
		idx, err := w.AppendLogEntry(fsm.EncodeCommitTxn(cmd))
		if err != nil {
			b.Fatalf("appending entry %d: %v", i, err)
		}
		if i <= baseKeys {
			if _, err := baseFSM.Apply(uint64(idx), cmd); err != nil {
				b.Fatalf("applying base entry %d: %v", i, err)
			}
		}
	}
	if err := w.Sync(); err != nil {
		b.Fatalf("syncing source WAL: %v", err)
	}
	return backup.Source{
		BaseMeta: snapshot.Meta{LastIncludedIndex: uint64(baseKeys), LastIncludedTerm: 1},
		BaseFSM:  baseFSM,
		WAL:      w,
	}
}

// BenchmarkExport measures Export's wall-clock cost — an RTO-adjacent,
// and directly RPO-relevant (how expensive it is to run a backup often
// enough to bound RPO tightly), metric — for both schedules at a
// representative size.
func BenchmarkExport(b *testing.B) {
	const baseKeys = 1000
	for _, suffix := range []int{0, 1000} {
		name := "snapshot-only"
		if suffix > 0 {
			name = "continuous-wal-archiving"
		}
		b.Run(name, func(b *testing.B) {
			srcDir := b.TempDir()
			src := benchSource(b, srcDir, baseKeys, suffix)
			until := src.BaseMeta.LastIncludedIndex
			if suffix > 0 {
				until = backup.UntilLatest
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				outDir := filepath.Join(b.TempDir(), fmt.Sprintf("backup-%d", i))
				if _, err := backup.Export(src, outDir, backup.ExportOptions{UntilIndex: until}); err != nil {
					b.Fatalf("Export: %v", err)
				}
			}
		})
	}
}

// BenchmarkRestore measures Restore's wall-clock cost — the direct RTO
// number: manifest validation + snapshot load + WAL suffix replay
// (docs/enterprise-v1-plan.md §6's RTO model).
func BenchmarkRestore(b *testing.B) {
	const baseKeys = 1000
	for _, suffix := range []int{0, 1000} {
		name := "snapshot-only"
		if suffix > 0 {
			name = "continuous-wal-archiving"
		}
		b.Run(name, func(b *testing.B) {
			srcDir := b.TempDir()
			src := benchSource(b, srcDir, baseKeys, suffix)
			until := src.BaseMeta.LastIncludedIndex
			if suffix > 0 {
				until = backup.UntilLatest
			}
			backupDir := filepath.Join(b.TempDir(), "backup")
			if _, err := backup.Export(src, backupDir, backup.ExportOptions{UntilIndex: until}); err != nil {
				b.Fatalf("Export: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dataDir := filepath.Join(b.TempDir(), fmt.Sprintf("restored-%d", i))
				if _, err := backup.Restore(backupDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
					b.Fatalf("Restore: %v", err)
				}
			}
		})
	}
}
