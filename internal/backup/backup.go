// Package backup implements ChronicleDB's Backup / Disaster Recovery /
// PITR capability (docs/enterprise-v1-plan.md §6, ADR-0016). A backup is
// a self-describing, versioned export of a consistent snapshot boundary
// plus the WAL log suffix needed to reach a chosen point in time — built
// entirely on internal/snapshot and internal/wal's own existing,
// validated encode/decode/durability mechanisms (never a second,
// independently-evolving on-disk format for "database backup" versus
// "Raft snapshot").
//
// This package deliberately does not know about internal/node or
// internal/raft: Export/Restore both operate on a snapshot.Meta/fsm.FSM
// pair and a *wal.WAL, exactly the same shape internal/node itself
// already manages, so a live node's own already-durable state can be
// exported without this package needing any Raft-specific knowledge
// (docs/architecture.md §5's dependency layering).
//
// A backup is architecturally distinct from a Raft snapshot/compaction
// cycle even though it reuses the same encoding: creating a backup never
// mutates the source's own retained WAL/snapshot state (no compaction,
// no pointer update) — it only reads what is already durable and copies
// it, through the same validated encode paths, into a new, independent
// directory (docs/enterprise-v1-plan.md §6: "backup semantics clearly
// separated from Raft snapshot/compaction semantics").
package backup

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/version"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// UntilLatest, passed as ExportOptions.UntilIndex or
// RestoreOptions.UntilIndex, means "as much as is currently available"
// — every WAL entry present in the source at export time, or, for
// Restore, every entry this backup captured (manifest.WALUntilIndex).
// It is a distinct sentinel from 0 specifically because 0 is itself a
// legitimate boundary value (a backup taken before any entry has ever
// been committed) — see Export/Restore's doc comments.
const UntilLatest = ^uint64(0)

// Source is what Export needs from a durable ChronicleDB history: a
// validated base snapshot boundary and content, plus the WAL to read the
// trailing suffix from. Both a live node (internal/node.Node.Backup) and
// an offline CLI backup (reading a stopped node's data directory)
// construct this the same way — from internal/snapshot.Manager.Load (or
// an empty, never-snapshotted boundary) and internal/wal.Open
// respectively — so Export itself never needs to know which case it is
// in.
type Source struct {
	BaseMeta snapshot.Meta
	BaseFSM  *fsm.FSM
	WAL      *wal.WAL
}

// ExportOptions configures Export.
type ExportOptions struct {
	// UntilIndex bounds how much of the source WAL's suffix beyond
	// BaseMeta.LastIncludedIndex to include. Passing
	// BaseMeta.LastIncludedIndex itself produces a snapshot-only backup
	// (RPO bounded by "time since last snapshot boundary"); passing
	// UntilLatest includes everything currently present in the source
	// WAL (continuous archiving, RPO bounded only by "time since this
	// export ran"); any other value must be >= BaseMeta.LastIncludedIndex
	// and produces a backup whose PITR range Restore can later select
	// within.
	UntilIndex uint64
	// ClusterID is recorded in the manifest for operator diagnostics
	// only (see Manifest.ClusterID's doc comment) — never validated by
	// Restore.
	ClusterID string
}

// Export durably writes a complete, self-contained, checksummed backup
// of src to outDir (docs/enterprise-v1-plan.md §6 "Architecture"):
// outDir/snapshot holds the base snapshot (via internal/snapshot.Manager,
// unmodified), outDir/wal holds a freshly-written WAL containing exactly
// the requested suffix (via internal/wal, unmodified — every appended
// record is copied through AppendLogEntry using the exact same encode
// path a live node's own log uses, never a raw byte copy of source
// segment files, so the manifest's own WAL segment layout is entirely
// this package's own, independent of the source's segment rotation
// history), and outDir/manifest.json is written last, atomically, only
// once every byte it references is already durable (BACKUP INTEGRITY;
// docs/enterprise-v1-plan.md §6 "Failure semantics": "a crash mid-export
// ... a manifest only exists once the backup is complete and valid").
//
// A crash or error at any point before the final writeManifest call
// leaves outDir with no manifest.json at all — Restore (and readManifest)
// always refuse such a directory outright, never partially trusting it.
func Export(src Source, outDir string, opts ExportOptions) (Manifest, error) {
	if opts.UntilIndex != UntilLatest && opts.UntilIndex < src.BaseMeta.LastIncludedIndex {
		return Manifest{}, fmt.Errorf("backup: ExportOptions.UntilIndex %d is before the base snapshot boundary %d", opts.UntilIndex, src.BaseMeta.LastIncludedIndex)
	}
	if err := storage.EnsureDir(outDir); err != nil {
		return Manifest{}, err
	}

	snapDir := filepath.Join(outDir, "snapshot")
	snapMgr, err := snapshot.NewManager(snapDir)
	if err != nil {
		return Manifest{}, fmt.Errorf("backup: preparing snapshot directory: %w", err)
	}
	if _, err := snapMgr.Create(src.BaseMeta, src.BaseFSM); err != nil {
		return Manifest{}, fmt.Errorf("backup: writing base snapshot: %w", err)
	}
	snapFileName, snapBytes, err := readSoleSnapshotFile(snapDir)
	if err != nil {
		return Manifest{}, err
	}

	walDir := filepath.Join(outDir, "wal")
	bw, err := newBaseWAL(walDir, src.BaseMeta.LastIncludedIndex)
	if err != nil {
		return Manifest{}, err
	}
	walUntil, err := copyWALSuffix(src.WAL, bw, src.BaseMeta.LastIncludedIndex, opts.UntilIndex)
	if err != nil {
		bw.Close()
		return Manifest{}, err
	}
	if err := bw.Sync(); err != nil {
		bw.Close()
		return Manifest{}, fmt.Errorf("backup: syncing backup WAL: %w", err)
	}
	if err := bw.Close(); err != nil {
		return Manifest{}, fmt.Errorf("backup: closing backup WAL: %w", err)
	}

	segs, err := describeWALSegments(walDir)
	if err != nil {
		return Manifest{}, err
	}

	m := Manifest{
		FormatVersion:      FormatVersion,
		ChronicleDBVersion: version.Version,
		ClusterID:          opts.ClusterID,
		LastIncludedIndex:  src.BaseMeta.LastIncludedIndex,
		LastIncludedTerm:   src.BaseMeta.LastIncludedTerm,
		SnapshotFile:       snapFileName,
		SnapshotSize:       int64(len(snapBytes)),
		SnapshotChecksum:   crc32.ChecksumIEEE(snapBytes),
		WALFromIndex:       src.BaseMeta.LastIncludedIndex + 1,
		WALUntilIndex:      walUntil,
		WALSegments:        segs,
		CreatedAtUnixNano:  time.Now().UnixNano(),
	}
	if err := writeManifest(outDir, m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// newBaseWAL opens a brand-new WAL at dir and rebases it onto baseIndex
// — AppendMetadataSnapshot records the durable "state up to baseIndex is
// covered by a snapshot, not by any entry in this log" pointer, and
// Truncate(baseIndex+1) bumps the WAL's own next-index assignment
// counter to baseIndex+1 with no physical entries required to justify
// it, exactly the mechanism internal/node.WALStorage.InstallSnapshot
// already uses for a follower adopting a peer's snapshot boundary with
// an empty local log (see wal.WAL.Truncate's doc comment) — reused here
// verbatim rather than reinvented, since Export's backup WAL and
// Restore's staging WAL are both, structurally, exactly that case: a
// fresh log that starts at a boundary a snapshot already covers.
func newBaseWAL(dir string, baseIndex uint64) (*wal.WAL, error) {
	w, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		return nil, fmt.Errorf("backup: opening WAL at %s: %w", dir, err)
	}
	if err := w.AppendMetadataSnapshot(baseIndex); err != nil {
		w.Close()
		return nil, fmt.Errorf("backup: recording base snapshot pointer at %s: %w", dir, err)
	}
	if err := w.Truncate(baseIndex + 1); err != nil {
		w.Close()
		return nil, fmt.Errorf("backup: rebasing WAL at %s to index %d: %w", dir, baseIndex+1, err)
	}
	return w, nil
}

// copyWALSuffix streams entries from src starting at baseIndex+1,
// appending each one's opaque payload — unread, uninterpreted, exactly
// as internal/wal itself treats it (docs/architecture.md §5) — into dst
// via AppendLogEntry, stopping once an entry's index exceeds untilIndex
// (unless untilIndex is UntilLatest, in which case every entry src
// currently holds is copied). It returns the last index actually copied
// (or baseIndex, unchanged, if none were).
func copyWALSuffix(src, dst *wal.WAL, baseIndex, untilIndex uint64) (uint64, error) {
	it, err := src.Replay(baseIndex + 1)
	if err != nil {
		return 0, fmt.Errorf("backup: opening source WAL replay from %d: %w", baseIndex+1, err)
	}
	defer it.Close()

	last := baseIndex
	for {
		rec, ok, err := it.Next()
		if err != nil {
			return 0, fmt.Errorf("backup: replaying source WAL: %w", err)
		}
		if !ok {
			break
		}
		if untilIndex != UntilLatest && rec.Index > untilIndex {
			break
		}
		idx, err := dst.AppendLogEntry(rec.Payload)
		if err != nil {
			return 0, fmt.Errorf("backup: copying WAL entry %d: %w", rec.Index, err)
		}
		if idx != rec.Index {
			return 0, fmt.Errorf("backup: internal index mismatch copying WAL entry (assigned %d, source had %d) — BACKUP CONSISTENCY violated", idx, rec.Index)
		}
		last = rec.Index
	}
	return last, nil
}

// readSoleSnapshotFile returns the name and bytes of the single *.snap
// file internal/snapshot.Manager retains in dir (V1's single-retention
// policy, internal/snapshot's own doc comment) — read back from disk
// rather than re-encoded, so the manifest's checksum covers exactly the
// bytes that will actually be read at restore time.
func readSoleSnapshotFile(dir string) (name string, data []byte, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, fmt.Errorf("backup: listing snapshot directory %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".snap") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			return "", nil, fmt.Errorf("backup: reading snapshot file %s: %w", p, err)
		}
		return e.Name(), b, nil
	}
	return "", nil, fmt.Errorf("backup: no snapshot file found in %s after Create", dir)
}

// describeWALSegments lists every WAL segment file physically present in
// walDir (in ascending id order) with its size and crc32 checksum, for
// the manifest's WALSegments field.
func describeWALSegments(walDir string) ([]WALSegmentInfo, error) {
	ids, err := storage.ListSegmentIDs(walDir)
	if err != nil {
		return nil, fmt.Errorf("backup: listing WAL segments in %s: %w", walDir, err)
	}
	segs := make([]WALSegmentInfo, 0, len(ids))
	for _, id := range ids {
		p := storage.SegmentPath(walDir, id)
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("backup: reading WAL segment %s: %w", p, err)
		}
		segs = append(segs, WALSegmentInfo{
			FileName: filepath.Base(p),
			Size:     int64(len(data)),
			Checksum: crc32.ChecksumIEEE(data),
		})
	}
	return segs, nil
}
