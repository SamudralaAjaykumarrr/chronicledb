package backup

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// RestoreOptions configures Restore.
type RestoreOptions struct {
	// UntilIndex is the PITR boundary: the last committed log index to
	// restore. UntilLatest (the default zero value is NOT this — see
	// UntilLatest's doc comment) restores everything the backup
	// captured (manifest.WALUntilIndex). Any other value must be within
	// [manifest.LastIncludedIndex, manifest.WALUntilIndex].
	UntilIndex uint64
	// Force permits Restore to proceed against a target data directory
	// that already contains WAL/snapshot state, destroying that
	// existing state first (docs/enterprise-v1-plan.md §6 "DESTRUCTIVE
	// RESTORE ISOLATION"). Callers gate this behind admin authorization
	// and an audit record — internal/backup itself has no notion of
	// principals or audit logs (docs/architecture.md §5 layering); it
	// only enforces that this flag was explicitly set.
	Force bool
}

// RestoreResult reports what Restore actually did.
type RestoreResult struct {
	// Manifest is the source backup's own manifest, unmodified — useful
	// for a caller to report exactly which backup (and its full
	// captured range) a restore was performed from.
	Manifest Manifest
	// RestoredUntilIndex is the actual PITR boundary this restore
	// applied: RestoreOptions.UntilIndex, or Manifest.WALUntilIndex if
	// UntilLatest was requested.
	RestoredUntilIndex uint64
}

// Restore validates backupDir's manifest and every component it
// references, then reconstructs a fresh, valid node data directory at
// dataDir — WAL and snapshot subdirectories in exactly the layout
// internal/node.Open already expects and validates, so restore is
// architecturally "recovery from a portable source" reusing the exact
// same validated recovery code paths as an ordinary restart, never a new
// decode path (docs/enterprise-v1-plan.md §6 "Architecture").
//
// Validation happens entirely before dataDir is ever touched (BACKUP
// INTEGRITY: a corrupted/truncated/tampered backup is rejected outright,
// never partially restored). The actual reconstruction happens in a
// staging directory beside dataDir and is only made visible via a single
// atomic directory rename as the last step (ATOMICITY / BACKUP
// CONSISTENCY): a crash or error at any point before that rename leaves
// dataDir exactly as Restore found it — empty/absent, or, if Force was
// requested, its pre-existing content untouched until the very last
// moment — so re-running Restore with the same arguments after an
// interruption is always safe (docs/enterprise-v1-plan.md §6 "Failure
// semantics": "restarting the restore from the same backup directory is
// safe/idempotent").
func Restore(backupDir, dataDir string, opts RestoreOptions) (RestoreResult, error) {
	m, err := readManifest(backupDir)
	if err != nil {
		return RestoreResult{}, err
	}

	snapBytes, err := verifySnapshotComponent(backupDir, m)
	if err != nil {
		return RestoreResult{}, err
	}
	if err := verifyWALSegmentComponents(backupDir, m); err != nil {
		return RestoreResult{}, err
	}

	until := opts.UntilIndex
	if until == UntilLatest {
		until = m.WALUntilIndex
	}
	if until < m.LastIncludedIndex || until > m.WALUntilIndex {
		return RestoreResult{}, fmt.Errorf("%w: restore-until index %d is outside this backup's range [%d, %d]", ErrInvalidBoundary, until, m.LastIncludedIndex, m.WALUntilIndex)
	}

	clean, err := dataDirIsClean(dataDir)
	if err != nil {
		return RestoreResult{}, err
	}
	if !clean && !opts.Force {
		return RestoreResult{}, fmt.Errorf("%w: %s", ErrTargetNotClean, dataDir)
	}

	staging := filepath.Clean(dataDir) + ".restore-staging"
	if err := os.RemoveAll(staging); err != nil {
		return RestoreResult{}, fmt.Errorf("backup: clearing stale restore staging directory %s: %w", staging, err)
	}
	staged := false
	defer func() {
		if !staged {
			os.RemoveAll(staging)
		}
	}()

	if err := buildStaging(staging, backupDir, m, snapBytes, until); err != nil {
		return RestoreResult{}, err
	}

	if !clean {
		// Only reached with Force set (checked above) — the existing
		// target is destroyed only now, after the entire replacement
		// has already been built and validated in staging, minimizing
		// the window in which dataDir holds neither the old nor the
		// new state.
		if err := os.RemoveAll(dataDir); err != nil {
			return RestoreResult{}, fmt.Errorf("backup: removing existing data directory %s before forced restore: %w", dataDir, err)
		}
	}
	parent := filepath.Dir(filepath.Clean(dataDir))
	if err := storage.EnsureDir(parent); err != nil {
		return RestoreResult{}, err
	}
	if err := os.Rename(staging, dataDir); err != nil {
		return RestoreResult{}, fmt.Errorf("backup: promoting restored data directory into place at %s: %w", dataDir, err)
	}
	staged = true
	if err := storage.SyncDir(parent); err != nil {
		return RestoreResult{}, err
	}

	return RestoreResult{Manifest: m, RestoredUntilIndex: until}, nil
}

// verifySnapshotComponent reads backupDir's snapshot file, checks it
// against the manifest's recorded size/checksum, and confirms it passes
// internal/snapshot's own independent decode validation — three layers
// (manifest checksum, internal snapshot checksum, internal consistency)
// before a single byte reaches dataDir.
func verifySnapshotComponent(backupDir string, m Manifest) ([]byte, error) {
	if m.SnapshotFile == "" {
		return nil, fmt.Errorf("%w: manifest names no snapshot file", ErrCorrupt)
	}
	p := filepath.Join(backupDir, "snapshot", m.SnapshotFile)
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: backup snapshot file %s", ErrMissingComponent, p)
		}
		return nil, fmt.Errorf("backup: reading backup snapshot %s: %w", p, err)
	}
	if int64(len(data)) != m.SnapshotSize || crc32.ChecksumIEEE(data) != m.SnapshotChecksum {
		return nil, fmt.Errorf("%w: backup snapshot %s does not match the manifest's recorded size/checksum", ErrCorrupt, p)
	}
	snap, err := snapshot.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("%w: backup snapshot %s failed internal validation: %v", ErrCorrupt, p, err)
	}
	if snap.Meta.LastIncludedIndex != m.LastIncludedIndex || snap.Meta.LastIncludedTerm != m.LastIncludedTerm {
		return nil, fmt.Errorf("%w: backup snapshot %s boundary (%d,%d) does not match manifest (%d,%d)",
			ErrCorrupt, p, snap.Meta.LastIncludedIndex, snap.Meta.LastIncludedTerm, m.LastIncludedIndex, m.LastIncludedTerm)
	}
	return data, nil
}

// verifyWALSegmentComponents checks every WAL segment file the manifest
// references against its recorded size/checksum, without yet asking
// internal/wal to parse any of them.
func verifyWALSegmentComponents(backupDir string, m Manifest) error {
	walDir := filepath.Join(backupDir, "wal")
	for _, seg := range m.WALSegments {
		p := filepath.Join(walDir, seg.FileName)
		data, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%w: backup WAL segment %s", ErrMissingComponent, p)
			}
			return fmt.Errorf("backup: reading backup WAL segment %s: %w", p, err)
		}
		if int64(len(data)) != seg.Size || crc32.ChecksumIEEE(data) != seg.Checksum {
			return fmt.Errorf("%w: backup WAL segment %s does not match the manifest's recorded size/checksum", ErrCorrupt, p)
		}
	}
	return nil
}

// buildStaging reconstructs a complete, valid node data directory at
// staging, in exactly the layout internal/node.Open expects (WAL
// segments directly at the directory root, a "snapshot" subdirectory —
// docs/storage.md §4; internal/node.Open opens
// wal.Open(cfg.DataDir, ...) and
// snapshot.NewManager(cfg.DataDir/"snapshot") side by side, never a
// nested "wal" subdirectory): the validated snapshot bytes installed via
// internal/snapshot.Manager.Install (the exact same validated-install
// path a follower uses for a peer-provided snapshot,
// docs/snapshots.md §7), and a freshly rebased WAL containing every
// backup entry up to and including until, replayed from the backup's
// own WAL copy via internal/wal.Replay.
func buildStaging(staging, backupDir string, m Manifest, snapBytes []byte, until uint64) error {
	if err := storage.EnsureDir(staging); err != nil {
		return err
	}

	snapMgr, err := snapshot.NewManager(filepath.Join(staging, "snapshot"))
	if err != nil {
		return fmt.Errorf("backup: preparing staging snapshot directory: %w", err)
	}
	installedSnap, err := snapMgr.Install(snapBytes)
	if err != nil {
		return fmt.Errorf("backup: installing snapshot into staging: %w", err)
	}

	sw, err := newBaseWAL(staging, m.LastIncludedIndex)
	if err != nil {
		return err
	}

	// Restore is a third path (besides internal/node's own committed
	// control-command apply and peer-snapshot install) that can bring a
	// data directory up to a cluster generation the freshly-opened
	// staging WAL — like any brand-new WAL — starts at generation 0.
	// Mirror internal/node.adoptClusterGeneration's rule here (Restore
	// runs entirely before node.Open is ever called, so that choke
	// point itself is unreachable from this package): durably persist
	// whatever generation the installed snapshot's FSM state carries,
	// so wal.Open's ErrUnsupportedGeneration check reads a durable value
	// that actually matches the restored FSM state, instead of always
	// silently reporting generation 0 regardless of what was backed up.
	if err := sw.SetClusterGeneration(installedSnap.FSM.ClusterGeneration()); err != nil {
		sw.Close()
		return fmt.Errorf("backup: persisting restored cluster generation: %w", err)
	}

	// The backup's own WAL copy is only ever read here (Replay + Close);
	// wal.Open is used rather than a bespoke read-only reader so this
	// goes through internal/wal's own recovery/validation exactly like
	// any other WAL directory (docs/architecture.md §5: never a second
	// decode path).
	srcWAL, _, err := wal.Open(filepath.Join(backupDir, "wal"), wal.Options{})
	if err != nil {
		sw.Close()
		return fmt.Errorf("backup: opening backup WAL for replay: %w", err)
	}
	if _, err := copyWALSuffix(srcWAL, sw, m.LastIncludedIndex, until); err != nil {
		srcWAL.Close()
		sw.Close()
		return err
	}
	if err := srcWAL.Close(); err != nil {
		sw.Close()
		return fmt.Errorf("backup: closing backup WAL source: %w", err)
	}
	if err := sw.Sync(); err != nil {
		sw.Close()
		return fmt.Errorf("backup: syncing staging WAL: %w", err)
	}
	if err := sw.Close(); err != nil {
		return fmt.Errorf("backup: closing staging WAL: %w", err)
	}
	return nil
}

// dataDirIsClean reports whether dataDir is absent or an empty
// directory — DESTRUCTIVE RESTORE ISOLATION's "explicitly clean data
// directory only" requirement, treated conservatively: any existing
// entry at all (WAL segments, a snapshot directory, or anything else)
// counts as not clean.
func dataDirIsClean(dataDir string) (bool, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("backup: checking target data directory %s: %w", dataDir, err)
	}
	return len(entries) == 0, nil
}
