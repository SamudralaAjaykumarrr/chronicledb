package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

const (
	snapshotFileSuffix = ".snap"
	snapshotIDWidth    = 20
	tmpDirName         = "tmp"
)

// Manager owns one node's snapshot directory: durable creation
// (docs/snapshots.md §3), validated loading with fallback
// (docs/snapshots.md §6), and installation of a snapshot received from
// a peer (docs/snapshots.md §7). Layout, under dataDir:
//
//	snapshot/
//	  <lastIncludedIndex, 20 digits>.snap
//	  tmp/                 in-progress files, never trusted
//
// V1 retains exactly one snapshot at a time (docs/snapshots.md §6's
// explicitly allowed simplification): a new snapshot's file is written
// and confirmed fully durable *before* any older one is deleted, so at
// least one valid snapshot is present on disk at every instant —
// stronger than the minimum §6 requires, and what makes the brief
// window where two files transiently coexist double as free,
// no-extra-code support for "fall back to the next-older valid
// snapshot" (Load already tries every candidate, newest first).
//
// Manager is not safe for concurrent use by multiple goroutines; it is
// designed to be owned by a single caller (internal/node's event-loop
// goroutine), mirroring every other durable-state owner in this
// codebase (internal/wal.WAL is the exception, having its own mutex,
// but Manager's own callers already serialize access the same way
// WALStorage does).
type Manager struct {
	dir string
}

// NewManager ensures dir and dir/tmp exist and removes any stale
// temporary files left behind by a crash during a prior Create/Install
// (docs/snapshots.md §4: "recovery... may clean up stale snapshot/tmp/
// contents" — never trusted, so their removal is not a correctness
// requirement, just hygiene).
func NewManager(dir string) (*Manager, error) {
	if err := storage.EnsureDir(dir); err != nil {
		return nil, err
	}
	tmp := filepath.Join(dir, tmpDirName)
	if err := storage.EnsureDir(tmp); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		return nil, fmt.Errorf("snapshot: listing tmp dir %s: %w", tmp, err)
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(tmp, e.Name())) // best-effort; never trusted regardless
	}
	return &Manager{dir: dir}, nil
}

func (m *Manager) tmpDir() string { return filepath.Join(m.dir, tmpDirName) }

func snapshotFileName(index uint64) string {
	return fmt.Sprintf("%0*d%s", snapshotIDWidth, index, snapshotFileSuffix)
}

func (m *Manager) path(index uint64) string {
	return filepath.Join(m.dir, snapshotFileName(index))
}

type candidate struct {
	index uint64
	path  string
}

// candidatesDescending lists every *.snap file in dir, parsed by
// filename, sorted by index descending (newest first) — used by Load's
// newest-first, fall-back-to-older-on-corruption search
// (docs/snapshots.md §6). Files that do not match the naming convention
// are ignored (they are not files this package ever created).
func (m *Manager) candidatesDescending() ([]candidate, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("snapshot: listing %s: %w", m.dir, err)
	}
	var out []candidate
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, snapshotFileSuffix) {
			continue
		}
		idStr := name[:len(name)-len(snapshotFileSuffix)]
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, candidate{index: id, path: filepath.Join(m.dir, name)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].index > out[j].index })
	return out, nil
}

// writeDurable performs the crash-safe temp-file + fsync + atomic
// rename + directory fsync sequence (docs/snapshots.md §3) for a
// snapshot whose framed bytes are already fully encoded, then prunes
// every other retained snapshot file now that the new one is confirmed
// durable (see Manager's doc comment on single-snapshot retention).
// index must match meta.LastIncludedIndex encoded inside data — callers
// are responsible for that consistency (both Create and Install decode
// index from the same meta they pass here).
func (m *Manager) writeDurable(index uint64, data []byte) error {
	finalPath := m.path(index)
	if err := storage.WriteFileDurable(m.tmpDir(), finalPath, data); err != nil {
		return err
	}
	return m.pruneExcept(index)
}

// pruneExcept deletes every retained snapshot file other than keepIndex
// (docs/snapshots.md §6's V1 "retain only the latest" choice). Never
// called until the new snapshot at keepIndex is itself already
// confirmed durable, so this never risks leaving zero valid snapshots
// on disk even if interrupted partway through (each deletion is its own
// fsync'd directory operation, exactly like WAL segment compaction).
func (m *Manager) pruneExcept(keepIndex uint64) error {
	cands, err := m.candidatesDescending()
	if err != nil {
		return err
	}
	for _, c := range cands {
		if c.index == keepIndex {
			continue
		}
		if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("snapshot: removing superseded snapshot %s: %w", c.path, err)
		}
	}
	return storage.SyncDir(m.dir)
}

// Create durably writes a brand-new snapshot for (meta, f) — encode,
// temp file, fsync, atomic rename, directory fsync, then prune any
// older retained snapshot (docs/snapshots.md §3). A crash at any point
// before the rename leaves only an orphaned, never-trusted temp file
// (docs/snapshots.md §4); a crash after the rename but before pruning
// simply leaves the old snapshot around too, which Load tolerates
// (newest-first, and the old one is still perfectly valid).
func (m *Manager) Create(meta Meta, f *fsm.FSM) (Meta, error) {
	data := Encode(meta, f)
	if err := m.writeDurable(meta.LastIncludedIndex, data); err != nil {
		return Meta{}, err
	}
	return meta, nil
}

// Install validates and durably installs a snapshot's raw encoded bytes
// as received from a peer (docs/snapshots.md §7 steps 2-3): data is
// fully decoded and checksum/version/consistency-validated *before*
// anything touches disk — an invalid payload never reaches storage at
// all, satisfying "a follower... never installs a snapshot it cannot
// verify" (§5).
func (m *Manager) Install(data []byte) (Snapshot, error) {
	snap, err := Decode(data)
	if err != nil {
		return Snapshot{}, err
	}
	if err := m.writeDurable(snap.Meta.LastIncludedIndex, data); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// Load finds, validates, and returns the newest legitimate snapshot at
// or before pointerIndex (docs/recovery.md §1 steps 2-4). pointerIndex
// is the durable WAL metadata pointer (internal/wal.WAL.Metadata's
// LatestSnapshotIndex) — the authoritative "officially adopted"
// boundary; Load deliberately never trusts a snapshot file whose index
// exceeds it; a Create/Install that completed writing its file but
// crashed before the corresponding AppendMetadataSnapshot call is
// exactly this case, and per docs/snapshots.md §4 such an orphaned file
// is correctly ignored (the log was never compacted against it either,
// so nothing is lost by ignoring it — recovery simply replays a bit
// more log than strictly necessary this one time).
//
// pointerIndex == 0 means "no snapshot has ever been adopted" and Load
// returns ok=false immediately without scanning the directory — trusting
// the pointer as primary, per docs/snapshots.md §3's "not by guessing
// from directory contents alone."
//
// If the file matching pointerIndex is missing or fails validation,
// Load falls back to the next-older candidate still present
// (docs/snapshots.md §6) — relevant during the brief transitional window
// where Manager's single-retention policy has not yet pruned an older
// file, or if some future policy retains more than one generation.
func (m *Manager) Load(pointerIndex uint64) (Snapshot, bool, error) {
	if pointerIndex == 0 {
		return Snapshot{}, false, nil
	}
	cands, err := m.candidatesDescending()
	if err != nil {
		return Snapshot{}, false, err
	}
	for _, c := range cands {
		if c.index > pointerIndex {
			continue
		}
		data, err := os.ReadFile(c.path)
		if err != nil {
			continue
		}
		snap, err := Decode(data)
		if err != nil {
			continue // corrupted: fall back to the next-older candidate
		}
		if snap.Meta.LastIncludedIndex != c.index {
			continue // filename/content mismatch: never trusted
		}
		return snap, true, nil
	}
	return Snapshot{}, false, nil
}

// Bytes returns the raw, already-encoded bytes of the retained snapshot
// matching index, for a leader to transmit to a lagging follower
// (docs/snapshots.md §7 step 1) — read directly off disk rather than
// re-encoding from a live FSM, so what is sent is exactly what this
// node's own recovery would trust.
func (m *Manager) Bytes(index uint64) ([]byte, bool, error) {
	data, err := os.ReadFile(m.path(index))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("snapshot: reading snapshot %d: %w", index, err)
	}
	return data, true, nil
}
