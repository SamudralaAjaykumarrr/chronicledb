package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// FormatVersion is the current backup manifest format version, checked
// before any manifest is ever trusted (mirrors internal/wal.FormatVersion
// and internal/snapshot.FormatVersion's identical discipline,
// docs/failure-model.md §6: "every durable format carries an explicit
// version field").
const FormatVersion uint8 = 1

const manifestFileName = "manifest.json"

// WALSegmentInfo records one backup WAL segment file's identity and
// integrity fingerprint, so Restore can detect a truncated/replaced/
// corrupted segment file before ever asking internal/wal to parse it
// (docs/enterprise-v1-plan.md §6 "Deterministic tests": "corruption-
// injection tests at every layer... truncated segment").
type WALSegmentInfo struct {
	FileName string `json:"fileName"`
	Size     int64  `json:"size"`
	Checksum uint32 `json:"checksum"`
}

// Manifest is a backup's self-describing metadata document
// (docs/enterprise-v1-plan.md §6 "Architecture"): everything Restore
// needs to validate and reconstruct a target data directory, without
// ever re-deriving it by guessing from the backup directory's contents
// alone (mirroring internal/snapshot.Manager.Load's "trust the pointer,
// not the directory listing" discipline).
//
// Manifest fields are never a correctness input to what gets restored
// except LastIncludedIndex/LastIncludedTerm and the WAL/snapshot
// integrity fields — ClusterID and CreatedAtUnixNano are purely
// diagnostic/operator bookkeeping (docs/architecture.md §4's "never
// wall-clock timestamps as a correctness input" rule, applied here to
// the manifest's own creation time exactly as it applies to
// CommitSeq/StartSeq elsewhere).
type Manifest struct {
	FormatVersion uint8 `json:"formatVersion"`
	// ChronicleDBVersion is the version.Version string of the binary
	// that produced this backup — diagnostic only, never checked by
	// Restore (a backup produced by one build is not required to be
	// restored by an identical build; ChronicleDB has no format
	// migration story yet — see docs/enterprise-v1-plan.md §7, out of
	// this phase's scope).
	ChronicleDBVersion string `json:"chronicledbVersion"`
	// ClusterID diagnostically identifies which cluster produced this
	// backup (docs/enterprise-v1-plan.md §6 "required cluster/database
	// metadata"). Never validated against a restore target: restoring a
	// backup into a differently-configured cluster (different peer set)
	// is a legitimate V1 use (e.g. disaster recovery onto replacement
	// hardware) and ClusterID mismatch must never block it.
	ClusterID string `json:"clusterId"`

	// LastIncludedIndex/LastIncludedTerm are this backup's base
	// snapshot boundary — internal/snapshot.Meta's own fields, carried
	// here so Restore can validate the snapshot file's claimed boundary
	// against what the manifest itself asserts.
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`

	SnapshotFile     string `json:"snapshotFile"`
	SnapshotSize     int64  `json:"snapshotSize"`
	SnapshotChecksum uint32 `json:"snapshotChecksum"`

	// WALFromIndex is the first log index this backup's WAL suffix
	// covers (always LastIncludedIndex+1). WALUntilIndex is the last
	// index actually captured: equal to LastIncludedIndex for a
	// snapshot-only backup (no trailing suffix), or the source WAL's
	// own head (or an operator-chosen bound) for a continuous/PITR
	// backup. Every WALSegments entry together covers exactly
	// [WALFromIndex, WALUntilIndex].
	WALFromIndex  uint64           `json:"walFromIndex"`
	WALUntilIndex uint64           `json:"walUntilIndex"`
	WALSegments   []WALSegmentInfo `json:"walSegments"`

	// CreatedAtUnixNano is diagnostic-only operator bookkeeping (see
	// this type's doc comment) — never read by Restore for any
	// correctness decision.
	CreatedAtUnixNano int64 `json:"createdAtUnixNano"`
}

// writeManifest encodes m as indented JSON followed by a trailing
// hex-crc32 line computed over exactly the JSON bytes written, then
// durably installs it at outDir/manifest.json via the same temp-file/
// fsync/atomic-rename/directory-fsync sequence internal/snapshot uses
// (docs/enterprise-v1-plan.md §6 "Failure semantics": "a
// .manifest.tmp-then-atomic-rename pattern... ensures a manifest only
// exists once the backup is complete and valid"). This must be the
// last step of Export: everything the manifest references (the
// snapshot file, every WAL segment file) must already be durably
// written before this call.
func writeManifest(outDir string, m Manifest) error {
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("backup: encoding manifest: %w", err)
	}
	crc := crc32.ChecksumIEEE(body)
	full := append(append([]byte{}, body...), []byte(fmt.Sprintf("\n%08x\n", crc))...)

	tmpDir := filepath.Join(outDir, "tmp")
	if err := storage.EnsureDir(tmpDir); err != nil {
		return err
	}
	return storage.WriteFileDurable(tmpDir, filepath.Join(outDir, manifestFileName), full)
}

// readManifest reads and validates dir/manifest.json's own checksum and
// format version (but not the components it references — callers do
// that separately, against the actual snapshot/WAL segment bytes).
func readManifest(dir string) (Manifest, error) {
	path := filepath.Join(dir, manifestFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, fmt.Errorf("%w: no manifest at %s (backup missing or never completed)", ErrMissingComponent, path)
		}
		return Manifest{}, fmt.Errorf("backup: reading manifest %s: %w", path, err)
	}

	trimmed := bytes.TrimRight(raw, "\n")
	nl := bytes.LastIndexByte(trimmed, '\n')
	if nl < 0 {
		return Manifest{}, fmt.Errorf("%w: manifest %s has no checksum trailer", ErrCorrupt, path)
	}
	body := trimmed[:nl]
	crcLine := strings.TrimSpace(string(trimmed[nl+1:]))
	wantCRC, err := strconv.ParseUint(crcLine, 16, 32)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: manifest %s has a malformed checksum trailer: %v", ErrCorrupt, path, err)
	}
	if uint32(wantCRC) != crc32.ChecksumIEEE(body) {
		return Manifest{}, fmt.Errorf("%w: manifest %s checksum mismatch", ErrCorrupt, path)
	}

	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return Manifest{}, fmt.Errorf("%w: manifest %s is not valid JSON: %v", ErrCorrupt, path, err)
	}
	if m.FormatVersion != FormatVersion {
		return Manifest{}, fmt.Errorf("%w: manifest format version %d, expected %d", ErrUnsupportedVersion, m.FormatVersion, FormatVersion)
	}
	return m, nil
}
