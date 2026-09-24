package snapshot

import (
	"errors"
	"os"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
)

// ScrubStats counts what a snapshot scrub actually looked at.
type ScrubStats struct {
	SnapshotsChecked int
	BytesRead        int64
}

// Scrub reads (os.ReadFile — no write-capable handle of any kind is ever
// opened, so there is nothing to structurally restrict the way
// storage.OpenSegmentReadOnly restricts internal/wal's scrub) every
// retained *.snap file in dir and reports every decode failure via
// Decode: bad magic, unsupported version, bad checksum, and the
// hasConfig/configLen cross-check Decode already performs internally.
// It additionally cross-checks the decoded Meta.LastIncludedIndex
// against the index encoded in the file's own name — a mismatch Decode
// itself cannot detect, since Decode never sees the filename.
//
// Deliberately a free function, not a *Manager method (docs/v0.6.0-
// plan.md §21.4): scrub never touches a live Manager or its caller's
// event-loop-confined state, working only off dir's contents, so it can
// run on its own goroutine with no synchronization against the node's
// normal snapshot-creation/pruning path — a snapshot pruned out from
// under a concurrent scrub is not an error (os.IsNotExist is skipped,
// not reported).
//
// onBytesRead, if non-nil, is called after each file is read, mirroring
// internal/wal.Scrub's identical rate-limiting hook.
func Scrub(dir string, onBytesRead func(n int)) ([]scrub.Finding, ScrubStats, error) {
	cands, err := listSnapshotCandidates(dir)
	if err != nil {
		return nil, ScrubStats{}, err
	}

	var findings []scrub.Finding
	var stats ScrubStats
	for _, c := range cands {
		data, err := os.ReadFile(c.path)
		if err != nil {
			if os.IsNotExist(err) {
				continue // pruned concurrently — no longer needed (§21.4)
			}
			findings = append(findings, scrub.Finding{File: c.path, Offset: 0, Kind: scrub.FindingUnreadable})
			continue
		}
		stats.SnapshotsChecked++
		stats.BytesRead += int64(len(data))
		if onBytesRead != nil {
			onBytesRead(len(data))
		}

		snap, err := Decode(data)
		if err != nil {
			findings = append(findings, scrub.Finding{File: c.path, Offset: 0, Kind: classifyDecodeErr(err)})
			continue
		}
		if snap.Meta.LastIncludedIndex != c.index {
			findings = append(findings, scrub.Finding{File: c.path, Offset: 0, Kind: scrub.FindingSnapshotDecodeFailed})
		}
	}
	return findings, stats, nil
}

func classifyDecodeErr(err error) scrub.FindingKind {
	if errors.Is(err, ErrUnsupportedVersion) {
		return scrub.FindingUnsupportedVersion
	}
	return scrub.FindingSnapshotDecodeFailed
}
