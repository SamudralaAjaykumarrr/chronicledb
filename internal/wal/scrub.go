package wal

import (
	"errors"
	"os"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// ScrubStats counts what a WAL scrub actually looked at.
type ScrubStats struct {
	SegmentsChecked int
	FramesChecked   int
	BytesRead       int64
}

// Scrub reads every WAL segment in dir read-only (storage.
// OpenSegmentReadOnly — SCRUB NON-DESTRUCTIVE, §27.10) and reports every
// framing/checksum/version/index-ordering problem found. It never
// returns a non-nil error for a corruption finding itself — corruption
// is reported via the returned findings, exactly like every other
// internal/node.Scrub subsystem (docs/recovery.md §4: "scrub reports,
// the operator acts") — err is non-nil only for a genuinely unexpected
// failure scrub cannot itself classify as a finding (e.g. dir itself
// cannot be listed).
//
// A torn tail in the highest-numbered (current, still-open) segment is
// legal (docs/wal.md §6) and is never reported; the identical torn-tail
// signature in any earlier segment is corruption (that segment should
// never still be open for writing) and is reported as FindingBadFraming.
//
// onBytesRead, if non-nil, is called after each segment is fully read,
// with the number of bytes just read — internal/node.Scrub uses this to
// implement -scrub-bytes-per-sec rate limiting across every subsystem's
// combined read volume; nil means unlimited (e.g. this package's own
// unit tests).
func Scrub(dir string, onBytesRead func(n int)) ([]scrub.Finding, ScrubStats, error) {
	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		return nil, ScrubStats{}, err
	}

	var findings []scrub.Finding
	var stats ScrubStats
	var lastLogIndex uint64 // 0 = "no LogEntry record seen yet across any segment so far"
	var currentSegmentID uint64
	if len(ids) > 0 {
		currentSegmentID = ids[len(ids)-1]
	}

	for _, id := range ids {
		seg, err := storage.OpenSegmentReadOnly(dir, id)
		if err != nil {
			if os.IsNotExist(err) {
				// Compacted away concurrently — by definition no longer
				// needed (docs/v0.6.0-plan.md §21.4).
				continue
			}
			findings = append(findings, scrub.Finding{File: storage.SegmentPath(dir, id), Offset: 0, Kind: scrub.FindingUnreadable})
			continue
		}
		stats.SegmentsChecked++

		size := seg.Size()
		data := make([]byte, size)
		if size > 0 {
			if _, err := seg.ReadAt(data, 0); err != nil && !errors.Is(err, storage.ErrShortRead) {
				findings = append(findings, scrub.Finding{File: seg.Path(), Offset: 0, Kind: scrub.FindingUnreadable})
				seg.Close()
				continue
			}
		}
		seg.Close()
		stats.BytesRead += int64(len(data))
		if onBytesRead != nil {
			onBytesRead(len(data))
		}

		offset := int64(0)
		for offset < int64(len(data)) {
			rec, frameLen, err := decodeFrameBytes(data[offset:])
			if err != nil {
				if errors.Is(err, errTornTail) {
					if id != currentSegmentID {
						findings = append(findings, scrub.Finding{File: seg.Path(), Offset: offset, Kind: scrub.FindingBadFraming})
					}
					break // never a resync strategy past a torn/corrupt point (RECOVERY NON-INVENTION)
				}
				findings = append(findings, scrub.Finding{File: seg.Path(), Offset: offset, Kind: classifyDecodeErr(err)})
				break
			}
			stats.FramesChecked++
			if rec.Type == RecordTypeLogEntry {
				if lastLogIndex != 0 && rec.Index != lastLogIndex+1 {
					findings = append(findings, scrub.Finding{File: seg.Path(), Offset: offset, Kind: scrub.FindingIndexOutOfOrder})
				}
				lastLogIndex = rec.Index
			}
			offset += int64(frameLen)
		}
	}
	return findings, stats, nil
}

// classifyDecodeErr maps a fully-framed-but-invalid decodeFrameBytes
// error to scrub's fixed finding vocabulary.
func classifyDecodeErr(err error) scrub.FindingKind {
	switch {
	case errors.Is(err, ErrUnsupportedVersion):
		return scrub.FindingUnsupportedVersion
	case errors.Is(err, ErrCorrupt):
		return scrub.FindingBadChecksum
	default:
		return scrub.FindingBadFraming
	}
}
