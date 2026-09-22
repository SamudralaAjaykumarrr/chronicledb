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

// scrubLogRecord is one physically-encountered RecordTypeLogEntry, kept
// with the file and offset it was found at so a contiguity finding can
// still be anchored to real bytes when it is raised in Scrub's second
// pass (see Scrub's own doc comment for why contiguity cannot be judged
// during the first).
type scrubLogRecord struct {
	index  uint64
	file   string
	offset int64
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
// # Index contiguity is judged in a second pass, boundary-aware
//
// Contiguity CANNOT be validated as records are encountered, for exactly
// the reason Open's own recovery scan cannot (see Open, which applies
// the identical rule): it depends on the winning Metadata record's
// LatestSnapshotIndex, and "last one wins" (docs/wal.md §9) means that
// value is only known for certain once the whole scan has finished.
//
// The rule, identical to Open's: any physically-encountered index at or
// before the boundary is superseded — never served, never trusted — and
// imposes no ordering requirement at all, wherever it appears; only the
// live suffix (indices strictly above the boundary) must be perfectly
// contiguous, and must begin at exactly boundary+1. This is not a
// relaxation: it is what a legitimate durable log actually looks like
// after a follower catches up by InstallSnapshot rather than by log.
// internal/node.WALStorage.InstallSnapshot's Truncate(boundary+1) is a
// counter-only forward jump over a gap this node never physically held
// entries for, and CompactBefore can never delete the current segment —
// so superseded pre-boundary entries routinely sit in the same segment
// as, and physically before, the genuinely live suffix. A flat
// index == previous+1 check reports corruption on exactly that shape,
// which Open accepts as correct.
//
// The "live suffix begins at boundary+1" half is applied only when no
// finding cut a segment's scan short. A corruption break can swallow an
// arbitrary number of earlier records, so the observed index set is
// incomplete and a missing start is already explained by the finding
// that was raised for it — raising a second, derived one would be noise.
// A legal torn tail in the current segment deliberately does NOT count:
// it can only remove a suffix, never an earlier index, so the start of
// the live suffix is unaffected and remains checkable. Pairwise
// contiguity between successive observed live indices is always
// checked, in every case.
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
	// logRecords records every physically-encountered LogEntry, in
	// append order, across every segment — validated below, once the
	// winning Metadata record is known.
	var logRecords []scrubLogRecord
	// boundary is the winning (last-encountered) Metadata record's
	// LatestSnapshotIndex, exactly as Open resolves it.
	var boundary uint64
	// scanAborted is set whenever a FINDING cut a segment's scan short,
	// leaving the observed index set possibly incomplete — see the doc
	// comment above for what it does and does not disable.
	scanAborted := false
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
			scanAborted = true
			continue
		}
		stats.SegmentsChecked++

		size := seg.Size()
		data := make([]byte, size)
		if size > 0 {
			if _, err := seg.ReadAt(data, 0); err != nil && !errors.Is(err, storage.ErrShortRead) {
				findings = append(findings, scrub.Finding{File: seg.Path(), Offset: 0, Kind: scrub.FindingUnreadable})
				scanAborted = true
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
						scanAborted = true
					}
					break // never a resync strategy past a torn/corrupt point (RECOVERY NON-INVENTION)
				}
				findings = append(findings, scrub.Finding{File: seg.Path(), Offset: offset, Kind: classifyDecodeErr(err)})
				scanAborted = true
				break
			}
			stats.FramesChecked++
			switch rec.Type {
			case RecordTypeLogEntry:
				logRecords = append(logRecords, scrubLogRecord{index: rec.Index, file: seg.Path(), offset: offset})
			case RecordTypeMetadata:
				if m, derr := decodeMetadata(rec.Payload); derr == nil {
					boundary = m.LatestSnapshotIndex // "last one wins" (docs/wal.md §9)
				} else {
					// The frame's checksum passed but its payload is not
					// a decodable Metadata record — Open treats this as
					// ErrCorrupt and refuses to start. Scrub reports it
					// and keeps scanning (scrub reports, it never
					// aborts), leaving boundary at its previous value: a
					// smaller boundary suppresses strictly less, which is
					// the fail-closed direction for a snapshot pointer
					// that can no longer be trusted.
					findings = append(findings, scrub.Finding{File: seg.Path(), Offset: offset, Kind: scrub.FindingBadFraming})
					scanAborted = true
				}
			}
			offset += int64(frameLen)
		}
	}

	findings = append(findings, scrubIndexContiguityFindings(logRecords, boundary, scanAborted)...)
	return findings, stats, nil
}

// scrubIndexContiguityFindings applies Open's own boundary-aware
// contiguity rule to every physically-encountered LogEntry, returning one
// finding per violation. See Scrub's doc comment for the rule and for
// what scanAborted suppresses.
func scrubIndexContiguityFindings(logRecords []scrubLogRecord, boundary uint64, scanAborted bool) []scrub.Finding {
	var findings []scrub.Finding
	var lastLive uint64 // meaningful only once haveLive is true
	haveLive := false
	for _, lr := range logRecords {
		if lr.index <= boundary {
			continue // superseded by the snapshot pointer: imposes no ordering requirement
		}
		if !haveLive {
			haveLive = true
			lastLive = lr.index
			if !scanAborted && lr.index != boundary+1 {
				// Genuinely missing history the snapshot does not cover.
				findings = append(findings, scrub.Finding{File: lr.file, Offset: lr.offset, Kind: scrub.FindingIndexOutOfOrder})
			}
			continue
		}
		if lr.index != lastLive+1 {
			findings = append(findings, scrub.Finding{File: lr.file, Offset: lr.offset, Kind: scrub.FindingIndexOutOfOrder})
		}
		lastLive = lr.index
	}
	return findings
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
