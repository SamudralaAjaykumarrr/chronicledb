package audit

import (
	"errors"
	"os"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/scrub"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// ScrubStats counts what an audit-log scrub actually looked at.
type ScrubStats struct {
	RecordsChecked int
	BytesRead      int64
}

// Scrub re-derives the audit log's hash chain at dir, read-only
// (storage.OpenSegmentReadOnly — SCRUB NON-DESTRUCTIVE, §27.10),
// reporting a break as FindingAuditChainBroken rather than a hard error
// — deliberately not built on ReadAll/Verify, which open segments
// read-write via storage.OpenSegment (correct for a live, appendable
// Log, wrong for scrub's own non-destructive contract; enforced by
// TestScrubOnlyOpensReadOnly).
//
// A torn tail in the highest-numbered (current, still-open) segment is
// legal (mirroring internal/wal.Scrub's identical convention — the live
// Log's own recoverTail already truncates exactly this on next Open) and
// is never reported; the same signature in an earlier segment is
// reported as FindingBadFraming.
func Scrub(dir string, onBytesRead func(n int)) ([]scrub.Finding, ScrubStats, error) {
	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		return nil, ScrubStats{}, err
	}

	var findings []scrub.Finding
	var stats ScrubStats
	var prev [hashSize]byte
	var wantSeq uint64
	var currentSegmentID uint64
	if len(ids) > 0 {
		currentSegmentID = ids[len(ids)-1]
	}

	for _, id := range ids {
		seg, err := storage.OpenSegmentReadOnly(dir, id)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			findings = append(findings, scrub.Finding{File: storage.SegmentPath(dir, id), Offset: 0, Kind: scrub.FindingUnreadable})
			continue
		}
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

		offset := 0
		for offset < len(data) {
			rec, frameLen, derr := decodeFrame(data[offset:])
			if derr != nil {
				if derr == errTornTail {
					if id != currentSegmentID {
						findings = append(findings, scrub.Finding{File: seg.Path(), Offset: int64(offset), Kind: scrub.FindingBadFraming})
					}
					break
				}
				findings = append(findings, scrub.Finding{File: seg.Path(), Offset: int64(offset), Kind: scrub.FindingAuditChainBroken})
				break
			}
			if rec.Seq != wantSeq || rec.PrevHash != prev {
				findings = append(findings, scrub.Finding{File: seg.Path(), Offset: int64(offset), Kind: scrub.FindingAuditChainBroken})
				break
			}
			prev = rec.RecordHash
			wantSeq++
			stats.RecordsChecked++
			offset += frameLen
		}
	}
	return findings, stats, nil
}
