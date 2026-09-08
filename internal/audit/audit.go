// Package audit implements ChronicleDB's tamper-evident administrative
// audit log (docs/enterprise-v1-plan.md §5 layer 6): "every
// authenticated administrative action (auth success/failure, /fault
// invocation, future backup/restore/membership/upgrade actions) is
// appended to a dedicated, hash-chained (each record includes the
// previous record's hash) audit log file, physically separate from the
// WAL — auditability must survive even if a reader has WAL access but
// not audit-log access, and vice versa."
//
// The on-disk format is deliberately "WAL-like in mechanism" (reuse,
// not reinvent, per docs/vision.md's "smallest technically real design"
// principle): framed, checksummed records built directly on
// internal/storage's append-only Segment primitive (the same layer
// internal/wal itself is built on) — never on internal/wal's own
// package, since an audit log is not part of the one logical Raft/WAL
// ordered history (docs/invariants.md CONSISTENT LOG RESPONSIBILITY:
// "no independently-authoritative second history... exists" refers to
// committed database state; the audit log is a separate, explicitly
// non-authoritative-for-state record of administrative activity, not a
// second copy of that history).
package audit

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sync"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/storage"
)

// FormatVersion is the current audit record format version, checked on
// read exactly like internal/wal's own record version
// (docs/enterprise-v1-plan.md §5 "Formats/APIs affected": "versioned
// like every other durable format").
const FormatVersion uint8 = 1

// MaxRecordPayloadSize bounds the payload length this package will ever
// trust or allocate for when reading, mirroring internal/wal's own
// bound (docs/failure-model.md §6).
const MaxRecordPayloadSize = 1 * 1024 * 1024 // 1 MiB: audit entries are small, structured records

const hashSize = sha256.Size // 32

// Frame layout, deliberately mirroring internal/wal/record.go's own
// header shape:
//
//	+----------+---------+-----------+------------+------------------------------+----------+---------------+
//	| type(1B) | seq(8B) | length(4B)| version(1B)| payload = prevHash(32B)||JSON | crc32(4B)| recordHash(32B)|
//	+----------+---------+-----------+------------+------------------------------+----------+---------------+
//
// crc32 detects torn/corrupted individual frames (identical purpose to
// internal/wal's own checksum). recordHash is the hash-chain link: it
// covers the header, the full payload (including the embedded
// prevHash), and the crc32 field, so tampering with any byte of a
// record — including forging a new, internally-consistent crc32 —
// still requires recomputing recordHash, which in turn breaks the NEXT
// record's embedded prevHash unless every subsequent record is also
// rewritten. Verify (below) detects exactly that break.
const (
	recordType       = 1 // single record kind; no second type exists in this log
	headerTypeOff    = 0
	headerSeqOff     = 1
	headerLengthOff  = 9
	headerVersionOff = 13
	headerSize       = 14
	checksumSize     = 4
)

// Entry is one logical audit record — a single administrative action's
// authentication/authorization outcome and, once authorized, its
// result.
type Entry struct {
	Seq       uint64 `json:"seq"`
	Timestamp int64  `json:"timestamp"` // Unix nanoseconds; diagnostic only, never a correctness input (docs/architecture.md §4's "never wall-clock timestamps" rule, applied here as it is to the backup manifest in §6)
	Principal string `json:"principal"`
	Role      string `json:"role"`
	Action    string `json:"action"`   // e.g. "propose", "fault.block", "admin.reload-tls"
	Endpoint  string `json:"endpoint"` // HTTP path
	Result    string `json:"result"`   // "allow", "deny_unauthenticated", "deny_unauthorized"
	Detail    string `json:"detail,omitempty"`
}

// record is one decoded, checksum-verified, chain-position-tagged audit
// record.
type record struct {
	Seq        uint64
	PrevHash   [hashSize]byte
	Entry      Entry
	RecordHash [hashSize]byte
}

// ErrWriteFailed wraps any error appending an entry — deliberately
// distinct from a plain I/O error so callers can recognize
// "audit-write failure" specifically, per docs/enterprise-v1-plan.md §5
// AUDIT COMPLETENESS: "audit-write failure blocks the action rather
// than silently succeeding without a record."
type ErrWriteFailed struct{ Err error }

func (e *ErrWriteFailed) Error() string { return fmt.Sprintf("audit: write failed: %v", e.Err) }
func (e *ErrWriteFailed) Unwrap() error { return e.Err }

// Log is a hash-chained, append-only audit log, physically separate
// from any WAL/data directory. A single Log is not safe for concurrent
// Append calls from multiple goroutines beyond the internal locking
// this type itself provides — Append is safe to call concurrently; it
// serializes internally so records are appended, and their sequence
// numbers/hash chain assigned, in a single well-defined order.
type Log struct {
	mu       sync.Mutex
	dir      string
	seg      *storage.Segment
	nextSeq  uint64
	lastHash [hashSize]byte
}

// Open opens (creating if necessary) an audit log rooted at dir. dir
// must be a directory dedicated to the audit log — never the same
// directory as a node's WAL/snapshot data (docs/enterprise-v1-plan.md
// §5: "physically separate from the WAL"). The log is a single
// ever-growing segment file (audit volumes are small relative to the
// WAL — no multi-segment rotation/compaction is implemented in V1; see
// docs/security.md's scope note).
func Open(dir string) (*Log, error) {
	if err := storage.EnsureDir(dir); err != nil {
		return nil, fmt.Errorf("audit: creating audit log directory %s: %w", dir, err)
	}
	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		return nil, fmt.Errorf("audit: listing audit log segments in %s: %w", dir, err)
	}
	var seg *storage.Segment
	if len(ids) == 0 {
		seg, err = storage.CreateSegment(dir, 0)
	} else {
		seg, err = storage.OpenSegment(dir, ids[len(ids)-1])
	}
	if err != nil {
		return nil, fmt.Errorf("audit: opening audit log segment: %w", err)
	}

	l := &Log{dir: dir, seg: seg}
	if err := l.recoverTail(); err != nil {
		seg.Close()
		return nil, err
	}
	return l, nil
}

// recoverTail replays every existing record to establish nextSeq and
// lastHash (the chain tip), and truncates a torn trailing write left by
// a crash mid-append — exactly the discipline internal/wal applies to
// its own final segment (docs/wal.md §6.1), applied here to the audit
// segment.
func (l *Log) recoverTail() error {
	data := make([]byte, l.seg.Size())
	if len(data) > 0 {
		if _, err := l.seg.ReadAt(data, 0); err != nil {
			return fmt.Errorf("audit: reading existing audit log for recovery: %w", err)
		}
	}
	offset := 0
	var prev [hashSize]byte
	var seq uint64
	for offset < len(data) {
		rec, frameLen, err := decodeFrame(data[offset:])
		if err != nil {
			if err == errTornTail {
				// A crash during an in-progress append: discard the
				// torn partial frame, matching internal/wal's own
				// truncate-torn-tail recovery policy.
				if terr := l.seg.Truncate(int64(offset)); terr != nil {
					return fmt.Errorf("audit: truncating torn tail: %w", terr)
				}
				break
			}
			return fmt.Errorf("audit: %w (audit log corruption is never silently repaired)", err)
		}
		if rec.PrevHash != prev {
			return fmt.Errorf("audit: hash chain broken at seq %d: prevHash does not match preceding record's hash — audit log corruption or tampering detected", rec.Seq)
		}
		prev = rec.RecordHash
		seq = rec.Seq + 1
		offset += frameLen
	}
	l.nextSeq = seq
	l.lastHash = prev
	return nil
}

// Append appends entry to the log, assigning it the next sequence
// number and chaining it to the current tip, then fsyncs before
// returning. A failure at any point (encode, append, fsync) is
// returned wrapped in *ErrWriteFailed and the log's in-memory chain
// state is left unchanged (the failed record was never durably
// committed to the chain, so a retry — or the caller's own decision to
// reject the triggering administrative action — is safe).
func (l *Log) Append(entry Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry.Seq = l.nextSeq
	frame, recordHash, err := encodeFrame(l.lastHash, entry)
	if err != nil {
		return &ErrWriteFailed{Err: err}
	}
	if _, err := l.seg.Append(frame); err != nil {
		return &ErrWriteFailed{Err: err}
	}
	if err := l.seg.Sync(); err != nil {
		return &ErrWriteFailed{Err: err}
	}
	l.nextSeq++
	l.lastHash = recordHash
	return nil
}

// Close closes the underlying segment file.
func (l *Log) Close() error { return l.seg.Close() }

// encodeFrame builds one on-disk frame for entry, chained after
// prevHash, returning the frame bytes and the resulting recordHash
// (the new chain tip).
func encodeFrame(prevHash [hashSize]byte, entry Entry) (frame []byte, recordHash [hashSize]byte, err error) {
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return nil, recordHash, fmt.Errorf("audit: encoding entry: %w", err)
	}
	payload := make([]byte, hashSize+len(entryJSON))
	copy(payload, prevHash[:])
	copy(payload[hashSize:], entryJSON)

	if len(payload) > MaxRecordPayloadSize {
		return nil, recordHash, fmt.Errorf("audit: encoded entry %d bytes exceeds max %d", len(payload), MaxRecordPayloadSize)
	}

	frame = make([]byte, headerSize+len(payload)+checksumSize+hashSize)
	frame[headerTypeOff] = recordType
	binary.BigEndian.PutUint64(frame[headerSeqOff:], entry.Seq)
	binary.BigEndian.PutUint32(frame[headerLengthOff:], uint32(len(payload)))
	frame[headerVersionOff] = FormatVersion
	copy(frame[headerSize:], payload)

	crc := crc32.ChecksumIEEE(frame[:headerSize+len(payload)])
	binary.BigEndian.PutUint32(frame[headerSize+len(payload):], crc)

	recordHash = sha256.Sum256(frame[:headerSize+len(payload)+checksumSize])
	copy(frame[headerSize+len(payload)+checksumSize:], recordHash[:])
	return frame, recordHash, nil
}

var errTornTail = fmt.Errorf("audit: torn tail (incomplete final record)")

// decodeFrame parses exactly one frame from the start of buf, mirroring
// internal/wal's decodeFrameBytes contract: never allocates beyond
// MaxRecordPayloadSize, never panics on malformed input, and
// distinguishes a torn tail (errTornTail — the signature of a crash
// mid-append) from a fully-framed-but-invalid record (checksum
// mismatch, unsupported version, oversized claim).
func decodeFrame(buf []byte) (rec record, frameLen int, err error) {
	if len(buf) < headerSize {
		return record{}, 0, errTornTail
	}
	typ := buf[headerTypeOff]
	seq := binary.BigEndian.Uint64(buf[headerSeqOff:])
	length := binary.BigEndian.Uint32(buf[headerLengthOff:])
	version := buf[headerVersionOff]

	if version != FormatVersion {
		return record{}, 0, fmt.Errorf("unsupported audit record version %d, expected %d", version, FormatVersion)
	}
	if typ != recordType {
		return record{}, 0, fmt.Errorf("unknown audit record type %d", typ)
	}

	need := headerSize + int64(length) + checksumSize + hashSize
	if int64(len(buf)) < need {
		return record{}, 0, errTornTail
	}
	if length > MaxRecordPayloadSize {
		return record{}, 0, fmt.Errorf("audit record claims %d bytes, max %d", length, MaxRecordPayloadSize)
	}
	if int(length) < hashSize {
		return record{}, 0, fmt.Errorf("audit record payload %d bytes too short to contain a prevHash", length)
	}

	payloadEnd := int64(headerSize) + int64(length)
	crcOff := payloadEnd
	hashOff := crcOff + checksumSize

	wantCRC := binary.BigEndian.Uint32(buf[crcOff : crcOff+checksumSize])
	gotCRC := crc32.ChecksumIEEE(buf[:payloadEnd])
	if gotCRC != wantCRC {
		return record{}, 0, fmt.Errorf("checksum mismatch at seq %d (record corrupted)", seq)
	}

	var storedHash [hashSize]byte
	copy(storedHash[:], buf[hashOff:hashOff+hashSize])
	gotHash := sha256.Sum256(buf[:hashOff])
	if gotHash != storedHash {
		return record{}, 0, fmt.Errorf("record hash mismatch at seq %d (tampering detected)", seq)
	}

	var prevHash [hashSize]byte
	copy(prevHash[:], buf[headerSize:headerSize+hashSize])
	entryJSON := buf[headerSize+hashSize : payloadEnd]
	var entry Entry
	if err := json.Unmarshal(entryJSON, &entry); err != nil {
		return record{}, 0, fmt.Errorf("decoding entry JSON at seq %d: %w", seq, err)
	}

	return record{Seq: seq, PrevHash: prevHash, Entry: entry, RecordHash: storedHash}, int(need), nil
}

// ReadAll reads and hash-chain-verifies every record in the audit log
// at dir, in order, returning the decoded entries. It is the read path
// for the future operator CLI (docs/enterprise-v1-plan.md §5
// Observability: "Audit log itself is queryable via the future operator
// CLI (§11), read-only, admin role required") and for Verify/tests.
func ReadAll(dir string) ([]Entry, error) {
	entries, _, err := readAllInternal(dir)
	return entries, err
}

// Verify re-derives the hash chain across every record in the audit log
// at dir and returns a non-nil error at the first inconsistency found —
// either a per-record checksum/hash mismatch (a record's bytes were
// altered without also recomputing its own stored hash) or a broken
// chain link (a record's embedded prevHash does not equal the actual
// hash of the record immediately before it — detects reordering,
// deletion, or a "fixed up" forged replacement that failed to also
// rewrite every subsequent record). This is the proof obligation
// docs/enterprise-v1-plan.md §5 requires: "Audit log hash-chain
// integrity is proven under fuzzing/tampering tests."
func Verify(dir string) error {
	_, _, err := readAllInternal(dir)
	return err
}

func readAllInternal(dir string) ([]Entry, [hashSize]byte, error) {
	ids, err := storage.ListSegmentIDs(dir)
	if err != nil {
		return nil, [hashSize]byte{}, fmt.Errorf("audit: listing audit log segments in %s: %w", dir, err)
	}
	var entries []Entry
	var prev [hashSize]byte
	var wantSeq uint64
	for _, id := range ids {
		seg, err := storage.OpenSegment(dir, id)
		if err != nil {
			return nil, prev, fmt.Errorf("audit: opening segment %d: %w", id, err)
		}
		data := make([]byte, seg.Size())
		if len(data) > 0 {
			if _, err := seg.ReadAt(data, 0); err != nil {
				seg.Close()
				return nil, prev, fmt.Errorf("audit: reading segment %d: %w", id, err)
			}
		}
		seg.Close()

		offset := 0
		for offset < len(data) {
			rec, frameLen, err := decodeFrame(data[offset:])
			if err != nil {
				if err == errTornTail {
					// A torn tail on the last segment is a benign
					// crash-during-append artifact (Open's recoverTail
					// already truncates it on the live log); on a
					// read-only Verify/ReadAll call against a possibly
					// still-open log, treat it the same way: stop
					// cleanly rather than reporting corruption for
					// bytes that were never a complete record.
					offset = len(data)
					break
				}
				return nil, prev, fmt.Errorf("audit: %w", err)
			}
			if rec.Seq != wantSeq {
				return nil, prev, fmt.Errorf("audit: sequence gap: expected seq %d, found %d", wantSeq, rec.Seq)
			}
			if rec.PrevHash != prev {
				return nil, prev, fmt.Errorf("audit: hash chain broken at seq %d", rec.Seq)
			}
			entries = append(entries, rec.Entry)
			prev = rec.RecordHash
			wantSeq++
			offset += frameLen
		}
	}
	return entries, prev, nil
}

// Path returns the default audit-log directory for a node's data
// directory, used when -audit-log-dir is not explicitly set
// (cmd/chronicledb-node/main.go): a subdirectory of DataDir, distinct
// from the WAL/snapshot subdirectories already living there
// (docs/storage.md §4), which still satisfies "physically separate from
// the WAL" (a different file, its own directory, its own format) while
// not requiring a mandatory extra flag for the common case.
func Path(dataDir string) string {
	return filepath.Join(dataDir, "audit")
}
