package fsm

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

// fsmStateVersion is the format version of the opaque state bytes
// EncodeState/DecodeState produce — the "state-machine metadata: format
// version" docs/snapshots.md §2 requires internal/fsm to describe its
// own serialized shape with. Distinct from internal/wal's frame version
// and from commitTxnCommandVersion, exactly as those two are distinct
// from each other (docs/wal.md §3 vs. command.go's own doc comment).
const fsmStateVersion uint8 = 1

// EncodeState serializes the FSM's entire deterministic state — every
// key's MVCC version chain (including tombstones) and the full
// RequestID outcome table — into opaque bytes suitable for a state-
// machine snapshot (docs/snapshots.md §2). It captures one atomic,
// consistent point-in-time view: FSM.mu is held for the whole call, so
// no concurrent Apply can interleave with it and no reader ever
// observes a snapshot straddling two different applied indices
// (docs/snapshots.md §3's "state mutations that arrive while the
// snapshot is being serialized must not be reflected" requirement).
//
// Encoding never depends on Go's unordered map iteration order: both
// the version chains (via mvcc.Store.Export) and the outcome table are
// sorted before being written, so two independently-constructed FSMs
// that applied the identical command history produce byte-identical
// EncodeState output (mirrored by TestEncodeStateDeterministic).
func (f *FSM) EncodeState() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return encodeState(f.store, f.outcomes, f.clusterGeneration)
}

// encodeState serializes store/outcomes/clusterGeneration.
// clusterGeneration is appended as a trailing 4-byte field ONLY when
// nonzero (docs/enterprise-v1-plan.md §7 "WAL/snapshot format
// version": "additive — existing v0.1.0-produced files remain
// readable, since generation 0 is defined as today's exact existing
// format, never redefined"): a not-yet-finalized cluster
// (clusterGeneration == 0, the default and the only value any
// pre-v0.4.0 binary ever produces or expects) encodes byte-identical
// output to every prior release, so a pre-v0.4.0 binary's own strict,
// non-tolerant DecodeState can still install/read a snapshot this
// binary produced before finalize — the rollback safety
// docs/enterprise-v1-plan.md §7 requires. Only once finalize has
// actually run (clusterGeneration > 0) does the extra field appear,
// at which point an old binary reading it is expected, correctly, to
// fail closed (see DecodeState's trailing-bytes check below) — that
// is the documented rollback boundary, not a bug.
func encodeState(store *mvcc.Store, outcomes map[RequestID]outcomeEntry, clusterGeneration uint32) []byte {
	chains := store.Export() // already sorted by key

	ids := make([]RequestID, 0, len(outcomes))
	for id := range outcomes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	size := 1 + 4 // version + numChains
	for _, kc := range chains {
		size += 4 + len(kc.Key) + 4
		for _, v := range kc.Versions {
			size += 8 + 1
			if !v.Tombstone {
				size += 4 + len(v.Value)
			}
		}
	}
	size += 4 // numOutcomes
	for _, id := range ids {
		e := outcomes[id]
		size += 4 + len(id) + len(fingerprint{})
		size += 1 // status
		size += 8 // CommitSeq (always present, 0 when meaningless)
		size += 4 + len(e.outcome.ConflictKey)
		size += 8 // ConflictLatestSeq
	}
	if clusterGeneration > 0 {
		size += 4
	}

	buf := make([]byte, size)
	off := 0
	buf[off] = fsmStateVersion
	off++
	binary.BigEndian.PutUint32(buf[off:], uint32(len(chains)))
	off += 4
	for _, kc := range chains {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(kc.Key)))
		off += 4
		off += copy(buf[off:], kc.Key)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(kc.Versions)))
		off += 4
		for _, v := range kc.Versions {
			binary.BigEndian.PutUint64(buf[off:], v.CommitSeq)
			off += 8
			if v.Tombstone {
				buf[off] = 1
			} else {
				buf[off] = 0
			}
			off++
			if !v.Tombstone {
				binary.BigEndian.PutUint32(buf[off:], uint32(len(v.Value)))
				off += 4
				off += copy(buf[off:], v.Value)
			}
		}
	}
	binary.BigEndian.PutUint32(buf[off:], uint32(len(ids)))
	off += 4
	for _, id := range ids {
		e := outcomes[id]
		binary.BigEndian.PutUint32(buf[off:], uint32(len(id)))
		off += 4
		off += copy(buf[off:], id)
		off += copy(buf[off:], e.fingerprint[:])
		buf[off] = byte(e.outcome.Status)
		off++
		binary.BigEndian.PutUint64(buf[off:], e.outcome.CommitSeq)
		off += 8
		binary.BigEndian.PutUint32(buf[off:], uint32(len(e.outcome.ConflictKey)))
		off += 4
		off += copy(buf[off:], e.outcome.ConflictKey)
		binary.BigEndian.PutUint64(buf[off:], e.outcome.ConflictLatestSeq)
		off += 8
	}
	if clusterGeneration > 0 {
		binary.BigEndian.PutUint32(buf[off:], clusterGeneration)
		off += 4
	}
	return buf[:off]
}

// DecodeState parses bytes produced by EncodeState and returns a fresh
// FSM whose MVCC store and RequestID outcome table are restored exactly
// (docs/recovery.md §1 step 4), plus maxCommitSeq — the largest
// CommitSeq value found anywhere in the decoded state (across every
// version and every committed outcome). A caller that knows the
// snapshot's own claimed lastIncludedIndex (internal/snapshot, which
// does know about Raft boundaries — internal/fsm deliberately does not,
// per docs/architecture.md §5's dependency rules) uses maxCommitSeq to
// spot-check internal consistency (docs/snapshots.md §5 point 3): no
// decoded state may reference a CommitSeq beyond the snapshot's own
// boundary.
//
// DecodeState never trusts a length or count field beyond the bytes
// actually remaining, mirroring DecodeCommitTxn's bounded-decoding
// discipline (docs/failure-model.md §6): it returns an error, never
// panics, on any malformed or truncated input, and never returns a
// partially-populated FSM on error.
func DecodeState(data []byte) (*FSM, uint64, error) {
	if len(data) < 1+4 {
		return nil, 0, fmt.Errorf("%w: state too short (%d bytes)", ErrMalformedCommand, len(data))
	}
	off := 0
	version := data[off]
	off++
	if version != fsmStateVersion {
		return nil, 0, fmt.Errorf("%w: state version %d, expected %d", ErrUnsupportedCommandVersion, version, fsmStateVersion)
	}

	var maxSeq uint64
	bumpMax := func(seq uint64) {
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	numChains := binary.BigEndian.Uint32(data[off:])
	off += 4
	const minChainSize = 4 + 4 // keyLen + numVersions, minimum
	if remaining := len(data) - off; numChains > uint32(remaining/minChainSize) {
		return nil, 0, fmt.Errorf("%w: declares %d chains but only %d bytes remain", ErrMalformedCommand, numChains, remaining)
	}
	chains := make([]mvcc.KeyChain, 0, numChains)
	for i := uint32(0); i < numChains; i++ {
		if len(data)-off < 4 {
			return nil, 0, fmt.Errorf("%w: truncated chain %d (key length)", ErrMalformedCommand, i)
		}
		keyLen := binary.BigEndian.Uint32(data[off:])
		off += 4
		if int64(keyLen) > int64(len(data)-off) {
			return nil, 0, fmt.Errorf("%w: truncated chain %d (key: declared %d, %d remain)", ErrMalformedCommand, i, keyLen, len(data)-off)
		}
		key := string(data[off : off+int(keyLen)])
		off += int(keyLen)

		if len(data)-off < 4 {
			return nil, 0, fmt.Errorf("%w: truncated chain %d (version count)", ErrMalformedCommand, i)
		}
		numVersions := binary.BigEndian.Uint32(data[off:])
		off += 4
		const minVersionSize = 8 + 1
		if remaining := len(data) - off; numVersions > uint32(remaining/minVersionSize) {
			return nil, 0, fmt.Errorf("%w: chain %d declares %d versions but only %d bytes remain", ErrMalformedCommand, i, numVersions, remaining)
		}
		versions := make([]mvcc.Version, 0, numVersions)
		for j := uint32(0); j < numVersions; j++ {
			if len(data)-off < 8+1 {
				return nil, 0, fmt.Errorf("%w: chain %d version %d truncated", ErrMalformedCommand, i, j)
			}
			seq := binary.BigEndian.Uint64(data[off:])
			off += 8
			tombstone := data[off] != 0
			off++
			var value []byte
			if !tombstone {
				if len(data)-off < 4 {
					return nil, 0, fmt.Errorf("%w: chain %d version %d truncated (value length)", ErrMalformedCommand, i, j)
				}
				valLen := binary.BigEndian.Uint32(data[off:])
				off += 4
				if int64(valLen) > int64(len(data)-off) {
					return nil, 0, fmt.Errorf("%w: chain %d version %d truncated (value: declared %d, %d remain)", ErrMalformedCommand, i, j, valLen, len(data)-off)
				}
				value = make([]byte, valLen)
				copy(value, data[off:off+int(valLen)])
				off += int(valLen)
			}
			bumpMax(seq)
			versions = append(versions, mvcc.Version{CommitSeq: seq, Value: value, Tombstone: tombstone})
		}
		chains = append(chains, mvcc.KeyChain{Key: key, Versions: versions})
	}

	if len(data)-off < 4 {
		return nil, 0, fmt.Errorf("%w: truncated outcome count", ErrMalformedCommand)
	}
	numOutcomes := binary.BigEndian.Uint32(data[off:])
	off += 4
	const fingerprintSize = 32 // sha256.Size, kept as a literal to avoid importing crypto/sha256 here
	const minOutcomeSize = 4 + fingerprintSize + 1 + 8 + 4 + 8
	if remaining := len(data) - off; numOutcomes > uint32(remaining/minOutcomeSize) {
		return nil, 0, fmt.Errorf("%w: declares %d outcomes but only %d bytes remain", ErrMalformedCommand, numOutcomes, remaining)
	}
	outcomes := make(map[RequestID]outcomeEntry, numOutcomes)
	for i := uint32(0); i < numOutcomes; i++ {
		if len(data)-off < 4 {
			return nil, 0, fmt.Errorf("%w: truncated outcome %d (RequestID length)", ErrMalformedCommand, i)
		}
		idLen := binary.BigEndian.Uint32(data[off:])
		off += 4
		if int64(idLen) > int64(len(data)-off) {
			return nil, 0, fmt.Errorf("%w: truncated outcome %d (RequestID: declared %d, %d remain)", ErrMalformedCommand, i, idLen, len(data)-off)
		}
		id := RequestID(data[off : off+int(idLen)])
		off += int(idLen)

		if len(data)-off < fingerprintSize+1+8+4 {
			return nil, 0, fmt.Errorf("%w: truncated outcome %d body", ErrMalformedCommand, i)
		}
		var fp fingerprint
		copy(fp[:], data[off:off+fingerprintSize])
		off += fingerprintSize
		status := Status(data[off])
		off++
		commitSeq := binary.BigEndian.Uint64(data[off:])
		off += 8
		conflictKeyLen := binary.BigEndian.Uint32(data[off:])
		off += 4
		if int64(conflictKeyLen) > int64(len(data)-off) {
			return nil, 0, fmt.Errorf("%w: outcome %d truncated (conflict key: declared %d, %d remain)", ErrMalformedCommand, i, conflictKeyLen, len(data)-off)
		}
		conflictKey := string(data[off : off+int(conflictKeyLen)])
		off += int(conflictKeyLen)
		if len(data)-off < 8 {
			return nil, 0, fmt.Errorf("%w: outcome %d truncated (conflict latest seq)", ErrMalformedCommand, i)
		}
		conflictLatestSeq := binary.BigEndian.Uint64(data[off:])
		off += 8

		outcome := Outcome{RequestID: id, Status: status, ConflictKey: conflictKey, ConflictLatestSeq: conflictLatestSeq}
		if status == StatusCommitted {
			outcome.CommitSeq = commitSeq
			bumpMax(commitSeq)
		}
		outcomes[id] = outcomeEntry{outcome: outcome, fingerprint: fp}
	}

	// Generation-aware trailing field (see encodeState's doc comment):
	// exactly 0 extra bytes is generation-0 state (every pre-v0.4.0
	// snapshot, and every not-yet-finalized v0.4.0+ one); exactly 4 extra
	// bytes is a clusterGeneration field appended by a finalized cluster.
	// Anything else is genuinely malformed, not a newer-but-unreadable
	// generation this build simply declines to guess at — an actual
	// future generation bump would introduce its own new trailing-field
	// case here, deliberately, the same way this one was added; it would
	// never silently fall through to "corrupt."
	var clusterGeneration uint32
	switch extra := len(data) - off; extra {
	case 0:
	case 4:
		clusterGeneration = binary.BigEndian.Uint32(data[off:])
		off += 4
	default:
		return nil, 0, fmt.Errorf("%w: %d trailing bytes after decoding state", ErrMalformedCommand, extra)
	}

	return &FSM{
		store:             mvcc.RestoreStore(chains),
		outcomes:          outcomes,
		controlOutcomes:   make(map[RequestID]Outcome),
		clusterGeneration: clusterGeneration,
	}, maxSeq, nil
}
