//go:build integration

// This file is release gate 7/gate 9's producer-side compatibility
// proof (docs/v0.6.0-plan.md §31 gate 9, §33 slice 13's SL-16 line):
// "a v0.6.0 node started with no new flags produces byte-identical WAL
// and snapshot output to v0.5.0 for the same input history." SL-16's
// other half — a real v0.5.0-produced data directory/backup opening
// correctly *under* v0.6.0 — is already proven in
// gate7_mixed_version_test.go's TestSL16_V050ProducedSnapshotAndBackupOpenUnderV060
// (the consumer/forward-compatibility direction); nothing previously
// exercised this producer-side claim itself, which docs/v0.6.0-plan.md
// §33 slice 13's internal/fsm/gc_test.go comment asserted was "proven
// in the mixed-binary suite" without actually being there (v0.6.0
// review F3).
//
// This drives the identical deterministic write sequence — same
// RequestIDs, same keys, same values, same order, same
// -snapshot-threshold — through a real, independently-built v0.5.0
// binary and this working tree's own v0.6.0 binary, each as a
// single-node cluster (term never advances past 1, no election ever
// happens, so index/term assignment is fully deterministic and
// comparable across two separate OS processes), and compares the
// resulting WAL segment files and snapshot file byte-for-byte. The
// audit/ subdirectory is deliberately excluded: ADR-0021's own
// Consequences section documents that v0.6.0 always opens an audit
// log even at -auth-mode=none, while v0.5.0 never did — a declared,
// intentional behavior difference outside gate 9's WAL/snapshot claim.
//
//	go test -tags=integration ./cmd/chronicledb-node/... -run TestSL16_ProducerSideByteIdenticalOutput -v
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
	"time"
)

// walNodeIDPattern matches internal/wal's own Metadata record encoding
// (internal/wal/metadata.go's encodeMetadata/decodeMetadata, and the
// record framing wrapping it, internal/wal/record.go's encodeRecord —
// both unexported, so reproduced here structurally rather than
// imported): a 2-byte big-endian idLen of exactly 32 (\x00\x20)
// immediately followed by the 32 lowercase-hex characters
// hex.EncodeToString of newNodeID's 16 random bytes always produces.
// That same record's frame also carries a trailing 4-byte
// crc32.ChecksumIEEE (record.go's checksumSize) which necessarily
// differs too, since it is computed over the frame including this same
// random id — normalizeWALBytes zeroes those 4 bytes directly (by
// position, immediately after each match) rather than folding them
// into this pattern: Go's regexp matches `.` against a UTF-8 rune, not
// a raw byte, and a checksum's 4 arbitrary bytes are not valid UTF-8
// text most of the time, which silently matched a variable number of
// *bytes* per rune and corrupted the replacement length (caught
// directly: two runs of the identical binary occasionally produced
// segments differing by 1-2 bytes purely from this over-matching, with
// no version difference involved at all).
//
// The id itself is the one field in an otherwise fully deterministic
// WAL segment that is *supposed* to differ between any two
// independently-`wal.Open`ed data directories, including two runs of
// the identical binary (verified directly) — a fresh, random,
// per-data-directory node identity (docs/wal.md §8), unrelated to gate
// 9's WAL-format-compatibility claim. normalizeWALBytes masks both the
// id and its covering checksum out before a byte comparison, the same
// way a byte-identical proof must mask out any other legitimately-
// random field rather than either ignoring the whole file or asserting
// a comparison it cannot actually pass.
var walNodeIDPattern = regexp.MustCompile(`\x00\x20[0-9a-f]{32}`)

func normalizeWALBytes(data []byte) []byte {
	out := append([]byte(nil), data...)
	for _, loc := range walNodeIDPattern.FindAllIndex(out, -1) {
		idStart, idEnd := loc[0]+2, loc[1] // skip the 2-byte \x00\x20 length prefix
		for i := idStart; i < idEnd; i++ {
			out[i] = '0'
		}
		for i := idEnd; i < idEnd+checksumTrailerSize && i < len(out); i++ {
			out[i] = 0
		}
	}
	return out
}

// checksumTrailerSize mirrors internal/wal/record.go's unexported
// checksumSize (the trailing crc32.ChecksumIEEE width every frame
// carries).
const checksumTrailerSize = 4

// produceWALAndSnapshot starts a fresh single-node cluster on bin, runs
// the fixed deterministic write sequence below against it until a
// local snapshot has been taken, then kills the process (every commit
// is durably fsynced before its HTTP response returns, so an ungraceful
// kill loses nothing already-acknowledged — the same property every
// other real-process test in this package already relies on) and
// returns its data directory for inspection.
func produceWALAndSnapshot(t *testing.T, bin string) string {
	t.Helper()
	nodes := newRealClusterWithFlags(t, bin, 1, "-snapshot-threshold=5")
	rn := nodes[0]
	leader := awaitLeaderV2(t, nodes, 10*time.Second)
	if leader.id != rn.id {
		t.Fatalf("single-node cluster's own node did not become leader")
	}

	const n = 8 // > snapshot-threshold, so exactly one snapshot is taken
	var lastReqID string
	for i := 0; i < n; i++ {
		lastReqID = fmt.Sprintf("sl16prod-%02d", i)
		pr, status, err := propose(rn, lastReqID, fmt.Sprintf("sl16prodk%02d", i), fmt.Sprintf("v%02d", i))
		if err != nil || status != 200 || pr.Status != "committed" {
			t.Fatalf("propose #%d against %s: resp=%+v status=%d err=%v", i, bin, pr, status, err)
		}
	}
	awaitOutcomeCommitted(t, rn, lastReqID, 10*time.Second)
	awaitCondition2(t, 10*time.Second, "a local snapshot is created (bin="+bin+")", func() bool {
		st, err := statusV2(rn)
		return err == nil && st.SnapshotIndex > 0
	})
	// WAL segment compaction (removing entries the snapshot now covers)
	// runs asynchronously on the event loop shortly *after* the snapshot
	// itself — SnapshotIndex/AppliedIndex alone are not a sufficient
	// quiescence signal, since compaction changes .seg file size without
	// changing either. Poll the actual WAL directory's own byte size
	// (top-level *.seg files only — snapshot/ and audit/ are excluded,
	// consistent with readComparableTree below) until it stops changing,
	// so a comparison never races a still-in-flight compaction.
	awaitCondition2(t, 10*time.Second, "the WAL directory's byte size stops changing (bin="+bin+")", func() bool {
		size1, err := walDirSize(rn.dataDir)
		if err != nil {
			return false
		}
		time.Sleep(200 * time.Millisecond)
		size2, err := walDirSize(rn.dataDir)
		return err == nil && size1 == size2
	})

	rn.crash()
	return rn.dataDir
}

// walDirSize returns the total byte size of dir's own top-level regular
// files only (the WAL's *.seg segments) — not snapshot/ or audit/,
// which change on their own separate schedules and are not what this
// helper's caller needs to quiesce.
func walDirSize(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// readComparableTree returns every regular file under dir, keyed by its
// path relative to dir, excluding the audit/ subdirectory (see this
// file's header comment for why audit is out of scope for gate 9's
// WAL/snapshot claim) and the raft-worktree-irrelevant tmp/ scratch
// directory internal/snapshot.Manager uses during Create (never
// contains a file once Create has returned, but excluded defensively
// rather than assumed empty).
func readComparableTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "audit" || info.Name() == "tmp" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[rel] = data
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", dir, err)
	}
	return out
}

func TestSL16_ProducerSideByteIdenticalOutput(t *testing.T) {
	newBin := buildBinary(t)
	oldBin := buildBinaryAtRef(t, preV060Commit)

	oldDir := produceWALAndSnapshot(t, oldBin)
	newDir := produceWALAndSnapshot(t, newBin)

	oldFiles := readComparableTree(t, oldDir)
	newFiles := readComparableTree(t, newDir)

	oldNames := make([]string, 0, len(oldFiles))
	for name := range oldFiles {
		oldNames = append(oldNames, name)
	}
	sort.Strings(oldNames)
	newNames := make([]string, 0, len(newFiles))
	for name := range newFiles {
		newNames = append(newNames, name)
	}
	sort.Strings(newNames)

	if fmt.Sprint(oldNames) != fmt.Sprint(newNames) {
		t.Fatalf("v0.5.0 and v0.6.0 produced different file sets for the identical input history:\nv0.5.0: %v\nv0.6.0: %v", oldNames, newNames)
	}
	if len(oldNames) == 0 {
		t.Fatal("no comparable WAL/snapshot files were produced at all — this test would prove nothing")
	}

	sawSnapshot := false
	sawWALNodeIDField := false
	for _, name := range oldNames {
		oldData, newData := oldFiles[name], newFiles[name]
		if filepath.Ext(name) == ".seg" {
			// Every *.seg file (WAL segments) carries internal/wal's
			// per-data-directory random node-identity field once, in its
			// leading Metadata record — see walNodeIDPattern's doc
			// comment. Snapshot files carry no such field (internal/snapshot.Meta
			// has none), so they are compared unmodified below.
			if walNodeIDPattern.Match(oldData) {
				sawWALNodeIDField = true
			}
			oldData = normalizeWALBytes(oldData)
			newData = normalizeWALBytes(newData)
		}
		if filepath.Dir(name) == "snapshot" {
			sawSnapshot = true
		}
		if len(oldData) != len(newData) {
			t.Fatalf("%s: v0.5.0 produced %d bytes, v0.6.0 produced %d bytes — not byte-identical", name, len(oldData), len(newData))
		}
		for i := range oldData {
			if oldData[i] != newData[i] {
				t.Fatalf("%s: v0.5.0 and v0.6.0 output first differs at byte offset %d (v0.5.0=0x%02x, v0.6.0=0x%02x) — not byte-identical", name, i, oldData[i], newData[i])
			}
		}
	}
	if !sawSnapshot {
		t.Fatal("no snapshot file was produced by either binary — this test would prove nothing about gate 9's snapshot claim")
	}
	if !sawWALNodeIDField {
		t.Fatal("walNodeIDPattern never matched any *.seg file — either internal/wal's Metadata encoding changed shape, or this normalization is silently vacuous; this test would prove nothing about gate 9's WAL claim")
	}
	t.Logf("v0.5.0 and v0.6.0 produced %d byte-identical files for the identical input history: %v", len(oldNames), oldNames)
}
