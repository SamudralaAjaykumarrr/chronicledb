package node

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// TestBackup_LiveClusterUnderConcurrentWriteLoad proves
// docs/enterprise-v1-plan.md §6's integration-test requirement: "real
// backup of a live three-node cluster under concurrent write load." A
// backup taken from the leader while proposals keep flowing must
// produce a valid, restorable backup reflecting some real, internally
// consistent prior point — never a torn or contradictory one — and must
// not interrupt ongoing replication (proposals issued during and after
// the backup call still commit normally).
func TestBackup_LiveClusterUnderConcurrentWriteLoad(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	committed := make(chan string, 1000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			reqID := fmt.Sprintf("load-%d", i)
			outcome, err := propose(t, leader, cmd(reqID, uint64(i), 0, fmt.Sprintf("load-key-%d", i), fmt.Sprintf("v%d", i)), 2*time.Second)
			if err == nil && outcome.Status == fsm.StatusCommitted {
				committed <- reqID
			}
		}
	}()

	// Let some writes land before taking the backup, so it is genuinely
	// "under load," not a backup of an idle cluster.
	time.Sleep(100 * time.Millisecond)

	outDir := filepath.Join(t.TempDir(), "backup")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := leader.Backup(ctx, outDir, true, "test-cluster")
	if err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("Backup during concurrent write load: %v", err)
	}

	// Writes keep flowing after the backup call returns — proving the
	// backup did not wedge or corrupt the live node's own replication
	// path.
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(committed)

	if m.WALUntilIndex == 0 && m.LastIncludedIndex == 0 {
		t.Fatalf("backup captured no state at all despite concurrent commits: %+v", m)
	}

	// Restore into a fresh, independent directory and confirm every
	// RequestID committed at or before the backup's own captured
	// boundary is present with the terminal outcome the live cluster
	// itself recorded — the backup reflects a real, consistent prior
	// point.
	dataDir := filepath.Join(t.TempDir(), "restored")
	if _, err := backup.Restore(outDir, dataDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restoredFSM := recoverFSMForTest(t, dataDir)

	checked := 0
	for reqID := range committed {
		outcome, ok := restoredFSM.GetOutcome(fsm.RequestID(reqID))
		if !ok {
			// A commit that landed after the backup's own captured
			// boundary is legitimately absent — RPO, not a bug. Only
			// flag it if its CommitSeq (recoverable via the leader's own
			// live FSM) is at or before what the backup captured.
			liveOutcome, liveOK := leader.FSM().GetOutcome(fsm.RequestID(reqID))
			if liveOK && liveOutcome.Status == fsm.StatusCommitted && liveOutcome.CommitSeq <= m.WALUntilIndex {
				t.Fatalf("RequestID %s committed at seq %d (<= backup boundary %d) but is missing from the restored state", reqID, liveOutcome.CommitSeq, m.WALUntilIndex)
			}
			continue
		}
		if outcome.Status != fsm.StatusCommitted {
			t.Fatalf("RequestID %s restored with status %v, want committed", reqID, outcome.Status)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no committed RequestID from the live load survived into the restored state to check")
	}
}

// TestBackup_DestructiveDisasterRecoveryDrill is the destructive
// disaster-recovery proof docs/enterprise-v1-plan.md §6 requires: back
// up a live three-node cluster, then simulate *total loss* by deleting
// every node's own data directory, restore a brand-new three-node
// cluster from the backup alone, and prove it reaches a state
// consistent with everything durably backed up — and that the restored
// cluster is a genuinely live, functioning cluster afterward (a further
// write commits normally), not merely a static, inert directory.
func TestBackup_DestructiveDisasterRecoveryDrill(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	const n = 20
	for i := 1; i <= n; i++ {
		reqID := fmt.Sprintf("drill-%d", i)
		outcome, err := propose(t, leader, cmd(reqID, uint64(i), 0, fmt.Sprintf("drill-key-%d", i), fmt.Sprintf("v%d", i)), 5*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("seeding commit %d: outcome=%+v err=%v", i, outcome, err)
		}
	}
	for _, id := range tc.ids {
		awaitCondition(t, 5*time.Second, fmt.Sprintf("%s applied up to %d", id, n), func() bool {
			return tc.node(id).Status().AppliedIndex >= n
		})
	}

	outDir := filepath.Join(t.TempDir(), "backup")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := leader.Backup(ctx, outDir, true, "drill-cluster")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if m.WALUntilIndex < n {
		t.Fatalf("backup only captured up to index %d, want at least %d", m.WALUntilIndex, n)
	}

	// TOTAL LOSS: stop every node and delete every one of their data
	// directories — simulating simultaneous loss of a majority of
	// nodes' persistent storage, docs/failure-model.md §5's explicitly
	// out-of-guarantee-scope scenario this phase's backup/restore path
	// is the documented resolution for.
	for _, id := range tc.ids {
		tc.crash(id)
	}
	newDirs := make(map[raft.NodeID]string, len(tc.ids))
	for _, id := range tc.ids {
		newDirs[id] = filepath.Join(t.TempDir(), string(id))
		if _, err := backup.Restore(outDir, newDirs[id], backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
			t.Fatalf("Restore for node %s: %v", id, err)
		}
	}

	// Stand up a brand-new cluster (same membership) whose data
	// directories are exactly the just-restored ones — never touching
	// the original (now-deleted) directories at all.
	tc.dirs = newDirs
	restored := make(map[raft.NodeID]*Node, len(tc.ids))
	for _, id := range tc.ids {
		restored[id] = tc.mustOpen(id)
	}
	tc.nodes = restored
	t.Cleanup(func() {
		for _, rn := range restored {
			rn.Stop()
		}
	})

	newLeaderID := tc.awaitLeader(10 * time.Second)
	newLeader := tc.node(newLeaderID)

	// A freshly elected leader only advances its commit index once it
	// has confirmed the log via its own current-term entry
	// (docs/raft.md's leader-completeness / election no-op rule); the
	// 20 pre-loaded entries this restored cluster starts with need a
	// moment past "a leader exists" before they are actually applied.
	awaitCondition(t, 10*time.Second, "restored cluster applies every pre-loss commit", func() bool {
		return newLeader.Status().AppliedIndex >= n
	})

	for i := 1; i <= n; i++ {
		reqID := fmt.Sprintf("drill-%d", i)
		outcome, ok := newLeader.FSM().GetOutcome(fsm.RequestID(reqID))
		if !ok || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("restored cluster is missing pre-loss commit %s: ok=%v outcome=%+v", reqID, ok, outcome)
		}
	}

	// The restored cluster is genuinely live, not merely an inert
	// directory: a fresh write after disaster recovery commits
	// normally.
	outcome, err := propose(t, newLeader, cmd("after-recovery", 1000, 0, "after-key", "after-val"), 5*time.Second)
	if err != nil || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("post-recovery propose: outcome=%+v err=%v", outcome, err)
	}
}

// recoverFSMForTest reconstructs the state a real node.Open's recovery
// sequence would arrive at from dataDir alone — base snapshot (if any)
// plus every WAL entry beyond it, applied in order — without paying for
// a full Node.Open (Raft transport, event loop, election). It is
// deliberately independent of internal/backup's own internals (mirrors
// internal/backup's own package tests' identical helper) so a passing
// assertion proves restore produced a directory real recovery logic
// reconstructs correctly, not merely that internal/backup's write path
// agrees with itself.
func recoverFSMForTest(t *testing.T, dataDir string) *fsm.FSM {
	t.Helper()
	w, _, err := wal.Open(dataDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening restored WAL at %s: %v", dataDir, err)
	}
	defer w.Close()

	meta := w.Metadata()
	var f *fsm.FSM
	if meta.LatestSnapshotIndex > 0 {
		snapMgr, err := snapshot.NewManager(filepath.Join(dataDir, "snapshot"))
		if err != nil {
			t.Fatalf("opening restored snapshot manager: %v", err)
		}
		snap, ok, err := snapMgr.Load(meta.LatestSnapshotIndex)
		if err != nil || !ok {
			t.Fatalf("loading restored snapshot at %d: ok=%v err=%v", meta.LatestSnapshotIndex, ok, err)
		}
		f = snap.FSM
	} else {
		f = fsm.New(mvcc.NewStore())
	}

	it, err := w.Replay(meta.LatestSnapshotIndex + 1)
	if err != nil {
		t.Fatalf("opening restored WAL replay: %v", err)
	}
	defer it.Close()
	for {
		rec, ok, err := it.Next()
		if err != nil {
			t.Fatalf("replaying restored WAL: %v", err)
		}
		if !ok {
			break
		}
		// A real node's own WAL log-entry payloads carry an 8-byte Raft
		// term ahead of the opaque FSM command bytes (WALStorage.Append /
		// encodeEntryPayload, node/storage.go) — internal/backup copies
		// these payloads verbatim (it never interprets them, per
		// docs/architecture.md §5), so a restored, real backup's WAL
		// entries carry that same term prefix and must be unwrapped the
		// same way before decoding as a CommitTxn command.
		_, data, err := decodeEntryPayload(rec.Payload)
		if err != nil {
			t.Fatalf("decoding restored entry %d envelope: %v", rec.Index, err)
		}
		cmd, err := fsm.DecodeCommitTxn(data)
		if err != nil {
			t.Fatalf("decoding restored entry %d: %v", rec.Index, err)
		}
		if _, err := f.Apply(rec.Index, cmd); err != nil {
			t.Fatalf("applying restored entry %d: %v", rec.Index, err)
		}
	}
	return f
}
