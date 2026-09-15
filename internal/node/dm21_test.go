package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/backup"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// dm21Fixture is what dm21BuildSourceBackup hands to both DM-21's
// positive proof and its negative control (§15 DM-21, §19 gate 3):
// a real backup of a real source cluster, taken deliberately without
// forcing a second snapshot after its last two membership changes, so
// both EntryConfig entries live only in the WAL suffix the backup
// copies (§23/F1) — exactly like the snapshot boundary itself, which
// is real and nonzero rather than the never-snapshotted index-0 case
// DM-16 already covers separately.
type dm21Fixture struct {
	backupDir    string
	snapBoundary raft.Index
	writeReqID   fsm.RequestID
	writeKey     string
	writeValue   string
}

// dm21BuildSourceBackup builds a 3-voter source cluster, finalizes it
// to generation 2, forces one real snapshot, then — strictly after
// that boundary — commits one ordinary write plus an AddLearner and a
// PromoteToVoter (dynamic-membership plan §15 DM-21 step 1), and takes
// a real, continuous backup capturing the whole WAL suffix beyond the
// snapshot.
func dm21BuildSourceBackup(t *testing.T) dm21Fixture {
	t.Helper()
	const snapshotThreshold = 50
	tc := newTestClusterWithSnapshotThreshold(t, 3, snapshotThreshold)
	leader := tc.leaderNode(5 * time.Second)
	mustFinalizeToMax(t, tc, leader)

	for i := 0; i < snapshotThreshold; i++ {
		key := fmt.Sprintf("dm21-pre-%d", i)
		if _, err := propose(t, leader, cmd(key, uint64(i+1), 0, key, "v"), 3*time.Second); err != nil {
			t.Fatalf("pre-snapshot propose #%d: %v", i, err)
		}
	}
	awaitCondition(t, 5*time.Second, "leader takes its first snapshot", func() bool {
		return leader.Status().SnapshotIndex > 0
	})
	snapBoundary := leader.Status().SnapshotIndex

	writeReqID := fsm.RequestID("dm21-post-write")
	if _, err := propose(t, leader, cmd(string(writeReqID), 100, 0, "postkey", "postvalue"), 3*time.Second); err != nil {
		t.Fatalf("post-snapshot propose: %v", err)
	}

	learnerAddr := freeAddrs(t, 1)[0]
	learnerID := raft.NodeID("n4")
	peerAddrs := make(map[raft.NodeID]string, len(tc.ids))
	for _, id := range tc.ids {
		peerAddrs[id] = tc.addrs[id]
	}
	sourceLearner, err := Open(Config{
		ID:                         learnerID,
		PeerAddrs:                  peerAddrs,
		ListenAddr:                 learnerAddr,
		DataDir:                    t.TempDir(),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening source learner n4: %v", err)
	}
	t.Cleanup(func() { sourceLearner.Stop() })

	addCtx, addCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer addCancel()
	if _, err := leader.AddLearner(addCtx, "dm21-add-n4", learnerID, learnerAddr); err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	awaitCondition(t, 5*time.Second, "n4 catches up", func() bool {
		return sourceLearner.Status().AppliedIndex >= uint64(leader.Status().LastIndex)
	})
	// A single-shot promote against the default PromotionMaxLagEntries
	// (0) races the leader's own matchIndex bookkeeping for the
	// learner (which lags the learner's self-reported AppliedIndex
	// slightly, by however long the last AppendEntriesResponse takes to
	// arrive back) — the same time-of-check/time-of-use race §16 itself
	// calls out, so this retries exactly like that suite's own promote
	// step.
	var promoteErr error
	awaitCondition(t, 5*time.Second, "PromoteToVoter eventually succeeds", func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		defer pcancel()
		_, err := leader.PromoteToVoter(pctx, "dm21-promote-n4", learnerID, 0)
		if err != nil {
			var lag *ErrLearnerNotCaughtUp
			if errors.As(err, &lag) {
				return false
			}
			promoteErr = err
			return true
		}
		return true
	})
	if promoteErr != nil {
		t.Fatalf("PromoteToVoter: %v", promoteErr)
	}
	awaitCondition(t, 5*time.Second, "cluster converges on 4 voters", func() bool {
		return leader.Status().VoterCount == 4
	})

	if leader.Status().SnapshotIndex != snapBoundary {
		t.Fatalf("test setup: a second snapshot happened (SnapshotIndex=%d, want %d) — the config entries must live only in the WAL suffix", leader.Status().SnapshotIndex, snapBoundary)
	}

	backupDir := t.TempDir()
	bctx, bcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer bcancel()
	if _, err := leader.Backup(bctx, backupDir, true, "dm21"); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	return dm21Fixture{
		backupDir:    backupDir,
		snapBoundary: snapBoundary,
		writeReqID:   writeReqID,
		writeKey:     "postkey",
		writeValue:   "postvalue",
	}
}

// TestDM21_RestoreCarriesNoSourceMembershipFromEitherCarrier is DM-21's
// positive proof (§15, §23/F1), steps 1-6 (minus 6's precise
// only-voided-entries-have-arrived transient, which this real,
// non-instrumented two-node harness has no way to freeze at that exact
// instant — replication here sends everything from the leader's
// snapshot boundary through its own LastIndex in one shot, including
// the very AddLearner entry that names the joining learner, so there
// is no observable gap between "voided entries applied" and "the real
// configuration adopted." What this test proves instead, end to end,
// is the safety property that transient would only be evidence for:
// the learner never fails on the voided entries and correctly adopts
// the real configuration that follows.
func TestDM21_RestoreCarriesNoSourceMembershipFromEitherCarrier(t *testing.T) {
	fx := dm21BuildSourceBackup(t)

	restoredDir := t.TempDir() + "/restored"
	if _, err := backup.Restore(fx.backupDir, restoredDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Step 3: byte-level assertions on the restored directory, before
	// any Core exists.
	mgr, err := snapshot.NewManager(restoredDir + "/snapshot")
	if err != nil {
		t.Fatalf("snapshot.NewManager: %v", err)
	}
	snap, ok, err := mgr.Load(uint64(fx.snapBoundary))
	if err != nil || !ok {
		t.Fatalf("loading restored snapshot at %d: ok=%v err=%v", fx.snapBoundary, ok, err)
	}
	if snap.Meta.HasConfiguration {
		t.Fatal("restored snapshot has HasConfiguration=true, want false (§7.6 part 1)")
	}

	rw, _, err := wal.Open(restoredDir, wal.Options{})
	if err != nil {
		t.Fatalf("opening restored WAL: %v", err)
	}
	rst, err := OpenWALStorage(rw)
	if err != nil {
		t.Fatalf("OpenWALStorage: %v", err)
	}
	lastIdx, err := rst.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex: %v", err)
	}
	entries, err := rst.Entries(fx.snapBoundary+1, lastIdx+1)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	rw.Close()

	configEntries := 0
	for _, e := range entries {
		if e.Type != raft.EntryConfig {
			continue
		}
		configEntries++
		_, requestID, targetID, targetAddr, cfg, establishes, err := raft.DecodeConfigEntry(e)
		if err != nil {
			t.Fatalf("DecodeConfigEntry at index %d: %v", e.Index, err)
		}
		if establishes {
			t.Fatalf("EntryConfig at index %d still establishes a configuration; want Voided (§7.6 part 2)", e.Index)
		}
		if requestID != "" || targetID != "" || targetAddr != "" || !cfg.IsZero() {
			t.Fatalf("Voided entry at index %d carries non-empty fields: requestID=%q targetID=%q targetAddr=%q cfg=%+v", e.Index, requestID, targetID, targetAddr, cfg)
		}
	}
	if configEntries != 2 {
		t.Fatalf("found %d EntryConfig entries in the restored WAL suffix, want exactly 2 (AddLearner(n4), PromoteToVoter(n4))", configEntries)
	}

	// Step 4/6: open the restored directory as a real node under a
	// wholly different peer set (single voter "m1"), assert it elects
	// itself leader under exactly its bootstrap configuration, and add
	// a second, brand-new learner "m2" — which can only catch up via a
	// real InstallSnapshot, since m1's own log does not extend back to
	// index 1.
	m1Addr := freeAddrs(t, 1)[0]
	m1ID := raft.NodeID("m1")
	m1, err := Open(Config{
		ID:                         m1ID,
		Peers:                      []raft.NodeID{m1ID},
		ListenAddr:                 m1Addr,
		DataDir:                    restoredDir,
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening restored node m1: %v", err)
	}
	defer m1.Stop()

	awaitCondition(t, 5*time.Second, "m1 elects itself leader under its bootstrap configuration", func() bool {
		return m1.Status().Role == raft.Leader
	})
	awaitCondition(t, 5*time.Second, "m1 applies its full restored log", func() bool {
		return m1.core.CommitIndex() == m1.core.LastIndex() && m1.appliedIndex >= uint64(m1.core.LastIndex())
	})

	cfg, atIdx := m1.core.ConfigAt(m1.core.LastIndex())
	if atIdx != 0 {
		t.Fatalf("ConfigAt(lastIndex()) established at index %d, want 0 (the bootstrap fallback, §6.3)", atIdx)
	}
	if len(cfg.Voters) != 1 || cfg.Voters[0].ID != m1ID {
		t.Fatalf("restored configuration = %+v, want exactly the bootstrap voter %q", cfg, m1ID)
	}
	for _, v := range cfg.Voters {
		if v.ID == "n1" || v.ID == "n2" || v.ID == "n3" || v.ID == "n4" {
			t.Fatalf("restored configuration retains a source NodeID: %+v", cfg)
		}
	}

	// Step 5 (scoped exception, §7.6): non-membership state is exact.
	if outcome, ok := m1.fsmachine.Load().GetOutcome(fx.writeReqID); !ok || outcome.Status != fsm.StatusCommitted {
		t.Fatalf("restored FSM outcome for %q = %+v, ok=%v, want a Committed outcome", fx.writeReqID, outcome, ok)
	}
	if value, found := m1.fsmachine.Load().Store().Visible(fx.writeKey, ^uint64(0)); !found || string(value) != fx.writeValue {
		t.Fatalf("restored value for key %q = %q, found=%v, want %q", fx.writeKey, value, found, fx.writeValue)
	}

	// Step 6: add m2. It must go through InstallSnapshot (m1's own log
	// does not reach back to index 1), accept the voided entries in the
	// replicated suffix without failing, and adopt the real
	// configuration from its own AddLearner entry.
	m2Addr := freeAddrs(t, 1)[0]
	m2ID := raft.NodeID("m2")
	m2PeerAddrs := map[raft.NodeID]string{m1ID: m1Addr}
	m2, err := Open(Config{
		ID:                         m2ID,
		PeerAddrs:                  m2PeerAddrs,
		ListenAddr:                 m2Addr,
		DataDir:                    t.TempDir(),
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening new learner m2: %v", err)
	}
	defer m2.Stop()

	addCtx, addCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer addCancel()
	if _, err := m1.AddLearner(addCtx, "dm21-add-m2", m2ID, m2Addr); err != nil {
		t.Fatalf("AddLearner(m2): %v", err)
	}

	awaitCondition(t, 5*time.Second, "m2 catches up without Node.fail", func() bool {
		return m2.Status().AppliedIndex >= uint64(m1.Status().LastIndex)
	})
	if err := m2.Err(); err != nil {
		t.Fatalf("m2 called Node.fail while accepting the replicated voided entries: %v", err)
	}
	awaitCondition(t, 5*time.Second, "m2 adopts the real configuration naming it a learner", func() bool {
		st := m2.Status()
		return st.VoterCount == 1 && st.LearnerCount == 1
	})
	if value, found := m2.fsmachine.Load().Store().Visible(fx.writeKey, ^uint64(0)); !found || string(value) != fx.writeValue {
		t.Fatalf("m2's replicated value for key %q = %q, found=%v, want %q", fx.writeKey, value, found, fx.writeValue)
	}
}

// TestDM21_NegativeControlWithoutVoidingSelfRemovedNeverElects is
// DM-21's negative control (§15 step 7, §19 gate 3): the exact same
// backup, restored with part 2 of the transform (WAL entry voiding)
// disabled, must leave the source's real AddLearner/PromoteToVoter
// entries intact in the staged WAL suffix — so the restored node,
// replaying its own log, reconstructs the SOURCE cluster's
// configuration (which never named it) instead of its own bootstrap
// configuration, observes its own exclusion, and — per §2.7 Rule 1's
// third clause (raft.Core.handleElectionTimeout) — never starts an
// election.
func TestDM21_NegativeControlWithoutVoidingSelfRemovedNeverElects(t *testing.T) {
	fx := dm21BuildSourceBackup(t)

	restore := backup.SetVoidRestoredMembershipEntriesForTest(false)
	defer restore()

	restoredDir := t.TempDir() + "/restored-negative"
	if _, err := backup.Restore(fx.backupDir, restoredDir, backup.RestoreOptions{UntilIndex: backup.UntilLatest}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	m1Addr := freeAddrs(t, 1)[0]
	m1ID := raft.NodeID("m1")
	m1, err := Open(Config{
		ID:                         m1ID,
		Peers:                      []raft.NodeID{m1ID},
		ListenAddr:                 m1Addr,
		DataDir:                    restoredDir,
		ElectionTimeoutTicks:       5,
		ElectionTimeoutJitterTicks: 5,
		HeartbeatTimeoutTicks:      1,
		TickInterval:               10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("opening restored node m1 (negative control): %v", err)
	}
	defer m1.Stop()

	awaitCondition(t, 5*time.Second, "m1 observes its own exclusion from the reconstructed source configuration", func() bool {
		return m1.selfRemoved()
	})

	// Bounded negative wait: across several real election-timeout
	// windows, m1 must never become Leader.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if m1.Status().Role == raft.Leader {
			t.Fatal("m1 became Leader despite observing its own removal from the reconstructed configuration — part 2 of the restore transform is not load-bearing")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
