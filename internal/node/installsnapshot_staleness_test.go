package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/snapshot"
)

// TestInstallSnapshotBelowCommitIndexIsNeverDurablyInstalled regresses a
// real defect found by the v0.5.0 final correctness review: the
// driver-side willAdvance predicate in handleInstallSnapshot must mirror
// raft.Core.handleInstallSnapshotRequest's staleness check EXACTLY, and
// that check rejects a snapshot at or below CommitIndex as well as one
// at or below SnapshotIndex (dynamic-membership plan §15/DM-10, the
// duplicated/delayed-message fault class in docs/failure-model.md).
//
// Before the fix, willAdvance consulted SnapshotIndex alone, so a
// delayed or duplicated MsgInstallSnapshotRequest whose LastIncludedIndex
// sat strictly between this node's SnapshotIndex and its CommitIndex was
// durably installed by the driver and then rejected by Core. The
// observable damage was total and permanent for that replica:
// WALStorage.InstallSnapshot deleted the committed entries in
// (LastIncludedIndex, CommitIndex], n.fsmachine was replaced by the older
// snapshot's state machine (every committed key vanished),
// n.appliedIndex rolled back below Core's own CommitIndex so those
// entries could never be re-applied (Core surfaces only NEWLY committed
// entries), and the node fail-stopped on the very next replicated entry
// with a non-contiguous append.
//
// The message crafted here is exactly what that fault class delivers: a
// well-formed, current-term InstallSnapshotRequest from the real leader
// carrying a boundary this follower has already replicated past.
func TestInstallSnapshotBelowCommitIndexIsNeverDurablyInstalled(t *testing.T) {
	tc := newTestCluster(t, 3)
	leaderID := tc.awaitLeader(5 * time.Second)
	leader := tc.node(leaderID)

	var followerID raft.NodeID
	for _, id := range tc.ids {
		if id != leaderID {
			followerID = id
			break
		}
	}
	follower := tc.node(followerID)

	const numKeys = 8
	outcomes := make([]fsm.Outcome, numKeys)
	for i := 0; i < numKeys; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("r%d", i), uint64(i+1), 0, fmt.Sprintf("k%d", i), "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose #%d: outcome=%+v err=%v", i, outcome, err)
		}
		outcomes[i] = outcome
	}
	awaitCondition(t, 5*time.Second, "follower applies every proposed key", func() bool {
		return uint64(follower.Status().AppliedIndex) >= outcomes[numKeys-1].CommitSeq
	})

	before := follower.Status()
	if before.SnapshotIndex != 0 {
		t.Fatalf("test setup: follower must not have snapshotted yet, got SnapshotIndex=%d", before.SnapshotIndex)
	}
	walFirstBefore := uint64(follower.walog.FirstIndex())

	// A boundary strictly above the follower's SnapshotIndex (0) and
	// strictly below its CommitIndex: exactly the window the old
	// predicate accepted and Core rejects.
	staleBoundary := uint64(before.CommitIndex) - 3
	if staleBoundary == 0 {
		t.Fatalf("test setup: CommitIndex %d too small to place a stale boundary strictly inside (0, CommitIndex)", before.CommitIndex)
	}

	// An EMPTY state machine at that boundary, so any rollback is
	// unmistakable rather than coincidentally equal to current state.
	emptyFSM := fsm.New(mvcc.NewStore())
	staleSnapshot := snapshot.Encode(snapshot.Meta{
		LastIncludedIndex: staleBoundary,
		LastIncludedTerm:  uint64(leader.Status().Term),
	}, emptyFSM, snapshot.MinReadVersion)

	leader.Transport().Send(raft.Message{
		Type:              raft.MsgInstallSnapshotRequest,
		From:              leaderID,
		To:                followerID,
		Term:              leader.Status().Term,
		LastIncludedIndex: raft.Index(staleBoundary),
		LastIncludedTerm:  leader.Status().Term,
		SnapshotData:      staleSnapshot,
	})

	// The stale message is accepted, answered, and otherwise ignored;
	// give the follower's event loop a bounded window to have processed
	// it before asserting nothing changed. Further writes past it are
	// what prove the node is still a working replica rather than merely
	// still running.
	for i := 0; i < 3; i++ {
		outcome, err := propose(t, leader, cmd(fmt.Sprintf("post%d", i), uint64(1000+i), 0, fmt.Sprintf("p%d", i), "v"), 3*time.Second)
		if err != nil || outcome.Status != fsm.StatusCommitted {
			t.Fatalf("Propose after the stale InstallSnapshot #%d: outcome=%+v err=%v", i, outcome, err)
		}
		awaitCondition(t, 3*time.Second, "follower applies a write issued after the stale InstallSnapshot", func() bool {
			return uint64(follower.Status().AppliedIndex) >= outcome.CommitSeq
		})
	}

	if err := follower.Err(); err != nil {
		t.Fatalf("follower fail-stopped after a stale InstallSnapshotRequest: %v", err)
	}
	if got := uint64(follower.walog.FirstIndex()); got != walFirstBefore {
		t.Fatalf("follower's durable log boundary moved to %d (was %d) for a snapshot Core rejected as stale — committed entries were discarded", got, walFirstBefore)
	}
	if got := follower.Status().SnapshotIndex; got != before.SnapshotIndex {
		t.Fatalf("follower's SnapshotIndex = %d, want %d (Core rejected the snapshot; the driver must not have adopted it either)", got, before.SnapshotIndex)
	}
	if got := follower.Status().CommitIndex; got < before.CommitIndex {
		t.Fatalf("follower's CommitIndex regressed: %d -> %d", before.CommitIndex, got)
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, ok, _ := follower.FSM().Store().Visible(key, outcomes[i].CommitSeq); !ok {
			t.Fatalf("committed key %s vanished from the follower's state machine after a stale InstallSnapshotRequest", key)
		}
	}
}
