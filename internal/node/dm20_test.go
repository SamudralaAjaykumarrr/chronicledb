package node

import (
	"context"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// runDM20FinalizationBoundarySchedule drives the exact deterministic
// schedule DM-20 needs (dynamic-membership plan §15, §23/F2): bring a
// three-node cluster to the point where the generation-2 finalize is
// committed and applied on the leader and on one follower ("other"),
// but not yet on a second follower F, which is kept isolated
// throughout that window; commit the first EntryConfig entry
// (AddLearner) the same way, via the leader and "other" alone; then
// heal F so its very next AppendEntriesRPC — the ordinary batched
// catch-up shape raft.Core.appendEntriesMessage always produces for a
// follower more than one entry behind (it sends everything from
// nextIndex through its own lastIndex in one message) — carries both
// the finalize entry and the EntryConfig entry together, with
// LeaderCommit already covering both. This forces F to persist the
// EntryConfig entry in the very processOutput pass in which it has not
// yet applied the finalize, i.e. while its own durable cluster
// generation is still 1 — the only way to reach that state at all,
// since ordinary in-order apply always adopts generation 2 (an earlier
// log entry) before ever applying a later EntryConfig.
//
// Returns F itself (still live and caught up) and the target entry's
// index, for the caller to inspect F's raw WAL record and then decide
// how to restart it.
func runDM20FinalizationBoundarySchedule(t *testing.T, learnerID raft.NodeID) (tc *testCluster, f *Node, fID raft.NodeID, entryIdx raft.Index) {
	t.Helper()
	tc = newTestCluster(t, 3)
	leader := tc.leaderNode(5 * time.Second)

	var other raft.NodeID
	for _, id := range tc.ids {
		if id == leader.cfg.ID {
			continue
		}
		if fID == "" {
			fID = id
		} else {
			other = id
		}
	}

	// Ready BEFORE isolating F, not after: Ready requires the leader to
	// have recorded a generation for every voter peer, and a generation
	// is only ever learned from a message this leader RECEIVES, so
	// isolating F first can strand it at Known=false permanently rather
	// than briefly (see awaitPrecheckReady's doc comment; that ordering
	// was a real -race flake here). Isolating afterwards does not
	// un-record it — PeerGenerationInfo has no recency requirement —
	// which is exactly what lets this schedule finalize while F is away.
	awaitPrecheckReady(t, leader, "before isolating F")

	tc.isolate(fID)
	requirePrecheckReadyNow(t, leader, "F's already-recorded generation must survive its isolation")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finalizeToMax(t, leader, ctx)
	awaitCondition(t, 5*time.Second, "the non-isolated follower converges on max generation", func() bool {
		return tc.node(other).Status().ClusterGeneration == leader.Status().MaxSupportedGeneration
	})

	outcome, err := leader.AddLearner(ctx, fsm.RequestID("dm20-add-"+string(learnerID)), learnerID, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("AddLearner: %v", err)
	}
	if outcome.Status != fsm.StatusCommitted {
		t.Fatalf("AddLearner outcome = %+v, want Committed", outcome)
	}
	entryIdx = raft.Index(outcome.CommitSeq)

	f = tc.node(fID)
	if got := f.Status().ClusterGeneration; got >= 2 {
		t.Fatalf("test setup: F already at generation %d while isolated, want still at 1", got)
	}
	if got := f.Status().LastIndex; got >= entryIdx {
		t.Fatalf("test setup: F already has entry %d while isolated, LastIndex=%d", entryIdx, got)
	}

	tc.heal(fID)
	awaitCondition(t, 5*time.Second, "F catches up past the EntryConfig entry", func() bool {
		return f.Status().AppliedIndex >= uint64(entryIdx)
	})

	return tc, f, fID, entryIdx
}

// readWALRecordPayload returns the raw, durable WAL payload bytes for
// n's log entry at idx, read directly from disk via internal/wal's own
// Replay iterator — never through the in-memory raft.Entry, which
// would mask any encoding defect (dynamic-membership plan §15 DM-20).
func readWALRecordPayload(t *testing.T, n *Node, idx raft.Index) []byte {
	t.Helper()
	it, err := n.walog.Replay(uint64(idx))
	if err != nil {
		t.Fatalf("Replay(%d): %v", idx, err)
	}
	defer it.Close()
	rec, ok, err := it.Next()
	if err != nil || !ok {
		t.Fatalf("expected a WAL record at index %d, ok=%v err=%v", idx, ok, err)
	}
	if rec.Index != uint64(idx) {
		t.Fatalf("Replay(%d) returned record at index %d", idx, rec.Index)
	}
	return rec.Payload
}

// hasTypedEntryHeader reports whether payload begins with the §6.1a
// typed-entry header (0xFF followed by raft.EntryConfig) at offset 8 —
// the exact byte-level shape DM-20 asserts, duplicated here rather than
// calling encodeEntryPayload/decodeEntryPayload so the check is
// independent of the code under test.
func hasTypedEntryHeader(payload []byte) bool {
	return len(payload) >= 10 && payload[8] == 0xFF && payload[9] == byte(raft.EntryConfig)
}

// TestDM20_EntryTypeSurvivesWALRoundTripAcrossFinalizationBoundary
// regresses DM-20 (§15, §23/F2): a follower that persists its first
// EntryConfig entry before it has itself applied the generation-2
// finalize must still durably write the typed-entry header — proven at
// the byte level, against the raw WAL record — and must restart
// cleanly with a byte-identical recovered configuration.
func TestDM20_EntryTypeSurvivesWALRoundTripAcrossFinalizationBoundary(t *testing.T) {
	tc, f, fID, entryIdx := runDM20FinalizationBoundarySchedule(t, "dm20-learner")

	payload := readWALRecordPayload(t, f, entryIdx)
	if !hasTypedEntryHeader(payload) {
		t.Fatalf("F's durable WAL payload for the EntryConfig entry at index %d does not begin with the typed-entry header (0xFF, EntryConfig): %x", entryIdx, payload)
	}

	preRestartConfig := f.core.ActiveConfig()
	if !preRestartConfig.IsMember("dm20-learner") {
		t.Fatal("test setup: F's pre-restart activeConfig does not include the learner it just caught up on")
	}

	tc.crash(fID)
	f2 := tc.restart(fID)

	if got := f2.core.ActiveConfig(); !got.Equal(preRestartConfig) {
		t.Fatalf("F's recovered activeConfig after restart = %+v, want byte-identical to its pre-restart one %+v", got, preRestartConfig)
	}

	select {
	case <-f2.Done():
		t.Fatalf("F failed on restart: %v", f2.Err())
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDM20_NegativeControlGenerationGatedEncodingFailsTheProof is
// DM-20's negative control (§15 step 5): re-running the identical
// schedule with the encoder gated on this node's durable cluster
// generation (revision 2's rejected §6.1a rule) instead of the entry's
// own type must make the byte-level assertion above fail, and must
// leave the restarted follower with a stale (pre-change) recovered
// configuration — proving the positive test actually exercises the
// defect it claims to guard, per §19 gate 3's discipline ("a regression
// test that cannot fail when the defect is reintroduced does not
// count").
func TestDM20_NegativeControlGenerationGatedEncodingFailsTheProof(t *testing.T) {
	testStripTypedHeaderOnStaleGeneration.Store(true)
	t.Cleanup(func() { testStripTypedHeaderOnStaleGeneration.Store(false) })

	tc, f, fID, entryIdx := runDM20FinalizationBoundarySchedule(t, "dm20-learner-neg")
	// The hook only needs to be active for F's own catch-up persist
	// above; clear it before anything else (including the restart
	// below, whose decode path never consults it anyway) so it cannot
	// leak into unrelated behavior.
	testStripTypedHeaderOnStaleGeneration.Store(false)

	payload := readWALRecordPayload(t, f, entryIdx)
	if hasTypedEntryHeader(payload) {
		t.Fatal("negative control: the typed-entry header survived with the encoder gated on durable generation instead of entry type — this negative control is not exercising the rejected rule")
	}

	// Isolate F before restarting it so the stale-configuration
	// assertion below is checked against F's own durable state alone,
	// before any fresh replication traffic from the still-live cluster
	// could reach it (matching DM-20's own "restart F from its durable
	// state alone" framing) — and deterministically, without racing a
	// heartbeat.
	tc.isolate(fID)
	tc.crash(fID)
	f2 := tc.restart(fID)

	if got := f2.core.ActiveConfig(); got.IsMember("dm20-learner-neg") {
		t.Fatalf("negative control: F's recovered configuration includes the learner despite the stripped typed header — expected a stale, pre-change configuration, got %+v", got)
	}
}
