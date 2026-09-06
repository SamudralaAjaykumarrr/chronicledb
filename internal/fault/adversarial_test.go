package fault

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file extends Phase 7's chaos laboratory (chaos_test.go) with
// Phase 10's deeper, more targeted adversarial Raft scenarios
// (docs/roadmap.md Phase 10 "ADVERSARIAL RAFT TESTING",
// "ADVERSARIAL RECOVERY"). These are deliberately different shapes
// than chaos_test.go's own suites, not a re-run of them: each test
// below engineers one specific, named cross-mechanism interaction
// (stale pre-snapshot messages, repeated compact/restart cycles,
// leader turnover landing exactly on a snapshot boundary) rather than
// a broad randomized action mix.

// TestChaos_StaleAppendEntriesAfterSnapshotInstall engineers exactly
// the scenario internal/raft/adversarial_test.go's
// TestStaleAppendEntriesBelowSnapshotBoundaryReportsMatchIndexWithoutMutation
// proves at the single-Core level, but end to end across a real
// cluster: an AppendEntriesRequest sent to a follower is deliberately
// held back (Transport.Delay) while that follower is crashed (not
// merely isolated — an isolated node's held traffic would just be
// replayed via ordinary AppendEntries once healed, exactly the reason
// chaos_test.go's own TestChaos_SnapshotMessageChaos documents for
// using Crash+Restart to force a genuine InstallSnapshot need), falls
// behind past a compacted boundary, and catches up via a real
// InstallSnapshot once restarted — then the long-held stale message is
// force-delivered (Transport.Take via Cluster.Deliver) well after the
// snapshot install completed. Asserts: no panic, no RAFT-LOG-MATCHING
// violation, the follower's state is unaffected by the stale message,
// and the cluster continues operating normally afterward.
func TestChaos_StaleAppendEntriesAfterSnapshotInstall(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(600); seed < 600+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			sched := rand.New(rand.NewSource(seed ^ 0x57a1e))
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: initial election failed to settle", seed)
			}
			leader := cl.Leaders()[0]
			var follower raft.NodeID
			for _, id := range peers {
				if id != leader {
					follower = id
					break
				}
			}

			// Propose one entry and capture (without delivering) the
			// AppendEntries this produces toward follower — this is the
			// message that will later become stale.
			cl.Propose(leader, []byte("entry-that-becomes-stale"))
			var staleID uint64
			var found bool
			for _, pm := range cl.Transport().Pending() {
				if pm.Message.To == follower && pm.Message.Type == raft.MsgAppendEntriesRequest {
					staleID = pm.ID
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("seed %d: expected a pending AppendEntries to %s right after Propose", seed, follower)
			}
			// Hold it aside indefinitely (far future tick) so ordinary
			// DeliverEligible sweeps never pick it up on their own —
			// this specific message must survive everything below
			// untouched, to be force-delivered stale at the very end.
			cl.Transport().Delay(staleID, cl.LogicalTick()+1_000_000)

			// Crash follower outright (see doc comment above for why
			// IsolateNode would not force a genuine InstallSnapshot
			// here) so it receives nothing further while down, forcing
			// it to fall behind past what the leader is about to
			// compact away.
			cl.Crash(follower)
			for i := 0; i < 15; i++ {
				cl.Propose(leader, []byte(fmt.Sprintf("k%d", i)))
				for j := 0; j < 10; j++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
				}
			}
			ci := cl.Node(leader).Core().CommitIndex()
			if ci == 0 {
				t.Fatalf("seed %d: leader failed to commit anything while follower was down", seed)
			}
			if !cl.Compact(leader, ci) {
				t.Fatalf("seed %d: leader failed to compact through its own CommitIndex %d", seed, ci)
			}

			cl.Restart(follower)
			for i := 0; i < 100 && cl.Node(follower).Core().SnapshotIndex() != cl.Node(leader).Core().SnapshotIndex(); i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			if got := cl.Node(follower).Core().SnapshotIndex(); got != cl.Node(leader).Core().SnapshotIndex() {
				t.Fatalf("seed %d: follower never caught up via snapshot install (SnapshotIndex=%d, want %d)", seed, got, cl.Node(leader).Core().SnapshotIndex())
			}

			beforeLast := cl.Node(follower).Core().LastIndex()
			beforeCommit := cl.Node(follower).Core().CommitIndex()
			beforeSnap := cl.Node(follower).Core().SnapshotIndex()

			// Force-deliver the long-held stale message now, well after
			// the follower has moved past it via a real snapshot
			// install — this must be a safe no-op, whichever of Drop,
			// deliver-as-is, or an occasional Duplicate first the
			// schedule picks.
			if sched.Intn(2) == 0 {
				cl.Deliver(staleID)
			} else {
				if dupID, ok := cl.Transport().Duplicate(staleID); ok {
					cl.Deliver(dupID)
				}
				cl.Deliver(staleID)
			}

			if cl.Node(follower).Core().LastIndex() != beforeLast ||
				cl.Node(follower).Core().CommitIndex() != beforeCommit ||
				cl.Node(follower).Core().SnapshotIndex() != beforeSnap {
				t.Fatalf("seed %d: stale pre-snapshot AppendEntries mutated follower state: last %d->%d commit %d->%d snap %d->%d",
					seed, beforeLast, cl.Node(follower).Core().LastIndex(),
					beforeCommit, cl.Node(follower).Core().CommitIndex(),
					beforeSnap, cl.Node(follower).Core().SnapshotIndex())
			}

			// The cluster must still make normal progress afterward.
			for i := 0; i < 30; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			finalLeader := cl.Leaders()
			if len(finalLeader) != 1 {
				t.Fatalf("seed %d: expected exactly one leader after the stale-message episode, got %v", seed, finalLeader)
			}
			cl.Propose(finalLeader[0], []byte("after-stale-episode"))
			for i := 0; i < 30 && cl.Node(follower).Core().CommitIndex() < cl.Node(finalLeader[0]).Core().CommitIndex(); i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			if cl.Node(follower).Core().CommitIndex() < ci {
				t.Fatalf("seed %d: follower regressed or failed to keep up after the stale-message episode", seed)
			}
		})
	}
}

// TestChaos_RepeatedCompactRestartCycle is the exact "Repeated compact
// / restart" adversarial pattern docs/roadmap.md Phase 10 names:
// snapshot -> compact -> restart -> append -> snapshot -> compact ->
// restart, several times over, verifying every index
// (SnapshotIndex/LastIndex/CommitIndex, and every live node's log
// content by logical index) stays globally correct throughout —
// LOG-COMPACTION-SAFETY and SNAPSHOT-SAFETY under repetition, not just
// a single compact/restart pair (which chaos_test.go's own
// TestChaos_CombinedRandomizedSchedule exercises only incidentally,
// mixed with many other fault kinds, not in this specific repeated
// shape).
func TestChaos_RepeatedCompactRestartCycle(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(700); seed < 700+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			sched := rand.New(rand.NewSource(seed ^ 0x6099ac7))
			oracle := newCommittedOracle()
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: initial election failed to settle", seed)
			}

			const cycles = 5
			proposals := 0
			for cycle := 0; cycle < cycles; cycle++ {
				leaders := cl.Leaders()
				if len(leaders) != 1 {
					// Give the cluster a chance to reconverge before
					// this cycle's own work, rather than failing on a
					// merely-transient gap between cycles.
					for i := 0; i < 60 && len(cl.Leaders()) != 1; i++ {
						cl.AdvanceTicks(1)
						cl.DeliverEligible()
					}
					leaders = cl.Leaders()
				}
				if len(leaders) != 1 {
					t.Fatalf("seed %d cycle %d: cluster failed to hold a single leader", seed, cycle)
				}
				leader := leaders[0]

				for i := 0; i < 8; i++ {
					proposals++
					cl.Propose(leader, []byte(fmt.Sprintf("cycle%d-cmd%d", cycle, proposals)))
				}
				for i := 0; i < 40; i++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
					oracle.observe(t, seed, cl)
				}

				// Compact every live node up through its own CommitIndex
				// (not just the leader's), then restart a randomly chosen
				// live node to prove the compacted boundary survives a
				// real restart before the next cycle piles more on top.
				for _, id := range peers {
					n := cl.Node(id)
					if n.Crashed() {
						continue
					}
					if ci := n.Core().CommitIndex(); ci > n.Core().SnapshotIndex() {
						cl.Compact(id, ci)
					}
				}
				restartTarget := peers[sched.Intn(len(peers))]
				cl.Crash(restartTarget)
				cl.Restart(restartTarget)

				for i := 0; i < 60 && len(cl.Leaders()) != 1; i++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
					oracle.observe(t, seed, cl)
				}

				// Global index sanity after every cycle: SnapshotIndex
				// never exceeds CommitIndex, CommitIndex never exceeds
				// LastIndex, for every live node.
				for _, id := range peers {
					n := cl.Node(id)
					if n.Crashed() {
						continue
					}
					c := n.Core()
					if c.SnapshotIndex() > c.CommitIndex() {
						t.Fatalf("seed %d cycle %d: node %s SnapshotIndex=%d > CommitIndex=%d", seed, cycle, id, c.SnapshotIndex(), c.CommitIndex())
					}
					if c.CommitIndex() > c.LastIndex() {
						t.Fatalf("seed %d cycle %d: node %s CommitIndex=%d > LastIndex=%d", seed, cycle, id, c.CommitIndex(), c.LastIndex())
					}
				}
			}

			// Final convergence: every live node's log must agree with
			// the oracle's committed history at every index it still
			// physically holds, after repeated compaction/restart.
			for i := 0; i < 60; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			oracle.observe(t, seed, cl)
			for _, id := range peers {
				n := cl.Node(id)
				if n.Crashed() {
					continue
				}
				for _, e := range n.Core().Entries() {
					if want, ok := oracle.byIndex[e.Index]; ok {
						if want.Term != e.Term || !bytes.Equal(want.Data, e.Data) {
							t.Fatalf("seed %d: node %s diverges from the committed-history oracle at index %d after repeated compact/restart: %+v vs %+v", seed, id, e.Index, e, want)
						}
					}
				}
			}
		})
	}
}

// TestChaos_LeaderChangeImmediatelyAfterSnapshot engineers "leader
// change immediately after snapshot" (docs/roadmap.md Phase 10): the
// current leader compacts its own log and is crashed in the very same
// round, before any follower necessarily knows about the new snapshot
// boundary, forcing the ensuing election and the new leader's own
// catch-up handling of a peer that may be at, or need to be brought to,
// that same boundary. Checked against the committedOracle for
// LEADER-COMPLETENESS: nothing legitimately committed before the crash
// is ever lost.
func TestChaos_LeaderChangeImmediatelyAfterSnapshot(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(800); seed < 800+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			oracle := newCommittedOracle()
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: initial election failed to settle", seed)
			}
			leader := cl.Leaders()[0]

			for i := 0; i < 10; i++ {
				cl.Propose(leader, []byte(fmt.Sprintf("pre-snap-%d", i)))
			}
			for i := 0; i < 30; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
				oracle.observe(t, seed, cl)
			}
			ci := cl.Node(leader).Core().CommitIndex()
			if ci == 0 {
				t.Fatalf("seed %d: leader committed nothing before the snapshot", seed)
			}

			// Compact the leader's log, then crash it in the same
			// "round" — no further ticks/delivery happens between the
			// compaction and the crash, so no follower has necessarily
			// even seen an updated leaderCommit reflecting the very
			// latest pre-crash state.
			if !cl.Compact(leader, ci) {
				t.Fatalf("seed %d: leader failed to compact through CommitIndex %d", seed, ci)
			}
			cl.Crash(leader)

			var newLeader raft.NodeID
			for i := 0; i < 150 && newLeader == ""; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
				oracle.observe(t, seed, cl)
				for _, id := range peers {
					if id == leader {
						continue
					}
					if cl.Node(id).Core().Role() == raft.Leader {
						newLeader = id
					}
				}
			}
			if newLeader == "" {
				t.Fatalf("seed %d: no new leader emerged after the old leader crashed immediately post-snapshot", seed)
			}

			// The new leader's own committed history must include
			// everything the oracle ever recorded as committed pre-crash
			// — LEADER-COMPLETENESS across a snapshot boundary. A
			// snapshot-compacted index is no longer present as a live
			// Entry, so this is checked via the oracle's own
			// independent record plus the new leader's ability to keep
			// committing further entries without ever regressing.
			cl.Propose(newLeader, []byte("post-crash-write"))
			for i := 0; i < 60 && cl.Node(newLeader).Core().CommitIndex() <= ci; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
				oracle.observe(t, seed, cl)
			}
			if cl.Node(newLeader).Core().CommitIndex() <= ci {
				t.Fatalf("seed %d: new leader never advanced past the pre-crash CommitIndex %d", seed, ci)
			}
		})
	}
}
