package fault

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestProperty_RandomizedScheduleNeverViolatesSafety runs many
// independently seeded, randomized combinations of proposals,
// partitions, heals, crashes, and restarts against a 3-node cluster
// (docs/testing-strategy.md §3.1's "deterministic distributed
// simulation" / §1's "chaos tests," scaled down to a fast smoke run
// rather than a long-running soak — Phase 7 owns the full chaos
// campaign) and checks, after every single scheduled action, the
// invariants that must hold unconditionally:
//
//   - RAFT-ELECTION-SAFETY: never two leaders in the same term.
//   - Every node's log is a prefix-compatible history at every shared
//     index it and any other node both hold (RAFT-LOG-MATCHING): two
//     nodes agreeing on (index, term) never disagree on that index's
//     Data.
//   - No node's CommitIndex ever exceeds its own log length.
//
// A failing seed is immediately reproducible by re-running this test
// with that single seed (docs/testing-strategy.md §3.2).
func TestProperty_RandomizedScheduleNeverViolatesSafety(t *testing.T) {
	const seeds = 40
	const stepsPerRun = 200

	for seed := int64(0); seed < seeds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runRandomizedSchedule(t, seed, stepsPerRun)
		})
	}
}

func runRandomizedSchedule(t *testing.T, seed int64, steps int) {
	t.Helper()
	peers := []raft.NodeID{"A", "B", "C"}
	cl := NewCluster(peers, ClusterOptions{
		ElectionTimeoutTicks:       8,
		ElectionTimeoutJitterTicks: 6,
		HeartbeatTimeoutTicks:      2,
		Seed:                       seed,
	})
	sched := rand.New(rand.NewSource(seed ^ 0x5eed))

	seenLeaderForTerm := map[raft.Term]raft.NodeID{}
	proposals := 0
	crashed := map[raft.NodeID]bool{}

	checkInvariants := func() {
		for _, id := range cl.Leaders() {
			term := cl.Node(id).Core().CurrentTerm()
			if prev, ok := seenLeaderForTerm[term]; ok && prev != id {
				t.Fatalf("seed %d: RAFT-ELECTION-SAFETY violated: %s and %s both led term %d", seed, prev, id, term)
			}
			seenLeaderForTerm[term] = id
		}
		// Pairwise log-matching: any two nodes agreeing on (index,
		// term) must agree on Data at that index too.
		for i, a := range peers {
			if crashed[a] {
				continue
			}
			ea := cl.Node(a).Core().Entries()
			for _, b := range peers[i+1:] {
				if crashed[b] {
					continue
				}
				eb := cl.Node(b).Core().Entries()
				n := len(ea)
				if len(eb) < n {
					n = len(eb)
				}
				for i := 0; i < n; i++ {
					if ea[i].Term == eb[i].Term && !bytes.Equal(ea[i].Data, eb[i].Data) {
						t.Fatalf("seed %d: RAFT-LOG-MATCHING violated at index %d: %s=%+v %s=%+v", seed, i+1, a, ea[i], b, eb[i])
					}
				}
			}
			if int(cl.Node(a).Core().CommitIndex()) > len(ea) {
				t.Fatalf("seed %d: node %s CommitIndex()=%d exceeds its own log length %d", seed, a, cl.Node(a).Core().CommitIndex(), len(ea))
			}
		}
	}

	for i := 0; i < steps; i++ {
		switch sched.Intn(7) {
		case 0, 1:
			cl.AdvanceTicks(1)
		case 2, 3:
			cl.DeliverEligible()
		case 4:
			if leaders := cl.Leaders(); len(leaders) == 1 {
				proposals++
				cl.Propose(leaders[0], []byte(fmt.Sprintf("cmd-%d", proposals)))
			}
		case 5:
			target := peers[sched.Intn(len(peers))]
			if crashed[target] {
				cl.Restart(target)
				crashed[target] = false
			} else {
				cl.Crash(target)
				crashed[target] = true
			}
		case 6:
			if sched.Intn(2) == 0 {
				a := peers[sched.Intn(len(peers))]
				var rest []raft.NodeID
				for _, p := range peers {
					if p != a {
						rest = append(rest, p)
					}
				}
				cl.Partition([]raft.NodeID{a}, rest)
			} else {
				cl.HealAll()
			}
		}
		checkInvariants()
	}
}
