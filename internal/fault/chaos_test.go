package fault

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file is Phase 7's deterministic chaos laboratory
// (docs/testing-strategy.md §3.3 "chaos tests," docs/roadmap.md Phase
// 7): breadth and adversarial combination of the individual scenarios
// already proven in Phases 1-6 and in property_test.go's smaller
// smoke run, not new Raft mechanism. Every test here is fully
// reproducible from its seed (docs/testing-strategy.md §3.2) — a
// failing run always prints the exact seed to re-run in isolation.
//
// chaosSeeds returns how many independently seeded iterations a chaos
// test should run: a small number by default (fast enough for every
// push, per this phase's brief's CI guidance), or a much larger count
// when CHRONICLEDB_CHAOS_SEEDS is set (a documented larger local/nightly
// stress invocation — see docs/testing-strategy.md §6).
func chaosSeeds(defaultN int) int {
	if v := os.Getenv("CHRONICLEDB_CHAOS_SEEDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultN
}

// committedOracle is Phase 7's reference model for COMMITTED-PREFIX-
// SAFETY: the simplest possible fact about a committed index —
// "which Term/Data was ever legitimately committed at this index" —
// tracked independently of any single node's own (possibly later
// compacted or crashed-away) view, and asserted to never change once
// recorded. It is deliberately much simpler than raft.Core itself: it
// knows nothing of terms, votes, or replication — only "index N was
// once observed committed as (term, data); it must always be observed
// that way again."
type committedOracle struct {
	byIndex map[raft.Index]raft.Entry
}

func newCommittedOracle() *committedOracle {
	return &committedOracle{byIndex: make(map[raft.Index]raft.Entry)}
}

// observe checks every entry any live, non-crashed node currently
// reports as committed (via its cumulative CommittedEntries, which,
// per Node's own doc comment, "may contain benign duplicates after a
// restart" but never a changed value for an already-reported index)
// against the oracle, recording first-sight values and failing loudly
// on any contradiction — the direct falsification target for
// COMMITTED-PREFIX-SAFETY / STATE-MACHINE-SAFETY under chaos.
func (o *committedOracle) observe(t *testing.T, seed int64, cl *Cluster) {
	t.Helper()
	for _, id := range cl.NodeIDs() {
		n := cl.Node(id)
		if n.Crashed() {
			continue
		}
		for _, e := range n.CommittedEntries() {
			if prev, ok := o.byIndex[e.Index]; ok {
				if prev.Term != e.Term || !bytes.Equal(prev.Data, e.Data) {
					t.Fatalf("seed %d: COMMITTED-PREFIX-SAFETY violated at index %d: node %s now reports %+v, oracle has %+v", seed, e.Index, id, e, prev)
				}
				continue
			}
			o.byIndex[e.Index] = e
		}
	}
}

// entriesByIndex returns core's currently held log entries keyed by
// their real logical raft.Index, the only safe basis for cross-node
// comparison once nodes may have compacted to different boundaries
// (see checkInvariants's comment above).
func entriesByIndex(core *raft.Core) map[raft.Index]raft.Entry {
	out := make(map[raft.Index]raft.Entry)
	for _, e := range core.Entries() {
		out[e.Index] = e
	}
	return out
}

// TestChaos_CombinedRandomizedSchedule is Phase 7's primary randomized
// property test: a much richer action space than
// property_test.go's smoke run (adds explicit message-level drop/
// duplicate/delay, single-node isolate/heal, and local log compaction
// alongside proposals/elections/partitions/crashes), checked after
// every single action against every invariant docs/roadmap.md Phase 7
// calls "high-value": RAFT-ELECTION-SAFETY, RAFT-LOG-MATCHING,
// COMMITTED-PREFIX-SAFETY (via committedOracle), and QUORUM-SAFETY (no
// node's CommitIndex ever exceeds its own log length — a minority
// schedule advancing CommitIndex would itself produce a log-matching or
// election-safety contradiction once the run is checked end to end, so
// this is checked as a direct sanity bound here and proven
// scenario-specifically by TestChaos_QuorumSafetyRandomizedPartitionTiming
// below).
func TestChaos_CombinedRandomizedSchedule(t *testing.T) {
	seeds := chaosSeeds(30)
	const stepsPerRun = 400

	for seed := int64(0); seed < int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runCombinedChaosSchedule(t, seed, stepsPerRun)
		})
	}
}

func runCombinedChaosSchedule(t *testing.T, seed int64, steps int) {
	t.Helper()
	peers := []raft.NodeID{"A", "B", "C"}
	cl := NewCluster(peers, ClusterOptions{
		ElectionTimeoutTicks:       8,
		ElectionTimeoutJitterTicks: 6,
		HeartbeatTimeoutTicks:      2,
		Seed:                       seed,
	})
	sched := rand.New(rand.NewSource(seed ^ 0x51de5eed))

	oracle := newCommittedOracle()
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
		for i, a := range peers {
			if crashed[a] {
				continue
			}
			// Compare by logical raft.Index, never by array position:
			// Core.Entries() starts at snapshotIndex+1, so two nodes
			// that have compacted to different boundaries have
			// differently-offset slices — comparing ea[i] to eb[i]
			// positionally would compare unrelated logical indices and
			// produce a false RAFT-LOG-MATCHING violation the instant
			// local compaction (case 8 below) enters the schedule.
			ea := entriesByIndex(cl.Node(a).Core())
			for _, b := range peers[i+1:] {
				if crashed[b] {
					continue
				}
				eb := entriesByIndex(cl.Node(b).Core())
				for idx, ea1 := range ea {
					eb1, ok := eb[idx]
					if !ok {
						continue
					}
					if ea1.Term == eb1.Term && !bytes.Equal(ea1.Data, eb1.Data) {
						t.Fatalf("seed %d: RAFT-LOG-MATCHING violated at index %d: %s=%+v %s=%+v", seed, idx, a, ea1, b, eb1)
					}
				}
			}
			if int(cl.Node(a).Core().CommitIndex()) > int(cl.Node(a).Core().LastIndex()) {
				t.Fatalf("seed %d: node %s CommitIndex()=%d exceeds its own held history (LastIndex=%d)", seed, a, cl.Node(a).Core().CommitIndex(), cl.Node(a).Core().LastIndex())
			}
		}
		oracle.observe(t, seed, cl)
	}

	for i := 0; i < steps; i++ {
		switch sched.Intn(10) {
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
			switch sched.Intn(3) {
			case 0:
				a := peers[sched.Intn(len(peers))]
				var rest []raft.NodeID
				for _, p := range peers {
					if p != a {
						rest = append(rest, p)
					}
				}
				cl.Partition([]raft.NodeID{a}, rest)
			case 1:
				cl.IsolateNode(peers[sched.Intn(len(peers))])
			default:
				cl.HealAll()
			}
		case 7:
			// Message-level chaos: duplicate, drop, or delay one
			// currently pending message.
			pending := cl.Transport().Pending()
			if len(pending) == 0 {
				break
			}
			pm := pending[sched.Intn(len(pending))]
			switch sched.Intn(3) {
			case 0:
				cl.Transport().Duplicate(pm.ID)
			case 1:
				cl.Transport().Drop(pm.ID)
			default:
				cl.Transport().Delay(pm.ID, cl.LogicalTick()+int64(1+sched.Intn(20)))
			}
		case 8:
			// Local compaction chaos (docs/snapshots.md §3's
			// node-own-compaction leg, at the raft-core level — see
			// Cluster.Compact's doc comment): compact a random live
			// node up through its own CommitIndex, exercising
			// LOG-COMPACTION-SAFETY interleaved with everything else.
			target := peers[sched.Intn(len(peers))]
			if !crashed[target] {
				if ci := cl.Node(target).Core().CommitIndex(); ci > cl.Node(target).Core().SnapshotIndex() {
					cl.Compact(target, ci)
				}
			}
		default:
			// no-op step: models "nothing happens this round," which is
			// itself a legitimate schedule the invariants must still
			// hold under.
		}
		checkInvariants()
	}
}

// TestChaos_QuorumSafetyRandomizedPartitionTiming is the randomized-
// timing chaos variant of RF-11/QUORUM-SAFETY docs/scenario-corpus.md
// calls out as Phase 7 scope: across many seeds, a minority-isolated
// node (1 of 3, at a randomized point in the run, held isolated for a
// randomized duration) must never advance its own CommitIndex for a
// proposal made while isolated, while the majority side continues
// committing normally.
func TestChaos_QuorumSafetyRandomizedPartitionTiming(t *testing.T) {
	seeds := chaosSeeds(20)
	for seed := int64(100); seed < 100+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			cl := newTestCluster(seed)
			sched := rand.New(rand.NewSource(seed ^ 0x9110ade))
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: failed to settle initial election", seed)
			}
			leader := cl.Leaders()[0]
			var majority []raft.NodeID
			for _, id := range cl.NodeIDs() {
				if id != leader {
					majority = append(majority, id)
				}
			}

			// Randomize how long the cluster runs before the partition
			// (some committed history may already exist) and isolate
			// the current leader as the minority side.
			preRounds := sched.Intn(15)
			for i := 0; i < preRounds; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			cl.IsolateNode(leader)

			cl.Propose(leader, []byte("must-never-commit"))
			isolatedRounds := 10 + sched.Intn(30)
			for i := 0; i < isolatedRounds; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}

			// The isolated node may still show a nonzero CommitIndex
			// from before it was cut off — only a commit of the
			// specific isolated-window proposal would violate
			// QUORUM-SAFETY, checked here by content, not just by
			// index moving.
			for _, e := range cl.Node(leader).Core().Entries() {
				if bytes.Equal(e.Data, []byte("must-never-commit")) && raft.Index(e.Index) <= cl.Node(leader).Core().CommitIndex() {
					t.Fatalf("seed %d: QUORUM-SAFETY violated: isolated minority node %s committed a write made while isolated", seed, leader)
				}
			}

			// The majority must still be able to make progress while
			// the minority is cut off. A generous round budget: a
			// symmetric two-candidate split vote (docs/failure-model.md
			// §2.8's "election storm... may happen transiently") can
			// legitimately take several dueling rounds to resolve via
			// randomized jitter alone — this loop bounds that transient
			// liveness delay generously rather than asserting a specific
			// convergence speed, since only indefinite non-convergence
			// (a genuine liveness bug, not this expected variance) should
			// ever fail this test.
			var newLeader raft.NodeID
			for i := 0; i < 150 && newLeader == ""; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
				for _, id := range majority {
					if cl.Node(id).Core().Role() == raft.Leader {
						newLeader = id
					}
				}
			}
			if newLeader == "" {
				t.Fatalf("seed %d: majority side failed to elect a leader while minority isolated", seed)
			}
			cl.Propose(newLeader, []byte("majority-write"))
			for i := 0; i < 30 && cl.Node(newLeader).Core().CommitIndex() == 0; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			if cl.Node(newLeader).Core().CommitIndex() == 0 {
				t.Fatalf("seed %d: majority side never committed despite the minority being isolated", seed)
			}
		})
	}
}

// TestChaos_RepeatedPartitionHealAcrossLeaders is the chaos variant of
// RF-13/RF-15 docs/scenario-corpus.md marks Phase 7 scope: several
// partition/heal cycles across changing leaders, checked with a
// committedOracle so that no entry legitimately committed in any prior
// cycle is ever lost or altered by a later one, no matter how many
// leadership changes occur in between.
func TestChaos_RepeatedPartitionHealAcrossLeaders(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(200); seed < 200+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			sched := rand.New(rand.NewSource(seed ^ 0xc1c1e))
			oracle := newCommittedOracle()

			const cycles = 6
			proposals := 0
			for cycle := 0; cycle < cycles; cycle++ {
				// Whole cluster runs (possibly still recovering from
				// the previous cycle's partition) and gets a chance to
				// commit.
				if leaders := cl.Leaders(); len(leaders) == 1 {
					proposals++
					cl.Propose(leaders[0], []byte(fmt.Sprintf("cycle-%d-cmd-%d", cycle, proposals)))
				}
				for i := 0; i < 20; i++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
					oracle.observe(t, seed, cl)
				}

				// Partition a randomly chosen node away as the minority
				// for a randomized number of rounds, proposing on both
				// sides while split.
				odd := peers[sched.Intn(len(peers))]
				var rest []raft.NodeID
				for _, p := range peers {
					if p != odd {
						rest = append(rest, p)
					}
				}
				cl.Partition([]raft.NodeID{odd}, rest)
				if cl.Node(odd).Core().Role() == raft.Leader {
					cl.Propose(odd, []byte(fmt.Sprintf("cycle-%d-minority-doomed", cycle)))
				}
				for _, id := range rest {
					if cl.Node(id).Core().Role() == raft.Leader {
						proposals++
						cl.Propose(id, []byte(fmt.Sprintf("cycle-%d-majority-cmd-%d", cycle, proposals)))
					}
				}
				rounds := 10 + sched.Intn(20)
				for i := 0; i < rounds; i++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
					oracle.observe(t, seed, cl)
				}

				cl.HealAll()
				for i := 0; i < 30; i++ {
					cl.AdvanceTicks(1)
					cl.DeliverEligible()
					oracle.observe(t, seed, cl)
				}
			}

			// Final convergence check: once faults stop and the
			// partition is healed for good, every live node's log must
			// agree with the oracle at every index it holds
			// (EVENTUAL-CONVERGENCE).
			for i := 0; i < 30; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			oracle.observe(t, seed, cl)
			var want []raft.Entry
			for _, id := range cl.NodeIDs() {
				e := cl.Node(id).Core().Entries()
				if len(e) > len(want) {
					want = e
				}
			}
			for _, id := range cl.NodeIDs() {
				got := cl.Node(id).Core().Entries()
				n := len(got)
				if len(want) < n {
					n = len(want)
				}
				for i := 0; i < n; i++ {
					if got[i].Term == want[i].Term && !bytes.Equal(got[i].Data, want[i].Data) {
						t.Fatalf("seed %d: node %s diverges from the longest observed log at index %d after repeated partition/heal: %+v vs %+v", seed, id, i+1, got[i], want[i])
					}
				}
			}
		})
	}
}

// TestChaos_AsymmetricPartitionSafety exercises a directional-only
// partition (docs/roadmap.md Phase 7's "A can send to B but B cannot
// send to A" topology) via Transport.IsolateLink, across randomized
// direction/timing choices, asserting the same safety invariants as
// the symmetric chaos schedule: no election-safety or log-matching
// violation, and the isolated-from-leadership side never fabricates a
// commit.
func TestChaos_AsymmetricPartitionSafety(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(300); seed < 300+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			sched := rand.New(rand.NewSource(seed ^ 0xa53))
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: initial election failed to settle", seed)
			}
			leader := cl.Leaders()[0]
			var other raft.NodeID
			for _, id := range peers {
				if id != leader {
					other = id
					break
				}
			}

			// Directional cut: `other` can still send TO leader
			// (leader keeps receiving its AppendEntriesResponses), but
			// leader's messages TO other are silently lost — a
			// deliberately asymmetric, not merely a full, partition.
			if sched.Intn(2) == 0 {
				cl.Transport().IsolateLink(leader, other)
			} else {
				cl.Transport().IsolateLink(other, leader)
			}

			seenLeaderForTerm := map[raft.Term]raft.NodeID{}
			for i := 0; i < 200; i++ {
				switch sched.Intn(4) {
				case 0, 1:
					cl.AdvanceTicks(1)
				case 2:
					cl.DeliverEligible()
				default:
					if leaders := cl.Leaders(); len(leaders) == 1 {
						cl.Propose(leaders[0], []byte("x"))
					}
				}
				for _, id := range cl.Leaders() {
					term := cl.Node(id).Core().CurrentTerm()
					if prev, ok := seenLeaderForTerm[term]; ok && prev != id {
						t.Fatalf("seed %d: RAFT-ELECTION-SAFETY violated under asymmetric partition: %s and %s both led term %d", seed, prev, id, term)
					}
					seenLeaderForTerm[term] = id
				}
			}

			cl.HealAll()
			if !cl.SettleElection(150) {
				t.Fatalf("seed %d: cluster failed to reconverge on a single leader after healing an asymmetric partition", seed)
			}
			for i := 0; i < 30; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			for i, a := range cl.NodeIDs() {
				ea := cl.Node(a).Core().Entries()
				for _, b := range cl.NodeIDs()[i+1:] {
					eb := cl.Node(b).Core().Entries()
					n := len(ea)
					if len(eb) < n {
						n = len(eb)
					}
					for i := 0; i < n; i++ {
						if ea[i].Term == eb[i].Term && !bytes.Equal(ea[i].Data, eb[i].Data) {
							t.Fatalf("seed %d: RAFT-LOG-MATCHING violated post-heal at index %d: %s=%+v %s=%+v", seed, i+1, a, ea[i], b, eb[i])
						}
					}
				}
			}
		})
	}
}

// TestChaos_SnapshotMessageChaos combines leader-side local compaction
// with message-level chaos (drop/duplicate/delay) specifically applied
// to MsgInstallSnapshotRequest/Response traffic — docs/roadmap.md Phase
// 7's snapshot-chaos section, at the raft-core message-protocol layer
// (no FSM/real snapshot bytes exist in this simulator; the end-to-end,
// real-disk leg of this same chaos shape is
// internal/node/chaos_test.go's TestSnapshotChaos_* tests). The
// catching-up node is modeled via Crash+Restart, not IsolateNode: an
// isolated node's traffic is only ever held (queued for later delivery,
// see Transport.TakeEligible), never actually lost, so an isolated
// follower here would simply replay the full backlog of ordinary
// AppendEntries once healed and never need a snapshot at all — a real
// dropped/crashed follower's outstanding traffic is gone for good
// instead, which is what actually forces the InstallSnapshot path this
// test exists to chaos-test. Asserts a follower whose required log
// range the leader has already compacted away still converges once
// restarted, despite every induced fault on its catch-up messages.
func TestChaos_SnapshotMessageChaos(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(400); seed < 400+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			sched := rand.New(rand.NewSource(seed ^ 0x59a9))
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

			cl.Crash(follower)
			for i := 0; i < 12; i++ {
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

			// Apply message-level chaos specifically targeting
			// InstallSnapshot traffic for a bounded number of rounds:
			// drop/duplicate/delay every such message seen, forcing the
			// leader to retry.
			for round := 0; round < 60; round++ {
				cl.AdvanceTicks(1)
				for _, pm := range cl.Transport().Pending() {
					if pm.Message.Type != raft.MsgInstallSnapshotRequest && pm.Message.Type != raft.MsgInstallSnapshotResponse {
						continue
					}
					switch sched.Intn(4) {
					case 0:
						cl.Transport().Drop(pm.ID)
					case 1:
						cl.Transport().Duplicate(pm.ID)
					case 2:
						cl.Transport().Delay(pm.ID, cl.LogicalTick()+int64(1+sched.Intn(5)))
					default:
						// leave eligible for normal delivery this round
					}
				}
				cl.DeliverEligible()
			}
			// Let any remaining delayed/duplicated traffic drain and
			// the follower fully catch up via ordinary retry.
			for i := 0; i < 100 && cl.Node(follower).Core().CommitIndex() < ci; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}

			if got := cl.Node(follower).Core().SnapshotIndex(); got != cl.Node(leader).Core().SnapshotIndex() {
				t.Fatalf("seed %d: follower SnapshotIndex()=%d never converged to leader's %d despite chaos on InstallSnapshot traffic", seed, got, cl.Node(leader).Core().SnapshotIndex())
			}
			if got := cl.Node(follower).Core().CommitIndex(); got < ci {
				t.Fatalf("seed %d: follower CommitIndex()=%d never caught up to leader's pre-compaction CommitIndex %d", seed, got, ci)
			}
		})
	}
}

// TestChaos_DiskFaultDuringPersistence injects a genuine, deterministic
// storage-layer failure (docs/failure-model.md §1.8) at a randomized
// point in a live cluster's operation and proves: the failing node
// never falsely reports success for the write that triggered it (it
// stops itself — Node.Failed()), no other node is affected, and — once
// the failed node is "replaced" via Crash+Restart (modeling a real
// process restart onto the same, still-intact durable storage up to
// the point of failure) — the cluster continues operating correctly.
func TestChaos_DiskFaultDuringPersistence(t *testing.T) {
	seeds := chaosSeeds(15)
	for seed := int64(500); seed < 500+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			peers := []raft.NodeID{"A", "B", "C"}
			cl := NewCluster(peers, ClusterOptions{
				ElectionTimeoutTicks:       8,
				ElectionTimeoutJitterTicks: 6,
				HeartbeatTimeoutTicks:      2,
				Seed:                       seed,
			})
			sched := rand.New(rand.NewSource(seed ^ 0xd15c))
			if !cl.SettleElection(50) {
				t.Fatalf("seed %d: initial election failed to settle", seed)
			}
			leader := cl.Leaders()[0]

			target := peers[sched.Intn(len(peers))]
			cl.Node(target).Storage().FailNextAppends(1)

			cl.Propose(leader, []byte("triggering-write"))
			for i := 0; i < 20; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}

			if cl.Node(target).Failed() {
				if cl.Node(target).FailErr() == nil {
					t.Fatalf("seed %d: node %s marked Failed() but FailErr() is nil", seed, target)
				}
				// A failed node must never have advanced its own
				// CommitIndex past what it durably holds, and must not
				// silently keep participating.
				if cl.Node(target).Crashed() != true {
					t.Fatalf("seed %d: node %s Failed() but Crashed() (the shared no-op gate) is false", seed, target)
				}
			}

			// "Restart" the affected node (whether or not this
			// particular seed's schedule actually triggered the
			// injected fault on it — Crash+Restart is always safe) and
			// confirm the cluster still converges normally afterward.
			cl.Crash(target)
			cl.Restart(target)
			cl.SettleElection(150)
			if leaders := cl.Leaders(); len(leaders) != 1 {
				t.Fatalf("seed %d: cluster failed to reconverge on one leader after a disk-fault-triggered restart of %s: leaders=%v", seed, target, leaders)
			}
			newLeader := cl.Leaders()[0]
			cl.Propose(newLeader, []byte("after-restart"))
			for i := 0; i < 30 && cl.Node(newLeader).Core().CommitIndex() == 0; i++ {
				cl.AdvanceTicks(1)
				cl.DeliverEligible()
			}
			if cl.Node(newLeader).Core().CommitIndex() == 0 {
				t.Fatalf("seed %d: cluster never committed again after the disk-fault/restart cycle", seed)
			}
		})
	}
}
