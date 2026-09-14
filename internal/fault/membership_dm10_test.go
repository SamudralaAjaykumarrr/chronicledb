package fault

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// TestDM10_CombinedRandomizedMembershipSchedule is DM-10 (dynamic-
// membership plan §15): TestChaos_CombinedRandomizedSchedule's action
// space (elections, partitions, isolation/heal, crash/restart, message
// drop/duplicate/delay, local compaction) extended with membership
// changes (add-learner, promote-to-voter, remove-server), checked after
// every single step against the same election-safety/log-matching/
// commit-index sanity checks that test already applies, the existing
// committedOracle (COMMITTED-PREFIX-SAFETY across the extra membership
// traffic), and the new independent configLineageOracle
// (CONFIGURATION BRANCH CONFINEMENT — §2.3/§17's W1 and W2 — never a
// two-element live-set window, which DM-22 proves would raise false
// alarms on a state this very schedule can reach).
//
// Seed/step matrix: chaosSeeds (docs/testing-strategy.md §6.5's
// existing CHRONICLEDB_CHAOS_SEEDS knob) seeds at stepsPerRun each by
// default for ordinary CI speed; a larger local/nightly run scales via
// the same environment variable every other chaos suite in this package
// already uses, per §15's explicit instruction to reuse that discipline
// rather than invent a second one.
func TestDM10_CombinedRandomizedMembershipSchedule(t *testing.T) {
	seeds := chaosSeeds(10)
	const stepsPerRun = 250

	for seed := int64(4000); seed < 4000+int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runDM10Schedule(t, seed, stepsPerRun)
		})
	}
}

// dm10Pool is the fixed universe of extra node IDs the schedule may
// AddNode+AddLearner over the run, beyond the four founding voters.
var dm10Pool = []raft.NodeID{"e", "f", "g", "h"}

func runDM10Schedule(t *testing.T, seed int64, steps int) {
	t.Helper()
	peers := []raft.NodeID{"a", "b", "c", "d"}
	cl := NewCluster(peers, ClusterOptions{
		ElectionTimeoutTicks:       8,
		ElectionTimeoutJitterTicks: 6,
		HeartbeatTimeoutTicks:      2,
		Seed:                       seed,
	})
	sched := rand.New(rand.NewSource(seed ^ 0xd4710de5eed))

	committed := newCommittedOracle()
	lineage := newConfigLineageOracle(peers)
	seenLeaderForTerm := map[raft.Term]raft.NodeID{}
	addedToCluster := map[raft.NodeID]bool{}
	proposals := 0
	configProposals := 0
	// globalCommitFloor is a monotonically-increasing, run-lifetime
	// lower bound on "already committed cluster-wide": the highest
	// CommitIndex any live node has ever itself directly validated (by
	// majority ack as leader, or by a trusted leader's LeaderCommit as
	// follower — never wrong, only possibly stale for a lagging node).
	// It must never be allowed to regress just because the specific
	// node that reported it is later crashed/isolated — the underlying
	// fact ("a majority once held this entry") stays true regardless of
	// who currently remembers it, exactly like committedOracle's own
	// byIndex map is never pruned. Recomputing it fresh from only
	// currently-live nodes each checkpoint was this test's own first
	// bug: crashing the one node that had observed the highest commit
	// made the floor drop back down and made a lagging follower's
	// perfectly ordinary nextIndex catch-up look like several
	// simultaneously outstanding membership changes.
	var globalCommitFloor raft.Index

	// allLive returns every node id this schedule has ever introduced
	// (founding peers plus any AddNode'd pool member), which is exactly
	// cl.NodeIDs() at any instant — kept as its own helper only for
	// readability at call sites below.
	allLive := func() []raft.NodeID { return cl.NodeIDs() }

	checkInvariants := func() {
		for _, id := range cl.Leaders() {
			term := cl.Node(id).Core().CurrentTerm()
			if prev, ok := seenLeaderForTerm[term]; ok && prev != id {
				t.Fatalf("seed %d: RAFT-ELECTION-SAFETY violated: %s and %s both led term %d", seed, prev, id, term)
			}
			seenLeaderForTerm[term] = id
		}
		live := allLive()
		for _, id := range live {
			if cl.Node(id).Crashed() {
				continue
			}
			if ci := cl.Node(id).Core().CommitIndex(); ci > globalCommitFloor {
				globalCommitFloor = ci
			}
		}
		for i, a := range live {
			if cl.Node(a).Crashed() {
				continue
			}
			ea := entriesByIndex(cl.Node(a).Core())
			for _, b := range live[i+1:] {
				if cl.Node(b).Crashed() {
					continue
				}
				eb := entriesByIndex(cl.Node(b).Core())
				for idx, ea1 := range ea {
					eb1, ok := eb[idx]
					if !ok {
						continue
					}
					if ea1.Term == eb1.Term && (ea1.Type != eb1.Type || !bytes.Equal(ea1.Data, eb1.Data)) {
						t.Fatalf("seed %d: RAFT-LOG-MATCHING violated at index %d: %s=%+v %s=%+v", seed, idx, a, ea1, b, eb1)
					}
				}
			}
			core := cl.Node(a).Core()
			if int(core.CommitIndex()) > int(core.LastIndex()) {
				t.Fatalf("seed %d: node %s CommitIndex()=%d exceeds its own held history (LastIndex=%d)", seed, a, core.CommitIndex(), core.LastIndex())
			}
			// SERIALIZED MEMBERSHIP CHANGE, checked directly against this
			// node's own raw log rather than via Core's gates: no live
			// node may ever hold more than one EntryConfig entry above
			// globalCommitFloor (genuinely uncommitted anywhere in the
			// cluster, not merely not-yet-caught-up on this one node) at
			// once.
			outstanding := 0
			for idx := globalCommitFloor + 1; idx <= core.LastIndex(); idx++ {
				if e, ok := core.EntryAt(idx); ok && e.Type == raft.EntryConfig {
					outstanding++
				}
			}
			if outstanding > 1 {
				t.Fatalf("seed %d: SERIALIZED MEMBERSHIP CHANGE violated: node %s holds %d EntryConfig entries above the cluster-wide commit floor (%d) at once", seed, a, outstanding, globalCommitFloor)
			}
		}
		committed.observe(t, seed, cl)
		lineage.observe(t, seed, cl)
	}

	for i := 0; i < steps; i++ {
		switch sched.Intn(13) {
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
			// Query the real Crashed() signal directly rather than
			// shadowing it in a locally-tracked map: Crashed() is true
			// both for an explicit cl.Crash() and for a genuine
			// persistence failure (Node.Failed(), e.g. a duplicated/
			// mistimed message producing a non-contiguous append under
			// this harness's synchronous Storage model) — a locally
			// tracked "crashed" bool that only follows this schedule's
			// own Crash/Restart calls silently desyncs from reality the
			// first time a node fails on its own, permanently stranding
			// it (this schedule would keep choosing "Crash" over
			// "Restart" for it forever, since its own bookkeeping never
			// learns the node is already down) — found by DM-10 itself
			// via the very stall this caused.
			live := allLive()
			target := live[sched.Intn(len(live))]
			if cl.Node(target).Crashed() {
				cl.Restart(target)
			} else {
				cl.Crash(target)
			}
		case 6:
			live := allLive()
			switch sched.Intn(3) {
			case 0:
				a := live[sched.Intn(len(live))]
				var rest []raft.NodeID
				for _, p := range live {
					if p != a {
						rest = append(rest, p)
					}
				}
				cl.Partition([]raft.NodeID{a}, rest)
			case 1:
				cl.IsolateNode(live[sched.Intn(len(live))])
			default:
				cl.HealAll()
			}
		case 7:
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
			live := allLive()
			target := live[sched.Intn(len(live))]
			if !cl.Node(target).Crashed() {
				core := cl.Node(target).Core()
				if ci := core.CommitIndex(); ci > core.SnapshotIndex() {
					cl.Compact(target, ci)
				}
			}
		case 9:
			// AddLearner: bring a new pool node into the cluster's
			// transport (once) and propose adding it as a learner
			// against the current leader, if any.
			leaders := cl.Leaders()
			if len(leaders) != 1 {
				break
			}
			leader := leaders[0]
			cfg := cl.Node(leader).Core().ActiveConfig()
			var candidate raft.NodeID
			for _, id := range dm10Pool {
				if !cfg.IsMember(id) {
					candidate = id
					break
				}
			}
			if candidate == "" {
				break
			}
			if !addedToCluster[candidate] {
				cl.AddNode(candidate)
				addedToCluster[candidate] = true
			}
			configProposals++
			_ = cl.ProposeConfigChange(leader, raft.AddLearnerChange, fmt.Sprintf("dm10-add-%d-%d", seed, configProposals), candidate, string(candidate)+":0")
		case 10:
			// PromoteToVoter: pick a random current learner that has
			// actually caught up (matchIndex == leader's LastIndex()).
			// This mirrors internal/node.PromoteToVoter's own
			// PromotionMaxLagEntries==0 default (dynamic-membership plan
			// §3.3) — an operational, not safety, gate that lives above
			// Core and that this Core-only harness must apply itself,
			// exactly as any reasonable caller of Core.ProposeConfigChange
			// would. Skipping it does not risk a safety violation (§3.3:
			// "promoting a badly-lagging learner is not a safety
			// violation"), but it does risk exactly the documented
			// availability hazard: promoting a learner that has never
			// replicated anything raises the quorum size to 2-of-2 without
			// a second node that can ever ack, and if that leader has
			// meanwhile shrunk to being the sole voter, this can leave
			// nothing able to elect a future leader at all — the new
			// voter can never legitimately grant a vote for anyone since
			// its own activeConfig stays the zero value until it
			// receives the very entry only a leader it cannot elect could
			// send it. A real operator, via the real admin API, is
			// refused 425 before ever reaching this state; this schedule
			// mirrors that refusal instead of re-discovering the same
			// documented hazard under a different name.
			leaders := cl.Leaders()
			if len(leaders) != 1 {
				break
			}
			leader := leaders[0]
			core := cl.Node(leader).Core()
			cfg := core.ActiveConfig()
			var caughtUp []raft.NodeID
			for _, m := range cfg.Learners {
				if core.MatchIndexOf(m.ID) == core.LastIndex() {
					caughtUp = append(caughtUp, m.ID)
				}
			}
			if len(caughtUp) == 0 {
				break
			}
			target := caughtUp[sched.Intn(len(caughtUp))]
			configProposals++
			_ = cl.ProposeConfigChange(leader, raft.PromoteToVoterChange, fmt.Sprintf("dm10-promote-%d-%d", seed, configProposals), target, "")
		case 11:
			// RemoveServer: pick a random current learner freely, or a
			// current voter (self-removal of the leader included,
			// deliberately — DM-2/DM-14 cover it in isolation; here it
			// is one more event this randomized schedule may or may not
			// select) PROVIDED the resulting voter count would stay at
			// or above 3. Core's own MINIMUM VOTER INVARIANT (§17)
			// refuses to ever remove the last voter, and that refusal
			// is a legitimate outcome this schedule tolerates — but the
			// 3-voter floor here is a different thing: §12.2's own
			// "operator policy rule," which requires explicit
			// operator confirmation (confirmVoterCount) for any result
			// below 3 voters and lives entirely in internal/node, not
			// Core. This Core-only harness has no such caller above it
			// to supply that confirmation, so an unguarded schedule can
			// walk a cluster down to a 1- or 2-voter configuration with
			// no more care than a real operator's accidental,
			// unconfirmed keystroke would — and then a single further
			// crash at the wrong moment (the self-removed leader itself,
			// permanently barred from voting again by §2.7 Rule 1's
			// second clause) can legitimately, permanently wedge a
			// cluster that was never given the fault tolerance to
			// survive it. That is §12.1/§12.2's own documented,
			// "permitted, discouraged, warned" territory, not a new
			// correctness gap — DM-19 exercises confirmVoterCount itself
			// directly; this schedule instead stays out of that
			// territory, exactly as a caller who never passes
			// confirmVoterCount would.
			leaders := cl.Leaders()
			if len(leaders) != 1 {
				break
			}
			leader := leaders[0]
			cfg := cl.Node(leader).Core().ActiveConfig()
			var members []raft.NodeID
			for _, m := range cfg.Learners {
				members = append(members, m.ID)
			}
			if len(cfg.Voters) > 3 {
				for _, m := range cfg.Voters {
					members = append(members, m.ID)
				}
			}
			if len(members) == 0 {
				break
			}
			target := members[sched.Intn(len(members))]
			if target == leader {
				// Self-removal is the one case where the proposer itself
				// is about to become permanently unable to vote or lead
				// again (§2.7 Rule 1's second clause), so the remaining
				// voters' own catch-up status is critical for the
				// cluster's continued liveness in a way it is not for an
				// ordinary (non-self) removal, where the proposer stays
				// available afterward to keep leading/replicating. A
				// remaining voter that has not yet caught up (e.g. a
				// just-promoted-but-not-yet-widely-replicated learner,
				// legitimately possible since a commit only ever requires
				// a majority, never every replica) is temporarily unable
				// to vote or lead itself, exactly like the promotion gate
				// above — and if enough of the resulting C_new is
				// simultaneously in that temporary state, no live node
				// can ever again assemble a majority: found by DM-10
				// itself. A real operator asking a healthy leader to step
				// down via self-removal would naturally check this first;
				// this schedule mirrors that instead of re-discovering
				// the same hazard under a different name.
				core := cl.Node(leader).Core()
				allCaughtUp := true
				for _, m := range cfg.Voters {
					if m.ID != leader && core.MatchIndexOf(m.ID) != core.LastIndex() {
						allCaughtUp = false
						break
					}
				}
				if !allCaughtUp {
					break
				}
			}
			configProposals++
			_ = cl.ProposeConfigChange(leader, raft.RemoveServerChange, fmt.Sprintf("dm10-remove-%d-%d", seed, configProposals), target, "")
		default:
			// no-op step
		}
		checkInvariants()
	}

	// QUORUM CONTINUITY, end-of-run: heal every fault and give the
	// cluster a generous, bounded window to settle on a single leader
	// and make further progress, proving the randomized fault/
	// membership schedule never left the cluster permanently wedged.
	for _, id := range allLive() {
		if cl.Node(id).Crashed() {
			cl.Restart(id)
		}
	}
	cl.HealAll()
	var finalLeader raft.NodeID
	for i := 0; i < 300 && finalLeader == ""; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		if leaders := cl.Leaders(); len(leaders) == 1 {
			finalLeader = leaders[0]
		}
		checkInvariants()
	}
	if finalLeader == "" {
		t.Fatalf("seed %d: cluster never settled on a single leader after the randomized schedule, even fully healed", seed)
	}
	proposals++
	cl.Propose(finalLeader, []byte(fmt.Sprintf("final-%d", proposals)))
	for i := 0; i < 60; i++ {
		cl.AdvanceTicks(1)
		cl.DeliverEligible()
		checkInvariants()
	}
	finalCommit := cl.Node(finalLeader).Core().CommitIndex()
	if finalCommit == 0 {
		t.Fatalf("seed %d: cluster made no commit progress at all after settling", seed)
	}
}
