package node

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/oracle"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/raft"
)

// This file is Phase 10's model-based adversarial testing centerpiece
// (docs/roadmap.md Phase 10 "MODEL-BASED TESTING", "REFERENCE MODEL",
// "STATE DIGESTS"; docs/adversarial-testing.md): a deterministic,
// seeded history of writes, RequestID retries, conflicting writes,
// multi-key transactions, leader crashes/restarts, and partitions is
// generated and executed against a real, disk/TCP-backed three-node
// cluster (reusing testCluster from node_test.go), while an
// independent internal/oracle model predicts each write's commit/abort
// outcome and tracks RequestID terminal-outcome stability. Every
// history step's real outcome is checked against the model as it
// happens (not just at the end), and the model's final committed
// key/value digest is compared against the real cluster's own,
// computed via an identical canonical digest function
// (oracle.CanonicalKVDigest) so the comparison is byte-for-byte, not a
// spot check.
//
// This exercises genuinely different territory than Phase 7's chaos
// suites: those check Raft-level invariants (election safety, log
// matching, committed-prefix) via a committedOracle over raw log
// entries; this test checks the *application-visible* contract (which
// RequestID/CommitTxnCommand combinations commit vs. abort, and what
// the final key/value state is) against an oracle that knows nothing
// about Raft, WAL, or MVCC internals at all — only the documented
// external contract (docs/mvcc.md §4, docs/transactions.md §6-7).

func adversarialSeeds(defaultN int) int {
	if v := os.Getenv("CHRONICLEDB_ADVERSARIAL_SEEDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultN
}

// modelStep is one action the history generator can choose.
type modelStep int

const (
	stepFreshWrite modelStep = iota
	stepMultiKeyWrite
	stepStaleConflictWrite
	stepRetrySameRequest
	stepCrashLeader
	stepRestartCrashed
	stepIsolateRandom
	stepHealAll
	stepNumKinds
)

// TestModel_AdversarialHistoryAgainstIndependentOracle is Phase 10's
// primary model-based test: for each seed, a fresh 3-node real cluster
// runs a deterministic history of the step kinds above, comparing
// every write's real outcome against oracle.KVModel's independent
// prediction and every RequestID's outcome against
// oracle.OutcomeTracker, then verifies the model's final digest
// matches the real cluster's own once every live node has caught up.
func TestModel_AdversarialHistoryAgainstIndependentOracle(t *testing.T) {
	seeds := adversarialSeeds(4)
	const steps = 24
	const keyspace = 5

	for seedI := 0; seedI < seeds; seedI++ {
		seed := int64(seedI)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runModelHistory(t, seed, steps, keyspace)
		})
	}
}

// TestModel_Regression_Seed1707LeadershipRetryGap pins the exact
// history (steps=24, keyspace=5, matching
// TestModel_AdversarialHistoryAgainstIndependentOracle's own
// parameters) that first exposed a one-shot-retry gap in this file's
// own submitAndCheck/stepRetrySameRequest helpers: seed 1707 isolates
// the current leader immediately after healing a previously-isolated
// former leader that has not yet processed the message that would
// step it down, which can legitimately require more than one election
// — and so more than one client-side retry — to resolve (see
// proposeUntilResolved's doc comment). This is a test-harness
// regression test, not evidence of a ChronicleDB bug: both
// NotLeaderError and ErrLeadershipLost are the documented, honest
// signals in that window. Runs unconditionally, independent of
// CHRONICLEDB_ADVERSARIAL_SEEDS, so CI always exercises this exact
// history rather than relying on someone remembering to run the full
// high-seed-count campaign.
func TestModel_Regression_Seed1707LeadershipRetryGap(t *testing.T) {
	runModelHistory(t, 1707, 24, 5)
}

func runModelHistory(t *testing.T, seed int64, steps, keyspace int) {
	t.Helper()
	tc := newTestCluster(t, 3)
	rec := oracle.NewRecorder(seed)
	model := oracle.NewKVModel()
	tracker := oracle.NewOutcomeTracker()
	sched := rand.New(rand.NewSource(seed ^ 0x0ad7e5a1))

	var watermark uint64 // highest CommitSeq observed committed so far
	var down raft.NodeID // at most one node crashed or isolated at a time
	var isolated bool
	type submitted struct {
		cmd fsm.CommitTxnCommand
		out fsm.Outcome
	}
	var everCommitted []submitted

	fail := func(format string, args ...interface{}) {
		t.Fatalf("%s\n\n%s", fmt.Sprintf(format, args...), rec.Tail(steps))
	}

	// currentLeader requires exactly one live, non-excluded node to
	// report Role==Leader before returning it (mirroring testCluster's
	// own awaitLeader) rather than returning the first match: right
	// after a heal/restart there is a real, brief window where a
	// stale former leader has not yet processed the message that would
	// step it down, and picking it would turn an entirely expected
	// transient "leadership lost while request was pending" into a
	// false test failure — a test-timing concern, not a ChronicleDB
	// correctness bug (see the project's own "test timing flakiness"
	// lesson for the same class of issue around snapshot creation).
	currentLeader := func(timeout time.Duration) (raft.NodeID, bool) {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			var found raft.NodeID
			count := 0
			for _, id := range tc.ids {
				if isolated && id == down {
					continue
				}
				n := tc.node(id)
				if n == nil {
					continue
				}
				if n.Status().Role == raft.Leader {
					found = id
					count++
				}
			}
			if count == 1 {
				return found, true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return "", false
	}

	statusOf := func(id raft.NodeID) (term uint64, role string) {
		n := tc.node(id)
		if n == nil {
			return 0, "crashed"
		}
		st := n.Status()
		return uint64(st.Term), st.Role.String()
	}

	// proposeUntilResolved submits c, retrying against whatever leader
	// currently exists each time the attempt fails, until it either
	// succeeds or an overall wall-clock deadline elapses. This is not
	// an arbitrary retry-count cap: NotLeaderError/ErrLeadershipLost/
	// ErrProposalSuperseded (internal/node/errors.go) are all documented
	// as honest "outcome unknown, retry by RequestID against the current
	// leader" signals with no promise that a *single* retry resolves
	// them — back-to-back faults in this history (e.g. isolating the
	// current leader before a just-healed former leader has processed
	// the message that steps it down) can legitimately need more than
	// one election to settle. A fixed one-shot retry treats that
	// entirely correct, honest behavior as a test failure; bounding by
	// a wall-clock deadline instead (reusing currentLeader's existing
	// deterministic poll-for-exactly-one-leader wait, never a bare
	// sleep) mirrors how a real client with a context deadline would
	// behave, and still fails for a genuine liveness gap once the
	// deadline is exhausted.
	proposeUntilResolved := func(kind string, c fsm.CommitTxnCommand, overall time.Duration) (outcome fsm.Outcome, leaderID raft.NodeID, term uint64, role string, err error) {
		deadline := time.Now().Add(overall)
		for attempt := 1; ; attempt++ {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return outcome, leaderID, term, role, err
			}
			waitFor := remaining
			if waitFor > 3*time.Second {
				waitFor = 3 * time.Second
			}
			var ok bool
			leaderID, ok = currentLeader(waitFor)
			if !ok {
				if err == nil {
					err = fmt.Errorf("no leader available")
				}
				continue
			}
			term, role = statusOf(leaderID)
			ctxTimeout := time.Until(deadline)
			if ctxTimeout > 3*time.Second {
				ctxTimeout = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
			outcome, err = tc.node(leaderID).Propose(ctx, c)
			cancel()
			op := kind
			if attempt > 1 {
				op = kind + fmt.Sprintf("(retry#%d)", attempt-1)
			}
			var outcomeStr string
			switch {
			case err != nil:
				outcomeStr = "err:" + err.Error()
			case outcome.Status == fsm.StatusCommitted:
				outcomeStr = fmt.Sprintf("COMMITTED(seq=%d)", outcome.CommitSeq)
			default:
				outcomeStr = fmt.Sprintf("ABORTED(key=%s)", outcome.ConflictKey)
			}
			rec.Record(oracle.Step{
				Node: string(leaderID), Term: term, Role: role,
				RequestID: string(c.RequestID), Op: op,
				Args:    fmt.Sprintf("startSeq=%d muts=%v", c.StartSeq, toOracleMutations(c.Mutations)),
				Outcome: outcomeStr,
			})
			if err == nil {
				return outcome, leaderID, term, role, nil
			}
		}
	}

	submitAndCheck := func(kind string, c fsm.CommitTxnCommand, wantCommit bool, conflictKey string) {
		outcome, _, _, _, err := proposeUntilResolved(kind, c, 15*time.Second)
		if err != nil {
			fail("seed %d step: %s failed to resolve against any leader within the retry deadline: %v", seed, kind, err)
		}

		muts := toOracleMutations(c.Mutations)
		fp := oracle.Fingerprint(c.TxnID, c.StartSeq, muts)

		committed := outcome.Status == fsm.StatusCommitted
		if err := tracker.Observe(string(c.RequestID), fp, oracle.RecordedOutcome{
			Committed: committed, CommitSeq: outcome.CommitSeq, ConflictKey: outcome.ConflictKey,
		}, kind); err != nil {
			fail("seed %d: %v", seed, err)
		}

		if wantCommit != committed {
			fail("seed %d: %s: real outcome committed=%v (%+v), oracle predicted committed=%v (conflictKey=%q) — MISMATCH between ChronicleDB and the independent model",
				seed, kind, committed, outcome, wantCommit, conflictKey)
		}
		if committed {
			model.Apply(outcome.CommitSeq, muts)
			if outcome.CommitSeq > watermark {
				watermark = outcome.CommitSeq
			}
			everCommitted = append(everCommitted, submitted{cmd: c, out: outcome})
		}
	}

	reqSeq := 0
	nextReqID := func() string {
		reqSeq++
		return fmt.Sprintf("model-seed%d-req%d", seed, reqSeq)
	}

	for i := 0; i < steps; i++ {
		chosen := modelStep(sched.Intn(int(stepNumKinds)))
		switch chosen {
		case stepFreshWrite, stepMultiKeyWrite:
			nMuts := 1
			if chosen == stepMultiKeyWrite {
				nMuts = 2
			}
			var muts []mvcc.Mutation
			for j := 0; j < nMuts; j++ {
				key := fmt.Sprintf("key-%d", sched.Intn(keyspace))
				val := fmt.Sprintf("v-%d-%d", i, j)
				muts = append(muts, mvcc.Mutation{Key: key, Value: []byte(val)})
			}
			c := fsm.CommitTxnCommand{
				RequestID: fsm.RequestID(nextReqID()),
				TxnID:     uint64(i + 1),
				StartSeq:  watermark,
				Mutations: muts,
			}
			wantCommit, _, _ := model.Predict(c.StartSeq, toOracleMutations(muts))
			submitAndCheck("write", c, wantCommit, "")

		case stepStaleConflictWrite:
			// Pick a key the model already has a committed version for,
			// and deliberately submit at StartSeq=0 (guaranteed stale
			// once anything has committed) to force a predictable
			// CONFLICT-CORRECTNESS check.
			keys := model.Keys()
			if len(keys) == 0 {
				continue
			}
			key := keys[sched.Intn(len(keys))]
			c := fsm.CommitTxnCommand{
				RequestID: fsm.RequestID(nextReqID()),
				TxnID:     uint64(1000 + i),
				StartSeq:  0,
				Mutations: []mvcc.Mutation{{Key: key, Value: []byte(fmt.Sprintf("stale-%d", i))}},
			}
			wantCommit, conflictKey, _ := model.Predict(c.StartSeq, toOracleMutations(c.Mutations))
			submitAndCheck("stale-conflict-write", c, wantCommit, conflictKey)

		case stepRetrySameRequest:
			if len(everCommitted) == 0 {
				continue
			}
			prev := everCommitted[sched.Intn(len(everCommitted))]
			outcome, _, _, _, err := proposeUntilResolved("retry", prev.cmd, 15*time.Second)
			if err != nil {
				fail("seed %d: retry of already-committed RequestID %s failed to resolve against any leader within the retry deadline: %v", seed, prev.cmd.RequestID, err)
			}
			if outcome.CommitSeq != prev.out.CommitSeq || outcome.Status != prev.out.Status {
				fail("seed %d: REQUEST-OUTCOME-STABILITY violated: RequestID %s originally %+v, retry now %+v",
					seed, prev.cmd.RequestID, prev.out, outcome)
			}
			fp := oracle.Fingerprint(prev.cmd.TxnID, prev.cmd.StartSeq, toOracleMutations(prev.cmd.Mutations))
			if err := tracker.Observe(string(prev.cmd.RequestID), fp, oracle.RecordedOutcome{
				Committed: true, CommitSeq: outcome.CommitSeq,
			}, "retry"); err != nil {
				fail("seed %d: %v", seed, err)
			}

		case stepCrashLeader:
			if down != "" {
				continue
			}
			leaderID, ok := currentLeader(3 * time.Second)
			if !ok {
				continue
			}
			tc.crash(leaderID)
			down = leaderID
			rec.Record(oracle.Step{Node: string(leaderID), Op: "crash", Fault: "kill"})

		case stepRestartCrashed:
			if down == "" || isolated {
				continue
			}
			tc.restart(down)
			rec.Record(oracle.Step{Node: string(down), Op: "restart"})
			down = ""

		case stepIsolateRandom:
			if down != "" {
				continue
			}
			target := tc.ids[sched.Intn(len(tc.ids))]
			tc.isolate(target)
			down = target
			isolated = true
			rec.Record(oracle.Step{Node: string(target), Op: "isolate", Fault: "partition"})

		case stepHealAll:
			if down == "" || !isolated {
				continue
			}
			tc.heal(down)
			rec.Record(oracle.Step{Node: string(down), Op: "heal"})
			down = ""
			isolated = false
		}
	}

	// Heal/restart anything still down, then wait for every node to
	// converge on the model's own watermark before the final digest
	// comparison — a premature comparison against a still-lagging node
	// would be a test-timing false positive, not a real divergence.
	if down != "" {
		if isolated {
			tc.heal(down)
		} else {
			tc.restart(down)
		}
		down = ""
		isolated = false
	}
	if _, ok := currentLeader(5 * time.Second); !ok {
		fail("seed %d: cluster failed to settle on a leader after final heal/restart", seed)
	}

	wantDigest := model.Digest()
	keys := model.Keys()
	for _, id := range tc.ids {
		id := id
		awaitCondition(t, 5*time.Second, fmt.Sprintf("node %s catches up to watermark %d", id, watermark), func() bool {
			n := tc.node(id)
			return n != nil && uint64(n.Status().AppliedIndex) >= watermark
		})
		n := tc.node(id)
		gotDigest := oracle.CanonicalKVDigest(keys, func(k string) ([]byte, bool) {
			return n.FSM().Store().Visible(k, uint64(n.Status().AppliedIndex))
		})
		if gotDigest != wantDigest {
			fail("seed %d: node %s final committed-state digest %s != oracle model digest %s (STATE-MACHINE-SAFETY / model divergence)",
				seed, id, gotDigest, wantDigest)
		}
	}
}

func toOracleMutations(muts []mvcc.Mutation) []oracle.KVMutation {
	out := make([]oracle.KVMutation, len(muts))
	for i, m := range muts {
		out[i] = oracle.KVMutation{Key: m.Key, Value: m.Value, Tombstone: m.Tombstone}
	}
	return out
}
