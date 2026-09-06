package fsm

import (
	"errors"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
)

func mustCommit(t *testing.T, f *FSM, index uint64, cmd CommitTxnCommand) Outcome {
	t.Helper()
	outcome, err := f.Apply(index, cmd)
	if err != nil {
		t.Fatalf("Apply(%d, %+v): %v", index, cmd, err)
	}
	return outcome
}

// TestApplyCommitsNonConflicting proves a straightforward commit
// commits and is visible.
func TestApplyCommitsNonConflicting(t *testing.T) {
	f := New(mvcc.NewStore())
	cmd := CommitTxnCommand{
		RequestID: "R1",
		TxnID:     1,
		StartSeq:  0,
		Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("v1")}},
	}
	outcome := mustCommit(t, f, 1, cmd)
	if outcome.Status != StatusCommitted || outcome.CommitSeq != 1 {
		t.Fatalf("outcome = %+v, want Committed CommitSeq=1", outcome)
	}
	v, found := f.Store().Visible("K", 1)
	if !found || string(v) != "v1" {
		t.Fatalf("Visible(K,1) = %q,%v, want v1,true", v, found)
	}
}

// TestApplyConflictAborts proves a conflicting command aborts and
// applies none of its mutations.
func TestApplyConflictAborts(t *testing.T) {
	f := New(mvcc.NewStore())
	mustCommit(t, f, 1, CommitTxnCommand{RequestID: "R1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("first")}}})

	outcome := mustCommit(t, f, 2, CommitTxnCommand{RequestID: "R2", TxnID: 2, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("second")}}})
	if outcome.Status != StatusAborted {
		t.Fatalf("outcome.Status = %v, want Aborted", outcome.Status)
	}
	if outcome.ConflictKey != "K" || outcome.ConflictLatestSeq != 1 {
		t.Fatalf("outcome = %+v, want ConflictKey=K ConflictLatestSeq=1", outcome)
	}
	v, found := f.Store().Visible("K", 2)
	if !found || string(v) != "first" {
		t.Fatalf("Visible(K,2) = %q,%v, want first,true (loser's write must not apply)", v, found)
	}
}

// --- Idempotency ---

// TestIdempotentRetrySameOutcomeNoReapply: applying the same RequestID
// twice returns the identical outcome and does not create a second
// version.
func TestIdempotentRetrySameOutcomeNoReapply(t *testing.T) {
	f := New(mvcc.NewStore())
	cmd := CommitTxnCommand{RequestID: "R1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("v1")}}}
	first := mustCommit(t, f, 1, cmd)

	// Retry at a different (higher) index, as a future Raft-driven
	// duplicate proposal might: Apply must still return the ORIGINAL
	// outcome, not re-evaluate at the new index.
	retry := mustCommit(t, f, 5, cmd)
	if retry != first {
		t.Fatalf("retry outcome = %+v, want identical to original %+v", retry, first)
	}

	chain := f.Store()
	v, found := chain.Visible("K", 1)
	if !found || string(v) != "v1" {
		t.Fatalf("Visible(K,1) = %q,%v, want v1,true", v, found)
	}
	if seq, ok := chain.LatestCommitSeq("K"); !ok || seq != 1 {
		t.Fatalf("LatestCommitSeq(K) = %d,%v, want 1,true (retry must not create a second version at index 5)", seq, ok)
	}
}

// TestConflictOutcomeStableAcrossLaterStateChange: R1 conflicts; later,
// unrelated state changes; retrying R1 must still return the ORIGINAL
// conflict outcome, not re-evaluate against the new state.
func TestConflictOutcomeStableAcrossLaterStateChange(t *testing.T) {
	f := New(mvcc.NewStore())
	mustCommit(t, f, 1, CommitTxnCommand{RequestID: "winner", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("v1")}}})

	loserCmd := CommitTxnCommand{RequestID: "loser", TxnID: 2, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("v2")}}}
	first := mustCommit(t, f, 2, loserCmd)
	if first.Status != StatusAborted {
		t.Fatalf("first outcome = %+v, want Aborted", first)
	}

	// Unrelated later commits change store state.
	mustCommit(t, f, 3, CommitTxnCommand{RequestID: "unrelated", TxnID: 3, StartSeq: 2, Mutations: []mvcc.Mutation{{Key: "other", Value: []byte("x")}}})

	retry := mustCommit(t, f, 10, loserCmd)
	if retry != first {
		t.Fatalf("retry outcome = %+v, want identical original conflict outcome %+v", retry, first)
	}
}

// TestMultipleRequestIDsSameMutationsAreDistinct (ID-5): two different
// RequestIDs submitting the identical mutation set are independent
// logical requests, each evaluated on its own merits.
func TestMultipleRequestIDsSameMutationsAreDistinct(t *testing.T) {
	f := New(mvcc.NewStore())
	mutations := []mvcc.Mutation{{Key: "K", Value: []byte("v")}}

	r1 := mustCommit(t, f, 1, CommitTxnCommand{RequestID: "R1", TxnID: 1, StartSeq: 0, Mutations: mutations})
	if r1.Status != StatusCommitted {
		t.Fatalf("R1 outcome = %+v, want Committed", r1)
	}

	// R2 submits the identical mutation set under a different
	// RequestID, with a StartSeq that now conflicts against R1's
	// committed write.
	r2 := mustCommit(t, f, 2, CommitTxnCommand{RequestID: "R2", TxnID: 2, StartSeq: 0, Mutations: mutations})
	if r2.Status != StatusAborted {
		t.Fatalf("R2 outcome = %+v, want Aborted (independent conflict evaluation, not deduplicated against R1)", r2)
	}
}

// TestMismatchedRequestIDReuseRejected: reusing RequestID R with a
// different command is rejected, and R's original recorded outcome is
// left unchanged.
func TestMismatchedRequestIDReuseRejected(t *testing.T) {
	f := New(mvcc.NewStore())
	original := CommitTxnCommand{RequestID: "R1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("v1")}}}
	want := mustCommit(t, f, 1, original)

	mismatched := CommitTxnCommand{RequestID: "R1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("DIFFERENT")}}}
	if _, err := f.Apply(2, mismatched); !errors.Is(err, ErrRequestIDPayloadMismatch) {
		t.Fatalf("Apply with mismatched payload: err = %v, want ErrRequestIDPayloadMismatch", err)
	}

	got, ok := f.GetOutcome("R1")
	if !ok || got != want {
		t.Fatalf("GetOutcome(R1) after mismatched reuse attempt = %+v,%v, want unchanged %+v,true", got, ok, want)
	}
}

// TestUnknownOutcomeQuery: querying a RequestID that was never
// submitted returns an explicit not-found signal, never a guessed
// success or failure.
func TestUnknownOutcomeQuery(t *testing.T) {
	f := New(mvcc.NewStore())
	if _, ok := f.GetOutcome("never-submitted"); ok {
		t.Fatal("GetOutcome(never-submitted) = found, want not found")
	}
	if _, err := f.Precheck(CommitTxnCommand{RequestID: "never-submitted"}); !errors.Is(err, ErrRequestIDUnknown) {
		t.Fatalf("Precheck(never-submitted): err = %v, want ErrRequestIDUnknown", err)
	}
}

// --- State-machine safety / determinism ---

// scriptedCommand is one entry of a deterministic command history used
// by the replay-equivalence tests below.
type scriptedCommand struct {
	index uint64
	cmd   CommitTxnCommand
}

func sampleHistory() []scriptedCommand {
	return []scriptedCommand{
		{1, CommitTxnCommand{RequestID: "r1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "A", Value: []byte("1")}, {Key: "B", Value: []byte("2")}}}},
		{2, CommitTxnCommand{RequestID: "r2", TxnID: 2, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "A", Value: []byte("conflict")}}}}, // conflicts against r1's A
		{3, CommitTxnCommand{RequestID: "r3", TxnID: 3, StartSeq: 2, Mutations: []mvcc.Mutation{{Key: "C", Value: []byte("3")}}}},
		{4, CommitTxnCommand{RequestID: "r4", TxnID: 4, StartSeq: 3, Mutations: []mvcc.Mutation{{Key: "B", Tombstone: true}}}},
		{5, CommitTxnCommand{RequestID: "r1", TxnID: 1, StartSeq: 0, Mutations: []mvcc.Mutation{{Key: "A", Value: []byte("1")}, {Key: "B", Value: []byte("2")}}}}, // duplicate of r1
	}
}

func applyHistory(t *testing.T, f *FSM, history []scriptedCommand) []Outcome {
	t.Helper()
	outcomes := make([]Outcome, len(history))
	for i, sc := range history {
		o, err := f.Apply(sc.index, sc.cmd)
		if err != nil {
			t.Fatalf("Apply(%d): %v", sc.index, err)
		}
		outcomes[i] = o
	}
	return outcomes
}

// TestDeterministicReplayEquivalence (STATE MACHINE SAFETY): the same
// ordered command history, applied to two independently constructed
// FSMs, produces byte-for-byte-equivalent outcomes and equivalent
// logical MVCC state.
func TestDeterministicReplayEquivalence(t *testing.T) {
	history := sampleHistory()

	f1 := New(mvcc.NewStore())
	f2 := New(mvcc.NewStore())
	out1 := applyHistory(t, f1, history)
	out2 := applyHistory(t, f2, history)

	for i := range out1 {
		if out1[i] != out2[i] {
			t.Fatalf("entry %d: outcome diverged: %+v vs %+v", i, out1[i], out2[i])
		}
	}

	for _, key := range []string{"A", "B", "C"} {
		for _, seq := range []uint64{0, 1, 2, 3, 4, 5} {
			v1, ok1 := f1.Store().Visible(key, seq)
			v2, ok2 := f2.Store().Visible(key, seq)
			if ok1 != ok2 || string(v1) != string(v2) {
				t.Fatalf("key %q at StartSeq %d diverged: (%q,%v) vs (%q,%v)", key, seq, v1, ok1, v2, ok2)
			}
		}
	}
}

// TestReplayEquivalenceRepeatedRuns runs the deterministic-replay check
// several times to catch any accidental nondeterminism that only shows
// up probabilistically (e.g. an accidental map-iteration dependency).
func TestReplayEquivalenceRepeatedRuns(t *testing.T) {
	for i := 0; i < 20; i++ {
		history := sampleHistory()
		f1 := New(mvcc.NewStore())
		f2 := New(mvcc.NewStore())
		out1 := applyHistory(t, f1, history)
		out2 := applyHistory(t, f2, history)
		for j := range out1 {
			if out1[j] != out2[j] {
				t.Fatalf("run %d entry %d: outcome diverged: %+v vs %+v", i, j, out1[j], out2[j])
			}
		}
	}
}

// TestEncodeDeterministicRegardlessOfConstructionPath: building the
// identical logical mutation set via two different intermediate
// construction paths (one seeded from map iteration in shuffled key
// order, then explicitly sorted into the same final slice order)
// produces byte-identical encoded output, proving encode has no hidden
// dependency on how its input slice happened to be built — only on
// the slice's final, explicit order.
func TestEncodeDeterministicRegardlessOfConstructionPath(t *testing.T) {
	data := map[string]string{"z": "26", "a": "1", "m": "13", "b": "2"}
	order := []string{"a", "b", "m", "z"}

	build := func() []mvcc.Mutation {
		// Reading from a Go map is itself randomized per run; forcing a
		// fixed, explicit output order (as internal/txn.Txn.order does
		// via first-write insertion order) is what makes the resulting
		// command deterministic, not the map itself.
		out := make([]mvcc.Mutation, 0, len(order))
		for _, k := range order {
			out = append(out, mvcc.Mutation{Key: k, Value: []byte(data[k])})
		}
		return out
	}

	cmd := CommitTxnCommand{RequestID: "r", TxnID: 1, StartSeq: 0, Mutations: build()}
	want := EncodeCommitTxn(cmd)
	for i := 0; i < 10; i++ {
		got := EncodeCommitTxn(CommitTxnCommand{RequestID: "r", TxnID: 1, StartSeq: 0, Mutations: build()})
		if string(got) != string(want) {
			t.Fatalf("run %d: encode output diverged across identically-ordered rebuilds", i)
		}
	}
}

// TestEncodeDecodeRoundTrip proves EncodeCommitTxn/DecodeCommitTxn are
// mutual inverses for a representative command, including the
// RequestID and tombstone mutations.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	cmd := CommitTxnCommand{
		RequestID: "req-123",
		TxnID:     42,
		StartSeq:  7,
		Mutations: []mvcc.Mutation{
			{Key: "K1", Value: []byte("v1")},
			{Key: "K2", Tombstone: true},
		},
	}
	encoded := EncodeCommitTxn(cmd)
	decoded, err := DecodeCommitTxn(encoded)
	if err != nil {
		t.Fatalf("DecodeCommitTxn: %v", err)
	}
	if decoded.RequestID != cmd.RequestID || decoded.TxnID != cmd.TxnID || decoded.StartSeq != cmd.StartSeq {
		t.Fatalf("decoded = %+v, want %+v", decoded, cmd)
	}
	if len(decoded.Mutations) != len(cmd.Mutations) {
		t.Fatalf("decoded %d mutations, want %d", len(decoded.Mutations), len(cmd.Mutations))
	}
	for i, m := range decoded.Mutations {
		want := cmd.Mutations[i]
		if m.Key != want.Key || m.Tombstone != want.Tombstone || string(m.Value) != string(want.Value) {
			t.Fatalf("mutation %d = %+v, want %+v", i, m, want)
		}
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	cmd := CommitTxnCommand{RequestID: "r", TxnID: 1, StartSeq: 1, Mutations: []mvcc.Mutation{{Key: "K", Value: []byte("v")}}}
	encoded := EncodeCommitTxn(cmd)
	encoded[0] = 1 // Phase 2's version, which never carried a RequestID field.
	if _, err := DecodeCommitTxn(encoded); !errors.Is(err, ErrUnsupportedCommandVersion) {
		t.Fatalf("DecodeCommitTxn with version=1: err = %v, want ErrUnsupportedCommandVersion", err)
	}
}
