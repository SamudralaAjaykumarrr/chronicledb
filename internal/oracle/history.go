package oracle

import (
	"fmt"
	"strings"
)

// Step is one recorded action in a deterministic adversarial history
// (docs/roadmap.md Phase 10 "HISTORY RECORDER"). Fields mirror exactly
// what that section asks for; every field is a small, already-bounded
// value (never a raw unbounded payload — Args/Outcome are short,
// human-readable summaries) so a Recorder can be dumped cheaply on
// failure without risking an unbounded log.
type Step struct {
	Seed             int64
	Index            int
	Node             string
	Term             uint64
	Role             string
	RequestID        string
	Op               string
	Args             string
	Outcome          string
	CommitIndex      uint64
	AppliedIndex     uint64
	SnapshotBoundary uint64
	Fault            string
}

func (s Step) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%d node=%s term=%d role=%s op=%s", s.Index, s.Node, s.Term, s.Role, s.Op)
	if s.Args != "" {
		fmt.Fprintf(&b, " args=%s", s.Args)
	}
	if s.RequestID != "" {
		fmt.Fprintf(&b, " reqID=%s", s.RequestID)
	}
	if s.Fault != "" {
		fmt.Fprintf(&b, " fault=%s", s.Fault)
	}
	fmt.Fprintf(&b, " -> %s [commit=%d applied=%d snap=%d]", s.Outcome, s.CommitIndex, s.AppliedIndex, s.SnapshotBoundary)
	return b.String()
}

// Recorder accumulates a bounded, replayable trace of Steps for one
// seeded adversarial run. "Replayable" here means precisely: the
// history-generating code in each adversarial test is itself a pure
// function of (seed, step count) — re-running with the same seed
// reproduces the identical operation sequence deterministically,
// per docs/testing-strategy.md §3.2's existing reproducibility
// discipline — and this Recorder exists only to make a *failing* run's
// exact sequence of already-executed steps inspectable in the test
// failure output. It is not a serialize-to-file/replay-from-file
// interpreter; see docs/adversarial-testing.md's "Replay strategy"
// section for the exact, honest scope of this claim.
type Recorder struct {
	seed  int64
	steps []Step
}

// NewRecorder returns a Recorder for one seeded run.
func NewRecorder(seed int64) *Recorder {
	return &Recorder{seed: seed}
}

// Seed returns the seed this Recorder's history was generated from.
func (r *Recorder) Seed() int64 { return r.seed }

// Record appends one Step. The Step's Seed/Index fields are filled in
// automatically if left zero.
func (r *Recorder) Record(s Step) {
	s.Seed = r.seed
	if s.Index == 0 {
		s.Index = len(r.steps) + 1
	}
	r.steps = append(r.steps, s)
}

// Steps returns every recorded step so far, in order.
func (r *Recorder) Steps() []Step { return append([]Step(nil), r.steps...) }

// Dump formats the full recorded history, one line per step, for
// inclusion in a test failure message (t.Fatalf) — a passing run never
// pays any cost beyond the Record calls themselves.
func (r *Recorder) Dump() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d, %d steps:\n", r.seed, len(r.steps))
	for _, s := range r.steps {
		b.WriteString(s.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// Tail formats only the last n recorded steps, for a shorter failure
// message when a full dump would be unwieldy.
func (r *Recorder) Tail(n int) string {
	start := 0
	if len(r.steps) > n {
		start = len(r.steps) - n
	}
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d, last %d of %d steps:\n", r.seed, len(r.steps)-start, len(r.steps))
	for _, s := range r.steps[start:] {
		b.WriteString(s.String())
		b.WriteByte('\n')
	}
	return b.String()
}
