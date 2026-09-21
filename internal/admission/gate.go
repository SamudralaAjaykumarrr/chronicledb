package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/metrics"
)

// Class distinguishes the kind of work a Gate admits
// (docs/v0.6.0-plan.md §5.1's ClassClientWrite | ClassClientRead |
// ClassAdmin) — carried for a caller that wants to branch on it (e.g.
// diagnostics); internal/admission itself never inspects or branches on
// a Gate's Class.
type Class int

const (
	ClassClientWrite Class = iota
	ClassClientRead
	ClassAdmin
)

// Limits configures a Gate's fixed capacity (docs/v0.6.0-plan.md §5.1).
type Limits struct {
	// MaxConcurrent is how many callers Acquire admits to run at once.
	// Must be > 0 — there is no "unlimited" configuration
	// (ADMISSION FAILS CLOSED, docs/v0.6.0-plan.md §5.6/§27.4): NewGate
	// refuses MaxConcurrent <= 0 with a specific, fatal-at-startup error.
	MaxConcurrent int
	// MaxQueueDepth is the waiting-room capacity beyond MaxConcurrent.
	// 0 means "no waiting at all": once MaxConcurrent callers are
	// admitted, every further Acquire rejects immediately.
	MaxQueueDepth int
	// MaxWait bounds how long a queued caller waits for a slot before
	// being rejected with ReasonQueueTimeout. 0 means "bounded only by
	// the caller's own ctx" (docs/v0.6.0-plan.md §5.5: neither value
	// participates in any correctness argument — C2 — every §27
	// safety property holds for MaxWait=0 and MaxWait=∞ alike).
	MaxWait time.Duration
}

// Gate is ChronicleDB's bounded-capacity admission primitive
// (docs/v0.6.0-plan.md §5.2, the answer to C1 — no unbounded queue
// disguised as admission control): the total number of goroutines that
// can be inside Acquire at any instant — running with a held slot, or
// still waiting for one — never exceeds MaxConcurrent+MaxQueueDepth, by
// channel capacity alone. There is no code path that grows that bound:
// a caller that cannot enter admission is rejected immediately, before
// any blocking wait and before consuming any further resource.
//
// Implementation note: admission (capacity MaxConcurrent+MaxQueueDepth)
// is the single gate a caller must pass to be inside Acquire at all —
// this is the literal quantity docs/v0.6.0-plan.md §5.2 names. slots
// (capacity MaxConcurrent) is the strictly smaller set of callers
// currently admitted to run, held from Acquire's return until release().
// A caller occupies admission for its entire time inside Acquire
// (waiting or, once past the second select, briefly claiming a slot)
// and continues to occupy admission for as long as it holds its slot —
// release() drains both together — so "total concurrently admitted or
// waiting" and "total concurrently running" are exactly MaxConcurrent+
// MaxQueueDepth and MaxConcurrent respectively, matching AC-1's exact-
// boundary requirement (MaxQueueDepth=0: N Acquire calls succeed, the
// N+1th rejects immediately with ReasonQueueFull).
type Gate struct {
	name string
	lim  Limits

	admission chan struct{} // cap == MaxConcurrent + MaxQueueDepth
	slots     chan struct{} // cap == MaxConcurrent

	// eff is the effective ceiling pressure can tighten
	// (SetEffectiveConcurrent, docs/v0.6.0-plan.md §6.4) — always
	// <= MaxConcurrent; advisory-downward only, it can never admit more
	// than MaxConcurrent.
	eff atomic.Int64
	// pressureReason is the Reason Acquire reports when the effective-
	// ceiling check below rejects a caller — set alongside
	// SetEffectiveConcurrent by whichever pressure state tightened the
	// ceiling. Carried as a separate field, rather than folded into
	// SetEffectiveConcurrent's own single-int signature, because §6.4's
	// LowSpace state can be entered for either a disk or a memory
	// reason and Gate itself has no pressure-semantics knowledge of its
	// own (it is a leaf primitive) to infer which.
	pressureReason atomic.Pointer[Reason]

	admittedTotal metrics.Counter
	rejectedTotal map[Reason]*metrics.Counter // fixed key set built at NewGate; read-only thereafter, so concurrent reads need no lock
	waitSeconds   *metrics.Histogram
}

// NewGate returns a Gate enforcing lim. Returns an error — never a
// usable Gate with a silently-adjusted limit — for any configuration
// that would leave admission unbounded or nonsensical
// (ADMISSION FAILS CLOSED): callers (cmd/chronicledb-node) treat this
// error as fatal at startup, the same posture invalid TLS/RBAC
// configuration already takes.
func NewGate(name string, lim Limits) (*Gate, error) {
	if lim.MaxConcurrent <= 0 {
		return nil, fmt.Errorf("admission: gate %q: MaxConcurrent must be > 0 (got %d) — there is no \"unlimited\" admission configuration (ADMISSION FAILS CLOSED)", name, lim.MaxConcurrent)
	}
	if lim.MaxQueueDepth < 0 {
		return nil, fmt.Errorf("admission: gate %q: MaxQueueDepth must be >= 0 (got %d)", name, lim.MaxQueueDepth)
	}
	if lim.MaxWait < 0 {
		return nil, fmt.Errorf("admission: gate %q: MaxWait must be >= 0 (got %v)", name, lim.MaxWait)
	}
	g := &Gate{
		name:          name,
		lim:           lim,
		admission:     make(chan struct{}, lim.MaxConcurrent+lim.MaxQueueDepth),
		slots:         make(chan struct{}, lim.MaxConcurrent),
		waitSeconds:   metrics.NewHistogram(metrics.DefaultLatencyBounds...),
		rejectedTotal: make(map[Reason]*metrics.Counter, len(KnownReasons)),
	}
	g.eff.Store(int64(lim.MaxConcurrent))
	for _, r := range KnownReasons {
		g.rejectedTotal[r] = &metrics.Counter{}
	}
	return g, nil
}

// Acquire blocks until it can admit the caller, the caller's ctx is
// canceled, or the caller is rejected for capacity — never anything
// else (docs/v0.6.0-plan.md §5.5: rejection is immediate and fast; with
// MaxQueueDepth=0 it is one non-blocking channel operation, no
// allocation, no lock, no syscall). On success, release must be called
// exactly once (calling it more than once is safe — it is idempotent)
// to free the held slot; on error, release is nil and no slot or queue
// capacity is held (docs/v0.6.0-plan.md §5.6 item 3: Acquire has no
// path returning a nil error without a held slot).
func (g *Gate) Acquire(ctx context.Context) (release func(), err error) {
	waitStart := time.Now()

	select {
	case g.admission <- struct{}{}:
	default:
		g.reject(ReasonQueueFull)
		return nil, &RejectedError{Reason: ReasonQueueFull, RetryAfter: ReasonQueueFull.DefaultRetryAfter()}
	}

	var timerC <-chan time.Time
	if g.lim.MaxWait > 0 {
		timer := time.NewTimer(g.lim.MaxWait)
		defer timer.Stop()
		timerC = timer.C
	}

	select {
	case g.slots <- struct{}{}:
		// fall through to the effective-ceiling check below.
	case <-ctx.Done():
		<-g.admission
		return nil, ctx.Err()
	case <-timerC:
		<-g.admission
		g.reject(ReasonQueueTimeout)
		return nil, &RejectedError{Reason: ReasonQueueTimeout, RetryAfter: ReasonQueueTimeout.DefaultRetryAfter()}
	}

	// Effective-ceiling check (docs/v0.6.0-plan.md §5.2 step 3): pressure
	// may have tightened the ceiling below MaxConcurrent between this
	// caller entering admission and reaching this point. The ceiling is
	// advisory-downward only — it can never admit more than
	// MaxConcurrent, which the slots channel's own capacity already
	// guarantees independent of this check.
	if g.InFlight() > int(g.eff.Load()) {
		<-g.slots
		<-g.admission
		reason := ReasonDiskPressure
		if r := g.pressureReason.Load(); r != nil {
			reason = *r
		}
		g.reject(reason)
		return nil, &RejectedError{Reason: reason, RetryAfter: reason.DefaultRetryAfter()}
	}

	g.waitSeconds.Observe(time.Since(waitStart).Seconds())
	g.admittedTotal.Inc()

	var once sync.Once
	release = func() {
		once.Do(func() {
			<-g.slots
			<-g.admission
		})
	}
	return release, nil
}

func (g *Gate) reject(r Reason) {
	if c, ok := g.rejectedTotal[r]; ok {
		c.Inc()
	}
}

// InFlight returns the number of callers currently holding a slot
// (i.e. currently admitted to run). Safe from any goroutine; a
// diagnostic snapshot, like every other metrics-adjacent value in this
// codebase, never a correctness dependency.
func (g *Gate) InFlight() int { return len(g.slots) }

// Queued returns the number of callers currently admitted (holding an
// admission ticket) but not yet holding a slot — i.e. genuinely
// blocked waiting. An instantaneous, racy snapshot by nature (like
// InFlight), clamped to 0 to avoid a transient negative reading across
// the two channels' independent length reads.
func (g *Gate) Queued() int {
	q := len(g.admission) - len(g.slots)
	if q < 0 {
		return 0
	}
	return q
}

// GateStats is a point-in-time diagnostic snapshot of a Gate
// (docs/v0.6.0-plan.md §11.3's per-gate /status fields).
type GateStats struct {
	Name                string
	InFlight            int
	Queued              int
	MaxConcurrent       int
	MaxQueueDepth       int
	EffectiveConcurrent int
	AdmittedTotal       uint64
	RejectedTotal       map[Reason]uint64
	WaitSeconds         metrics.HistogramSnapshot
}

// Stats returns a point-in-time snapshot of g.
func (g *Gate) Stats() GateStats {
	rej := make(map[Reason]uint64, len(g.rejectedTotal))
	for r, c := range g.rejectedTotal {
		rej[r] = c.Value()
	}
	return GateStats{
		Name:                g.name,
		InFlight:            g.InFlight(),
		Queued:              g.Queued(),
		MaxConcurrent:       g.lim.MaxConcurrent,
		MaxQueueDepth:       g.lim.MaxQueueDepth,
		EffectiveConcurrent: int(g.eff.Load()),
		AdmittedTotal:       g.admittedTotal.Value(),
		RejectedTotal:       rej,
		WaitSeconds:         g.waitSeconds.Snapshot(),
	}
}

// SetEffectiveConcurrent tightens (or restores) g's admitted-work
// ceiling in response to resource pressure (docs/v0.6.0-plan.md §6.4).
// n is clamped to [0, MaxConcurrent] — it can never raise the ceiling
// above the gate's own configured MaxConcurrent. Pair with
// SetPressureReason so a subsequent rejection reports the reason that
// caused the tightening.
func (g *Gate) SetEffectiveConcurrent(n int) {
	if n > g.lim.MaxConcurrent {
		n = g.lim.MaxConcurrent
	}
	if n < 0 {
		n = 0
	}
	g.eff.Store(int64(n))
}

// SetPressureReason records which Reason Acquire reports when the
// effective-ceiling check rejects a caller. See Gate.pressureReason's
// doc comment for why this is separate from SetEffectiveConcurrent.
func (g *Gate) SetPressureReason(r Reason) {
	g.pressureReason.Store(&r)
}

// Name returns the name this Gate was constructed with (the "gate"
// metric label value, docs/v0.6.0-plan.md §11.2).
func (g *Gate) Name() string { return g.name }
