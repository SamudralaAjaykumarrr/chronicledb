package sql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/admission"
)

// TestSQLStatementGateSaturates is docs/v0.6.0-plan.md §9.2/§33 slice 6:
// a statement is gated before any parser/planner/scan work, and once
// the (small, test-configured) gate is saturated by long-running
// statements, a further statement is rejected with
// *admission.RejectedError rather than executing.
func TestSQLStatementGateSaturates(t *testing.T) {
	mgr := openStandaloneManager(t, t.TempDir())
	const limit = 3
	e := newStandaloneEngineWithGateLimitForTest(mgr, limit)
	sess := NewSession(e)
	if _, err := sess.Execute(context.Background(), "CREATE TABLE t (id INTEGER PRIMARY KEY)", "create"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Hold the gate saturated via a long-running transaction on OTHER
	// sessions (a BEGIN's own gate acquisition is released once BEGIN
	// itself returns — see ExecuteStatement's per-statement, not per-
	// transaction, release rule) — so instead saturate it directly by
	// holding `limit` concurrent Acquire calls open.
	engineImpl := e.(interface{ sqlGate() *admission.Gate })
	gate := engineImpl.sqlGate()

	var releases []func()
	for i := 0; i < limit; i++ {
		release, err := gate.Acquire(context.Background())
		if err != nil {
			t.Fatalf("saturating acquire %d: %v", i, err)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, r := range releases {
			r()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := sess.Execute(ctx, "SELECT * FROM t WHERE id = 1", "req-1")
	if err == nil {
		t.Fatal("expected the statement gate to reject while saturated, got success")
	}
	var rej *admission.RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("expected *admission.RejectedError, got %v", err)
	}

	// Release one slot; the next statement must now succeed.
	releases[0]()
	releases = releases[1:]
	if _, err := sess.Execute(context.Background(), "SELECT * FROM t WHERE id = 1", "req-2"); err != nil {
		t.Fatalf("Execute after a slot freed: %v", err)
	}
}

// TestSQLStatementGateBoundsPerStatementNotPerTransaction proves the
// gate is released when EACH statement's ExecuteStatement call returns,
// not held for the whole explicit transaction — mirroring
// BeginReadIndex's identical rule (docs/v0.6.0-plan.md §5.4/§9.2): a
// BEGIN followed by several statements inside it must not accumulate
// held gate slots.
func TestSQLStatementGateBoundsPerStatementNotPerTransaction(t *testing.T) {
	mgr := openStandaloneManager(t, t.TempDir())
	const limit = 1
	e := newStandaloneEngineWithGateLimitForTest(mgr, limit)
	sess := NewSession(e)
	gate := e.(interface{ sqlGate() *admission.Gate }).sqlGate()
	if _, err := sess.Execute(context.Background(), "CREATE TABLE t (id INTEGER PRIMARY KEY)", "create"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	ctx := context.Background()
	if _, err := sess.Execute(ctx, "BEGIN", ""); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if got := gate.InFlight(); got != 0 {
		t.Fatalf("InFlight() after BEGIN returned = %d, want 0 (gate released per statement)", got)
	}
	if _, err := sess.Execute(ctx, "SELECT * FROM t WHERE id = 1", ""); err != nil {
		t.Fatalf("SELECT inside transaction: %v", err)
	}
	if got := gate.InFlight(); got != 0 {
		t.Fatalf("InFlight() after SELECT returned = %d, want 0", got)
	}
	if _, err := sess.Execute(ctx, "COMMIT", "req-commit"); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}
	if got := gate.InFlight(); got != 0 {
		t.Fatalf("InFlight() after COMMIT returned = %d, want 0", got)
	}
}

// TestSQLStatementGateConcurrentUsersRaceSafe drives real concurrent
// Sessions (distinct client sessions sharing one Engine, the intended
// production shape) against a small gate, asserting no panic/race and
// that InFlight never exceeds the configured limit.
func TestSQLStatementGateConcurrentUsersRaceSafe(t *testing.T) {
	mgr := openStandaloneManager(t, t.TempDir())
	const limit = 4
	e := newStandaloneEngineWithGateLimitForTest(mgr, limit)
	gate := e.(interface{ sqlGate() *admission.Gate }).sqlGate()
	setupSess := NewSession(e)
	if _, err := setupSess.Execute(context.Background(), "CREATE TABLE t (id INTEGER PRIMARY KEY)", "create"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	var wg sync.WaitGroup
	const sessions = 20
	wg.Add(sessions)
	for i := 0; i < sessions; i++ {
		go func(i int) {
			defer wg.Done()
			sess := NewSession(e)
			for j := 0; j < 10; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, _ = sess.Execute(ctx, "SELECT * FROM t WHERE id = 1", "")
				cancel()
				if got := gate.InFlight(); got > limit {
					t.Errorf("InFlight() = %d, want <= %d", got, limit)
				}
			}
		}(i)
	}
	wg.Wait()
	if got := gate.InFlight(); got != 0 {
		t.Fatalf("InFlight() after quiescence = %d, want 0", got)
	}
}
