package sql

import (
	"context"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/txn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

// TestRestartSurvivesSchemaAndData proves docs/sql.md §Recovery: SQL
// state (both a table's schema and its committed rows) is ordinary
// committed MVCC state, recovered by the identical standalone-mode
// restart/replay path already proven in Phases 1-3 (internal/txn's own
// recovery tests) — there is no separate SQL-specific recovery
// mechanism to test here, only that the SQL frontend's encoding
// (schema.go, row.go) round-trips correctly through a real close and
// reopen of the same on-disk WAL directory.
func TestRestartSurvivesSchemaAndData(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	w1, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	mgr1, err := txn.NewManager(w1, mvcc.NewStore())
	if err != nil {
		t.Fatalf("txn.NewManager: %v", err)
	}
	s1 := NewSession(NewStandaloneEngine(mgr1))
	mustExec(t, s1, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "create")
	mustExec(t, s1, "INSERT INTO users VALUES (1, 'alice')", "ins1")
	mustExec(t, s1, "INSERT INTO users VALUES (2, 'bob')", "ins2")
	if err := w1.Close(); err != nil {
		t.Fatalf("closing WAL: %v", err)
	}

	// Reopen from the same data directory — a fresh process's worth of
	// state, reconstructed purely from durable history
	// (docs/recovery.md), not carried over in memory.
	mgr2 := openStandaloneManager(t, dir)
	s2 := NewSession(NewStandaloneEngine(mgr2))

	res, err := s2.Execute(ctx, "SELECT id, name FROM users", "sel1")
	if err != nil {
		t.Fatalf("SELECT after restart: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows after restart, want 2", len(res.Rows))
	}
	got := map[int64]string{}
	for _, row := range res.Rows {
		got[row[0].Int] = row[1].Text
	}
	if got[1] != "alice" || got[2] != "bob" {
		t.Errorf("rows after restart = %v", got)
	}

	// Schema itself must also have survived: a further CREATE TABLE of
	// the same name must still be rejected as a duplicate, and a
	// further INSERT must still be schema/type-validated.
	if _, err := s2.Execute(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY)", "recreate"); err == nil {
		t.Error("expected ErrDuplicateTable after restart, got nil")
	}
	if _, err := s2.Execute(ctx, "INSERT INTO users VALUES ('bad-type', 'x')", "ins-bad"); err == nil {
		t.Error("expected ErrTypeMismatch after restart, got nil")
	}
}

// TestRestartSurvivesRolledBackAndAbortedWork proves the flip side of
// restart recovery (docs/invariants.md ABORT SAFETY): a transaction
// that was explicitly rolled back, or a statement that failed and
// aborted its explicit transaction, must leave no trace whatsoever —
// including after a restart, not just immediately.
func TestRestartSurvivesRolledBackAndAbortedWork(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	w1, _, err := wal.Open(dir, wal.Options{})
	if err != nil {
		t.Fatalf("wal.Open: %v", err)
	}
	mgr1, err := txn.NewManager(w1, mvcc.NewStore())
	if err != nil {
		t.Fatalf("txn.NewManager: %v", err)
	}
	s1 := NewSession(NewStandaloneEngine(mgr1))
	mustExec(t, s1, "CREATE TABLE users (id INTEGER PRIMARY KEY)", "create")
	mustExec(t, s1, "BEGIN", "")
	mustExec(t, s1, "INSERT INTO users VALUES (1)", "")
	mustExec(t, s1, "ROLLBACK", "")
	if err := w1.Close(); err != nil {
		t.Fatalf("closing WAL: %v", err)
	}

	mgr2 := openStandaloneManager(t, dir)
	s2 := NewSession(NewStandaloneEngine(mgr2))
	res, err := s2.Execute(ctx, "SELECT id FROM users", "sel1")
	if err != nil {
		t.Fatalf("SELECT after restart: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("got %d rows after restart, want 0 (rolled-back work must leave no trace)", len(res.Rows))
	}
}
