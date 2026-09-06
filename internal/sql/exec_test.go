package sql

import (
	"context"
	"errors"
	"testing"
)

func mustExec(t *testing.T, s *Session, sqlText, requestID string) Result {
	t.Helper()
	res, err := s.Execute(context.Background(), sqlText, requestID)
	if err != nil {
		t.Fatalf("Execute(%q): unexpected error: %v", sqlText, err)
	}
	return res
}

func newTestSession(t *testing.T) *Session {
	return NewSession(newStandaloneTestEngine(t))
}

// --- Schema / CREATE TABLE ---

func TestExecCreateTable(t *testing.T) {
	s := newTestSession(t)
	res := mustExec(t, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", "r1")
	if res.Kind != ResultOK {
		t.Errorf("Kind = %v, want ResultOK", res.Kind)
	}
}

func TestExecCreateTableDuplicate(t *testing.T) {
	s := newTestSession(t)
	mustExec(t, s, "CREATE TABLE users (id INTEGER PRIMARY KEY)", "r1")
	_, err := s.Execute(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY)", "r2")
	if !errors.Is(err, ErrDuplicateTable) {
		t.Errorf("err = %v, want ErrDuplicateTable", err)
	}
}

func TestExecCreateTableInvalidSchema(t *testing.T) {
	s := newTestSession(t)
	_, err := s.Execute(context.Background(), "CREATE TABLE t (id INTEGER)", "r1")
	if !errors.Is(err, ErrMissingPrimaryKey) {
		t.Errorf("err = %v, want ErrMissingPrimaryKey", err)
	}
}

// --- INSERT ---

func setupUsersTable(t *testing.T, s *Session) {
	t.Helper()
	mustExec(t, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)", "create-users")
}

func TestExecInsertValid(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)
	res := mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "ins1")
	if res.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", res.RowsAffected)
	}
	if res.CommitSeq == 0 {
		t.Errorf("CommitSeq = 0, want nonzero for a mutating auto-commit statement")
	}
}

func TestExecInsertTypeMismatch(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)
	_, err := s.Execute(context.Background(), "INSERT INTO users VALUES ('not-an-int', 'alice', true)", "ins1")
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("err = %v, want ErrTypeMismatch", err)
	}
}

func TestExecInsertDuplicateKey(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)
	mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "ins1")
	_, err := s.Execute(context.Background(), "INSERT INTO users VALUES (1, 'bob', false)", "ins2")
	if !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Errorf("err = %v, want ErrDuplicatePrimaryKey", err)
	}
}

func TestExecInsertRetrySameRequestID(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)
	r1 := mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "same-request-id")

	// A genuine retry (same RequestID) of the identical logical
	// request must return the same outcome without erroring or
	// duplicating the row (docs/transactions.md §6). It is executed as
	// a fresh Session.Execute call — the client resubmitting the exact
	// same statement text under the same RequestID, which is the
	// documented retry contract.
	r2, err := s.Execute(context.Background(), "INSERT INTO users VALUES (1, 'alice', true)", "same-request-id")
	if err != nil {
		t.Fatalf("retry with same RequestID: unexpected error: %v", err)
	}
	if r2.CommitSeq != r1.CommitSeq {
		t.Errorf("retry CommitSeq = %d, want identical to original %d", r2.CommitSeq, r1.CommitSeq)
	}

	// Confirm the row was not duplicated/mutated: exactly one row.
	sel := mustExec(t, s, "SELECT * FROM users", "sel1")
	if len(sel.Rows) != 1 {
		t.Errorf("got %d rows after retry, want 1", len(sel.Rows))
	}
}

func TestExecInsertColumnList(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)
	mustExec(t, s, "INSERT INTO users (name, id, active) VALUES ('carol', 3, false)", "ins1")
	res := mustExec(t, s, "SELECT id, name, active FROM users WHERE id = 3", "sel1")
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	row := res.Rows[0]
	if row[0].Int != 3 || row[1].Text != "carol" || row[2].Bool != false {
		t.Errorf("row = %+v", row)
	}
}

func TestExecInsertMissingColumnRejected(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)
	_, err := s.Execute(context.Background(), "INSERT INTO users (id, name) VALUES (1, 'alice')", "ins1")
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Errorf("err = %v, want ErrUnsupportedFeature (no NULLs/defaults)", err)
	}
}

func TestExecInsertUnknownTable(t *testing.T) {
	s := newTestSession(t)
	_, err := s.Execute(context.Background(), "INSERT INTO ghost VALUES (1)", "ins1")
	if !errors.Is(err, ErrUnknownTable) {
		t.Errorf("err = %v, want ErrUnknownTable", err)
	}
}

// --- SELECT ---

func seedUsers(t *testing.T, s *Session) {
	t.Helper()
	setupUsersTable(t, s)
	mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "seed1")
	mustExec(t, s, "INSERT INTO users VALUES (2, 'bob', false)", "seed2")
	mustExec(t, s, "INSERT INTO users VALUES (3, 'carol', true)", "seed3")
}

func TestExecSelectPrimaryKeyLookup(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	res := mustExec(t, s, "SELECT * FROM users WHERE id = 2", "sel1")
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	if res.Rows[0][1].Text != "bob" {
		t.Errorf("row = %+v", res.Rows[0])
	}
}

func TestExecSelectMissingRow(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	res := mustExec(t, s, "SELECT * FROM users WHERE id = 999", "sel1")
	if len(res.Rows) != 0 {
		t.Errorf("got %d rows, want 0", len(res.Rows))
	}
}

func TestExecSelectProjection(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	res := mustExec(t, s, "SELECT name FROM users WHERE id = 1", "sel1")
	if len(res.Columns) != 1 || res.Columns[0] != "name" {
		t.Fatalf("Columns = %v", res.Columns)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Text != "alice" {
		t.Errorf("Rows = %v", res.Rows)
	}
}

func TestExecSelectFullScanDeterministic(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	res1 := mustExec(t, s, "SELECT id FROM users", "sel1")
	res2 := mustExec(t, s, "SELECT id FROM users", "sel2")
	if len(res1.Rows) != 3 || len(res2.Rows) != 3 {
		t.Fatalf("got %d/%d rows, want 3/3", len(res1.Rows), len(res2.Rows))
	}
	for i := range res1.Rows {
		if res1.Rows[i][0].Int != res2.Rows[i][0].Int {
			t.Errorf("non-deterministic scan order: %v vs %v", res1.Rows, res2.Rows)
		}
	}
	// Ascending by primary-key encoding for this all-positive-integer
	// case (a consequence of rowKey's big-endian encoding, not a
	// documented guarantee this subset makes — see docs/sql.md §5.2).
	ids := []int64{res1.Rows[0][0].Int, res1.Rows[1][0].Int, res1.Rows[2][0].Int}
	seen := map[int64]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct ids, got %v", ids)
	}
}

func TestExecSelectUnknownColumn(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "SELECT ghost FROM users", "sel1")
	if !errors.Is(err, ErrUnknownColumn) {
		t.Errorf("err = %v, want ErrUnknownColumn", err)
	}
}

func TestExecSelectInvalidPredicate(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "SELECT * FROM users WHERE name = 'alice'", "sel1")
	if !errors.Is(err, ErrInvalidPredicate) {
		t.Errorf("err = %v, want ErrInvalidPredicate", err)
	}
}

// --- UPDATE ---

func TestExecUpdateValid(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	res := mustExec(t, s, "UPDATE users SET name = 'alice2', active = false WHERE id = 1", "upd1")
	if res.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", res.RowsAffected)
	}
	sel := mustExec(t, s, "SELECT name, active FROM users WHERE id = 1", "sel1")
	if sel.Rows[0][0].Text != "alice2" || sel.Rows[0][1].Bool != false {
		t.Errorf("row after update = %v", sel.Rows[0])
	}
}

func TestExecUpdateMissingRow(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "UPDATE users SET name = 'x' WHERE id = 999", "upd1")
	if !errors.Is(err, ErrRowNotFound) {
		t.Errorf("err = %v, want ErrRowNotFound", err)
	}
}

func TestExecUpdateInvalidColumn(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "UPDATE users SET ghost = 'x' WHERE id = 1", "upd1")
	if !errors.Is(err, ErrUnknownColumn) {
		t.Errorf("err = %v, want ErrUnknownColumn", err)
	}
}

func TestExecUpdateTypeMismatch(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "UPDATE users SET name = 42 WHERE id = 1", "upd1")
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("err = %v, want ErrTypeMismatch", err)
	}
}

func TestExecUpdateCannotModifyPrimaryKey(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "UPDATE users SET id = 99 WHERE id = 1", "upd1")
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Errorf("err = %v, want ErrUnsupportedFeature", err)
	}
}

// --- DELETE ---

func TestExecDeleteValid(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	res := mustExec(t, s, "DELETE FROM users WHERE id = 2", "del1")
	if res.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", res.RowsAffected)
	}
	sel := mustExec(t, s, "SELECT * FROM users WHERE id = 2", "sel1")
	if len(sel.Rows) != 0 {
		t.Errorf("row still visible after delete: %v", sel.Rows)
	}
}

func TestExecDeleteMissingRow(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	_, err := s.Execute(context.Background(), "DELETE FROM users WHERE id = 999", "del1")
	if !errors.Is(err, ErrRowNotFound) {
		t.Errorf("err = %v, want ErrRowNotFound", err)
	}
}

func TestExecDeleteTombstoneNotResurrectedByFullScan(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	mustExec(t, s, "DELETE FROM users WHERE id = 1", "del1")
	res := mustExec(t, s, "SELECT id FROM users", "sel1")
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows after delete, want 2", len(res.Rows))
	}
	for _, row := range res.Rows {
		if row[0].Int == 1 {
			t.Errorf("deleted row id=1 still present in full scan: %v", res.Rows)
		}
	}
}

func TestExecDeleteThenReinsertSamePrimaryKey(t *testing.T) {
	s := newTestSession(t)
	seedUsers(t, s)
	mustExec(t, s, "DELETE FROM users WHERE id = 1", "del1")
	res := mustExec(t, s, "INSERT INTO users VALUES (1, 'alice-again', true)", "ins-again")
	if res.RowsAffected != 1 {
		t.Errorf("re-insert after delete should succeed, RowsAffected=%d", res.RowsAffected)
	}
}

// --- Explicit transactions ---

func TestExecExplicitTransactionCommit(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)

	mustExec(t, s, "BEGIN", "")
	if !s.InTransaction() {
		t.Fatal("InTransaction() = false after BEGIN")
	}
	mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "")
	mustExec(t, s, "INSERT INTO users VALUES (2, 'bob', false)", "")
	// Both inserts must be visible to a read inside the same still-open
	// transaction, before COMMIT (own-write visibility, docs/mvcc.md §3).
	sel := mustExec(t, s, "SELECT id FROM users", "")
	if len(sel.Rows) != 2 {
		t.Fatalf("got %d rows visible inside open transaction, want 2", len(sel.Rows))
	}
	commitRes := mustExec(t, s, "COMMIT", "txn-commit-1")
	if s.InTransaction() {
		t.Error("InTransaction() = true after COMMIT")
	}
	if commitRes.CommitSeq == 0 {
		t.Error("COMMIT result has zero CommitSeq")
	}

	// A fresh session/transaction confirms both rows are really
	// committed, not just locally visible.
	res := mustExec(t, s, "SELECT id FROM users", "sel-after")
	if len(res.Rows) != 2 {
		t.Errorf("got %d rows after COMMIT, want 2", len(res.Rows))
	}
}

func TestExecExplicitTransactionRollback(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)

	mustExec(t, s, "BEGIN", "")
	mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "")
	mustExec(t, s, "ROLLBACK", "")
	if s.InTransaction() {
		t.Error("InTransaction() = true after ROLLBACK")
	}

	res := mustExec(t, s, "SELECT id FROM users", "sel-after")
	if len(res.Rows) != 0 {
		t.Errorf("got %d rows after ROLLBACK, want 0 (no trace)", len(res.Rows))
	}
}

func TestExecBeginWhileAlreadyActive(t *testing.T) {
	s := newTestSession(t)
	mustExec(t, s, "BEGIN", "")
	_, err := s.Execute(context.Background(), "BEGIN", "")
	if !errors.Is(err, ErrTransactionAlreadyActive) {
		t.Errorf("err = %v, want ErrTransactionAlreadyActive", err)
	}
}

func TestExecCommitWithoutBegin(t *testing.T) {
	s := newTestSession(t)
	_, err := s.Execute(context.Background(), "COMMIT", "r1")
	if !errors.Is(err, ErrNoActiveTransaction) {
		t.Errorf("err = %v, want ErrNoActiveTransaction", err)
	}
}

func TestExecRollbackWithoutBegin(t *testing.T) {
	s := newTestSession(t)
	_, err := s.Execute(context.Background(), "ROLLBACK", "")
	if !errors.Is(err, ErrNoActiveTransaction) {
		t.Errorf("err = %v, want ErrNoActiveTransaction", err)
	}
}

func TestExecExplicitTransactionAbortsWholeTransactionOnStatementError(t *testing.T) {
	s := newTestSession(t)
	setupUsersTable(t, s)

	mustExec(t, s, "BEGIN", "")
	mustExec(t, s, "INSERT INTO users VALUES (1, 'alice', true)", "")
	// This statement fails (duplicate primary key) — per docs/sql.md
	// §Transactions this aborts the whole explicit transaction, not
	// just this one statement.
	_, err := s.Execute(context.Background(), "INSERT INTO users VALUES (1, 'dup', true)", "")
	if !errors.Is(err, ErrDuplicatePrimaryKey) {
		t.Fatalf("err = %v, want ErrDuplicatePrimaryKey", err)
	}
	if s.InTransaction() {
		t.Error("InTransaction() = true after a statement error inside an explicit transaction, want the transaction aborted")
	}

	// Neither insert (including the first, otherwise-valid one) was
	// ever committed.
	res := mustExec(t, s, "SELECT id FROM users", "sel-after")
	if len(res.Rows) != 0 {
		t.Errorf("got %d rows, want 0 (whole transaction aborted)", len(res.Rows))
	}
}

func TestExecWriteSkewIsPossibleUnderSnapshotIsolation(t *testing.T) {
	// docs/mvcc.md §1.1: Snapshot Isolation does not prevent write
	// skew. This is a living counterexample against any accidental
	// SERIALIZABLE claim (docs/invariants.md ISOLATION TRUTHFULNESS),
	// exercised through the SQL frontend rather than the raw txn API.
	engine := newStandaloneTestEngine(t)
	s := NewSession(engine)
	mustExec(t, s, "CREATE TABLE bal (id INTEGER PRIMARY KEY, amount INTEGER)", "create")
	mustExec(t, s, "INSERT INTO bal VALUES (1, 10)", "seed1")
	mustExec(t, s, "INSERT INTO bal VALUES (2, 10)", "seed2")

	s1 := NewSession(engine)
	s2 := NewSession(engine)
	mustExec(t, s1, "BEGIN", "")
	mustExec(t, s2, "BEGIN", "")

	r1 := mustExec(t, s1, "SELECT amount FROM bal WHERE id = 1", "")
	r2 := mustExec(t, s2, "SELECT amount FROM bal WHERE id = 2", "")
	x := r1.Rows[0][0].Int
	y := r2.Rows[0][0].Int
	if x+y < 15 {
		t.Fatalf("test precondition violated: x+y = %d", x+y)
	}
	mustExec(t, s1, "UPDATE bal SET amount = -5 WHERE id = 1", "")
	mustExec(t, s2, "UPDATE bal SET amount = -5 WHERE id = 2", "")

	if _, err := s1.Execute(context.Background(), "COMMIT", "wc1"); err != nil {
		t.Fatalf("s1 COMMIT: %v", err)
	}
	if _, err := s2.Execute(context.Background(), "COMMIT", "wc2"); err != nil {
		t.Fatalf("s2 COMMIT: %v", err)
	}

	final := mustExec(t, s, "SELECT amount FROM bal", "final")
	total := int64(0)
	for _, row := range final.Rows {
		total += row[0].Int
	}
	if total >= 0 {
		t.Errorf("expected write skew to violate amount1+amount2>=0 under SI, got total=%d", total)
	}
}
