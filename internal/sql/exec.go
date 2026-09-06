// exec.go is the execution layer (docs/sql.md §Execution Path): it
// turns an already-validated plan (plan.go) into calls against a Txn
// (engine.go) — the identical transactional/MVCC/FSM/Raft/quorum path
// every other ChronicleDB mutation already goes through. There is no
// SQL-specific mutation path here: every write below is a Txn.Write or
// Txn.Delete call, and every statement's durability/atomicity/
// isolation/replication behavior is inherited entirely from that Txn's
// backing Engine (ADR-0013) — this file only ever decides *which* keys
// to read/write, never *how* a write becomes durable or replicated.
package sql

import (
	"context"
	"fmt"
)

// ResultKind distinguishes a Result that carries rows (SELECT) from
// one that does not.
type ResultKind int

const (
	ResultOK ResultKind = iota
	ResultRows
)

// Result is the outcome of one executed statement.
type Result struct {
	Kind ResultKind

	// RowsAffected is 1 for a successful INSERT/UPDATE/DELETE, 0 for
	// every other statement kind (docs/sql.md: this subset's
	// predicates only ever address at most one row, by primary key —
	// there is no multi-row UPDATE/DELETE).
	RowsAffected int

	// Columns/Rows are set only when Kind == ResultRows (SELECT).
	// Rows[i][j] corresponds to Columns[j], in the SELECT's requested
	// projection order.
	Columns []string
	Rows    [][]Value

	// CommitSeq is the CommitSeq assigned to the transaction that
	// executed this statement (docs/architecture.md §3) — set on the
	// statement that actually triggers a commit: an implicit
	// auto-commit statement (docs/sql.md §Transactions), or an explicit
	// COMMIT. Zero for every other statement (including one executed
	// inside a still-open explicit transaction).
	CommitSeq uint64
}

// Session is one client's SQL execution context: it tracks whether an
// explicit BEGIN...COMMIT/ROLLBACK transaction is currently open
// (docs/sql.md §Transactions) and, if not, runs each statement as its
// own single-statement auto-commit transaction. A Session is not safe
// for concurrent use by multiple goroutines — exactly like
// internal/txn.Txn, ordinary use is single-goroutine-per-session.
type Session struct {
	engine Engine
	txn    Txn
}

// NewSession returns a Session that begins new transactions against
// engine.
func NewSession(engine Engine) *Session { return &Session{engine: engine} }

// InTransaction reports whether this Session currently has an
// explicit, not-yet-committed/rolled-back BEGIN open.
func (s *Session) InTransaction() bool { return s.txn != nil }

// Execute parses and executes one SQL statement (docs/sql.md
// §SQL Frontend API). requestID is the client-supplied idempotency key
// (docs/architecture.md §3) attached to whichever statement actually
// triggers a commit — see Result.CommitSeq's doc comment for exactly
// when that is. requestID is ignored for a statement that does not
// trigger a commit (a SELECT inside an open explicit transaction, or
// BEGIN itself); the caller is not required to synthesize a
// placeholder for those cases specifically because it is never
// consulted for them, but passing one unconditionally is always safe.
func (s *Session) Execute(ctx context.Context, sqlText string, requestID string) (Result, error) {
	stmt, err := Parse(sqlText)
	if err != nil {
		return Result{}, err
	}
	return s.ExecuteStatement(ctx, stmt, requestID)
}

// ExecuteStatement executes an already-parsed Statement — the entry
// point for a caller that parsed once and wants to execute (or
// re-execute, e.g. for a retry) without re-parsing.
func (s *Session) ExecuteStatement(ctx context.Context, stmt Statement, requestID string) (Result, error) {
	switch stmt.(type) {
	case *BeginStmt:
		return s.execBegin(ctx)
	case *CommitStmt:
		return s.execCommit(requestID)
	case *RollbackStmt:
		return s.execRollback()
	default:
		return s.executeDML(ctx, stmt, requestID)
	}
}

func (s *Session) execBegin(ctx context.Context) (Result, error) {
	if s.txn != nil {
		return Result{}, ErrTransactionAlreadyActive
	}
	t, err := s.engine.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	s.txn = t
	return Result{Kind: ResultOK}, nil
}

func (s *Session) execCommit(requestID string) (Result, error) {
	if s.txn == nil {
		return Result{}, ErrNoActiveTransaction
	}
	t := s.txn
	s.txn = nil
	seq, err := t.Commit(requestID)
	if err != nil {
		return Result{}, err
	}
	return Result{Kind: ResultOK, CommitSeq: seq}, nil
}

func (s *Session) execRollback() (Result, error) {
	if s.txn == nil {
		return Result{}, ErrNoActiveTransaction
	}
	t := s.txn
	s.txn = nil
	if err := t.Abort(); err != nil {
		return Result{}, err
	}
	return Result{Kind: ResultOK}, nil
}

// executeDML runs a CREATE TABLE/INSERT/SELECT/UPDATE/DELETE statement.
// Inside an open explicit transaction, it executes against that same
// Txn without committing (docs/transactions.md §3: the whole explicit
// transaction becomes one CommitTxn command at COMMIT, not one per
// statement) and, per docs/sql.md §Transactions, a statement error
// aborts the entire explicit transaction rather than leaving it open in
// a partially-failed state — this subset does not implement recovering
// from one failed statement within an otherwise-still-usable
// transaction. Outside an explicit transaction, it begins, executes,
// and commits a fresh single-statement transaction — the documented
// auto-commit behavior (docs/sql.md §Transactions).
func (s *Session) executeDML(ctx context.Context, stmt Statement, requestID string) (Result, error) {
	explicit := s.txn != nil

	t := s.txn
	if !explicit {
		var err error
		t, err = s.engine.Begin(ctx)
		if err != nil {
			return Result{}, err
		}
	}

	// Idempotency short-circuit for a genuine retry of an auto-commit
	// mutating statement (docs/transactions.md §6): if requestID
	// already has a durably recorded outcome, return it directly
	// without re-executing the statement at all. This is required, not
	// just an optimization — see mutationRetryResult's doc comment for
	// why re-running (e.g.) INSERT's duplicate-primary-key check
	// against post-effect state would otherwise incorrectly reject the
	// identical retry it is supposed to make safe.
	//
	// This check MUST run after Begin, not before: on a replicated
	// Engine, Begin's ReadIndex round trip (docs/replication.md §4)
	// blocks until this node's own applied state has caught up to a
	// fresh watermark that necessarily already includes the original
	// commit (Raft's leader-completeness guarantee — a new leader's
	// log already contains every previously committed entry, so a
	// retry's own ReadIndex target is always at or beyond it). Checking
	// LookupOutcome before Begin would race a possibly-still-catching-
	// up node: the lookup could miss even though the row is *about to*
	// become visible from underneath the very statement this short-
	// circuit was trying to protect, causing the exact incorrect
	// "duplicate key" rejection this short-circuit exists to prevent.
	if !explicit && requestID != "" && isMutatingStatement(stmt) {
		if outcome, found := s.engine.LookupOutcome(requestID); found {
			_ = t.Abort()
			return mutationRetryResult(stmt, outcome)
		}
	}

	result, err := execOne(t, stmt)
	if err != nil {
		if explicit {
			s.txn = nil
		}
		_ = t.Abort()
		return Result{}, err
	}

	if !explicit {
		seq, cerr := t.Commit(requestID)
		if cerr != nil {
			return Result{}, cerr
		}
		result.CommitSeq = seq
	}
	return result, nil
}

// isMutatingStatement reports whether stmt is a statement kind whose
// auto-commit execution durably records a RequestID outcome
// (docs/transactions.md §9: a pure read never does), and is therefore
// eligible for the idempotency short-circuit above.
func isMutatingStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *CreateTableStmt, *InsertStmt, *UpdateStmt, *DeleteStmt:
		return true
	default:
		return false
	}
}

// mutationRetryResult reconstructs the Result a genuine retry
// (identical RequestID) of a mutating auto-commit statement must
// return, purely from its already-decided RequestOutcome
// (docs/transactions.md §6) — without touching the target table's
// current state at all.
//
// This is not merely an optimization: INSERT's duplicate-primary-key
// check, in particular, reads the target row to decide whether to
// reject the statement. By the time a legitimate retry runs, that read
// would see the *original* attempt's own already-committed row and
// incorrectly reject the retry as "a duplicate" — even though it is
// the same logical request, which docs/transactions.md §6 guarantees
// must succeed identically on retry, not be re-evaluated. Short-
// circuiting on the recorded RequestOutcome, before any such
// statement-specific validation runs, is what makes the retry
// contract actually hold at the SQL layer.
func mutationRetryResult(stmt Statement, outcome RequestOutcome) (Result, error) {
	if !outcome.Committed {
		return Result{}, fmt.Errorf("%w: retried RequestID previously resolved ABORTED (first-committer-wins conflict)", ErrConflict)
	}
	if _, ok := stmt.(*CreateTableStmt); ok {
		return Result{Kind: ResultOK, CommitSeq: outcome.CommitSeq}, nil
	}
	return Result{Kind: ResultOK, RowsAffected: 1, CommitSeq: outcome.CommitSeq}, nil
}

// execOne dispatches one non-transaction-control statement to its
// planner+executor.
func execOne(t Txn, stmt Statement) (Result, error) {
	switch st := stmt.(type) {
	case *CreateTableStmt:
		return execCreateTable(t, st)
	case *InsertStmt:
		return execInsert(t, st)
	case *SelectStmt:
		return execSelect(t, st)
	case *UpdateStmt:
		return execUpdate(t, st)
	case *DeleteStmt:
		return execDelete(t, st)
	default:
		return Result{}, fmt.Errorf("%w: %T", ErrUnsupportedStatement, stmt)
	}
}

// execCreateTable implements docs/sql.md §CREATE TABLE: validate the
// declared schema, check for a duplicate table name against committed
// state (not any local cache), and write the encoded schema through
// the real transaction path — schema mutation is not special-cased
// outside of it.
func execCreateTable(t Txn, stmt *CreateTableStmt) (Result, error) {
	schema, err := buildSchema(stmt)
	if err != nil {
		return Result{}, err
	}
	key := schemaKey(schema.Table)
	_, found, err := t.Read(key)
	if err != nil {
		return Result{}, err
	}
	if found {
		return Result{}, fmt.Errorf("%w: %q", ErrDuplicateTable, schema.Table)
	}
	if err := t.Write(key, encodeSchema(schema)); err != nil {
		return Result{}, err
	}
	return Result{Kind: ResultOK}, nil
}

// execInsert implements docs/sql.md §INSERT: resolve and type-check
// against the table's real committed schema, reject a primary key that
// already has a visible row, then write the encoded row.
func execInsert(t Txn, stmt *InsertStmt) (Result, error) {
	schema, err := getSchema(t, stmt.Table)
	if err != nil {
		return Result{}, err
	}
	plan, err := planInsert(schema, stmt)
	if err != nil {
		return Result{}, err
	}
	key := rowKey(schema.Table, plan.pk)
	_, found, err := t.Read(key)
	if err != nil {
		return Result{}, err
	}
	if found {
		return Result{}, fmt.Errorf("%w: table %q primary key %s", ErrDuplicatePrimaryKey, schema.Table, plan.pk)
	}
	if err := t.Write(key, encodeRow(schema, plan.values)); err != nil {
		return Result{}, err
	}
	return Result{Kind: ResultOK, RowsAffected: 1}, nil
}

// execSelect implements docs/sql.md §SELECT's two execution shapes: a
// primary-key point lookup (Txn.Read, O(1)-ish) when the statement has
// a WHERE clause, or a full-table scan (Txn.ScanPrefix, O(size of the
// whole store) — see docs/sql.md §5.2) when it does not.
func execSelect(t Txn, stmt *SelectStmt) (Result, error) {
	schema, err := getSchema(t, stmt.Table)
	if err != nil {
		return Result{}, err
	}
	plan, err := planSelect(schema, stmt)
	if err != nil {
		return Result{}, err
	}
	columns := make([]string, len(plan.columnIdx))
	for i, ci := range plan.columnIdx {
		columns[i] = schema.Columns[ci].Name
	}

	var rows [][]Value
	if plan.pkPredicate != nil {
		b, found, err := t.Read(rowKey(schema.Table, *plan.pkPredicate))
		if err != nil {
			return Result{}, err
		}
		if found {
			values, err := decodeRow(schema, b)
			if err != nil {
				return Result{}, err
			}
			rows = append(rows, projectRow(values, plan.columnIdx))
		}
	} else {
		kvs, err := t.ScanPrefix(rowKeyPrefix(schema.Table))
		if err != nil {
			return Result{}, err
		}
		rows = make([][]Value, 0, len(kvs))
		for _, kv := range kvs {
			values, err := decodeRow(schema, kv.Value)
			if err != nil {
				return Result{}, err
			}
			rows = append(rows, projectRow(values, plan.columnIdx))
		}
	}
	return Result{Kind: ResultRows, Columns: columns, Rows: rows}, nil
}

func projectRow(values []Value, idx []int) []Value {
	out := make([]Value, len(idx))
	for i, ci := range idx {
		out[i] = values[ci]
	}
	return out
}

// execUpdate implements docs/sql.md §UPDATE: the target row must exist
// (docs/sql.md §5.4 — a missing row is an explicit ErrRowNotFound in
// this subset, not a silent zero-rows-affected success), and the whole
// read-modify-write is one read plus one write against the same Txn, so
// it participates in that Txn's ordinary first-committer-wins conflict
// check at commit time exactly like any other write (docs/mvcc.md §4)
// — there is no separate locking mechanism.
func execUpdate(t Txn, stmt *UpdateStmt) (Result, error) {
	schema, err := getSchema(t, stmt.Table)
	if err != nil {
		return Result{}, err
	}
	plan, err := planUpdate(schema, stmt)
	if err != nil {
		return Result{}, err
	}
	key := rowKey(schema.Table, plan.pk)
	b, found, err := t.Read(key)
	if err != nil {
		return Result{}, err
	}
	if !found {
		return Result{}, fmt.Errorf("%w: table %q primary key %s", ErrRowNotFound, schema.Table, plan.pk)
	}
	values, err := decodeRow(schema, b)
	if err != nil {
		return Result{}, err
	}
	for _, a := range plan.assignments {
		values[a.Index] = a.Value
	}
	if err := t.Write(key, encodeRow(schema, values)); err != nil {
		return Result{}, err
	}
	return Result{Kind: ResultOK, RowsAffected: 1}, nil
}

// execDelete implements docs/sql.md §DELETE: the target row must
// exist (identically to UPDATE's ErrRowNotFound rule), and the delete
// itself is an ordinary Txn.Delete — a real MVCC tombstone written
// through the same transactional/replicated path as every other
// mutation (docs/mvcc.md §2), not a special "SQL delete" mechanism.
func execDelete(t Txn, stmt *DeleteStmt) (Result, error) {
	schema, err := getSchema(t, stmt.Table)
	if err != nil {
		return Result{}, err
	}
	plan, err := planDelete(schema, stmt)
	if err != nil {
		return Result{}, err
	}
	key := rowKey(schema.Table, plan.pk)
	_, found, err := t.Read(key)
	if err != nil {
		return Result{}, err
	}
	if !found {
		return Result{}, fmt.Errorf("%w: table %q primary key %s", ErrRowNotFound, schema.Table, plan.pk)
	}
	if err := t.Delete(key); err != nil {
		return Result{}, err
	}
	return Result{Kind: ResultOK, RowsAffected: 1}, nil
}
