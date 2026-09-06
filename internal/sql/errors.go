package sql

import "errors"

// Sentinel errors for the constrained SQL frontend (docs/sql.md §6).
// Every error a caller can act on programmatically is one of these,
// wrapped with fmt.Errorf("%w: ...") for context at the call site —
// never a bare, unwrapped string. Parser/lexer errors additionally
// carry a byte offset (see PositionError) so a caller can point at the
// exact failing input.
var (
	// ErrSyntax indicates the lexer or parser rejected malformed input
	// (docs/sql.md §3): an unterminated string, an invalid token, a
	// statement that doesn't match the supported grammar, or similar.
	ErrSyntax = errors.New("sql: syntax error")

	// ErrUnsupportedStatement indicates input that lexes and could
	// plausibly be SQL, but names a statement kind outside the Phase 8
	// surface (docs/sql.md §2) — e.g. CREATE INDEX, JOIN, a subquery.
	ErrUnsupportedStatement = errors.New("sql: unsupported statement")

	// ErrUnsupportedFeature indicates a syntactically valid statement
	// that uses a specific feature this constrained subset does not
	// implement (e.g. a non-equality predicate operator, a NULL
	// literal, an unsupported column type).
	ErrUnsupportedFeature = errors.New("sql: unsupported SQL feature")

	// ErrInvalidIdentifier indicates a table or column name outside the
	// allowed identifier grammar (docs/sql.md §4) — empty, too long,
	// containing a disallowed character, or a reserved keyword used as
	// a name.
	ErrInvalidIdentifier = errors.New("sql: invalid identifier")

	// ErrLimitExceeded indicates an input exceeded one of the fixed
	// parser/schema limits in docs/sql.md §7 (statement size, column
	// count, string literal size, identifier length).
	ErrLimitExceeded = errors.New("sql: input exceeds a fixed limit")

	// ErrUnknownTable indicates a statement referenced a table with no
	// recorded schema (no prior successful CREATE TABLE, or a table
	// name that does not match any recorded schema exactly).
	ErrUnknownTable = errors.New("sql: unknown table")

	// ErrDuplicateTable indicates a CREATE TABLE named a table that
	// already has a recorded schema.
	ErrDuplicateTable = errors.New("sql: duplicate table")

	// ErrUnknownColumn indicates a statement referenced a column name
	// not present in the target table's schema.
	ErrUnknownColumn = errors.New("sql: unknown column")

	// ErrDuplicateColumn indicates a CREATE TABLE declared the same
	// column name (after identifier normalization, docs/sql.md §4)
	// more than once.
	ErrDuplicateColumn = errors.New("sql: duplicate column")

	// ErrTypeMismatch indicates a literal's type does not match the
	// declared type of the column it is being assigned or compared to.
	ErrTypeMismatch = errors.New("sql: type mismatch")

	// ErrMissingPrimaryKey indicates a CREATE TABLE declared zero
	// PRIMARY KEY columns, or an INSERT did not supply a value for the
	// table's primary-key column.
	ErrMissingPrimaryKey = errors.New("sql: missing primary key")

	// ErrMultiplePrimaryKeys indicates a CREATE TABLE declared more
	// than one PRIMARY KEY column — this subset supports exactly one.
	ErrMultiplePrimaryKeys = errors.New("sql: multiple primary key columns")

	// ErrDuplicatePrimaryKey indicates an INSERT's primary-key value
	// already has a visible, non-tombstoned row in the target table as
	// of the executing transaction's snapshot.
	ErrDuplicatePrimaryKey = errors.New("sql: duplicate primary key")

	// ErrInvalidPredicate indicates a WHERE clause outside the single
	// supported shape (docs/sql.md §2.4: exactly one equality predicate
	// on the table's primary-key column).
	ErrInvalidPredicate = errors.New("sql: invalid predicate")

	// ErrRowNotFound indicates an UPDATE or DELETE's WHERE predicate
	// matched no visible row (docs/sql.md §5.4-5.5: this constrained
	// subset treats a missing target row as an explicit error, not a
	// silent zero-rows-affected success).
	ErrRowNotFound = errors.New("sql: row not found")

	// ErrNoActiveTransaction indicates COMMIT or ROLLBACK was executed
	// with no prior BEGIN on this Session.
	ErrNoActiveTransaction = errors.New("sql: no active transaction")

	// ErrTransactionAlreadyActive indicates BEGIN was executed while
	// this Session already has an open explicit transaction.
	ErrTransactionAlreadyActive = errors.New("sql: transaction already active")

	// ErrConflict indicates a mutating statement's transaction hit a
	// Snapshot Isolation write-write conflict at commit time
	// (docs/mvcc.md §4) — the identical first-committer-wins rule
	// already proven in Phases 1-7, surfaced through the SQL frontend
	// rather than a new mechanism.
	ErrConflict = errors.New("sql: write-write conflict (first-committer-wins)")
)
