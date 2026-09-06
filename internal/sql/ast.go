package sql

// Statement is the sealed set of AST nodes the parser can produce
// (docs/sql.md §2: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE,
// BEGIN, COMMIT, ROLLBACK). AST nodes are explicit typed structs, not
// raw token slices or SQL substrings — execution never re-parses text
// (docs/sql.md §Parser/AST: "avoid passing raw SQL strings deep into
// execution").
type Statement interface {
	stmtNode()
}

// ColumnDef is one column declaration inside a CREATE TABLE.
type ColumnDef struct {
	Name       string // already identifier-normalized (foldIdent)
	Type       ColumnType
	PrimaryKey bool
}

// CreateTableStmt is `CREATE TABLE name (col type [PRIMARY KEY], ...)`.
type CreateTableStmt struct {
	Table   string
	Columns []ColumnDef
}

// InsertStmt is `INSERT INTO name [(col, ...)] VALUES (literal, ...)`.
// Columns is nil when the statement did not name an explicit column
// list — the binder (plan.go) resolves that to "every column, in
// schema-declared order" against the target table's actual schema.
type InsertStmt struct {
	Table   string
	Columns []string
	Values  []Literal
}

// LiteralKind distinguishes which field of Literal is meaningful.
type LiteralKind int

const (
	LiteralInt LiteralKind = iota
	LiteralString
	LiteralBool
)

// Literal is a parsed SQL literal, before it is checked against any
// column's declared type (that check happens in the binder, plan.go,
// once the target column's type is known) — the AST layer itself does
// not need schema knowledge to represent a literal.
type Literal struct {
	Kind LiteralKind
	Int  int64
	Str  string
	Bool bool
}

// Predicate is this subset's only supported WHERE shape: a single
// equality comparison, `column = literal` (docs/sql.md §2.4). AND/OR,
// non-equality operators, and predicates on non-primary-key columns are
// all rejected by the binder (plan.go), not representable here beyond
// this one shape — there is deliberately no general expression tree.
type Predicate struct {
	Column string
	Value  Literal
}

// SelectStmt is `SELECT col, ... | * FROM name [WHERE predicate]`.
// Columns is nil for `SELECT *`.
type SelectStmt struct {
	Table   string
	Columns []string
	Where   *Predicate
}

// Assignment is one `column = literal` pair inside an UPDATE's SET
// clause.
type Assignment struct {
	Column string
	Value  Literal
}

// UpdateStmt is `UPDATE name SET col = literal, ... WHERE predicate`.
// WHERE is mandatory (docs/sql.md §2.5) — an UPDATE naming no predicate
// at all is rejected by the parser, not merely discouraged.
type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Predicate
}

// DeleteStmt is `DELETE FROM name WHERE predicate`. WHERE is mandatory,
// identically to UpdateStmt.
type DeleteStmt struct {
	Table string
	Where Predicate
}

// BeginStmt is `BEGIN` — starts an explicit multi-statement
// transaction on the executing Session (docs/sql.md §Transactions).
type BeginStmt struct{}

// CommitStmt is `COMMIT` — submits the Session's open explicit
// transaction's accumulated mutation set as one deterministic
// CommitTxn command (docs/transactions.md §3).
type CommitStmt struct{}

// RollbackStmt is `ROLLBACK` — discards the Session's open explicit
// transaction's accumulated local writes without a trace
// (docs/transactions.md §2).
type RollbackStmt struct{}

func (*CreateTableStmt) stmtNode() {}
func (*InsertStmt) stmtNode()      {}
func (*SelectStmt) stmtNode()      {}
func (*UpdateStmt) stmtNode()      {}
func (*DeleteStmt) stmtNode()      {}
func (*BeginStmt) stmtNode()       {}
func (*CommitStmt) stmtNode()      {}
func (*RollbackStmt) stmtNode()    {}
