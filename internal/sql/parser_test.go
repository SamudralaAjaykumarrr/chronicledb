package sql

import (
	"errors"
	"testing"
)

func mustParse(t *testing.T, src string) Statement {
	t.Helper()
	stmt, err := Parse(src)
	if err != nil {
		t.Fatalf("Parse(%q): unexpected error: %v", src, err)
	}
	return stmt
}

func TestParseCreateTableValid(t *testing.T) {
	stmt := mustParse(t, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)")
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	if ct.Table != "users" {
		t.Errorf("Table = %q, want %q", ct.Table, "users")
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("len(Columns) = %d, want 3", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].Type != TypeInteger || !ct.Columns[0].PrimaryKey {
		t.Errorf("Columns[0] = %+v, want id/INTEGER/PK", ct.Columns[0])
	}
	if ct.Columns[1].Name != "name" || ct.Columns[1].Type != TypeText || ct.Columns[1].PrimaryKey {
		t.Errorf("Columns[1] = %+v, want name/TEXT/not-PK", ct.Columns[1])
	}
	if ct.Columns[2].Name != "active" || ct.Columns[2].Type != TypeBoolean {
		t.Errorf("Columns[2] = %+v, want active/BOOLEAN", ct.Columns[2])
	}
}

func TestParseCreateTableCaseInsensitive(t *testing.T) {
	stmt := mustParse(t, "create table Users (Id integer primary key)")
	ct := stmt.(*CreateTableStmt)
	if ct.Table != "users" {
		t.Errorf("Table = %q, want folded %q", ct.Table, "users")
	}
	if ct.Columns[0].Name != "id" {
		t.Errorf("Columns[0].Name = %q, want folded %q", ct.Columns[0].Name, "id")
	}
}

func TestParseInsertValid(t *testing.T) {
	stmt := mustParse(t, "INSERT INTO users VALUES (1, 'alice', true)")
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("got %T, want *InsertStmt", stmt)
	}
	if ins.Table != "users" || ins.Columns != nil {
		t.Errorf("Table=%q Columns=%v", ins.Table, ins.Columns)
	}
	if len(ins.Values) != 3 || ins.Values[0].Kind != LiteralInt || ins.Values[0].Int != 1 {
		t.Errorf("Values = %+v", ins.Values)
	}
	if ins.Values[1].Kind != LiteralString || ins.Values[1].Str != "alice" {
		t.Errorf("Values[1] = %+v", ins.Values[1])
	}
	if ins.Values[2].Kind != LiteralBool || !ins.Values[2].Bool {
		t.Errorf("Values[2] = %+v", ins.Values[2])
	}
}

func TestParseInsertWithColumnList(t *testing.T) {
	stmt := mustParse(t, "INSERT INTO users (name, id) VALUES ('bob', 2)")
	ins := stmt.(*InsertStmt)
	if len(ins.Columns) != 2 || ins.Columns[0] != "name" || ins.Columns[1] != "id" {
		t.Errorf("Columns = %v", ins.Columns)
	}
}

func TestParseSelectVariants(t *testing.T) {
	stmt := mustParse(t, "SELECT * FROM users")
	sel := stmt.(*SelectStmt)
	if sel.Table != "users" || sel.Columns != nil || sel.Where != nil {
		t.Errorf("got %+v", sel)
	}

	stmt2 := mustParse(t, "SELECT id, name FROM users WHERE id = 5")
	sel2 := stmt2.(*SelectStmt)
	if len(sel2.Columns) != 2 || sel2.Columns[0] != "id" || sel2.Columns[1] != "name" {
		t.Errorf("Columns = %v", sel2.Columns)
	}
	if sel2.Where == nil || sel2.Where.Column != "id" || sel2.Where.Value.Int != 5 {
		t.Errorf("Where = %+v", sel2.Where)
	}
}

func TestParseSelectNegativeIntPredicate(t *testing.T) {
	stmt := mustParse(t, "SELECT * FROM t WHERE id = -5")
	sel := stmt.(*SelectStmt)
	if sel.Where == nil || sel.Where.Value.Int != -5 {
		t.Errorf("Where = %+v", sel.Where)
	}
}

func TestParseUpdateValid(t *testing.T) {
	stmt := mustParse(t, "UPDATE users SET name = 'carol', active = false WHERE id = 1")
	upd := stmt.(*UpdateStmt)
	if upd.Table != "users" {
		t.Errorf("Table = %q", upd.Table)
	}
	if len(upd.Assignments) != 2 {
		t.Fatalf("len(Assignments) = %d", len(upd.Assignments))
	}
	if upd.Assignments[0].Column != "name" || upd.Assignments[0].Value.Str != "carol" {
		t.Errorf("Assignments[0] = %+v", upd.Assignments[0])
	}
	if upd.Where.Column != "id" || upd.Where.Value.Int != 1 {
		t.Errorf("Where = %+v", upd.Where)
	}
}

func TestParseUpdateRequiresWhere(t *testing.T) {
	_, err := Parse("UPDATE users SET name = 'x'")
	if !errors.Is(err, ErrInvalidPredicate) {
		t.Errorf("err = %v, want ErrInvalidPredicate", err)
	}
}

func TestParseDeleteValid(t *testing.T) {
	stmt := mustParse(t, "DELETE FROM users WHERE id = 7")
	del := stmt.(*DeleteStmt)
	if del.Table != "users" || del.Where.Column != "id" || del.Where.Value.Int != 7 {
		t.Errorf("got %+v", del)
	}
}

func TestParseDeleteRequiresWhere(t *testing.T) {
	_, err := Parse("DELETE FROM users")
	if !errors.Is(err, ErrInvalidPredicate) {
		t.Errorf("err = %v, want ErrInvalidPredicate", err)
	}
}

func TestParseBeginCommitRollback(t *testing.T) {
	if _, ok := mustParse(t, "BEGIN").(*BeginStmt); !ok {
		t.Error("BEGIN did not parse to *BeginStmt")
	}
	if _, ok := mustParse(t, "COMMIT").(*CommitStmt); !ok {
		t.Error("COMMIT did not parse to *CommitStmt")
	}
	if _, ok := mustParse(t, "ROLLBACK").(*RollbackStmt); !ok {
		t.Error("ROLLBACK did not parse to *RollbackStmt")
	}
}

func TestParseTrailingSemicolonAccepted(t *testing.T) {
	mustParse(t, "SELECT * FROM users;")
}

func TestParseTrailingGarbageRejected(t *testing.T) {
	_, err := Parse("SELECT * FROM users; SELECT * FROM users")
	if !errors.Is(err, ErrSyntax) {
		t.Errorf("err = %v, want ErrSyntax", err)
	}
}

func TestParseMalformedSyntax(t *testing.T) {
	cases := []string{
		"CREATE TABLE (id INTEGER PRIMARY KEY)", // missing table name
		"CREATE TABLE t id INTEGER",             // missing parens
		"INSERT INTO t VALUES",                  // missing parens/values
		"SELECT FROM t",                         // missing column list/star
		"SELECT * t",                            // missing FROM
		"UPDATE t WHERE id = 1",                 // missing SET
		"DELETE t WHERE id = 1",                 // missing FROM
		"",                                      // empty
		"   ",                                   // whitespace only
	}
	for _, src := range cases {
		if _, err := Parse(src); err == nil {
			t.Errorf("Parse(%q): expected error, got nil", src)
		}
	}
}

func TestParseUnsupportedStatement(t *testing.T) {
	cases := []string{
		"DROP TABLE t",
		"CREATE INDEX idx ON t (id)",
		"SELECT * FROM t JOIN u ON t.id = u.id",
		"ALTER TABLE t ADD COLUMN c INTEGER",
	}
	for _, src := range cases {
		_, err := Parse(src)
		if err == nil {
			t.Errorf("Parse(%q): expected error, got nil", src)
			continue
		}
		if !errors.Is(err, ErrUnsupportedStatement) && !errors.Is(err, ErrSyntax) {
			t.Errorf("Parse(%q): err = %v, want ErrUnsupportedStatement or ErrSyntax", src, err)
		}
	}
}

func TestParseInvalidPredicateShapes(t *testing.T) {
	cases := []string{
		"SELECT * FROM t WHERE id > 5",           // unsupported operator
		"SELECT * FROM t WHERE id = 1 AND x = 2", // conjunction
	}
	for _, src := range cases {
		if _, err := Parse(src); err == nil {
			t.Errorf("Parse(%q): expected error, got nil", src)
		}
	}
}

func TestParseUnsupportedColumnType(t *testing.T) {
	_, err := Parse("CREATE TABLE t (id VARCHAR PRIMARY KEY)")
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Errorf("err = %v, want ErrUnsupportedFeature", err)
	}
}

func TestParseNullLiteralRejected(t *testing.T) {
	_, err := Parse("INSERT INTO t VALUES (NULL)")
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Errorf("err = %v, want ErrUnsupportedFeature", err)
	}
}

func TestParseKeywordAsIdentifierRejected(t *testing.T) {
	cases := []string{
		"CREATE TABLE select (id INTEGER PRIMARY KEY)",
		"CREATE TABLE t (from INTEGER PRIMARY KEY)",
		"SELECT * FROM where",
	}
	for _, src := range cases {
		_, err := Parse(src)
		if !errors.Is(err, ErrInvalidIdentifier) {
			t.Errorf("Parse(%q): err = %v, want ErrInvalidIdentifier", src, err)
		}
	}
}

func TestParseBadLiterals(t *testing.T) {
	cases := []string{
		"INSERT INTO t VALUES ('unterminated)",
		"INSERT INTO t VALUES (12abc)",
		"SELECT * FROM t WHERE id = @",
	}
	for _, src := range cases {
		if _, err := Parse(src); err == nil {
			t.Errorf("Parse(%q): expected error, got nil", src)
		}
	}
}

func TestParseStringEscapedQuote(t *testing.T) {
	stmt := mustParse(t, "INSERT INTO t VALUES ('it''s')")
	ins := stmt.(*InsertStmt)
	if ins.Values[0].Str != "it's" {
		t.Errorf("Str = %q, want %q", ins.Values[0].Str, "it's")
	}
}

func TestParseIdentifierLengthLimit(t *testing.T) {
	long := make([]byte, MaxIdentifierBytes+1)
	for i := range long {
		long[i] = 'a'
	}
	_, err := Parse("SELECT * FROM " + string(long))
	if !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestParseStatementLengthLimit(t *testing.T) {
	huge := make([]byte, MaxStatementBytes+1)
	for i := range huge {
		huge[i] = ' '
	}
	_, err := Parse(string(huge))
	if !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestParseIntegerOverflowRejected(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE id = 999999999999999999999999999999")
	if err == nil {
		t.Error("expected error for oversized integer literal")
	}
}

func TestParseNeverPanicsOnArbitraryInput(t *testing.T) {
	inputs := []string{
		"(((((((((((", ")))))))))))", "''''''''''", "=====",
		"CREATE", "CREATE TABLE", "CREATE TABLE t (", "CREATE TABLE t ()",
		"SELECT", "SELECT *", "SELECT * FROM", "WHERE", ";;;;;",
		"\x00\x01\x02", "SELECT * FROM t WHERE",
	}
	for _, src := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Parse(%q) panicked: %v", src, r)
				}
			}()
			Parse(src)
		}()
	}
}
