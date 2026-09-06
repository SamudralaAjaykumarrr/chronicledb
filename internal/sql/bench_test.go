// Microbenchmarks for internal/sql (docs/roadmap.md Phase 9 §SQL):
// the lexer/parser in isolation, and each DML statement kind executed
// against a real, standalone (single-process, real internal/wal-backed
// internal/txn.Manager) engine — never a mock of persistence
// (docs/roadmap.md's "do not cheat benchmarks"). Parser cost is
// deliberately kept separate from execution cost per this phase's
// brief ("do not combine parser cost with database execution unless
// the benchmark explicitly says end-to-end").
//
// Run: go test ./internal/sql/... -run '^$' -bench . -benchmem
package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/SamudralaAjaykumarrr/chronicledb/internal/fsm"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/mvcc"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/txn"
	"github.com/SamudralaAjaykumarrr/chronicledb/internal/wal"
)

func openBenchManager(b *testing.B) *txn.Manager {
	b.Helper()
	w, _, err := wal.Open(b.TempDir(), wal.Options{})
	if err != nil {
		b.Fatalf("wal.Open: %v", err)
	}
	b.Cleanup(func() { w.Close() })
	mgr, err := txn.NewManager(w, mvcc.NewStore())
	if err != nil {
		b.Fatalf("txn.NewManager: %v", err)
	}
	return mgr
}

func newBenchSession(b *testing.B) *Session {
	b.Helper()
	return NewSession(NewStandaloneEngine(openBenchManager(b)))
}

func mustExecBench(b *testing.B, s *Session, sqlText, requestID string) Result {
	b.Helper()
	res, err := s.Execute(context.Background(), sqlText, requestID)
	if err != nil {
		b.Fatalf("Execute(%q): %v", sqlText, err)
	}
	return res
}

// --- Parser-only benchmarks (docs/roadmap.md §SQL "lexer/parser") ---

func BenchmarkSQLParseCreateTable(b *testing.B) {
	src := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(src); err != nil {
			b.Fatalf("Parse: %v", err)
		}
	}
}

func BenchmarkSQLParseInsert(b *testing.B) {
	src := "INSERT INTO users VALUES (1, 'alice', true)"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(src); err != nil {
			b.Fatalf("Parse: %v", err)
		}
	}
}

func BenchmarkSQLParseSelectByPrimaryKey(b *testing.B) {
	src := "SELECT * FROM users WHERE id = 1"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Parse(src); err != nil {
			b.Fatalf("Parse: %v", err)
		}
	}
}

// --- Bind/plan benchmark (docs/roadmap.md §SQL "semantic planning") ---

// BenchmarkSQLBindInsert isolates planInsert's schema-resolution/
// type-checking cost (plan.go) from both parsing and execution: the
// statement is parsed once outside the timed loop, and the schema is
// read once via a throwaway Txn per iteration only because getSchema
// requires a Txn — the timed cost is dominated by binding, not by the
// trivial in-memory schema read on a tiny store.
func BenchmarkSQLBindInsert(b *testing.B) {
	mgr := openBenchManager(b)
	setupTx := mgr.Begin()
	schema, err := buildSchemaFromSQL(setupTx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)")
	if err != nil {
		b.Fatalf("setup schema: %v", err)
	}
	if _, err := setupTx.Commit(fsm.RequestID("create-users")); err != nil {
		b.Fatalf("commit schema: %v", err)
	}
	stmt, err := Parse("INSERT INTO users VALUES (1, 'alice', true)")
	if err != nil {
		b.Fatalf("Parse: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := planInsert(schema, insertStmt); err != nil {
			b.Fatalf("planInsert: %v", err)
		}
	}
}

// buildSchemaFromSQL is a small bench-only helper: parse+create a
// table through the real engine, then read back its committed Schema
// exactly as getSchema (plan.go) does, for BenchmarkSQLBindInsert's
// isolated planInsert call.
func buildSchemaFromSQL(t *txn.Txn, createSQL string) (Schema, error) {
	stmt, err := Parse(createSQL)
	if err != nil {
		return Schema{}, err
	}
	cts := stmt.(*CreateTableStmt)
	schema, err := buildSchema(cts)
	if err != nil {
		return Schema{}, err
	}
	if err := t.Write(schemaKey(schema.Table), encodeSchema(schema)); err != nil {
		return Schema{}, err
	}
	return schema, nil
}

// --- End-to-end statement benchmarks (real standalone engine) ---

func BenchmarkSQLInsert(b *testing.B) {
	s := newBenchSession(b)
	mustExecBench(b, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)", "create-users")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlText := fmt.Sprintf("INSERT INTO users VALUES (%d, 'name', true)", i)
		if _, err := s.Execute(context.Background(), sqlText, fmt.Sprintf("ins-%d", i)); err != nil {
			b.Fatalf("INSERT: %v", err)
		}
	}
}

// BenchmarkSQLPrimaryKeySelect repeatedly reads the same single row —
// the point-lookup shape docs/sql.md §2.4 supports (an equality-on-
// primary-key predicate).
func BenchmarkSQLPrimaryKeySelect(b *testing.B) {
	s := newBenchSession(b)
	mustExecBench(b, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)", "create-users")
	mustExecBench(b, s, "INSERT INTO users VALUES (1, 'alice', true)", "ins-1")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := s.Execute(context.Background(), "SELECT * FROM users WHERE id = 1", "")
		if err != nil {
			b.Fatalf("SELECT: %v", err)
		}
		if len(res.Rows) != 1 {
			b.Fatalf("SELECT returned %d rows, want 1", len(res.Rows))
		}
	}
}

// BenchmarkSQLUpdate repeatedly updates the same single row — an
// UPDATE's cost (schema read + primary-key read + write) does not
// depend on which row is targeted, so this isolates per-operation cost
// from table size.
func BenchmarkSQLUpdate(b *testing.B) {
	s := newBenchSession(b)
	mustExecBench(b, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)", "create-users")
	mustExecBench(b, s, "INSERT INTO users VALUES (1, 'alice', true)", "ins-1")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlText := fmt.Sprintf("UPDATE users SET name = 'name-%d' WHERE id = 1", i)
		if _, err := s.Execute(context.Background(), sqlText, fmt.Sprintf("upd-%d", i)); err != nil {
			b.Fatalf("UPDATE: %v", err)
		}
	}
}

// BenchmarkSQLDelete deletes b.N distinct, pre-inserted rows — DELETE
// requires the target row to exist (docs/sql.md), so unlike UPDATE/
// SELECT this cannot repeat the same row. Setup runs before
// b.ResetTimer, matching this phase's brief ("do not put expensive
// validation inside the timed inner loop unless deliberate... use
// setup/teardown correctly").
func BenchmarkSQLDelete(b *testing.B) {
	s := newBenchSession(b)
	mustExecBench(b, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)", "create-users")
	for i := 0; i < b.N; i++ {
		sqlText := fmt.Sprintf("INSERT INTO users VALUES (%d, 'name', true)", i)
		if _, err := s.Execute(context.Background(), sqlText, fmt.Sprintf("ins-%d", i)); err != nil {
			b.Fatalf("setup INSERT: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlText := fmt.Sprintf("DELETE FROM users WHERE id = %d", i)
		if _, err := s.Execute(context.Background(), sqlText, fmt.Sprintf("del-%d", i)); err != nil {
			b.Fatalf("DELETE: %v", err)
		}
	}
}

// BenchmarkSQLFullTableScan measures a predicate-less SELECT
// (docs/sql.md §5.2: necessarily a full scan of the entire store, not
// an indexed range scan) at several controlled row counts.
func BenchmarkSQLFullTableScan(b *testing.B) {
	for _, n := range []int{10, 100, 1_000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			s := newBenchSession(b)
			mustExecBench(b, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)", "create-users")
			for i := 0; i < n; i++ {
				sqlText := fmt.Sprintf("INSERT INTO users VALUES (%d, 'name', true)", i)
				if _, err := s.Execute(context.Background(), sqlText, fmt.Sprintf("ins-%d", i)); err != nil {
					b.Fatalf("setup INSERT: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := s.Execute(context.Background(), "SELECT * FROM users", "")
				if err != nil {
					b.Fatalf("SELECT: %v", err)
				}
				if len(res.Rows) != n {
					b.Fatalf("SELECT returned %d rows, want %d", len(res.Rows), n)
				}
			}
		})
	}
}
