package sql

import (
	"errors"
	"reflect"
	"testing"
)

func TestBuildSchemaValid(t *testing.T) {
	stmt := mustParse(t, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").(*CreateTableStmt)
	s, err := buildSchema(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Table != "users" || s.PrimaryKey != 0 {
		t.Errorf("got %+v", s)
	}
	if len(s.Columns) != 2 {
		t.Fatalf("len(Columns) = %d", len(s.Columns))
	}
}

func TestBuildSchemaDuplicateColumn(t *testing.T) {
	stmt := mustParse(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, id TEXT)").(*CreateTableStmt)
	_, err := buildSchema(stmt)
	if !errors.Is(err, ErrDuplicateColumn) {
		t.Errorf("err = %v, want ErrDuplicateColumn", err)
	}
}

func TestBuildSchemaMissingPrimaryKey(t *testing.T) {
	stmt := mustParse(t, "CREATE TABLE t (id INTEGER, name TEXT)").(*CreateTableStmt)
	_, err := buildSchema(stmt)
	if !errors.Is(err, ErrMissingPrimaryKey) {
		t.Errorf("err = %v, want ErrMissingPrimaryKey", err)
	}
}

func TestBuildSchemaMultiplePrimaryKeys(t *testing.T) {
	stmt := mustParse(t, "CREATE TABLE t (id INTEGER PRIMARY KEY, other TEXT PRIMARY KEY)").(*CreateTableStmt)
	_, err := buildSchema(stmt)
	if !errors.Is(err, ErrMultiplePrimaryKeys) {
		t.Errorf("err = %v, want ErrMultiplePrimaryKeys", err)
	}
}

func TestBuildSchemaTooManyColumns(t *testing.T) {
	cols := make([]ColumnDef, 0, MaxColumns+1)
	cols = append(cols, ColumnDef{Name: "pk", Type: TypeInteger, PrimaryKey: true})
	for i := 0; i < MaxColumns; i++ {
		cols = append(cols, ColumnDef{Name: "c" + string(rune('a'+i%26)) + string(rune('0'+i%10)), Type: TypeInteger})
	}
	stmt := &CreateTableStmt{Table: "t", Columns: cols}
	_, err := buildSchema(stmt)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestSchemaEncodeDecodeRoundTrip(t *testing.T) {
	s := Schema{
		Table: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInteger},
			{Name: "name", Type: TypeText},
			{Name: "active", Type: TypeBoolean},
		},
		PrimaryKey: 0,
	}
	b := encodeSchema(s)
	got, err := decodeSchema(b)
	if err != nil {
		t.Fatalf("decodeSchema: %v", err)
	}
	if !reflect.DeepEqual(s, got) {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v", got, s)
	}
}

func TestSchemaDecodeRejectsMalformed(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{1},
		{9, 0, 0},           // wrong version
		{1, 0, 5, 'a', 'b'}, // truncated table name
	}
	for i, b := range cases {
		if _, err := decodeSchema(b); err == nil {
			t.Errorf("case %d: decodeSchema(%v): expected error, got nil", i, b)
		}
	}
}

func TestSchemaDecodeNeverPanics(t *testing.T) {
	for i := 0; i < 2000; i++ {
		b := randBytes(i % 64)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decodeSchema panicked on %v: %v", b, r)
				}
			}()
			decodeSchema(b)
		}()
	}
}

// randBytes returns a small deterministic pseudo-random byte slice for
// decode-never-panics smoke coverage (not a substitute for the real Go
// fuzz targets in fuzz_test.go, which use the fuzzing engine's own
// corpus/mutation machinery).
func randBytes(n int) []byte {
	b := make([]byte, n)
	x := uint32(2166136261)
	for i := range b {
		x ^= uint32(i) + 0x9e3779b9
		x *= 16777619
		b[i] = byte(x >> 24)
	}
	return b
}
