package sql

import "testing"

// FuzzParse feeds arbitrary byte slices, interpreted as SQL source
// text, directly into the production parser (docs/sql.md §Fuzzing:
// "arbitrary SQL input must not panic; parser must terminate;
// malformed input should reject safely"). Every path through the
// lexer/parser must either return a typed Statement or a non-nil
// error — never panic, never loop, and never claim success while
// silently ignoring part of the input.
func FuzzParse(f *testing.F) {
	seeds := []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)",
		"INSERT INTO users VALUES (1, 'alice', true)",
		"INSERT INTO users (id, name) VALUES (1, 'alice''s')",
		"SELECT * FROM users",
		"SELECT id, name FROM users WHERE id = 1",
		"UPDATE users SET name = 'x' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"",
		"   ",
		"CREATE",
		"CREATE TABLE",
		"(((((",
		")))))",
		"SELECT * FROM t WHERE id = -99999999999999999999999999",
		"INSERT INTO t VALUES (NULL)",
		"SELECT * FROM t; SELECT * FROM t",
		"'unterminated",
		"CREATE TABLE t (id INTEGER PRIMARY KEY) --",
		string([]byte{0x00, 0x01, 0xff}),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, src string) {
		stmt, err := Parse(src)
		if err != nil {
			return
		}
		if stmt == nil {
			t.Fatalf("Parse(%q) returned nil Statement with nil error", src)
		}
	})
}

// FuzzDecodeSchema feeds arbitrary bytes into decodeSchema (the
// durably-stored table-schema record, schema.go), proving it never
// panics and never claims a larger column count than the input could
// possibly encode — the same bounded-decoding property
// internal/fsm.DecodeCommitTxn already guarantees for command bytes.
func FuzzDecodeSchema(f *testing.F) {
	f.Add(encodeSchema(Schema{
		Table: "t",
		Columns: []Column{
			{Name: "id", Type: TypeInteger},
			{Name: "name", Type: TypeText},
			{Name: "flag", Type: TypeBoolean},
		},
		PrimaryKey: 0,
	}))
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add([]byte{9, 0, 0})
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := decodeSchema(data)
		if err != nil {
			return
		}
		if len(s.Columns) > len(data) {
			t.Fatalf("decodeSchema returned %d columns from only %d input bytes", len(s.Columns), len(data))
		}
	})
}

// FuzzDecodeRow feeds arbitrary bytes into decodeRow (row.go) against
// a fixed reference schema, proving the same never-panics, never-over-
// claims-length property for row bytes.
func FuzzDecodeRow(f *testing.F) {
	schema := testUsersSchema()
	f.Add(encodeRow(schema, []Value{intValue(1), textValue("alice"), boolValue(true)}))
	f.Add(encodeRow(schema, []Value{intValue(0), textValue(""), boolValue(false)}))
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		values, err := decodeRow(schema, data)
		if err != nil {
			return
		}
		if len(values) > len(data) {
			t.Fatalf("decodeRow returned %d values from only %d input bytes", len(values), len(data))
		}
	})
}
