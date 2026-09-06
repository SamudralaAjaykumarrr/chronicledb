package sql

import (
	"reflect"
	"testing"
)

func testUsersSchema() Schema {
	return Schema{
		Table: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInteger},
			{Name: "name", Type: TypeText},
			{Name: "active", Type: TypeBoolean},
		},
		PrimaryKey: 0,
	}
}

func TestRowEncodeDecodeRoundTrip(t *testing.T) {
	schema := testUsersSchema()
	values := []Value{intValue(42), textValue("alice"), boolValue(true)}
	b := encodeRow(schema, values)
	got, err := decodeRow(schema, b)
	if err != nil {
		t.Fatalf("decodeRow: %v", err)
	}
	if !reflect.DeepEqual(values, got) {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v", got, values)
	}
}

func TestRowEncodeDecodeEmptyText(t *testing.T) {
	schema := testUsersSchema()
	values := []Value{intValue(0), textValue(""), boolValue(false)}
	b := encodeRow(schema, values)
	got, err := decodeRow(schema, b)
	if err != nil {
		t.Fatalf("decodeRow: %v", err)
	}
	if !reflect.DeepEqual(values, got) {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v", got, values)
	}
}

func TestRowDecodeRejectsColumnCountMismatch(t *testing.T) {
	schema := testUsersSchema()
	shortSchema := Schema{Table: "users", Columns: schema.Columns[:2], PrimaryKey: 0}
	b := encodeRow(schema, []Value{intValue(1), textValue("x"), boolValue(true)})
	if _, err := decodeRow(shortSchema, b); err == nil {
		t.Error("expected error for column-count mismatch, got nil")
	}
}

func TestRowDecodeRejectsTypeMismatch(t *testing.T) {
	schema := testUsersSchema()
	b := encodeRow(schema, []Value{intValue(1), textValue("x"), boolValue(true)})
	// Corrupt the first column's type tag (offset 3: version(1)+numCols(2)).
	corrupted := append([]byte(nil), b...)
	corrupted[3] = byte(TypeText)
	if _, err := decodeRow(schema, corrupted); err == nil {
		t.Error("expected error for type mismatch, got nil")
	}
}

func TestRowDecodeNeverPanics(t *testing.T) {
	schema := testUsersSchema()
	for i := 0; i < 2000; i++ {
		b := randBytes(i % 64)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decodeRow panicked on %v: %v", b, r)
				}
			}()
			decodeRow(schema, b)
		}()
	}
}

func TestRowKeyDeterministicAndDistinct(t *testing.T) {
	k1 := rowKey("users", intValue(1))
	k2 := rowKey("users", intValue(1))
	if k1 != k2 {
		t.Errorf("rowKey not deterministic: %q != %q", k1, k2)
	}
	k3 := rowKey("users", intValue(2))
	if k1 == k3 {
		t.Errorf("rowKey collision between different PK values: %q", k1)
	}
	// Different tables, same PK value, must not collide.
	k4 := rowKey("accounts", intValue(1))
	if k1 == k4 {
		t.Errorf("rowKey collision between different tables: %q", k1)
	}
	// A table name that is a prefix of another must not cause the
	// shorter table's row-key prefix to match the longer table's rows.
	kA := rowKey("a", intValue(1))
	kAB := rowKey("ab", intValue(1))
	if kA == kAB {
		t.Errorf("rowKey collision between prefix-related table names")
	}
	prefixA := rowKeyPrefix("a")
	if len(kAB) >= len(prefixA) && kAB[:len(prefixA)] == prefixA {
		t.Errorf("table %q's row key %q incorrectly matches table %q's row-key prefix %q", "ab", kAB, "a", prefixA)
	}
}

func TestRowKeyPKTypesDoNotCollide(t *testing.T) {
	// A text PK "\x01" (byte 1) must not collide with an integer PK
	// encoding that happens to start with the same leading byte
	// pattern — the leading type tag in encodePKValue prevents this.
	intKey := rowKey("t", intValue(1))
	textKey := rowKey("t", textValue("\x01\x00\x00\x00\x00\x00\x00\x00\x01"))
	if intKey == textKey {
		t.Errorf("cross-type PK collision: %q", intKey)
	}
}

func TestSchemaKeyDistinctFromRowKey(t *testing.T) {
	if schemaKey("t") == rowKey("t", intValue(1)) {
		t.Error("schemaKey collides with a rowKey")
	}
}
