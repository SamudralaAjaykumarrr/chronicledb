package sql

import (
	"encoding/binary"
	"fmt"
)

// Key namespace (docs/sql.md §1: "how tables/rows/schema map into
// ChronicleDB state"). Every SQL-owned MVCC key starts with one of
// these two control-byte tags, neither of which can ever appear inside
// a table/column identifier (the lexer's identifier grammar is
// restricted to ASCII letters/digits/underscore) — so a schema key and
// any row key, for any table name, are guaranteed to never collide with
// each other or with a key some future non-SQL feature might choose,
// as long as that feature avoids these two tag bytes.
const (
	schemaKeyTag byte = 0x01
	rowKeyTag    byte = 0x02
)

// schemaKey returns the MVCC key a table's committed Schema is stored
// under.
func schemaKey(table string) string {
	return string(schemaKeyTag) + table
}

// rowKeyPrefix returns the common prefix of every row key belonging to
// table — used both to build one specific row's key (append the
// encoded primary-key value) and as the scan prefix for a full-table
// SELECT (plan.go, exec.go). The 0x00 separator prevents two
// differently-named tables from having one's rowKeyPrefix be a proper
// prefix of the other's row keys (e.g. table "a" vs table "ab") —
// without it, an empty-primary-key encoding edge case could otherwise
// make scanning "a"'s rows accidentally match some of "ab"'s.
func rowKeyPrefix(table string) string {
	return string(rowKeyTag) + table + "\x00"
}

// rowKey returns the exact MVCC key for one row: table's row-key
// prefix plus a deterministic encoding of its primary-key value
// (docs/sql.md §1). Two different primary-key values of the same
// declared column type always encode to different byte strings; values
// of different types never collide because encodePKValue's leading
// type tag differs.
func rowKey(table string, pk Value) string {
	return rowKeyPrefix(table) + string(encodePKValue(pk))
}

// encodePKValue deterministically encodes one primary-key value.
// Ordering of the resulting bytes is not relied upon anywhere (this
// subset never does a range/ORDER BY scan over primary-key values,
// only prefix-scans a whole table — docs/sql.md §5.2) — only byte-exact
// determinism and cross-type non-collision matter here.
func encodePKValue(v Value) []byte {
	switch v.Type {
	case TypeInteger:
		buf := make([]byte, 9)
		buf[0] = byte(TypeInteger)
		binary.BigEndian.PutUint64(buf[1:], uint64(v.Int))
		return buf
	case TypeBoolean:
		buf := make([]byte, 2)
		buf[0] = byte(TypeBoolean)
		if v.Bool {
			buf[1] = 1
		}
		return buf
	case TypeText:
		buf := make([]byte, 1+len(v.Text))
		buf[0] = byte(TypeText)
		copy(buf[1:], v.Text)
		return buf
	default:
		// Unreachable given a Value produced by this package's own
		// literal-binding path (plan.go), which always sets a valid
		// ColumnType. Encoding a zero-value Value would otherwise
		// silently collide with nothing meaningful, so fail loudly in
		// a way a caller cannot mistake for a real key instead.
		panic(fmt.Sprintf("sql: encodePKValue: invalid ColumnType %d", v.Type))
	}
}

// rowRecordVersion is the format version of one row's encoded column
// values (encodeRow/decodeRow below).
const rowRecordVersion uint8 = 1

// encodeRow deterministically encodes every column of one row, in
// schema.Columns order (never map order — values is already a slice
// aligned 1:1 with schema.Columns, fixed by the binder, plan.go).
// Layout:
//
//	version(1B) numCols(2B) per column: type(1B) encoded-value
//	  INTEGER: 8B big-endian   TEXT: len(4B) + bytes   BOOLEAN: 1B
//
// The type tag is redundant with the schema (decodeRow cross-checks it
// against the schema passed in) but makes a corrupted row fail closed
// with a clear mismatch rather than misinterpreting bytes under the
// wrong type.
func encodeRow(schema Schema, values []Value) []byte {
	size := 1 + 2
	for _, v := range values {
		size++
		switch v.Type {
		case TypeInteger:
			size += 8
		case TypeBoolean:
			size++
		case TypeText:
			size += 4 + len(v.Text)
		}
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = rowRecordVersion
	off++
	binary.BigEndian.PutUint16(buf[off:], uint16(len(values)))
	off += 2
	for _, v := range values {
		buf[off] = byte(v.Type)
		off++
		switch v.Type {
		case TypeInteger:
			binary.BigEndian.PutUint64(buf[off:], uint64(v.Int))
			off += 8
		case TypeBoolean:
			if v.Bool {
				buf[off] = 1
			}
			off++
		case TypeText:
			binary.BigEndian.PutUint32(buf[off:], uint32(len(v.Text)))
			off += 4
			off += copy(buf[off:], v.Text)
		}
	}
	return buf
}

// decodeRow parses bytes previously produced by encodeRow, validating
// the decoded column count and each column's type against schema.
// Never trusts a declared length beyond bytes actually remaining
// (docs/failure-model.md §6), matching every other decoder in this
// codebase.
func decodeRow(schema Schema, b []byte) ([]Value, error) {
	const fixedHeader = 1 + 2
	if len(b) < fixedHeader {
		return nil, fmt.Errorf("%w: row record too short (%d bytes)", ErrSyntax, len(b))
	}
	off := 0
	version := b[off]
	off++
	if version != rowRecordVersion {
		return nil, fmt.Errorf("%w: row record version %d, expected %d", ErrUnsupportedFeature, version, rowRecordVersion)
	}
	numCols := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if numCols != len(schema.Columns) {
		return nil, fmt.Errorf("%w: row has %d columns, schema for %q declares %d", ErrSyntax, numCols, schema.Table, len(schema.Columns))
	}
	values := make([]Value, numCols)
	for i := 0; i < numCols; i++ {
		if len(b)-off < 1 {
			return nil, fmt.Errorf("%w: truncated row column %d type", ErrSyntax, i)
		}
		ct := ColumnType(b[off])
		off++
		if ct != schema.Columns[i].Type {
			return nil, fmt.Errorf("%w: row column %d (%q) has encoded type %v, schema declares %v", ErrTypeMismatch, i, schema.Columns[i].Name, ct, schema.Columns[i].Type)
		}
		switch ct {
		case TypeInteger:
			if len(b)-off < 8 {
				return nil, fmt.Errorf("%w: truncated row column %d (INTEGER)", ErrSyntax, i)
			}
			values[i] = Value{Type: TypeInteger, Int: int64(binary.BigEndian.Uint64(b[off:]))}
			off += 8
		case TypeBoolean:
			if len(b)-off < 1 {
				return nil, fmt.Errorf("%w: truncated row column %d (BOOLEAN)", ErrSyntax, i)
			}
			values[i] = Value{Type: TypeBoolean, Bool: b[off] != 0}
			off++
		case TypeText:
			if len(b)-off < 4 {
				return nil, fmt.Errorf("%w: truncated row column %d (TEXT) length", ErrSyntax, i)
			}
			strLen := int(binary.BigEndian.Uint32(b[off:]))
			off += 4
			if strLen < 0 || strLen > len(b)-off {
				return nil, fmt.Errorf("%w: truncated row column %d (TEXT): declared %d bytes, %d remain", ErrSyntax, i, strLen, len(b)-off)
			}
			values[i] = Value{Type: TypeText, Text: string(b[off : off+strLen])}
			off += strLen
		default:
			return nil, fmt.Errorf("%w: row column %d has unrecognized encoded type %d", ErrUnsupportedFeature, i, ct)
		}
	}
	if off != len(b) {
		return nil, fmt.Errorf("%w: %d trailing bytes after decoding row", ErrSyntax, len(b)-off)
	}
	return values, nil
}
