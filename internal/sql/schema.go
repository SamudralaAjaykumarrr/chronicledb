package sql

import (
	"encoding/binary"
	"fmt"
)

// schemaRecordVersion is the format version of the encoded Schema
// bytes stored under a table's schema key (encodeSchema/decodeSchema
// below) — distinct from any other format-version byte in this
// codebase (e.g. internal/fsm's commitTxnCommandVersion), governing
// only this record's own layout.
const schemaRecordVersion uint8 = 1

// Column is one column's name and declared type, as recorded in a
// table's committed Schema. Order matters: Schema.Columns' order is
// the canonical column order used for deterministic row encoding
// (row.go) and for "every column, in schema order" INSERT/SELECT
// resolution (plan.go) — fixed once at CREATE TABLE time, never
// reordered afterward (this subset has no ALTER TABLE).
type Column struct {
	Name string
	Type ColumnType
}

// Schema is one table's deterministic, replicated schema
// (docs/sql.md §1: how tables map into ChronicleDB's existing KV/MVCC
// state). A Schema is immutable once committed — this subset has no
// ALTER TABLE or DROP TABLE (docs/sql.md §8).
type Schema struct {
	Table      string
	Columns    []Column
	PrimaryKey int // index into Columns of the single PRIMARY KEY column
}

// column looks up a column by its already-normalized name.
func (s *Schema) column(name string) (Column, int, bool) {
	for i, c := range s.Columns {
		if c.Name == name {
			return c, i, true
		}
	}
	return Column{}, -1, false
}

func (s *Schema) primaryKeyColumn() Column { return s.Columns[s.PrimaryKey] }

// buildSchema validates a parsed CreateTableStmt against docs/sql.md
// §CREATE TABLE's requirements and produces the canonical Schema that
// will be committed. All validation happens here, once, before any
// durable state is touched — a rejected CREATE TABLE never partially
// mutates anything (execution, exec.go, only calls this before issuing
// any write).
func buildSchema(stmt *CreateTableStmt) (Schema, error) {
	if len(stmt.Columns) == 0 {
		return Schema{}, fmt.Errorf("%w: CREATE TABLE %q must declare at least one column", ErrUnsupportedFeature, stmt.Table)
	}
	if len(stmt.Columns) > MaxColumns {
		return Schema{}, fmt.Errorf("%w: CREATE TABLE %q declares %d columns, maximum is %d", ErrLimitExceeded, stmt.Table, len(stmt.Columns), MaxColumns)
	}
	seen := make(map[string]bool, len(stmt.Columns))
	cols := make([]Column, len(stmt.Columns))
	pkIndex := -1
	for i, c := range stmt.Columns {
		if seen[c.Name] {
			return Schema{}, fmt.Errorf("%w: column %q declared more than once", ErrDuplicateColumn, c.Name)
		}
		seen[c.Name] = true
		if !c.Type.valid() {
			return Schema{}, fmt.Errorf("%w: column %q has unsupported type %v", ErrUnsupportedFeature, c.Name, c.Type)
		}
		if c.PrimaryKey {
			if pkIndex != -1 {
				return Schema{}, fmt.Errorf("%w: both %q and %q declared PRIMARY KEY", ErrMultiplePrimaryKeys, cols[pkIndex].Name, c.Name)
			}
			pkIndex = i
		}
		cols[i] = Column{Name: c.Name, Type: c.Type}
	}
	if pkIndex == -1 {
		return Schema{}, fmt.Errorf("%w: CREATE TABLE %q declares no PRIMARY KEY column", ErrMissingPrimaryKey, stmt.Table)
	}
	return Schema{Table: stmt.Table, Columns: cols, PrimaryKey: pkIndex}, nil
}

// encodeSchema serializes s deterministically (docs/invariants.md
// DETERMINISM BOUNDARY's spirit: s.Columns is already an explicit,
// fixed-order slice — never a map — so encoding never depends on
// unordered iteration). Layout:
//
//	version(1B) tableLen(2B) table(tableLen) numCols(2B) pkIndex(2B)
//	  per column: nameLen(2B) name(nameLen) type(1B)
func encodeSchema(s Schema) []byte {
	size := 1 + 2 + len(s.Table) + 2 + 2
	for _, c := range s.Columns {
		size += 2 + len(c.Name) + 1
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = schemaRecordVersion
	off++
	binary.BigEndian.PutUint16(buf[off:], uint16(len(s.Table)))
	off += 2
	off += copy(buf[off:], s.Table)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(s.Columns)))
	off += 2
	binary.BigEndian.PutUint16(buf[off:], uint16(s.PrimaryKey))
	off += 2
	for _, c := range s.Columns {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(c.Name)))
		off += 2
		off += copy(buf[off:], c.Name)
		buf[off] = byte(c.Type)
		off++
	}
	return buf
}

// decodeSchema parses bytes previously produced by encodeSchema. It
// never trusts a declared length/count beyond the bytes actually
// remaining in b, mirroring internal/fsm.DecodeCommitTxn's bounded-
// decoding discipline (docs/failure-model.md §6) — a corrupted or
// truncated schema record fails closed with an error, never panics.
func decodeSchema(b []byte) (Schema, error) {
	var s Schema
	const fixedHeader = 1 + 2
	if len(b) < fixedHeader {
		return s, fmt.Errorf("%w: schema record too short (%d bytes)", ErrSyntax, len(b))
	}
	off := 0
	version := b[off]
	off++
	if version != schemaRecordVersion {
		return s, fmt.Errorf("%w: schema record version %d, expected %d", ErrUnsupportedFeature, version, schemaRecordVersion)
	}
	tableLen := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	if tableLen > len(b)-off {
		return s, fmt.Errorf("%w: truncated schema table name (declared %d bytes, %d remain)", ErrSyntax, tableLen, len(b)-off)
	}
	s.Table = string(b[off : off+tableLen])
	off += tableLen

	const afterTableFixed = 2 + 2
	if len(b)-off < afterTableFixed {
		return s, fmt.Errorf("%w: truncated schema header", ErrSyntax)
	}
	numCols := int(binary.BigEndian.Uint16(b[off:]))
	off += 2
	pkIndex := int(binary.BigEndian.Uint16(b[off:]))
	off += 2

	const minEncodedColumn = 2 + 1
	remaining := len(b) - off
	if numCols > remaining/minEncodedColumn {
		return s, fmt.Errorf("%w: schema declares %d columns but only %d bytes remain", ErrSyntax, numCols, remaining)
	}
	if pkIndex >= numCols {
		return s, fmt.Errorf("%w: schema primary-key index %d out of range for %d columns", ErrSyntax, pkIndex, numCols)
	}

	cols := make([]Column, 0, numCols)
	for i := 0; i < numCols; i++ {
		if len(b)-off < 2 {
			return s, fmt.Errorf("%w: truncated column %d name length", ErrSyntax, i)
		}
		nameLen := int(binary.BigEndian.Uint16(b[off:]))
		off += 2
		if nameLen > len(b)-off {
			return s, fmt.Errorf("%w: truncated column %d name", ErrSyntax, i)
		}
		name := string(b[off : off+nameLen])
		off += nameLen
		if len(b)-off < 1 {
			return s, fmt.Errorf("%w: truncated column %d type", ErrSyntax, i)
		}
		ct := ColumnType(b[off])
		off++
		if !ct.valid() {
			return s, fmt.Errorf("%w: column %q has unrecognized encoded type %d", ErrUnsupportedFeature, name, ct)
		}
		cols = append(cols, Column{Name: name, Type: ct})
	}
	if off != len(b) {
		return s, fmt.Errorf("%w: %d trailing bytes after decoding schema", ErrSyntax, len(b)-off)
	}
	s.Columns = cols
	s.PrimaryKey = pkIndex
	return s, nil
}
