package sql

import (
	"fmt"
	"strconv"
)

// parseIntLiteral parses a lexed integer literal's digit text (already
// validated by the lexer to be a well-formed, bounded-length optional
// sign plus digits) into an int64, rejecting overflow explicitly rather
// than silently wrapping.
func parseIntLiteral(text string) (int64, error) {
	return strconv.ParseInt(text, 10, 64)
}

// ColumnType is one of this subset's deliberately small set of scalar
// types (docs/sql.md §2.2, docs/non-goals.md §SQL surface's "a limited
// type system"). Values start at 1, not 0, so a zero ColumnType is
// never mistaken for a valid decoded type when reading schema/row
// bytes back (schema.go, row.go).
type ColumnType uint8

const (
	TypeInteger ColumnType = 1 // Go int64
	TypeText    ColumnType = 2 // UTF-8 string, no declared length limit beyond MaxStringLiteralBytes at the SQL-literal level
	TypeBoolean ColumnType = 3
)

func (t ColumnType) String() string {
	switch t {
	case TypeInteger:
		return "INTEGER"
	case TypeText:
		return "TEXT"
	case TypeBoolean:
		return "BOOLEAN"
	default:
		return fmt.Sprintf("ColumnType(%d)", uint8(t))
	}
}

func (t ColumnType) valid() bool { return t == TypeInteger || t == TypeText || t == TypeBoolean }

// columnTypeFromKeyword maps a lower-cased type keyword token to its
// ColumnType, or ok=false if it names something outside this subset's
// three supported types.
func columnTypeFromKeyword(kw string) (ColumnType, bool) {
	switch kw {
	case "integer":
		return TypeInteger, true
	case "text":
		return TypeText, true
	case "boolean":
		return TypeBoolean, true
	default:
		return 0, false
	}
}

// Value is one typed scalar — a parsed literal, a bound predicate
// value, or one column's value in a result row. Exactly one of the
// Int/Text/Bool fields is meaningful, selected by Type.
type Value struct {
	Type ColumnType
	Int  int64
	Text string
	Bool bool
}

func (v Value) String() string {
	switch v.Type {
	case TypeInteger:
		return fmt.Sprintf("%d", v.Int)
	case TypeText:
		return v.Text
	case TypeBoolean:
		return fmt.Sprintf("%t", v.Bool)
	default:
		return "<invalid>"
	}
}

func intValue(i int64) Value   { return Value{Type: TypeInteger, Int: i} }
func textValue(s string) Value { return Value{Type: TypeText, Text: s} }
func boolValue(b bool) Value   { return Value{Type: TypeBoolean, Bool: b} }
