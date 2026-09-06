package sql

// Fixed parser/schema limits (docs/sql.md §7), applied before any
// unbounded allocation is made from untrusted input — the same
// bounded-decoding discipline internal/fsm.DecodeCommitTxn already
// applies to durable command bytes (docs/failure-model.md §6),
// applied here to SQL source text instead.
const (
	// MaxStatementBytes bounds one SQL statement's total source length.
	MaxStatementBytes = 64 * 1024
	// MaxIdentifierBytes bounds a table or column name's length.
	MaxIdentifierBytes = 128
	// MaxStringLiteralBytes bounds one string literal's decoded length.
	MaxStringLiteralBytes = 64 * 1024
	// MaxColumns bounds the number of columns a CREATE TABLE may
	// declare, and the number of columns/values an INSERT may name.
	MaxColumns = 64
	// MaxIntegerDigits bounds the number of digits an integer literal
	// may have before it is even attempted against strconv.ParseInt —
	// large enough for any valid int64 (19 digits, plus a sign), small
	// enough to reject a hostile multi-megabyte digit string cheaply
	// rather than allocating/parsing it first.
	MaxIntegerDigits = 32
)
