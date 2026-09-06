package sql

import (
	"fmt"
	"strings"
)

// PositionError reports a lexer or parser failure at a specific byte
// offset into the original statement text, so a caller (a CLI, a test)
// can point at exactly what was wrong rather than a bare message.
type PositionError struct {
	Pos int    // byte offset into the source statement
	Msg string // human-readable detail
	Err error  // sentinel this wraps (ErrSyntax, ErrLimitExceeded, ...)
}

func (e *PositionError) Error() string {
	return fmt.Sprintf("sql: at byte %d: %s", e.Pos, e.Msg)
}

func (e *PositionError) Unwrap() error { return e.Err }

func posErr(pos int, sentinel error, format string, args ...interface{}) *PositionError {
	return &PositionError{Pos: pos, Msg: fmt.Sprintf(format, args...), Err: sentinel}
}

// foldIdent normalizes an identifier for case-insensitive comparison
// and storage (docs/sql.md §4): ASCII letters only are folded to lower
// case. This is deliberately not strings.ToLower (which is
// locale-independent in Go but, per docs/invariants.md DETERMINISM
// BOUNDARY's spirit of avoiding any locale-flavored text
// transformation on state-affecting paths, this subset restricts
// identifiers to the ASCII letter/digit/underscore grammar the lexer
// already enforces, so a byte-wise ASCII fold is both sufficient and
// maximally simple to reason about).
func foldIdent(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c - 'A' + 'a'
		}
	}
	return string(b)
}

// lexer tokenizes one SQL statement (docs/sql.md §3). It never panics:
// every method is bounds-checked against len(l.src), and any malformed
// or oversized input is reported as an error from next(), never a
// crash — required for safe use against arbitrary/fuzzed input
// (docs/failure-model.md §6).
type lexer struct {
	src string
	pos int
}

func newLexer(src string) (*lexer, error) {
	if len(src) > MaxStatementBytes {
		return nil, posErr(MaxStatementBytes, ErrLimitExceeded, "statement exceeds maximum length of %d bytes", MaxStatementBytes)
	}
	return &lexer{src: src}, nil
}

func (l *lexer) peekByte() (byte, bool) {
	if l.pos >= len(l.src) {
		return 0, false
	}
	return l.src[l.pos], true
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
func isIdentCont(c byte) bool { return isIdentStart(c) || isDigit(c) }

func (l *lexer) skipSpace() {
	for {
		c, ok := l.peekByte()
		if !ok || !isSpace(c) {
			return
		}
		l.pos++
	}
}

// next returns the next token, or a non-nil error for malformed input
// (unterminated string, invalid character, oversized literal). Calling
// next again after tokEOF continues to return tokEOF, never an error —
// callers loop until they see tokEOF, not until an error.
func (l *lexer) next() (token, error) {
	l.skipSpace()
	start := l.pos
	c, ok := l.peekByte()
	if !ok {
		return token{kind: tokEOF, pos: start}, nil
	}

	switch {
	case c == '(':
		l.pos++
		return token{kind: tokLParen, pos: start}, nil
	case c == ')':
		l.pos++
		return token{kind: tokRParen, pos: start}, nil
	case c == ',':
		l.pos++
		return token{kind: tokComma, pos: start}, nil
	case c == ';':
		l.pos++
		return token{kind: tokSemicolon, pos: start}, nil
	case c == '=':
		l.pos++
		return token{kind: tokEquals, pos: start}, nil
	case c == '*':
		l.pos++
		return token{kind: tokStar, pos: start}, nil
	case c == '\'':
		return l.lexString(start)
	case isDigit(c) || (c == '-' && l.hasDigitAfterSign()):
		return l.lexInt(start)
	case isIdentStart(c):
		return l.lexIdent(start)
	default:
		return token{}, posErr(start, ErrSyntax, "unexpected character %q", c)
	}
}

func (l *lexer) hasDigitAfterSign() bool {
	return l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])
}

func (l *lexer) lexIdent(start int) (token, error) {
	for {
		c, ok := l.peekByte()
		if !ok || !isIdentCont(c) {
			break
		}
		l.pos++
	}
	if l.pos-start > MaxIdentifierBytes {
		return token{}, posErr(start, ErrLimitExceeded, "identifier exceeds maximum length of %d bytes", MaxIdentifierBytes)
	}
	return token{kind: tokIdent, text: foldIdent(l.src[start:l.pos]), pos: start}, nil
}

func (l *lexer) lexInt(start int) (token, error) {
	if c, ok := l.peekByte(); ok && c == '-' {
		l.pos++
	}
	digitsStart := l.pos
	for {
		c, ok := l.peekByte()
		if !ok || !isDigit(c) {
			break
		}
		l.pos++
	}
	if l.pos == digitsStart {
		return token{}, posErr(start, ErrSyntax, "malformed integer literal")
	}
	if l.pos-start > MaxIntegerDigits {
		return token{}, posErr(start, ErrLimitExceeded, "integer literal exceeds maximum length of %d digits", MaxIntegerDigits)
	}
	return token{kind: tokInt, text: l.src[start:l.pos], pos: start}, nil
}

// lexString scans a single-quoted string literal. The only supported
// escape is the standard SQL doubled-quote (” inside a string means a
// literal single quote) — no backslash escapes, kept deliberately
// minimal (docs/sql.md §3). An unterminated string (no matching closing
// quote before end of input) is a syntax error, never a silent partial
// read.
func (l *lexer) lexString(start int) (token, error) {
	l.pos++ // consume opening quote
	var b strings.Builder
	for {
		c, ok := l.peekByte()
		if !ok {
			return token{}, posErr(start, ErrSyntax, "unterminated string literal")
		}
		if c == '\'' {
			l.pos++
			if nc, ok := l.peekByte(); ok && nc == '\'' {
				// Doubled quote: literal single quote, string continues.
				b.WriteByte('\'')
				l.pos++
				continue
			}
			break // closing quote
		}
		b.WriteByte(c)
		l.pos++
		if b.Len() > MaxStringLiteralBytes {
			return token{}, posErr(start, ErrLimitExceeded, "string literal exceeds maximum length of %d bytes", MaxStringLiteralBytes)
		}
	}
	return token{kind: tokString, text: b.String(), pos: start}, nil
}
