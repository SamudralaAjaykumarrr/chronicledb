package sql

import "fmt"

// tokenKind enumerates every lexical token the constrained SQL subset
// needs (docs/sql.md §3). Kept deliberately small: this subset has no
// arithmetic, no comparison operators beyond "=", and no comments.
type tokenKind int

const (
	tokEOF tokenKind = iota
	tokError

	tokIdent  // bare identifier or keyword (case-folded, see foldIdent)
	tokInt    // integer literal
	tokString // single-quoted string literal

	tokLParen    // (
	tokRParen    // )
	tokComma     // ,
	tokSemicolon // ;
	tokEquals    // =
	tokStar      // *
)

func (k tokenKind) String() string {
	switch k {
	case tokEOF:
		return "EOF"
	case tokError:
		return "ERROR"
	case tokIdent:
		return "IDENT"
	case tokInt:
		return "INT"
	case tokString:
		return "STRING"
	case tokLParen:
		return "("
	case tokRParen:
		return ")"
	case tokComma:
		return ","
	case tokSemicolon:
		return ";"
	case tokEquals:
		return "="
	case tokStar:
		return "*"
	default:
		return fmt.Sprintf("tokenKind(%d)", int(k))
	}
}

// token is one lexed unit plus its source position (byte offset into
// the original statement text), used to build precise syntax-error
// messages.
type token struct {
	kind tokenKind
	text string // literal text: identifier name (already case-folded), decoded string contents, or digits for an int
	pos  int    // byte offset of the token's first byte in the source
}

// keywords is the fixed, small reserved-word set for this SQL subset
// (docs/sql.md §4). A keyword can never be used as a table or column
// identifier — this subset does not implement the usual SQL escape of
// quoting a keyword to use it as a name, to keep the grammar and its
// error messages simple and unambiguous. Keys are already lower-case;
// lookups fold the candidate identifier the same way (foldIdent).
var keywords = map[string]bool{
	"create": true, "table": true, "insert": true, "into": true,
	"values": true, "select": true, "from": true, "where": true,
	"update": true, "set": true, "delete": true, "primary": true,
	"key": true, "integer": true, "text": true, "boolean": true,
	"true": true, "false": true, "begin": true, "commit": true,
	"rollback": true, "null": true,
}

func isKeyword(folded string) bool { return keywords[folded] }
