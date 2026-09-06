package sql

import (
	"errors"
	"testing"
)

func lexAll(t *testing.T, src string) ([]token, error) {
	t.Helper()
	lx, err := newLexer(src)
	if err != nil {
		return nil, err
	}
	var toks []token
	for {
		tok, err := lx.next()
		if err != nil {
			return toks, err
		}
		toks = append(toks, tok)
		if tok.kind == tokEOF {
			return toks, nil
		}
	}
}

func TestLexerBasicTokens(t *testing.T) {
	toks, err := lexAll(t, "SELECT * FROM t WHERE id = 1;")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantKinds := []tokenKind{tokIdent, tokStar, tokIdent, tokIdent, tokIdent, tokIdent, tokEquals, tokInt, tokSemicolon, tokEOF}
	if len(toks) != len(wantKinds) {
		t.Fatalf("got %d tokens, want %d: %+v", len(toks), len(wantKinds), toks)
	}
	for i, k := range wantKinds {
		if toks[i].kind != k {
			t.Errorf("token %d: kind = %v, want %v", i, toks[i].kind, k)
		}
	}
}

func TestLexerUnterminatedString(t *testing.T) {
	_, err := lexAll(t, "'abc")
	if !errors.Is(err, ErrSyntax) {
		t.Errorf("err = %v, want ErrSyntax", err)
	}
}

func TestLexerInvalidToken(t *testing.T) {
	for _, src := range []string{"@", "#", "$", "%", "&", "^"} {
		if _, err := lexAll(t, src); !errors.Is(err, ErrSyntax) {
			t.Errorf("lex(%q): err = %v, want ErrSyntax", src, err)
		}
	}
}

func TestLexerOversizedStringLiteral(t *testing.T) {
	huge := "'" + string(make([]byte, MaxStringLiteralBytes+10)) + "'"
	_, err := lexAll(t, huge)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestLexerOversizedIdentifier(t *testing.T) {
	long := make([]byte, MaxIdentifierBytes+1)
	for i := range long {
		long[i] = 'x'
	}
	_, err := lexAll(t, string(long))
	if !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestLexerOversizedInteger(t *testing.T) {
	long := make([]byte, MaxIntegerDigits+1)
	for i := range long {
		long[i] = '9'
	}
	_, err := lexAll(t, string(long))
	if !errors.Is(err, ErrLimitExceeded) {
		t.Errorf("err = %v, want ErrLimitExceeded", err)
	}
}

func TestLexerEscapedQuoteInString(t *testing.T) {
	toks, err := lexAll(t, "'don''t'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if toks[0].kind != tokString || toks[0].text != "don't" {
		t.Errorf("got %+v, want string %q", toks[0], "don't")
	}
}

func TestLexerEmptyString(t *testing.T) {
	toks, err := lexAll(t, "''")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if toks[0].kind != tokString || toks[0].text != "" {
		t.Errorf("got %+v, want empty string", toks[0])
	}
}

func TestLexerNegativeInteger(t *testing.T) {
	toks, err := lexAll(t, "-42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if toks[0].kind != tokInt || toks[0].text != "-42" {
		t.Errorf("got %+v", toks[0])
	}
}

func TestLexerBareMinusIsSyntaxError(t *testing.T) {
	// A lone '-' with no digit after it is not a valid token in this
	// subset (no subtraction operator exists) — it must fail cleanly,
	// not be silently absorbed into something else.
	_, err := lexAll(t, "- ")
	if !errors.Is(err, ErrSyntax) {
		t.Errorf("err = %v, want ErrSyntax", err)
	}
}

func TestLexerNeverPanics(t *testing.T) {
	inputs := []string{
		"", "'", "''''''", "-", "--", string([]byte{0}), string([]byte{0xff}),
		"\t\n\r ", "1234567890123456789012345678901234567890",
	}
	for _, src := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("lex(%q) panicked: %v", src, r)
				}
			}()
			lexAll(t, src)
		}()
	}
}

func TestFoldIdentASCIIOnly(t *testing.T) {
	if got := foldIdent("Users"); got != "users" {
		t.Errorf("foldIdent(Users) = %q, want %q", got, "users")
	}
	if got := foldIdent("USERS"); got != "users" {
		t.Errorf("foldIdent(USERS) = %q, want %q", got, "users")
	}
	if got := foldIdent("users_2"); got != "users_2" {
		t.Errorf("foldIdent(users_2) = %q, want %q", got, "users_2")
	}
}
