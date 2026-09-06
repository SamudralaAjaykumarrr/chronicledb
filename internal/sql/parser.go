package sql

import "fmt"

// parser is a small hand-written recursive-descent parser over a
// pre-lexed token slice (docs/sql.md §Parser: "intentionally small and
// auditable... avoid a massive parser generator"). Every production
// below corresponds 1:1 to a line of the grammar documented in
// docs/sql.md §2. The parser never panics on malformed input: every
// token access goes through peek/advance/expect, which are bounds-
// checked against p.toks (always terminated by a tokEOF sentinel) and
// return a *PositionError rather than indexing out of range.
type parser struct {
	toks []token
	pos  int
}

// Parse lexes and parses one SQL statement, returning its typed AST
// (docs/sql.md §Parser/AST). A single optional trailing ';' is
// accepted; anything else after the statement's own grammar — a second
// statement, trailing garbage — is a syntax error: this subset executes
// exactly one statement per Parse call, never a batch (docs/sql.md
// §Transactions: multi-statement work is expressed via explicit
// BEGIN/COMMIT/ROLLBACK calls, not by concatenating statements in one
// string).
func Parse(src string) (Statement, error) {
	lx, err := newLexer(src)
	if err != nil {
		return nil, err
	}
	var toks []token
	for {
		t, err := lx.next()
		if err != nil {
			return nil, err
		}
		toks = append(toks, t)
		if t.kind == tokEOF {
			break
		}
	}
	p := &parser{toks: toks}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if p.at(tokSemicolon) {
		p.advance()
	}
	if !p.at(tokEOF) {
		return nil, p.errf("unexpected trailing input after statement")
	}
	return stmt, nil
}

func (p *parser) peek() token         { return p.toks[p.pos] }
func (p *parser) at(k tokenKind) bool { return p.peek().kind == k }
func (p *parser) advance() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) errf(format string, args ...interface{}) *PositionError {
	return posErr(p.peek().pos, ErrSyntax, format, args...)
}

// expect consumes and returns the current token if it has kind k,
// otherwise returns a syntax error naming what was expected.
func (p *parser) expect(k tokenKind) (token, error) {
	if !p.at(k) {
		return token{}, p.errf("expected %s, found %s %q", k, p.peek().kind, p.peek().text)
	}
	return p.advance(), nil
}

// expectKeyword consumes an identifier token whose folded text equals
// kw, otherwise returns a syntax error. Keywords are lexed as ordinary
// tokIdent tokens (token.go's keyword set is used only to reject
// keyword-as-name, not to give keywords a distinct token kind) — this
// keeps the lexer's token kind set small.
func (p *parser) expectKeyword(kw string) error {
	if !p.at(tokIdent) || p.peek().text != kw {
		return p.errf("expected keyword %q, found %s %q", kw, p.peek().kind, p.peek().text)
	}
	p.advance()
	return nil
}

func (p *parser) atKeyword(kw string) bool {
	return p.at(tokIdent) && p.peek().text == kw
}

// expectName consumes an identifier that is not a reserved keyword,
// enforcing docs/sql.md §4's "keywords may never be used as names"
// rule at the single point every table/column name passes through.
func (p *parser) expectName() (string, error) {
	if !p.at(tokIdent) {
		return "", p.errf("expected identifier, found %s %q", p.peek().kind, p.peek().text)
	}
	t := p.peek()
	if isKeyword(t.text) {
		return "", posErr(t.pos, ErrInvalidIdentifier, "%q is a reserved keyword and cannot be used as a name", t.text)
	}
	p.advance()
	return t.text, nil
}

func (p *parser) parseStatement() (Statement, error) {
	if !p.at(tokIdent) {
		return nil, p.errf("expected a statement keyword, found %s %q", p.peek().kind, p.peek().text)
	}
	switch p.peek().text {
	case "create":
		return p.parseCreateTable()
	case "insert":
		return p.parseInsert()
	case "select":
		return p.parseSelect()
	case "update":
		return p.parseUpdate()
	case "delete":
		return p.parseDelete()
	case "begin":
		p.advance()
		return &BeginStmt{}, nil
	case "commit":
		p.advance()
		return &CommitStmt{}, nil
	case "rollback":
		p.advance()
		return &RollbackStmt{}, nil
	default:
		return nil, fmt.Errorf("%w: %q is not a supported statement keyword", ErrUnsupportedStatement, p.peek().text)
	}
}

// parseCreateTable parses `CREATE TABLE name (col type [PRIMARY KEY], ...)`.
func (p *parser) parseCreateTable() (Statement, error) {
	if err := p.expectKeyword("create"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("table"); err != nil {
		return nil, err
	}
	table, err := p.expectName()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var cols []ColumnDef
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if len(cols) > MaxColumns {
			return nil, posErr(p.peek().pos, ErrLimitExceeded, "CREATE TABLE declares more than the maximum of %d columns", MaxColumns)
		}
		if p.at(tokComma) {
			p.advance()
			continue
		}
		break
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return &CreateTableStmt{Table: table, Columns: cols}, nil
}

func (p *parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.expectName()
	if err != nil {
		return ColumnDef{}, err
	}
	if !p.at(tokIdent) {
		return ColumnDef{}, p.errf("expected a column type, found %s %q", p.peek().kind, p.peek().text)
	}
	typeTok := p.peek()
	ct, ok := columnTypeFromKeyword(typeTok.text)
	if !ok {
		return ColumnDef{}, fmt.Errorf("%w: %q is not a supported column type (supported: INTEGER, TEXT, BOOLEAN)", ErrUnsupportedFeature, typeTok.text)
	}
	p.advance()
	pk := false
	if p.atKeyword("primary") {
		p.advance()
		if err := p.expectKeyword("key"); err != nil {
			return ColumnDef{}, err
		}
		pk = true
	}
	return ColumnDef{Name: name, Type: ct, PrimaryKey: pk}, nil
}

// parseInsert parses `INSERT INTO name [(col, ...)] VALUES (literal, ...)`.
func (p *parser) parseInsert() (Statement, error) {
	if err := p.expectKeyword("insert"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("into"); err != nil {
		return nil, err
	}
	table, err := p.expectName()
	if err != nil {
		return nil, err
	}
	var columns []string
	if p.at(tokLParen) {
		p.advance()
		for {
			name, err := p.expectName()
			if err != nil {
				return nil, err
			}
			columns = append(columns, name)
			if len(columns) > MaxColumns {
				return nil, posErr(p.peek().pos, ErrLimitExceeded, "INSERT names more than the maximum of %d columns", MaxColumns)
			}
			if p.at(tokComma) {
				p.advance()
				continue
			}
			break
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("values"); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var values []Literal
	for {
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		values = append(values, lit)
		if len(values) > MaxColumns {
			return nil, posErr(p.peek().pos, ErrLimitExceeded, "INSERT supplies more than the maximum of %d values", MaxColumns)
		}
		if p.at(tokComma) {
			p.advance()
			continue
		}
		break
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return &InsertStmt{Table: table, Columns: columns, Values: values}, nil
}

// parseSelect parses `SELECT col, ... | * FROM name [WHERE predicate]`.
func (p *parser) parseSelect() (Statement, error) {
	if err := p.expectKeyword("select"); err != nil {
		return nil, err
	}
	var columns []string
	if p.at(tokStar) {
		p.advance()
	} else {
		for {
			name, err := p.expectName()
			if err != nil {
				return nil, err
			}
			columns = append(columns, name)
			if len(columns) > MaxColumns {
				return nil, posErr(p.peek().pos, ErrLimitExceeded, "SELECT names more than the maximum of %d columns", MaxColumns)
			}
			if p.at(tokComma) {
				p.advance()
				continue
			}
			break
		}
	}
	if err := p.expectKeyword("from"); err != nil {
		return nil, err
	}
	table, err := p.expectName()
	if err != nil {
		return nil, err
	}
	var where *Predicate
	if p.atKeyword("where") {
		p.advance()
		pred, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		where = &pred
	}
	return &SelectStmt{Table: table, Columns: columns, Where: where}, nil
}

// parseUpdate parses `UPDATE name SET col = literal, ... WHERE predicate`.
// WHERE is mandatory (docs/sql.md §2.5).
func (p *parser) parseUpdate() (Statement, error) {
	if err := p.expectKeyword("update"); err != nil {
		return nil, err
	}
	table, err := p.expectName()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("set"); err != nil {
		return nil, err
	}
	var assigns []Assignment
	for {
		name, err := p.expectName()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokEquals); err != nil {
			return nil, err
		}
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		assigns = append(assigns, Assignment{Column: name, Value: lit})
		if len(assigns) > MaxColumns {
			return nil, posErr(p.peek().pos, ErrLimitExceeded, "UPDATE names more than the maximum of %d assignments", MaxColumns)
		}
		if p.at(tokComma) {
			p.advance()
			continue
		}
		break
	}
	if !p.atKeyword("where") {
		return nil, fmt.Errorf("%w: UPDATE requires a WHERE clause on the primary key in this subset", ErrInvalidPredicate)
	}
	p.advance()
	where, err := p.parsePredicate()
	if err != nil {
		return nil, err
	}
	return &UpdateStmt{Table: table, Assignments: assigns, Where: where}, nil
}

// parseDelete parses `DELETE FROM name WHERE predicate`. WHERE is
// mandatory, identically to UPDATE.
func (p *parser) parseDelete() (Statement, error) {
	if err := p.expectKeyword("delete"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("from"); err != nil {
		return nil, err
	}
	table, err := p.expectName()
	if err != nil {
		return nil, err
	}
	if !p.atKeyword("where") {
		return nil, fmt.Errorf("%w: DELETE requires a WHERE clause on the primary key in this subset", ErrInvalidPredicate)
	}
	p.advance()
	where, err := p.parsePredicate()
	if err != nil {
		return nil, err
	}
	return &DeleteStmt{Table: table, Where: where}, nil
}

// parsePredicate parses this subset's one supported WHERE shape:
// `column = literal` (docs/sql.md §2.4). Whether column actually names
// the target table's primary key is a semantic question answered later
// by the binder (plan.go), not the parser.
func (p *parser) parsePredicate() (Predicate, error) {
	name, err := p.expectName()
	if err != nil {
		return Predicate{}, err
	}
	if _, err := p.expect(tokEquals); err != nil {
		return Predicate{}, err
	}
	lit, err := p.parseLiteral()
	if err != nil {
		return Predicate{}, err
	}
	return Predicate{Column: name, Value: lit}, nil
}

func (p *parser) parseLiteral() (Literal, error) {
	t := p.peek()
	switch t.kind {
	case tokInt:
		p.advance()
		n, err := parseIntLiteral(t.text)
		if err != nil {
			return Literal{}, posErr(t.pos, ErrSyntax, "invalid integer literal %q: %v", t.text, err)
		}
		return Literal{Kind: LiteralInt, Int: n}, nil
	case tokString:
		p.advance()
		return Literal{Kind: LiteralString, Str: t.text}, nil
	case tokIdent:
		switch t.text {
		case "true":
			p.advance()
			return Literal{Kind: LiteralBool, Bool: true}, nil
		case "false":
			p.advance()
			return Literal{Kind: LiteralBool, Bool: false}, nil
		case "null":
			return Literal{}, fmt.Errorf("%w: NULL values are not supported in this subset — every column must be given an explicit value", ErrUnsupportedFeature)
		}
	}
	return Literal{}, p.errf("expected a literal (integer, string, TRUE, or FALSE), found %s %q", t.kind, t.text)
}
