// plan.go is the semantic validation/binding layer between the parser
// and execution (docs/sql.md §Planning/Validation): it resolves table
// and column names against a table's actual committed Schema, checks
// every literal's type against its target column, validates predicate
// shape, and normalizes column order — producing a small, already-
// validated plan value exec.go executes with no further semantic
// checks of its own. None of this needs a cost model, statistics, or
// alternative execution strategies (docs/sql.md explicitly disclaims a
// general optimizer) — every plan here has exactly one possible
// physical execution.
package sql

import "fmt"

// getSchema reads table's committed Schema through t (docs/sql.md §1).
// Schema reads flow through the identical Txn every row read/write
// does — a schema is just another piece of committed MVCC state, not a
// separately cached catalog — so a SELECT/INSERT/UPDATE/DELETE inside
// an explicit multi-statement transaction sees the same schema
// snapshot consistency Snapshot Isolation already gives every other
// read (docs/mvcc.md §3).
func getSchema(t Txn, table string) (Schema, error) {
	b, found, err := t.Read(schemaKey(table))
	if err != nil {
		return Schema{}, err
	}
	if !found {
		return Schema{}, fmt.Errorf("%w: %q", ErrUnknownTable, table)
	}
	return decodeSchema(b)
}

// bindLiteral checks a parsed Literal's kind against a column's
// declared type and produces the typed Value to store or compare.
func bindLiteral(lit Literal, want ColumnType) (Value, error) {
	switch lit.Kind {
	case LiteralInt:
		if want != TypeInteger {
			return Value{}, fmt.Errorf("%w: integer literal assigned to a %v column", ErrTypeMismatch, want)
		}
		return intValue(lit.Int), nil
	case LiteralString:
		if want != TypeText {
			return Value{}, fmt.Errorf("%w: string literal assigned to a %v column", ErrTypeMismatch, want)
		}
		return textValue(lit.Str), nil
	case LiteralBool:
		if want != TypeBoolean {
			return Value{}, fmt.Errorf("%w: boolean literal assigned to a %v column", ErrTypeMismatch, want)
		}
		return boolValue(lit.Bool), nil
	default:
		return Value{}, fmt.Errorf("%w: unrecognized literal kind", ErrSyntax)
	}
}

// bindPredicate validates pred against schema: this subset's only
// supported WHERE shape is an equality match on the table's primary
// key column (docs/sql.md §2.4) — anything else (a non-primary-key
// column, an unsupported operator, a conjunction) is rejected here,
// not silently reinterpreted.
func bindPredicate(schema Schema, pred Predicate) (Value, error) {
	pkCol := schema.primaryKeyColumn()
	if pred.Column != pkCol.Name {
		return Value{}, fmt.Errorf("%w: WHERE in this subset must equality-match the primary key column %q, not %q", ErrInvalidPredicate, pkCol.Name, pred.Column)
	}
	return bindLiteral(pred.Value, pkCol.Type)
}

// insertPlan is INSERT's fully-resolved, already-validated operation:
// values is aligned 1:1 with schema.Columns, regardless of what column
// order the original statement used.
type insertPlan struct {
	schema Schema
	pk     Value
	values []Value
}

// planInsert resolves an INSERT's (optional) explicit column list
// against schema, type-checks every value, and requires every column
// to receive a value (docs/sql.md §2.3: no NULLs, no defaults, in this
// subset — every column must be given explicitly).
func planInsert(schema Schema, stmt *InsertStmt) (insertPlan, error) {
	var targetCols []int
	if stmt.Columns == nil {
		if len(stmt.Values) != len(schema.Columns) {
			return insertPlan{}, fmt.Errorf("%w: INSERT into %q supplies %d values but the table has %d columns and no explicit column list was given",
				ErrUnsupportedFeature, schema.Table, len(stmt.Values), len(schema.Columns))
		}
		targetCols = make([]int, len(schema.Columns))
		for i := range targetCols {
			targetCols[i] = i
		}
	} else {
		if len(stmt.Columns) != len(stmt.Values) {
			return insertPlan{}, fmt.Errorf("%w: INSERT into %q names %d columns but supplies %d values", ErrUnsupportedFeature, schema.Table, len(stmt.Columns), len(stmt.Values))
		}
		seenNames := make(map[string]bool, len(stmt.Columns))
		targetCols = make([]int, len(stmt.Columns))
		for i, name := range stmt.Columns {
			if seenNames[name] {
				return insertPlan{}, fmt.Errorf("%w: column %q named more than once in INSERT's column list", ErrDuplicateColumn, name)
			}
			seenNames[name] = true
			_, idx, ok := schema.column(name)
			if !ok {
				return insertPlan{}, fmt.Errorf("%w: %q", ErrUnknownColumn, name)
			}
			targetCols[i] = idx
		}
		if len(seenNames) != len(schema.Columns) {
			return insertPlan{}, fmt.Errorf("%w: INSERT into %q must supply a value for every column in this subset (no NULLs or defaults)", ErrUnsupportedFeature, schema.Table)
		}
	}

	values := make([]Value, len(schema.Columns))
	filled := make([]bool, len(schema.Columns))
	for i, lit := range stmt.Values {
		colIdx := targetCols[i]
		v, err := bindLiteral(lit, schema.Columns[colIdx].Type)
		if err != nil {
			return insertPlan{}, fmt.Errorf("column %q: %w", schema.Columns[colIdx].Name, err)
		}
		values[colIdx] = v
		filled[colIdx] = true
	}
	for i, ok := range filled {
		if !ok {
			return insertPlan{}, fmt.Errorf("%w: INSERT into %q missing a value for column %q (no NULLs or defaults in this subset)",
				ErrUnsupportedFeature, schema.Table, schema.Columns[i].Name)
		}
	}
	return insertPlan{schema: schema, pk: values[schema.PrimaryKey], values: values}, nil
}

// selectPlan is SELECT's fully-resolved operation: columnIdx names,
// for each projected output column in requested order, its index into
// schema.Columns; pkPredicate is nil for a full-table scan, non-nil for
// a primary-key point lookup (docs/sql.md §5.1-5.2).
type selectPlan struct {
	schema      Schema
	columnIdx   []int
	pkPredicate *Value
}

func planSelect(schema Schema, stmt *SelectStmt) (selectPlan, error) {
	var idx []int
	if stmt.Columns == nil {
		idx = make([]int, len(schema.Columns))
		for i := range idx {
			idx[i] = i
		}
	} else {
		idx = make([]int, len(stmt.Columns))
		for i, name := range stmt.Columns {
			_, ci, ok := schema.column(name)
			if !ok {
				return selectPlan{}, fmt.Errorf("%w: %q", ErrUnknownColumn, name)
			}
			idx[i] = ci
		}
	}
	var pk *Value
	if stmt.Where != nil {
		v, err := bindPredicate(schema, *stmt.Where)
		if err != nil {
			return selectPlan{}, err
		}
		pk = &v
	}
	return selectPlan{schema: schema, columnIdx: idx, pkPredicate: pk}, nil
}

// columnAssignment is one resolved `column = value` pair from an
// UPDATE's SET clause: Index is the column's position in schema.Columns.
type columnAssignment struct {
	Index int
	Value Value
}

// updatePlan is UPDATE's fully-resolved operation.
type updatePlan struct {
	schema      Schema
	pk          Value
	assignments []columnAssignment
}

func planUpdate(schema Schema, stmt *UpdateStmt) (updatePlan, error) {
	pk, err := bindPredicate(schema, stmt.Where)
	if err != nil {
		return updatePlan{}, err
	}
	seen := make(map[int]bool, len(stmt.Assignments))
	assignments := make([]columnAssignment, 0, len(stmt.Assignments))
	for _, a := range stmt.Assignments {
		_, idx, ok := schema.column(a.Column)
		if !ok {
			return updatePlan{}, fmt.Errorf("%w: %q", ErrUnknownColumn, a.Column)
		}
		if seen[idx] {
			return updatePlan{}, fmt.Errorf("%w: column %q assigned more than once", ErrDuplicateColumn, a.Column)
		}
		seen[idx] = true
		if idx == schema.PrimaryKey {
			return updatePlan{}, fmt.Errorf("%w: UPDATE may not modify the primary key column %q in this subset", ErrUnsupportedFeature, a.Column)
		}
		v, err := bindLiteral(a.Value, schema.Columns[idx].Type)
		if err != nil {
			return updatePlan{}, fmt.Errorf("column %q: %w", a.Column, err)
		}
		assignments = append(assignments, columnAssignment{Index: idx, Value: v})
	}
	return updatePlan{schema: schema, pk: pk, assignments: assignments}, nil
}

// deletePlan is DELETE's fully-resolved operation.
type deletePlan struct {
	schema Schema
	pk     Value
}

func planDelete(schema Schema, stmt *DeleteStmt) (deletePlan, error) {
	pk, err := bindPredicate(schema, stmt.Where)
	if err != nil {
		return deletePlan{}, err
	}
	return deletePlan{schema: schema, pk: pk}, nil
}
