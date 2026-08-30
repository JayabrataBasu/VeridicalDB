package exec

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// combinedSchema wraps a two-table schema with a name→index map so that both
// qualified (table.column) and unqualified column names resolve into a row that
// is the target row followed by the FROM/USING row.
type combinedSchema struct {
	Schema    *catalog.Schema
	columnMap map[string]int
}

// ColumnByName resolves a qualified or unqualified column name.
func (cs *combinedSchema) ColumnByName(name string) (*catalog.Column, int) {
	if idx, ok := cs.columnMap[strings.ToUpper(name)]; ok {
		return &cs.Schema.Columns[idx], idx
	}
	return nil, -1
}

// buildCombinedSchema builds a combinedSchema for `<table1> , <table2>` where a
// combined row is schema1's columns followed by schema2's. Unqualified names
// prefer table1 on collision (Postgres resolves the target table first).
func buildCombinedSchema(table1, alias1 string, schema1 *catalog.Schema, table2, alias2 string, schema2 *catalog.Schema) *combinedSchema {
	cs := &combinedSchema{
		Schema:    &catalog.Schema{Columns: make([]catalog.Column, len(schema1.Columns)+len(schema2.Columns))},
		columnMap: make(map[string]int),
	}

	prefix1 := table1
	if alias1 != "" {
		prefix1 = alias1
	}
	for i, col := range schema1.Columns {
		nc := col
		nc.Name = prefix1 + "." + col.Name
		cs.Schema.Columns[i] = nc
		cs.columnMap[strings.ToUpper(prefix1+"."+col.Name)] = i
		cs.columnMap[strings.ToUpper(col.Name)] = i
	}

	prefix2 := table2
	if alias2 != "" {
		prefix2 = alias2
	}
	off := len(schema1.Columns)
	for i, col := range schema2.Columns {
		nc := col
		nc.Name = prefix2 + "." + col.Name
		cs.Schema.Columns[off+i] = nc
		cs.columnMap[strings.ToUpper(prefix2+"."+col.Name)] = off + i
		if _, idx := schema1.ColumnByName(col.Name); idx < 0 {
			cs.columnMap[strings.ToUpper(col.Name)] = off + i
		}
	}

	return cs
}

// evalExprCombinedMVCC evaluates a scalar expression against a combined row.
func (e *MVCCExecutor) evalExprCombinedMVCC(expr ast.Expression, cs *combinedSchema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *ast.LiteralExpr:
		return ex.Value, nil

	case *ast.ColumnRef:
		col, idx := cs.ColumnByName(ex.Name)
		if col == nil {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	case *ast.BinaryExpr:
		switch ex.Op {
		case token.TOKEN_PLUS, token.TOKEN_MINUS, token.TOKEN_STAR, token.TOKEN_SLASH:
			left, err := e.evalExprCombinedMVCC(ex.Left, cs, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}
			right, err := e.evalExprCombinedMVCC(ex.Right, cs, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}
			return evalArithmetic(left, right, ex.Op)
		}
		return catalog.Value{}, fmt.Errorf("unsupported binary operator in expression: %v", ex.Op)

	case *ast.FunctionExpr:
		args := make([]catalog.Value, len(ex.Args))
		for i, a := range ex.Args {
			v, err := e.evalExprCombinedMVCC(a, cs, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}
			args[i] = v
		}
		return evalFunction(ex.Name, args)

	default:
		// Other expression shapes: fall back to the full MVCC evaluator against
		// the combined schema (its column names are the qualified forms).
		return e.evalExpr(expr, cs.Schema, row, tx)
	}
}

// evalConditionCombinedMVCC evaluates a boolean expression against a combined row.
func (e *MVCCExecutor) evalConditionCombinedMVCC(expr ast.Expression, cs *combinedSchema, row []catalog.Value, tx *txn.Transaction) (bool, error) {
	switch ex := expr.(type) {
	case *ast.LiteralExpr:
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
		return !ex.Value.IsNull, nil

	case *ast.BinaryExpr:
		switch ex.Op {
		case token.TOKEN_AND:
			l, err := e.evalConditionCombinedMVCC(ex.Left, cs, row, tx)
			if err != nil || !l {
				return false, err
			}
			return e.evalConditionCombinedMVCC(ex.Right, cs, row, tx)
		case token.TOKEN_OR:
			l, err := e.evalConditionCombinedMVCC(ex.Left, cs, row, tx)
			if err != nil {
				return false, err
			}
			if l {
				return true, nil
			}
			return e.evalConditionCombinedMVCC(ex.Right, cs, row, tx)
		case token.TOKEN_EQ, token.TOKEN_NE, token.TOKEN_LT, token.TOKEN_LE, token.TOKEN_GT, token.TOKEN_GE:
			l, err := e.evalExprCombinedMVCC(ex.Left, cs, row, tx)
			if err != nil {
				return false, err
			}
			r, err := e.evalExprCombinedMVCC(ex.Right, cs, row, tx)
			if err != nil {
				return false, err
			}
			return compareValues(l, r, ex.Op)
		}

	case *ast.UnaryExpr:
		if ex.Op == token.TOKEN_NOT {
			v, err := e.evalConditionCombinedMVCC(ex.Expr, cs, row, tx)
			if err != nil {
				return false, err
			}
			return !v, nil
		}

	case *ast.IsNullExpr:
		v, err := e.evalExprCombinedMVCC(ex.Expr, cs, row, tx)
		if err != nil {
			return false, err
		}
		if ex.Not {
			return !v.IsNull, nil
		}
		return v.IsNull, nil
	}

	return false, fmt.Errorf("unsupported condition type in multi-table DML: %T", expr)
}

// executeUpdateWithFromMVCC handles `UPDATE target SET ... FROM other WHERE ...`.
// Each target row is matched against every FROM row; the first match wins, its
// SET expressions are evaluated over the combined row, and the write-back goes
// through the normal MVCC apply path so constraints, triggers and indexes run.
func (e *MVCCExecutor) executeUpdateWithFromMVCC(stmt *ast.UpdateStmt, targetMeta *catalog.TableMeta, tx *txn.Transaction) (*Result, error) {
	fromMeta, err := e.mtm.Catalog().GetTable(stmt.FromTable)
	if err != nil {
		return nil, fmt.Errorf("FROM table %q: %w", stmt.FromTable, err)
	}

	cs := buildCombinedSchema(
		stmt.TableName, stmt.TableAlias, targetMeta.Schema,
		stmt.FromTable, stmt.FromAlias, fromMeta.Schema,
	)

	fromRows, err := e.materializeRows(stmt.FromTable, tx)
	if err != nil {
		return nil, err
	}
	targetLen := len(targetMeta.Schema.Columns)

	var cands []mvccUpdateCandidate
	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		for _, fromRow := range fromRows {
			combined := make([]catalog.Value, 0, targetLen+len(fromRow))
			combined = append(combined, row.Values...)
			combined = append(combined, fromRow...)

			if stmt.Where != nil {
				match, err := e.evalConditionCombinedMVCC(stmt.Where, cs, combined, tx)
				if err != nil {
					return false, err
				}
				if !match {
					continue
				}
			}

			newRow := make([]catalog.Value, targetLen)
			copy(newRow, row.Values)
			for _, assign := range stmt.Assignments {
				col, idx := targetMeta.Schema.ColumnByName(assign.Column)
				if col == nil {
					return false, fmt.Errorf("unknown column: %s", assign.Column)
				}
				val, err := e.evalExprCombinedMVCC(assign.Value, cs, combined, tx)
				if err != nil {
					return false, err
				}
				val, err = coerceValueMVCC(val, col.Type)
				if err != nil {
					return false, fmt.Errorf("column %s: %w", assign.Column, err)
				}
				newRow[idx] = val
			}
			if err := e.validateCheckConstraints(targetMeta.Schema, newRow); err != nil {
				return false, err
			}

			oldRow := make([]catalog.Value, len(row.Values))
			copy(oldRow, row.Values)
			cands = append(cands, mvccUpdateCandidate{rid: row.RID, oldRow: oldRow, newRow: newRow})
			break // first match wins
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if err := e.applyMVCCUpdates(stmt.TableName, targetMeta, cands, tx); err != nil {
		return nil, err
	}
	return &Result{
		Message:      fmt.Sprintf("%d row(s) updated.", len(cands)),
		RowsAffected: len(cands),
	}, nil
}

// executeDeleteWithUsingMVCC handles `DELETE FROM target USING other WHERE ...`.
func (e *MVCCExecutor) executeDeleteWithUsingMVCC(stmt *ast.DeleteStmt, targetMeta *catalog.TableMeta, tx *txn.Transaction) (*Result, error) {
	usingMeta, err := e.mtm.Catalog().GetTable(stmt.UsingTable)
	if err != nil {
		return nil, fmt.Errorf("USING table %q: %w", stmt.UsingTable, err)
	}

	cs := buildCombinedSchema(
		stmt.TableName, stmt.TableAlias, targetMeta.Schema,
		stmt.UsingTable, stmt.UsingAlias, usingMeta.Schema,
	)

	usingRows, err := e.materializeRows(stmt.UsingTable, tx)
	if err != nil {
		return nil, err
	}

	var cands []mvccDeleteCandidate
	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		for _, usingRow := range usingRows {
			combined := make([]catalog.Value, 0, len(row.Values)+len(usingRow))
			combined = append(combined, row.Values...)
			combined = append(combined, usingRow...)

			if stmt.Where != nil {
				match, err := e.evalConditionCombinedMVCC(stmt.Where, cs, combined, tx)
				if err != nil {
					return false, err
				}
				if !match {
					continue
				}
			}
			values := make([]catalog.Value, len(row.Values))
			copy(values, row.Values)
			cands = append(cands, mvccDeleteCandidate{rid: row.RID, values: values})
			break
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if err := e.applyMVCCDeletes(stmt.TableName, targetMeta, cands, tx); err != nil {
		return nil, err
	}
	return &Result{
		Message:      fmt.Sprintf("%d row(s) deleted.", len(cands)),
		RowsAffected: len(cands),
	}, nil
}

// mvccUpdateCandidate is a target row queued for update: the old tuple's RID and
// values, and the fully-formed replacement row.
type mvccUpdateCandidate struct {
	rid    storage.RID
	oldRow []catalog.Value
	newRow []catalog.Value
}

// mvccDeleteCandidate is a target row queued for deletion.
type mvccDeleteCandidate struct {
	rid    storage.RID
	values []catalog.Value
}

// applyMVCCUpdates runs the shared write-back for a batch of update candidates:
// constraint checks, BEFORE/AFTER triggers, index maintenance, and the
// MVCC mark-deleted + insert-new-version pair. Used by both plain UPDATE and
// UPDATE ... FROM.
func (e *MVCCExecutor) applyMVCCUpdates(tableName string, meta *catalog.TableMeta, cands []mvccUpdateCandidate, tx *txn.Transaction) error {
	for _, u := range cands {
		if err := e.checkVarcharLengths(meta.Schema, u.newRow); err != nil {
			return err
		}
		if err := e.checkUniqueness(tableName, meta.Schema, u.newRow, u.rid, tx); err != nil {
			return err
		}
		if err := e.checkForeignKeys(meta, u.newRow, tx); err != nil {
			return err
		}

		if err := e.fireBeforeUpdateTriggers(tableName, u.oldRow, u.newRow, meta.Schema); err != nil {
			return fmt.Errorf("BEFORE UPDATE trigger failed: %w", err)
		}
		if err := e.updateIndexesOnDelete(tableName, u.rid, u.oldRow, meta.Schema); err != nil {
			return fmt.Errorf("update index remove failed: %w", err)
		}
		if err := e.mtm.MarkDeleted(u.rid, tx); err != nil {
			return fmt.Errorf("update failed: %w", err)
		}
		newRID, err := e.mtm.Insert(tableName, u.newRow, tx)
		if err != nil {
			return fmt.Errorf("update insert failed: %w", err)
		}
		if err := e.updateIndexesOnInsert(tableName, newRID, u.newRow, meta.Schema); err != nil {
			return fmt.Errorf("update index insert failed: %w", err)
		}
		if err := e.fireAfterUpdateTriggers(tableName, u.oldRow, u.newRow, meta.Schema); err != nil {
			return fmt.Errorf("AFTER UPDATE trigger failed: %w", err)
		}
	}
	return nil
}

// applyMVCCDeletes runs the shared write-back for a batch of delete candidates:
// FK-RESTRICT check, BEFORE/AFTER triggers, index cleanup, and mark-deleted.
// Used by both plain DELETE and DELETE ... USING.
func (e *MVCCExecutor) applyMVCCDeletes(tableName string, meta *catalog.TableMeta, cands []mvccDeleteCandidate, tx *txn.Transaction) error {
	for _, d := range cands {
		if err := e.checkReferencingForeignKeys(meta, d.values, tx); err != nil {
			return err
		}
		if err := e.fireBeforeDeleteTriggers(tableName, d.values, meta.Schema); err != nil {
			return fmt.Errorf("BEFORE DELETE trigger failed: %w", err)
		}
		if err := e.updateIndexesOnDelete(tableName, d.rid, d.values, meta.Schema); err != nil {
			return fmt.Errorf("delete index cleanup failed: %w", err)
		}
		if err := e.mtm.MarkDeleted(d.rid, tx); err != nil {
			return fmt.Errorf("delete failed: %w", err)
		}
		if err := e.fireAfterDeleteTriggers(tableName, d.values, meta.Schema); err != nil {
			return fmt.Errorf("AFTER DELETE trigger failed: %w", err)
		}
	}
	return nil
}

// materializeRows returns a snapshot copy of every MVCC-visible row of a table.
func (e *MVCCExecutor) materializeRows(tableName string, tx *txn.Transaction) ([][]catalog.Value, error) {
	var rows [][]catalog.Value
	err := e.mtm.Scan(tableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rc := make([]catalog.Value, len(row.Values))
		copy(rc, row.Values)
		rows = append(rows, rc)
		return true, nil
	})
	return rows, err
}
