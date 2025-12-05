package sql

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// MVCCExecutor executes SQL statements with MVCC transaction support.
type MVCCExecutor struct {
	mtm *catalog.MVCCTableManager
}

// NewMVCCExecutor creates a new MVCC-aware executor.
func NewMVCCExecutor(mtm *catalog.MVCCTableManager) *MVCCExecutor {
	return &MVCCExecutor{mtm: mtm}
}

// Execute executes a SQL statement within a transaction context.
// For BEGIN/COMMIT/ROLLBACK, tx can be nil and session handles them.
func (e *MVCCExecutor) Execute(stmt Statement, tx *txn.Transaction) (*Result, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.executeCreate(s)
	case *DropTableStmt:
		return e.executeDrop(s)
	case *InsertStmt:
		return e.executeInsert(s, tx)
	case *SelectStmt:
		return e.executeSelect(s, tx)
	case *UpdateStmt:
		return e.executeUpdate(s, tx)
	case *DeleteStmt:
		return e.executeDelete(s, tx)
	case *BeginStmt, *CommitStmt, *RollbackStmt:
		// These are handled by the session, not the executor
		return nil, fmt.Errorf("transaction statements should be handled by session")
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *MVCCExecutor) executeCreate(stmt *CreateTableStmt) (*Result, error) {
	cols := make([]catalog.Column, len(stmt.Columns))
	for i, def := range stmt.Columns {
		cols[i] = catalog.Column{
			ID:      i,
			Name:    def.Name,
			Type:    def.Type,
			NotNull: def.NotNull,
		}
	}

	if err := e.mtm.CreateTable(stmt.TableName, cols); err != nil {
		return nil, err
	}

	return &Result{
		Message: fmt.Sprintf("Table '%s' created.", stmt.TableName),
	}, nil
}

func (e *MVCCExecutor) executeDrop(stmt *DropTableStmt) (*Result, error) {
	if err := e.mtm.DropTable(stmt.TableName); err != nil {
		return nil, err
	}
	return &Result{
		Message: fmt.Sprintf("Table '%s' dropped.", stmt.TableName),
	}, nil
}

func (e *MVCCExecutor) executeInsert(stmt *InsertStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("INSERT requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Build values array
	values := make([]catalog.Value, len(meta.Columns))

	// Initialize all values to NULL
	for i := range values {
		values[i] = catalog.Null(meta.Columns[i].Type)
	}

	// If column list specified, map values to columns
	if len(stmt.Columns) > 0 {
		if len(stmt.Columns) != len(stmt.Values) {
			return nil, fmt.Errorf("column count (%d) doesn't match value count (%d)",
				len(stmt.Columns), len(stmt.Values))
		}
		for i, colName := range stmt.Columns {
			col, idx := meta.Schema.ColumnByName(colName)
			if col == nil {
				return nil, fmt.Errorf("unknown column: %s", colName)
			}
			val, err := e.evalExpr(stmt.Values[i], meta.Schema, nil)
			if err != nil {
				return nil, err
			}
			val, err = coerceValueMVCC(val, col.Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", colName, err)
			}
			values[idx] = val
		}
	} else {
		// Positional values
		if len(stmt.Values) != len(meta.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(meta.Columns), len(stmt.Values))
		}
		for i, expr := range stmt.Values {
			val, err := e.evalExpr(expr, meta.Schema, nil)
			if err != nil {
				return nil, err
			}
			val, err = coerceValueMVCC(val, meta.Columns[i].Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", meta.Columns[i].Name, err)
			}
			values[i] = val
		}
	}

	// Insert with MVCC
	_, err = e.mtm.Insert(stmt.TableName, values, tx)
	if err != nil {
		return nil, err
	}

	return &Result{
		Message:      "1 row inserted.",
		RowsAffected: 1,
	}, nil
}

func (e *MVCCExecutor) executeSelect(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("SELECT requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Determine output columns
	var outCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for _, col := range meta.Columns {
			outCols = append(outCols, col.Name)
			colIndices = append(colIndices, col.ID)
		}
	} else {
		for _, sc := range stmt.Columns {
			col, idx := meta.Schema.ColumnByName(sc.Name)
			if col == nil {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			outCols = append(outCols, sc.Name)
			colIndices = append(colIndices, idx)
		}
	}

	// Scan and filter using MVCC visibility
	var rows [][]catalog.Value

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil // Continue scanning
			}
		}

		// Project columns
		outRow := make([]catalog.Value, len(colIndices))
		for i, idx := range colIndices {
			outRow[i] = row.Values[idx]
		}
		rows = append(rows, outRow)
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	return &Result{
		Columns: outCols,
		Rows:    rows,
	}, nil
}

func (e *MVCCExecutor) executeUpdate(stmt *UpdateStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("UPDATE requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Collect rows to update
	var toUpdate []struct {
		rid    storage.RID
		newRow []catalog.Value
	}

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}

		// Build new row with updates
		newRow := make([]catalog.Value, len(row.Values))
		copy(newRow, row.Values)

		for _, assign := range stmt.Assignments {
			col, idx := meta.Schema.ColumnByName(assign.Column)
			if col == nil {
				return false, fmt.Errorf("unknown column: %s", assign.Column)
			}
			val, err := e.evalExpr(assign.Value, meta.Schema, row.Values)
			if err != nil {
				return false, err
			}
			val, err = coerceValueMVCC(val, col.Type)
			if err != nil {
				return false, fmt.Errorf("column %s: %w", assign.Column, err)
			}
			newRow[idx] = val
		}

		toUpdate = append(toUpdate, struct {
			rid    storage.RID
			newRow []catalog.Value
		}{rid: row.RID, newRow: newRow})

		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Perform updates: mark old tuple deleted, insert new tuple
	for _, u := range toUpdate {
		// Mark old tuple as deleted
		if err := e.mtm.MarkDeleted(u.rid, tx); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}
		// Insert new version
		if _, err := e.mtm.Insert(stmt.TableName, u.newRow, tx); err != nil {
			return nil, fmt.Errorf("update insert failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) updated.", len(toUpdate)),
		RowsAffected: len(toUpdate),
	}, nil
}

func (e *MVCCExecutor) executeDelete(stmt *DeleteStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("DELETE requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Collect RIDs to delete
	var toDelete []storage.RID

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}

		toDelete = append(toDelete, row.RID)
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Mark tuples as deleted
	for _, rid := range toDelete {
		if err := e.mtm.MarkDeleted(rid, tx); err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) deleted.", len(toDelete)),
		RowsAffected: len(toDelete),
	}, nil
}

// evalExpr evaluates an expression to a value.
func (e *MVCCExecutor) evalExpr(expr Expression, schema *catalog.Schema, row []catalog.Value) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		if row == nil {
			return catalog.Value{}, fmt.Errorf("cannot reference column %s without row context", ex.Name)
		}
		col, idx := schema.ColumnByName(ex.Name)
		if col == nil {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	default:
		return catalog.Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evalCondition evaluates a boolean expression.
func (e *MVCCExecutor) evalCondition(expr Expression, schema *catalog.Schema, row []catalog.Value) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalCondition(ex.Left, schema, row)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalCondition(ex.Right, schema, row)

		case TOKEN_OR:
			left, err := e.evalCondition(ex.Left, schema, row)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalCondition(ex.Right, schema, row)

		case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
			leftVal, err := e.evalExpr(ex.Left, schema, row)
			if err != nil {
				return false, err
			}
			rightVal, err := e.evalExpr(ex.Right, schema, row)
			if err != nil {
				return false, err
			}
			return compareValuesMVCC(leftVal, rightVal, ex.Op)

		default:
			return false, fmt.Errorf("unsupported operator in condition: %v", ex.Op)
		}

	case *LiteralExpr:
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
		return false, fmt.Errorf("expected boolean expression")

	default:
		return false, fmt.Errorf("unsupported condition type: %T", expr)
	}
}

// compareValues compares two values with the given operator.
// Note: This function is shared with executor.go, using the same name
// compareValuesMVCC is local to avoid redeclaration
func compareValuesMVCC(left, right catalog.Value, op TokenType) (bool, error) {
	// Handle NULL comparisons
	if left.IsNull || right.IsNull {
		return false, nil // NULL comparisons are always false
	}

	// Type coercion for comparison
	if left.Type != right.Type {
		// Try to coerce right to left's type
		coerced, err := coerceValueMVCC(right, left.Type)
		if err != nil {
			return false, nil // Incompatible types
		}
		right = coerced
	}

	var cmp int
	switch left.Type {
	case catalog.TypeInt32:
		if left.Int32 < right.Int32 {
			cmp = -1
		} else if left.Int32 > right.Int32 {
			cmp = 1
		}
	case catalog.TypeInt64:
		if left.Int64 < right.Int64 {
			cmp = -1
		} else if left.Int64 > right.Int64 {
			cmp = 1
		}
	case catalog.TypeText:
		cmp = strings.Compare(left.Text, right.Text)
	case catalog.TypeBool:
		if left.Bool == right.Bool {
			cmp = 0
		} else if !left.Bool {
			cmp = -1
		} else {
			cmp = 1
		}
	case catalog.TypeTimestamp:
		if left.Timestamp.Before(right.Timestamp) {
			cmp = -1
		} else if left.Timestamp.After(right.Timestamp) {
			cmp = 1
		}
	default:
		return false, fmt.Errorf("unsupported type for comparison: %v", left.Type)
	}

	switch op {
	case TOKEN_EQ:
		return cmp == 0, nil
	case TOKEN_NE:
		return cmp != 0, nil
	case TOKEN_LT:
		return cmp < 0, nil
	case TOKEN_LE:
		return cmp <= 0, nil
	case TOKEN_GT:
		return cmp > 0, nil
	case TOKEN_GE:
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator: %v", op)
	}
}

// coerceValueMVCC attempts to convert a value to the target type.
func coerceValueMVCC(v catalog.Value, targetType catalog.DataType) (catalog.Value, error) {
	if v.IsNull {
		return catalog.Null(targetType), nil
	}

	if v.Type == targetType {
		return v, nil
	}

	// INT32 -> INT64
	if v.Type == catalog.TypeInt32 && targetType == catalog.TypeInt64 {
		return catalog.NewInt64(int64(v.Int32)), nil
	}

	// INT64 -> INT32 (with range check)
	if v.Type == catalog.TypeInt64 && targetType == catalog.TypeInt32 {
		if v.Int64 < -2147483648 || v.Int64 > 2147483647 {
			return catalog.Value{}, fmt.Errorf("value %d out of INT32 range", v.Int64)
		}
		return catalog.NewInt32(int32(v.Int64)), nil
	}

	return catalog.Value{}, fmt.Errorf("cannot coerce %v to %v", v.Type, targetType)
}
