package sql

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Executor executes SQL statements against a TableManager.
type Executor struct {
	tm *catalog.TableManager
}

// NewExecutor creates a new Executor.
func NewExecutor(tm *catalog.TableManager) *Executor {
	return &Executor{tm: tm}
}

// Result represents the result of executing a SQL statement.
type Result struct {
	Message      string
	Columns      []string
	Rows         [][]catalog.Value
	RowsAffected int
}

// Execute executes a SQL statement and returns a result.
func (e *Executor) Execute(stmt Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.executeCreate(s)
	case *DropTableStmt:
		return e.executeDrop(s)
	case *InsertStmt:
		return e.executeInsert(s)
	case *SelectStmt:
		return e.executeSelect(s)
	case *UpdateStmt:
		return e.executeUpdate(s)
	case *DeleteStmt:
		return e.executeDelete(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Executor) executeCreate(stmt *CreateTableStmt) (*Result, error) {
	cols := make([]catalog.Column, len(stmt.Columns))
	for i, c := range stmt.Columns {
		cols[i] = catalog.Column{
			Name:    c.Name,
			Type:    c.Type,
			NotNull: c.NotNull,
		}
	}

	// Use storage type from statement (default is "ROW")
	storageType := strings.ToLower(stmt.StorageType)
	if storageType == "" {
		storageType = "row"
	}

	if err := e.tm.CreateTableWithStorage(stmt.TableName, cols, storageType); err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Table '%s' created", stmt.TableName)
	if storageType == "column" {
		msg += " (columnar storage)"
	}
	return &Result{Message: msg + "."}, nil
}

func (e *Executor) executeDrop(stmt *DropTableStmt) (*Result, error) {
	if err := e.tm.Catalog().DropTable(stmt.TableName); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table '%s' dropped.", stmt.TableName)}, nil
}

func (e *Executor) executeInsert(stmt *InsertStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Build values array matching schema order
	values := make([]catalog.Value, len(meta.Schema.Columns))

	if len(stmt.Columns) == 0 {
		// No column list provided - values must match schema order
		if len(stmt.Values) != len(meta.Schema.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(meta.Schema.Columns), len(stmt.Values))
		}
		for i, expr := range stmt.Values {
			val, err := e.evalExpr(expr, meta.Schema, nil)
			if err != nil {
				return nil, err
			}
			// Coerce type if needed
			val, err = coerceValue(val, meta.Schema.Columns[i].Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", meta.Schema.Columns[i].Name, err)
			}
			values[i] = val
		}
	} else {
		// Column list provided - match values to columns
		if len(stmt.Columns) != len(stmt.Values) {
			return nil, fmt.Errorf("column count (%d) doesn't match value count (%d)", len(stmt.Columns), len(stmt.Values))
		}

		// Initialize with nulls
		for i, col := range meta.Schema.Columns {
			values[i] = catalog.Null(col.Type)
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
			val, err = coerceValue(val, col.Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", colName, err)
			}
			values[idx] = val
		}
	}

	_, err = e.tm.Insert(stmt.TableName, values)
	if err != nil {
		return nil, err
	}

	return &Result{Message: "1 row inserted.", RowsAffected: 1}, nil
}

func (e *Executor) executeSelect(stmt *SelectStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Determine which columns to return
	var outputCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for i, c := range meta.Schema.Columns {
			outputCols = append(outputCols, c.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			col, idx := meta.Schema.ColumnByName(sc.Name)
			if col == nil {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			outputCols = append(outputCols, col.Name)
			colIndices = append(colIndices, idx)
		}
	}

	// Scan all rows (sequential scan)
	var resultRows [][]catalog.Value
	err = e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil // continue scanning
			}
		}

		// Project columns
		projectedRow := make([]catalog.Value, len(colIndices))
		for i, idx := range colIndices {
			projectedRow[i] = row[idx]
		}
		resultRows = append(resultRows, projectedRow)
		return true, nil // continue
	})

	if err != nil {
		return nil, err
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

func (e *Executor) executeUpdate(stmt *UpdateStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// For now, we do a simple approach: scan, filter, update in place
	// This is not efficient but works for Stage 3
	var ridsToUpdate []storage.RID
	var newRows [][]catalog.Value

	err = e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}

		// Apply updates
		newRow := make([]catalog.Value, len(row))
		copy(newRow, row)

		for _, assign := range stmt.Assignments {
			col, idx := meta.Schema.ColumnByName(assign.Column)
			if col == nil {
				return false, fmt.Errorf("unknown column: %s", assign.Column)
			}
			val, err := e.evalExpr(assign.Value, meta.Schema, row)
			if err != nil {
				return false, err
			}
			val, err = coerceValue(val, col.Type)
			if err != nil {
				return false, fmt.Errorf("column %s: %w", assign.Column, err)
			}
			newRow[idx] = val
		}

		ridsToUpdate = append(ridsToUpdate, rid)
		newRows = append(newRows, newRow)
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Delete old rows and insert new rows
	// This is a simple delete+insert approach for Stage 3
	for i, rid := range ridsToUpdate {
		if err := e.tm.Delete(rid); err != nil {
			return nil, fmt.Errorf("update delete failed: %w", err)
		}
		_, err := e.tm.Insert(stmt.TableName, newRows[i])
		if err != nil {
			return nil, fmt.Errorf("update insert failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) updated.", len(ridsToUpdate)),
		RowsAffected: len(ridsToUpdate),
	}, nil
}

func (e *Executor) executeDelete(stmt *DeleteStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Collect RIDs to delete
	var ridsToDelete []storage.RID

	err = e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}
		ridsToDelete = append(ridsToDelete, rid)
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Actually delete the rows
	for _, rid := range ridsToDelete {
		if err := e.tm.Delete(rid); err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) deleted.", len(ridsToDelete)),
		RowsAffected: len(ridsToDelete),
	}, nil
}

// scanTable scans all rows in a table and calls fn for each.
// This is a simple sequential scan implementation.
func (e *Executor) scanTable(tableName string, schema *catalog.Schema, fn func(rid storage.RID, row []catalog.Value) (bool, error)) error {
	// We need to scan through pages. For now, use a simple approach:
	// Try fetching RIDs starting from page 0, slot 0.
	// This is inefficient but works for small datasets.

	for pageID := uint32(0); pageID < 10000; pageID++ { // reasonable limit
		foundAny := false
		for slotID := uint16(0); slotID < 1000; slotID++ {
			rid := storage.RID{Table: tableName, Page: pageID, Slot: slotID}
			row, err := e.tm.Fetch(tableName, rid)
			if err != nil {
				// Check if it's "invalid slot" - means we've exceeded slot count on this page
				if strings.Contains(err.Error(), "invalid slot") {
					break // try next page
				}
				// "slot empty" means deleted slot - skip it but keep scanning
				if strings.Contains(err.Error(), "slot empty") {
					foundAny = true // a deleted slot still counts as having slots
					continue
				}
				// Other errors - could be end of data
				if strings.Contains(err.Error(), "corrupt") {
					break
				}
				continue // skip this slot
			}

			foundAny = true
			cont, err := fn(rid, row)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		if !foundAny {
			// No slots at all on this page, we're done
			return nil
		}
	}

	return nil
}

// evalExpr evaluates an expression to a value.
func (e *Executor) evalExpr(expr Expression, schema *catalog.Schema, row []catalog.Value) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		if row == nil {
			return catalog.Value{}, fmt.Errorf("cannot reference column %s without a row context", ex.Name)
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
func (e *Executor) evalCondition(expr Expression, schema *catalog.Schema, row []catalog.Value) (bool, error) {
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
			left, err := e.evalExpr(ex.Left, schema, row)
			if err != nil {
				return false, err
			}
			right, err := e.evalExpr(ex.Right, schema, row)
			if err != nil {
				return false, err
			}
			return compareValues(left, right, ex.Op)
		}

	case *UnaryExpr:
		if ex.Op == TOKEN_NOT {
			val, err := e.evalCondition(ex.Expr, schema, row)
			if err != nil {
				return false, err
			}
			return !val, nil
		}

	case *LiteralExpr:
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
	}

	return false, fmt.Errorf("cannot evaluate expression as condition: %T", expr)
}

// compareValues compares two values with the given operator.
func compareValues(left, right catalog.Value, op TokenType) (bool, error) {
	// Handle NULL comparisons
	if left.IsNull || right.IsNull {
		// NULL compared to anything is always false (except for IS NULL which we don't have yet)
		return false, nil
	}

	// Type coercion for comparison
	if left.Type != right.Type {
		// Try to coerce right to left's type
		var err error
		right, err = coerceValue(right, left.Type)
		if err != nil {
			return false, err
		}
	}

	var cmp int // -1 = less, 0 = equal, 1 = greater

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
		return false, fmt.Errorf("cannot compare type %v", left.Type)
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
		return false, fmt.Errorf("unknown comparison operator: %v", op)
	}
}

// coerceValue attempts to coerce a value to the target type.
func coerceValue(val catalog.Value, targetType catalog.DataType) (catalog.Value, error) {
	if val.IsNull {
		return catalog.Null(targetType), nil
	}

	if val.Type == targetType {
		return val, nil
	}

	// Int32 to Int64
	if val.Type == catalog.TypeInt32 && targetType == catalog.TypeInt64 {
		return catalog.NewInt64(int64(val.Int32)), nil
	}

	// Int64 to Int32 (if in range)
	if val.Type == catalog.TypeInt64 && targetType == catalog.TypeInt32 {
		if val.Int64 >= -2147483648 && val.Int64 <= 2147483647 {
			return catalog.NewInt32(int32(val.Int64)), nil
		}
		return catalog.Value{}, fmt.Errorf("value %d out of range for INT", val.Int64)
	}

	return catalog.Value{}, fmt.Errorf("cannot coerce %v to %v", val.Type, targetType)
}
