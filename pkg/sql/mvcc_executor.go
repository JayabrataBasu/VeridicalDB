package sql

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// MVCCExecutor executes SQL statements with MVCC transaction support.
type MVCCExecutor struct {
	mtm      *catalog.MVCCTableManager
	indexMgr *btree.IndexManager
}

// NewMVCCExecutor creates a new MVCC-aware executor.
func NewMVCCExecutor(mtm *catalog.MVCCTableManager) *MVCCExecutor {
	return &MVCCExecutor{mtm: mtm}
}

// SetIndexManager sets the index manager for index maintenance during DML operations.
func (e *MVCCExecutor) SetIndexManager(mgr *btree.IndexManager) {
	e.indexMgr = mgr
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
	case *AlterTableStmt:
		return e.executeAlter(s)
	case *TruncateTableStmt:
		return e.executeTruncate(s)
	case *ShowStmt:
		return e.executeShow(s)
	case *ExplainStmt:
		return e.executeExplain(s, tx)
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

	// Use storage type from statement (default is "ROW")
	storageType := strings.ToLower(stmt.StorageType)
	if storageType == "" {
		storageType = "row"
	}

	if err := e.mtm.CreateTableWithStorage(stmt.TableName, cols, storageType); err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Table '%s' created", stmt.TableName)
	if storageType == "column" {
		msg += " (columnar storage)"
	}
	return &Result{
		Message: msg + ".",
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
	rid, err := e.mtm.Insert(stmt.TableName, values, tx)
	if err != nil {
		return nil, err
	}

	// Update indexes for the new row
	if err := e.updateIndexesOnInsert(stmt.TableName, rid, values, meta.Schema); err != nil {
		return nil, fmt.Errorf("index update failed: %w", err)
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

	// Try to use an index scan first
	result, used, err := e.executeSelectWithIndex(stmt, tx)
	if err != nil {
		return nil, err
	}
	if used {
		return result, nil // Index scan succeeded
	}

	// Fall back to full table scan
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
		for i, col := range meta.Columns {
			outCols = append(outCols, col.Name)
			colIndices = append(colIndices, i) // Use slice index, not col.ID
		}
	} else {
		for _, sc := range stmt.Columns {
			col, idx := meta.Schema.ColumnByName(sc.Name)
			if col == nil {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			// Use alias if specified, otherwise use column name
			if sc.Alias != "" {
				outCols = append(outCols, sc.Alias)
			} else {
				outCols = append(outCols, sc.Name)
			}
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
		oldRow []catalog.Value
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

		// Keep old row for index cleanup
		oldRow := make([]catalog.Value, len(row.Values))
		copy(oldRow, row.Values)

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
			oldRow []catalog.Value
			newRow []catalog.Value
		}{rid: row.RID, oldRow: oldRow, newRow: newRow})

		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Perform updates: mark old tuple deleted, insert new tuple
	for _, u := range toUpdate {
		// Remove old index entries
		if err := e.updateIndexesOnDelete(stmt.TableName, u.rid, u.oldRow, meta.Schema); err != nil {
			return nil, fmt.Errorf("update index remove failed: %w", err)
		}

		// Mark old tuple as deleted
		if err := e.mtm.MarkDeleted(u.rid, tx); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}

		// Insert new version
		newRID, err := e.mtm.Insert(stmt.TableName, u.newRow, tx)
		if err != nil {
			return nil, fmt.Errorf("update insert failed: %w", err)
		}

		// Add new index entries
		if err := e.updateIndexesOnInsert(stmt.TableName, newRID, u.newRow, meta.Schema); err != nil {
			return nil, fmt.Errorf("update index insert failed: %w", err)
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

	// Collect rows to delete (need values for index cleanup)
	var toDelete []struct {
		rid    storage.RID
		values []catalog.Value
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

		// Copy row values for index cleanup
		values := make([]catalog.Value, len(row.Values))
		copy(values, row.Values)
		toDelete = append(toDelete, struct {
			rid    storage.RID
			values []catalog.Value
		}{rid: row.RID, values: values})
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Mark tuples as deleted and clean up indexes
	for _, d := range toDelete {
		// Remove index entries
		if err := e.updateIndexesOnDelete(stmt.TableName, d.rid, d.values, meta.Schema); err != nil {
			return nil, fmt.Errorf("delete index cleanup failed: %w", err)
		}

		if err := e.mtm.MarkDeleted(d.rid, tx); err != nil {
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

	case *BinaryExpr:
		// Handle arithmetic operations
		switch ex.Op {
		case TOKEN_PLUS, TOKEN_MINUS, TOKEN_STAR, TOKEN_SLASH:
			left, err := e.evalExpr(ex.Left, schema, row)
			if err != nil {
				return catalog.Value{}, err
			}
			right, err := e.evalExpr(ex.Right, schema, row)
			if err != nil {
				return catalog.Value{}, err
			}
			return evalArithmetic(left, right, ex.Op)
		}
		return catalog.Value{}, fmt.Errorf("unsupported binary operator in expression: %v", ex.Op)

	case *FunctionExpr:
		// Evaluate function arguments
		args := make([]catalog.Value, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := e.evalExpr(arg, schema, row)
			if err != nil {
				return catalog.Value{}, err
			}
			args[i] = val
		}
		return evalFunction(ex.Name, args)

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

	case *UnaryExpr:
		if ex.Op == TOKEN_NOT {
			result, err := e.evalCondition(ex.Expr, schema, row)
			if err != nil {
				return false, err
			}
			return !result, nil
		}
		return false, fmt.Errorf("unsupported unary operator: %v", ex.Op)

	case *InExpr:
		// Evaluate IN expression
		leftVal, err := e.evalExpr(ex.Left, schema, row)
		if err != nil {
			return false, err
		}
		if leftVal.IsNull {
			return false, nil // NULL IN (...) is always false
		}

		found := false
		for _, valExpr := range ex.Values {
			rightVal, err := e.evalExpr(valExpr, schema, row)
			if err != nil {
				return false, err
			}
			if rightVal.IsNull {
				continue // Skip NULL values in the list
			}
			eq, err := compareValuesMVCC(leftVal, rightVal, TOKEN_EQ)
			if err != nil {
				return false, err
			}
			if eq {
				found = true
				break
			}
		}

		if ex.Not {
			return !found, nil
		}
		return found, nil

	case *BetweenExpr:
		// Evaluate BETWEEN expression
		val, err := e.evalExpr(ex.Expr, schema, row)
		if err != nil {
			return false, err
		}
		if val.IsNull {
			return false, nil // NULL BETWEEN ... is always false
		}

		lowVal, err := e.evalExpr(ex.Low, schema, row)
		if err != nil {
			return false, err
		}
		highVal, err := e.evalExpr(ex.High, schema, row)
		if err != nil {
			return false, err
		}

		// val >= low AND val <= high
		geLow, err := compareValuesMVCC(val, lowVal, TOKEN_GE)
		if err != nil {
			return false, err
		}
		leHigh, err := compareValuesMVCC(val, highVal, TOKEN_LE)
		if err != nil {
			return false, err
		}

		result := geLow && leHigh
		if ex.Not {
			return !result, nil
		}
		return result, nil

	case *LikeExpr:
		// Evaluate LIKE expression
		val, err := e.evalExpr(ex.Expr, schema, row)
		if err != nil {
			return false, err
		}
		if val.IsNull {
			return false, nil // NULL LIKE ... is always false
		}

		patternVal, err := e.evalExpr(ex.Pattern, schema, row)
		if err != nil {
			return false, err
		}
		if patternVal.IsNull {
			return false, nil
		}

		// Both must be text for LIKE
		if val.Type != catalog.TypeText || patternVal.Type != catalog.TypeText {
			return false, fmt.Errorf("LIKE requires text operands")
		}

		result := matchLikePattern(val.Text, patternVal.Text, ex.CaseInsensitive)
		if ex.Not {
			return !result, nil
		}
		return result, nil

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

// updateIndexesOnInsert updates all indexes on a table after an INSERT.
func (e *MVCCExecutor) updateIndexesOnInsert(tableName string, rid storage.RID, values []catalog.Value, schema *catalog.Schema) error {
	if e.indexMgr == nil {
		return nil // No index manager, nothing to do
	}

	indexes := e.indexMgr.ListIndexes(tableName)
	for _, meta := range indexes {
		// Build key from column values
		key, err := e.buildIndexKey(meta.Columns, values, schema)
		if err != nil {
			return fmt.Errorf("build key for index %s: %w", meta.Name, err)
		}

		// Insert into index: key -> RID
		if err := e.indexMgr.Insert(meta.Name, key, rid); err != nil {
			return fmt.Errorf("insert into index %s: %w", meta.Name, err)
		}
	}

	return nil
}

// updateIndexesOnDelete updates all indexes on a table after a DELETE.
// Note: For MVCC, we don't actually delete from indexes immediately.
// The index will contain stale entries that are filtered during query.
// A proper implementation would need garbage collection.
func (e *MVCCExecutor) updateIndexesOnDelete(tableName string, _ storage.RID, values []catalog.Value, schema *catalog.Schema) error {
	if e.indexMgr == nil {
		return nil
	}

	indexes := e.indexMgr.ListIndexes(tableName)
	for _, meta := range indexes {
		// Build key from column values
		key, err := e.buildIndexKey(meta.Columns, values, schema)
		if err != nil {
			return fmt.Errorf("build key for index %s: %w", meta.Name, err)
		}

		// Delete from index
		if err := e.indexMgr.Delete(meta.Name, key); err != nil {
			return fmt.Errorf("delete from index %s: %w", meta.Name, err)
		}
	}

	return nil
}

// buildIndexKey builds an index key from column values.
func (e *MVCCExecutor) buildIndexKey(columns []string, values []catalog.Value, schema *catalog.Schema) ([]byte, error) {
	if len(columns) == 1 {
		// Single column index
		col, idx := schema.ColumnByName(columns[0])
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", columns[0])
		}
		return encodeIndexValue(values[idx])
	}

	// Composite index - collect all parts and combine
	var parts [][]byte
	for _, colName := range columns {
		col, idx := schema.ColumnByName(colName)
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", colName)
		}
		part, err := encodeIndexValue(values[idx])
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	// Call EncodeCompositeKey with variadic expansion
	return btree.EncodeCompositeKey(parts...), nil
}

// encodeIndexValue encodes a catalog.Value for use as an index key.
func encodeIndexValue(v catalog.Value) ([]byte, error) {
	if v.IsNull {
		// NULL values get a special encoding (0x00 prefix)
		return []byte{0x00}, nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return btree.EncodeIntKey(int64(v.Int32)), nil
	case catalog.TypeInt64:
		return btree.EncodeIntKey(v.Int64), nil
	case catalog.TypeBool:
		if v.Bool {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case catalog.TypeText:
		// Prefix with 0x01 to distinguish from NULL
		return append([]byte{0x01}, []byte(v.Text)...), nil
	default:
		return nil, fmt.Errorf("unsupported type for index: %v", v.Type)
	}
}

// Note: encodeRID and decodeRID are reserved for future use when implementing
// proper index-based RID encoding/decoding for garbage collection.
// They are commented out to avoid unused function warnings.
/*
// encodeRID encodes a RID as a value for index entries.
func encodeRID(rid storage.RID) []byte {
	// Encode as: [Page:4][Slot:2]
	buf := make([]byte, 6)
	buf[0] = byte(rid.Page >> 24)
	buf[1] = byte(rid.Page >> 16)
	buf[2] = byte(rid.Page >> 8)
	buf[3] = byte(rid.Page)
	buf[4] = byte(rid.Slot >> 8)
	buf[5] = byte(rid.Slot)
	return buf
}

// decodeRID decodes a RID from index entry value.
func decodeRID(tableName string, data []byte) (storage.RID, error) {
	if len(data) < 6 {
		return storage.RID{}, fmt.Errorf("invalid RID data: too short")
	}
	page := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	slot := uint16(data[4])<<8 | uint16(data[5])
	return storage.RID{Table: tableName, Page: page, Slot: slot}, nil
}
*/

// IndexScanInfo holds information about an index scan that can be used.
type IndexScanInfo struct {
	IndexName string
	Key       []byte
	Op        TokenType // The comparison operator (=, <, >, <=, >=)
	Column    string
}

// findUsableIndex checks if an index can be used for a WHERE clause.
// Returns nil if no suitable index is found.
func (e *MVCCExecutor) findUsableIndex(tableName string, where Expression, _ *catalog.Schema) *IndexScanInfo {
	if e.indexMgr == nil || where == nil {
		return nil
	}

	// Look for simple equality conditions: column = literal
	switch ex := where.(type) {
	case *BinaryExpr:
		// Only equality for now - range scans could be added later
		if ex.Op == TOKEN_EQ {
			// Check if left is column and right is literal (or vice versa)
			var colName string
			var literal *LiteralExpr

			if col, ok := ex.Left.(*ColumnRef); ok {
				if lit, ok := ex.Right.(*LiteralExpr); ok {
					colName = col.Name
					literal = lit
				}
			} else if col, ok := ex.Right.(*ColumnRef); ok {
				if lit, ok := ex.Left.(*LiteralExpr); ok {
					colName = col.Name
					literal = lit
				}
			}

			if colName != "" && literal != nil {
				// Look for an index on this column
				indexes := e.indexMgr.ListIndexes(tableName)
				for _, meta := range indexes {
					if len(meta.Columns) == 1 && strings.EqualFold(meta.Columns[0], colName) {
						// Found a matching single-column index
						key, err := encodeIndexValue(literal.Value)
						if err == nil {
							return &IndexScanInfo{
								IndexName: meta.Name,
								Key:       key,
								Op:        TOKEN_EQ,
								Column:    colName,
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// executeIndexScan performs an index scan using the given IndexScanInfo.
func (e *MVCCExecutor) executeIndexScan(_ string, scanInfo *IndexScanInfo, _ *txn.Transaction) ([]storage.RID, error) {
	switch scanInfo.Op {
	case TOKEN_EQ:
		// For equality, use SearchAll to get all matching RIDs (handles non-unique indexes)
		rids, err := e.indexMgr.SearchAll(scanInfo.IndexName, scanInfo.Key)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil, nil // No matches
			}
			return nil, err
		}
		return rids, nil

	// TODO: Range scans for <, >, <=, >=
	default:
		return nil, fmt.Errorf("unsupported index scan operator: %v", scanInfo.Op)
	}
}

// executeSelectWithIndex performs a SELECT using an index scan when possible.
func (e *MVCCExecutor) executeSelectWithIndex(stmt *SelectStmt, tx *txn.Transaction) (*Result, bool, error) {
	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, false, err
	}

	// Check if we can use an index
	scanInfo := e.findUsableIndex(stmt.TableName, stmt.Where, meta.Schema)
	if scanInfo == nil {
		return nil, false, nil // No usable index, fall back to table scan
	}

	// Perform index scan
	rids, err := e.executeIndexScan(stmt.TableName, scanInfo, tx)
	if err != nil {
		return nil, false, err
	}

	// Determine output columns
	var outCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for i, col := range meta.Columns {
			outCols = append(outCols, col.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			col, idx := meta.Schema.ColumnByName(sc.Name)
			if col == nil {
				return nil, false, fmt.Errorf("unknown column: %s", sc.Name)
			}
			outCols = append(outCols, sc.Name)
			colIndices = append(colIndices, idx)
		}
	}

	// Fetch and filter rows by RID
	var rows [][]catalog.Value
	for _, rid := range rids {
		// Fetch the row using MVCC visibility
		row, err := e.mtm.FetchWithMVCC(stmt.TableName, rid)
		if err != nil {
			continue // Row may have been deleted
		}

		// Check MVCC visibility
		if !txn.IsVisible(row.Header, tx.Snapshot, e.mtm.TxnManager(), tx.ID) {
			continue
		}

		// Apply any remaining WHERE conditions
		// (the index only covers part of the WHERE clause potentially)
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values)
			if err != nil {
				return nil, false, err
			}
			if !match {
				continue
			}
		}

		// Project columns
		outRow := make([]catalog.Value, len(colIndices))
		for i, idx := range colIndices {
			outRow[i] = row.Values[idx]
		}
		rows = append(rows, outRow)
	}

	return &Result{
		Columns: outCols,
		Rows:    rows,
	}, true, nil
}

// executeAlter handles ALTER TABLE statements.
func (e *MVCCExecutor) executeAlter(stmt *AlterTableStmt) (*Result, error) {
	meta, err := e.mtm.GetTableMeta(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	switch stmt.Action {
	case "ADD COLUMN":
		// Check if column already exists
		for _, col := range meta.Schema.Columns {
			if strings.EqualFold(col.Name, stmt.ColumnDef.Name) {
				return nil, fmt.Errorf("column %q already exists in table %q", stmt.ColumnDef.Name, stmt.TableName)
			}
		}
		// Add the new column to schema
		newCol := catalog.Column{
			Name:       stmt.ColumnDef.Name,
			Type:       stmt.ColumnDef.Type,
			NotNull:    stmt.ColumnDef.NotNull,
			HasDefault: stmt.ColumnDef.HasDefault,
		}
		// Handle default value
		if stmt.ColumnDef.HasDefault && stmt.ColumnDef.Default != nil {
			if lit, ok := stmt.ColumnDef.Default.(*LiteralExpr); ok {
				val := lit.Value
				newCol.DefaultValue = &val
			}
		}
		meta.Schema.Columns = append(meta.Schema.Columns, newCol)
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.mtm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q added to table %q.", stmt.ColumnDef.Name, stmt.TableName)}, nil

	case "DROP COLUMN":
		// Find and remove the column
		idx := -1
		for i, col := range meta.Schema.Columns {
			if strings.EqualFold(col.Name, stmt.ColumnName) {
				idx = i
				break
			}
		}
		if idx == -1 {
			return nil, fmt.Errorf("column %q does not exist in table %q", stmt.ColumnName, stmt.TableName)
		}
		// Remove column from schema
		meta.Schema.Columns = append(meta.Schema.Columns[:idx], meta.Schema.Columns[idx+1:]...)
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.mtm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q dropped from table %q.", stmt.ColumnName, stmt.TableName)}, nil

	case "RENAME TO":
		// Rename the table
		if err := e.mtm.RenameTable(stmt.TableName, stmt.NewName); err != nil {
			return nil, fmt.Errorf("failed to rename table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Table %q renamed to %q.", stmt.TableName, stmt.NewName)}, nil

	case "RENAME COLUMN":
		// Find and rename the column
		found := false
		for i, col := range meta.Schema.Columns {
			if strings.EqualFold(col.Name, stmt.ColumnName) {
				meta.Schema.Columns[i].Name = stmt.NewName
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column %q does not exist in table %q", stmt.ColumnName, stmt.TableName)
		}
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.mtm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q renamed to %q in table %q.", stmt.ColumnName, stmt.NewName, stmt.TableName)}, nil

	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action: %s", stmt.Action)
	}
}

// executeTruncate handles TRUNCATE TABLE statements.
func (e *MVCCExecutor) executeTruncate(stmt *TruncateTableStmt) (*Result, error) {
	// Verify table exists
	_, err := e.mtm.GetTableMeta(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	// Truncate the table (delete all rows)
	count, err := e.mtm.TruncateTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate table: %w", err)
	}

	return &Result{
		Message:      fmt.Sprintf("Table %q truncated. %d row(s) deleted.", stmt.TableName, count),
		RowsAffected: count,
	}, nil
}

// executeShow handles SHOW statements.
func (e *MVCCExecutor) executeShow(stmt *ShowStmt) (*Result, error) {
	switch stmt.ShowType {
	case "TABLES":
		// List all tables
		tables := e.mtm.ListTables()
		result := &Result{
			Columns: []string{"table_name"},
			Rows:    make([][]catalog.Value, 0, len(tables)),
		}
		for _, t := range tables {
			result.Rows = append(result.Rows, []catalog.Value{catalog.NewText(t)})
		}
		return result, nil

	case "CREATE TABLE":
		// Show CREATE TABLE statement
		meta, err := e.mtm.GetTableMeta(stmt.TableName)
		if err != nil {
			return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
		}
		ddl := generateCreateTableDDL(meta)
		return &Result{
			Columns: []string{"Create Table"},
			Rows:    [][]catalog.Value{{catalog.NewText(ddl)}},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", stmt.ShowType)
	}
}

// executeExplain executes an EXPLAIN statement
func (e *MVCCExecutor) executeExplain(stmt *ExplainStmt, tx *txn.Transaction) (*Result, error) {
	selectStmt, ok := stmt.Statement.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("EXPLAIN only supports SELECT statements")
	}

	// Get table metadata to validate the table exists
	meta, err := e.mtm.GetTableMeta(selectStmt.TableName)
	if err != nil {
		return nil, err
	}

	// Create a planner and generate the plan
	planner := NewPlanner(e.indexMgr)
	plan := planner.Plan(selectStmt, meta)

	// Build the explanation
	var details []string

	// Basic plan type
	planExplain := plan.Explain()
	details = append(details, planExplain)

	if plan.Type == PlanIndexScan {
		details = append(details, "  Using index: "+plan.IndexName)
	}

	if plan.RemainingWhere != nil {
		details = append(details, "  Filter: <where clause>")
	}

	if selectStmt.OrderBy != nil {
		details = append(details, "  Sort: ORDER BY clause")
	}

	if selectStmt.Limit != nil {
		details = append(details, "  Limit: LIMIT clause")
	}

	// If ANALYZE is specified, also run the query and show row count
	if stmt.Analyze {
		result, err := e.executeSelect(selectStmt, tx)
		if err != nil {
			return nil, fmt.Errorf("EXPLAIN ANALYZE failed: %w", err)
		}
		details = append(details, fmt.Sprintf("  Actual rows: %d", len(result.Rows)))
	}

	// Build result rows
	var explanation [][]catalog.Value
	for _, detail := range details {
		explanation = append(explanation, []catalog.Value{catalog.NewText(detail)})
	}

	return &Result{
		Columns: []string{"QUERY PLAN"},
		Rows:    explanation,
	}, nil
}
