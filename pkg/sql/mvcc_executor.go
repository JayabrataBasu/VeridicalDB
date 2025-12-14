package sql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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
	case *CreateViewStmt:
		return e.executeCreateView(s)
	case *DropTableStmt:
		return e.executeDrop(s)
	case *DropViewStmt:
		return e.executeDropView(s)
	case *InsertStmt:
		return e.executeInsert(s, tx)
	case *SelectStmt:
		return e.executeSelect(s, tx)
	case *UnionStmt:
		return e.executeUnion(s, tx)
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
	case *MergeStmt:
		return e.executeMerge(s, tx)
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
		col := catalog.Column{
			ID:            i,
			Name:          def.Name,
			Type:          def.Type,
			NotNull:       def.NotNull,
			PrimaryKey:    def.PrimaryKey,
			HasDefault:    def.HasDefault,
			AutoIncrement: def.AutoIncrement,
		}

		// If there's a default value, evaluate it and store it
		if def.HasDefault && def.Default != nil {
			defaultVal, err := e.evalExpr(def.Default, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("invalid default value for column %s: %w", def.Name, err)
			}
			col.DefaultValue = &defaultVal
		}

		// If there's a CHECK constraint, serialize it to string for storage
		if def.Check != nil {
			col.CheckExpr = exprToString(def.Check)
		}

		cols[i] = col
	}

	var foreignKeys []catalog.ForeignKey

	// Process table-level FKs
	for _, fkDef := range stmt.ForeignKeys {
		fk := catalog.ForeignKey{
			Name:       fkDef.ConstraintName,
			Columns:    fkDef.Columns,
			RefTable:   fkDef.RefTable,
			RefColumns: fkDef.RefColumns,
		}
		foreignKeys = append(foreignKeys, fk)
	}

	// Process inline FKs
	for _, c := range stmt.Columns {
		if c.ReferencesTable != "" {
			refCols := []string{}
			if c.ReferencesColumn != "" {
				refCols = append(refCols, c.ReferencesColumn)
			} else {
				// Resolve PK of referenced table
				refTable, err := e.mtm.Catalog().GetTable(c.ReferencesTable)
				if err != nil {
					return nil, fmt.Errorf("referenced table %q not found", c.ReferencesTable)
				}
				var pkColName string
				for _, rc := range refTable.Columns {
					if rc.PrimaryKey {
						if pkColName != "" {
							return nil, fmt.Errorf("referenced table %q has composite primary key, must specify column", c.ReferencesTable)
						}
						pkColName = rc.Name
					}
				}
				if pkColName == "" {
					return nil, fmt.Errorf("referenced table %q has no primary key", c.ReferencesTable)
				}
				refCols = append(refCols, pkColName)
			}

			fk := catalog.ForeignKey{
				Name:       fmt.Sprintf("fk_%s_%s_%d", stmt.TableName, c.Name, time.Now().UnixNano()),
				Columns:    []string{c.Name},
				RefTable:   c.ReferencesTable,
				RefColumns: refCols,
			}
			foreignKeys = append(foreignKeys, fk)
		}
	}

	// Use storage type from statement (default is "ROW")
	storageType := strings.ToLower(stmt.StorageType)
	if storageType == "" {
		storageType = "row"
	}

	if err := e.mtm.CreateTableWithStorage(stmt.TableName, cols, foreignKeys, storageType); err != nil {
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

	totalInserted := 0

	// Process each row in ValuesList
	for rowIdx, rowValues := range stmt.ValuesList {
		// Build values array
		values := make([]catalog.Value, len(meta.Columns))

		// Initialize all values to NULL
		for i := range values {
			values[i] = catalog.Null(meta.Columns[i].Type)
		}

		// If column list specified, map values to columns
		if len(stmt.Columns) > 0 {
			if len(stmt.Columns) != len(rowValues) {
				return nil, fmt.Errorf("row %d: column count (%d) doesn't match value count (%d)",
					rowIdx+1, len(stmt.Columns), len(rowValues))
			}
			for i, colName := range stmt.Columns {
				col, idx := meta.Schema.ColumnByName(colName)
				if col == nil {
					return nil, fmt.Errorf("unknown column: %s", colName)
				}
				val, err := e.evalExpr(rowValues[i], meta.Schema, nil)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
				}
				val, err = coerceValueMVCC(val, col.Type)
				if err != nil {
					return nil, fmt.Errorf("row %d, column %s: %w", rowIdx+1, colName, err)
				}
				values[idx] = val
			}
		} else {
			// Positional values
			if len(rowValues) != len(meta.Columns) {
				return nil, fmt.Errorf("row %d: expected %d values, got %d", rowIdx+1, len(meta.Columns), len(rowValues))
			}
			for i, expr := range rowValues {
				val, err := e.evalExpr(expr, meta.Schema, nil)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
				}
				val, err = coerceValueMVCC(val, meta.Columns[i].Type)
				if err != nil {
					return nil, fmt.Errorf("row %d, column %s: %w", rowIdx+1, meta.Columns[i].Name, err)
				}
				values[i] = val
			}
		}

		// Validate CHECK constraints
		if err := e.validateCheckConstraints(meta.Schema, values); err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		// Insert with MVCC
		rid, err := e.mtm.Insert(stmt.TableName, values, tx)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		// Update indexes for the new row
		if err := e.updateIndexesOnInsert(stmt.TableName, rid, values, meta.Schema); err != nil {
			return nil, fmt.Errorf("row %d: index update failed: %w", rowIdx+1, err)
		}

		totalInserted++
	}

	if totalInserted == 1 {
		return &Result{Message: "1 row inserted.", RowsAffected: 1}, nil
	}
	return &Result{
		Message:      fmt.Sprintf("%d rows inserted.", totalInserted),
		RowsAffected: totalInserted,
	}, nil
}

func (e *MVCCExecutor) executeSelect(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("SELECT requires an active transaction")
	}

	// Handle JOINs separately
	if len(stmt.Joins) > 0 {
		return e.executeSelectWithJoins(stmt, tx)
	}

	// Check if query contains window functions
	hasWindowFunctions := false
	for _, col := range stmt.Columns {
		if col.Expression != nil {
			if _, ok := col.Expression.(*WindowFuncExpr); ok {
				hasWindowFunctions = true
				break
			}
		}
	}

	if hasWindowFunctions {
		return e.executeSelectWithWindowFunctions(stmt, tx)
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
	var colExpressions []Expression // For expressions like CASE

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for i, col := range meta.Columns {
			outCols = append(outCols, col.Name)
			colIndices = append(colIndices, i) // Use slice index, not col.ID
			colExpressions = append(colExpressions, nil)
		}
	} else {
		for _, sc := range stmt.Columns {
			// Handle expressions (like CASE)
			if sc.Expression != nil {
				alias := sc.Alias
				if alias == "" {
					alias = "expr" // Default alias for expressions
				}
				outCols = append(outCols, alias)
				colIndices = append(colIndices, -1) // -1 means this is an expression
				colExpressions = append(colExpressions, sc.Expression)
			} else {
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
				colExpressions = append(colExpressions, nil)
			}
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

		// Project columns (including expressions)
		outRow := make([]catalog.Value, len(colIndices))
		for i, idx := range colIndices {
			if idx >= 0 {
				// Regular column
				outRow[i] = row.Values[idx]
			} else {
				// Expression (like CASE)
				val, err := e.evalExpr(colExpressions[i], meta.Schema, row.Values)
				if err != nil {
					return false, fmt.Errorf("error evaluating expression: %w", err)
				}
				outRow[i] = val
			}
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

		// Validate CHECK constraints on the updated row
		if err := e.validateCheckConstraints(meta.Schema, newRow); err != nil {
			return false, err
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

	case *CaseExpr:
		return e.evalCaseExpr(ex, schema, row)

	case *CastExpr:
		return e.evalCastExpr(ex, schema, row)

	case *IsNullExpr:
		// IS NULL in value context returns boolean
		val, err := e.evalExpr(ex.Expr, schema, row)
		if err != nil {
			return catalog.Value{}, err
		}
		result := val.IsNull
		if ex.Not {
			return catalog.NewBool(!result), nil
		}
		return catalog.NewBool(result), nil

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

		// Check if this is a subquery IN
		if ex.Subquery != nil {
			// Subqueries in MVCC executor are not yet supported in evalCondition context
			// They would need transaction context to be passed through
			return false, fmt.Errorf("IN subquery not yet supported in MVCC executor")
		}

		// Value list IN
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

	case *IsNullExpr:
		// Evaluate IS NULL / IS NOT NULL
		val, err := e.evalExpr(ex.Expr, schema, row)
		if err != nil {
			return false, err
		}
		result := val.IsNull
		if ex.Not {
			return !result, nil // IS NOT NULL
		}
		return result, nil // IS NULL

	case *SubqueryExpr:
		// Subqueries in MVCC executor need transaction context
		return false, fmt.Errorf("subqueries not yet supported in MVCC executor")

	case *ExistsExpr:
		// EXISTS subqueries in MVCC executor need transaction context
		return false, fmt.Errorf("EXISTS subqueries not yet supported in MVCC executor")

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

// executeSelectWithJoins handles SELECT with JOIN clauses for MVCC executor.
func (e *MVCCExecutor) executeSelectWithJoins(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	if len(stmt.Joins) != 1 {
		return nil, fmt.Errorf("only single JOIN is currently supported")
	}

	join := stmt.Joins[0]
	if join.JoinType != "INNER" && join.JoinType != "LEFT" && join.JoinType != "RIGHT" && join.JoinType != "FULL" && join.JoinType != "CROSS" {
		return nil, fmt.Errorf("unsupported JOIN type: %s", join.JoinType)
	}

	cat := e.mtm.Catalog()
	leftMeta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Handle LATERAL joins with subquery
	if join.Lateral && join.Subquery != nil {
		return e.executeSelectWithLateralJoin(stmt, tx, leftMeta, join)
	}

	rightMeta, err := cat.GetTable(join.TableName)
	if err != nil {
		return nil, err
	}

	// Build combined schema
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(leftMeta.Schema.Columns)+len(rightMeta.Schema.Columns)),
	}
	colTableMap := make(map[string]int)

	for i, col := range leftMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colTableMap[stmt.TableName+"."+col.Name] = i
		colTableMap[col.Name] = i
	}

	leftLen := len(leftMeta.Schema.Columns)
	rightLen := len(rightMeta.Schema.Columns)
	for i, col := range rightMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colTableMap[join.TableName+"."+col.Name] = leftLen + i
		if _, exists := colTableMap[col.Name]; !exists {
			colTableMap[col.Name] = leftLen + i
		}
	}

	// Create NULL rows for outer joins
	nullRightRow := make([]catalog.Value, rightLen)
	for i, col := range rightMeta.Schema.Columns {
		nullRightRow[i] = catalog.Null(col.Type)
	}
	nullLeftRow := make([]catalog.Value, leftLen)
	for i, col := range leftMeta.Schema.Columns {
		nullLeftRow[i] = catalog.Null(col.Type)
	}

	// Collect rows from both tables
	var leftRows [][]catalog.Value
	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		leftRows = append(leftRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	var rightRows [][]catalog.Value
	err = e.mtm.Scan(join.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		rightRows = append(rightRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Perform join based on type
	var joinedRows [][]catalog.Value

	switch join.JoinType {
	case "INNER":
		for _, leftRow := range leftRows {
			for _, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					joinedRows = append(joinedRows, combinedRow)
				}
			}
		}

	case "LEFT":
		for _, leftRow := range leftRows {
			foundMatch := false
			for _, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					foundMatch = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
			if !foundMatch {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], nullRightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

	case "RIGHT":
		rightRowMatches := make(map[int]bool)
		for _, leftRow := range leftRows {
			for i, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					rightRowMatches[i] = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
		}
		// Add unmatched right rows
		for i, rightRow := range rightRows {
			if !rightRowMatches[i] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, nullLeftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

	case "FULL":
		// Track which left and right rows have been matched
		leftRowMatches := make(map[int]bool)
		rightRowMatches := make(map[int]bool)

		// Match left with right
		for i, leftRow := range leftRows {
			for j, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					leftRowMatches[i] = true
					rightRowMatches[j] = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
		}

		// Add unmatched left rows with NULLs for right side
		for i, leftRow := range leftRows {
			if !leftRowMatches[i] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], nullRightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

		// Add unmatched right rows with NULLs for left side
		for j, rightRow := range rightRows {
			if !rightRowMatches[j] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, nullLeftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

	case "CROSS":
		// CROSS JOIN: Cartesian product of both tables (no condition)
		for _, leftRow := range leftRows {
			for _, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}
	}

	// Apply WHERE filter
	var filteredRows [][]catalog.Value
	for _, row := range joinedRows {
		if stmt.Where != nil {
			match, err := e.evalJoinConditionMVCC(stmt.Where, combinedSchema, row, colTableMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// Determine output columns
	var outputCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for i, c := range combinedSchema.Columns {
			outputCols = append(outputCols, c.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			idx, ok := colTableMap[sc.Name]
			if !ok {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			if sc.Alias != "" {
				outputCols = append(outputCols, sc.Alias)
			} else {
				outputCols = append(outputCols, sc.Name)
			}
			colIndices = append(colIndices, idx)
		}
	}

	// Project columns
	resultRows := make([][]catalog.Value, len(filteredRows))
	for i, row := range filteredRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			projectedRow[j] = row[idx]
		}
		resultRows[i] = projectedRow
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			idx, ok := colTableMap[ob.Column]
			if !ok {
				return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
			}
			orderByIndices[i] = idx
		}
		sortRowsMVCC(filteredRows, stmt.OrderBy, orderByIndices)
		for i, row := range filteredRows {
			projectedRow := make([]catalog.Value, len(colIndices))
			for j, idx := range colIndices {
				projectedRow[j] = row[idx]
			}
			resultRows[i] = projectedRow
		}
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		offset := int(*stmt.Offset)
		if offset >= len(resultRows) {
			resultRows = nil
		} else {
			resultRows = resultRows[offset:]
		}
	}
	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if limit < len(resultRows) {
			resultRows = resultRows[:limit]
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = deduplicateRows(resultRows)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// evalJoinConditionMVCC evaluates a join condition for MVCC executor.
func (e *MVCCExecutor) evalJoinConditionMVCC(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalJoinConditionMVCC(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalJoinConditionMVCC(ex.Right, schema, row, colMap)

		case TOKEN_OR:
			left, err := e.evalJoinConditionMVCC(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalJoinConditionMVCC(ex.Right, schema, row, colMap)

		default:
			// Comparison operators
			leftVal, err := e.evalJoinExprMVCC(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			rightVal, err := e.evalJoinExprMVCC(ex.Right, schema, row, colMap)
			if err != nil {
				return false, err
			}
			return compareValuesMVCC(leftVal, rightVal, ex.Op)
		}
	case *UnaryExpr:
		if ex.Op == TOKEN_NOT {
			result, err := e.evalJoinConditionMVCC(ex.Expr, schema, row, colMap)
			if err != nil {
				return false, err
			}
			return !result, nil
		}
	}
	return false, fmt.Errorf("unsupported join condition type: %T", expr)
}

// evalJoinExprMVCC evaluates an expression in a join context for MVCC executor.
func (e *MVCCExecutor) evalJoinExprMVCC(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *ColumnRef:
		// ColumnRef.Name may be "table.column" or just "column"
		idx, ok := colMap[ex.Name]
		if !ok {
			return catalog.Value{}, fmt.Errorf("unknown column in join: %s", ex.Name)
		}
		return row[idx], nil
	case *LiteralExpr:
		return ex.Value, nil
	}
	return catalog.Value{}, fmt.Errorf("unsupported expression type in join: %T", expr)
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

// evalCaseExpr evaluates a CASE WHEN expression.
func (e *MVCCExecutor) evalCaseExpr(caseExpr *CaseExpr, schema *catalog.Schema, row []catalog.Value) (catalog.Value, error) {
	// Simple CASE: CASE operand WHEN val1 THEN res1 ...
	// Searched CASE: CASE WHEN cond1 THEN res1 ...

	if caseExpr.Operand != nil {
		// Simple CASE - compare operand with each WHEN value
		operandVal, err := e.evalExpr(caseExpr.Operand, schema, row)
		if err != nil {
			return catalog.Value{}, err
		}

		for _, when := range caseExpr.Whens {
			whenVal, err := e.evalExpr(when.Condition, schema, row)
			if err != nil {
				return catalog.Value{}, err
			}

			// Compare operand with WHEN value
			equal, err := compareValuesMVCC(operandVal, whenVal, TOKEN_EQ)
			if err != nil {
				return catalog.Value{}, err
			}

			if equal {
				return e.evalExpr(when.Result, schema, row)
			}
		}
	} else {
		// Searched CASE - evaluate each WHEN condition as boolean
		for _, when := range caseExpr.Whens {
			condResult, err := e.evalCondition(when.Condition, schema, row)
			if err != nil {
				return catalog.Value{}, err
			}

			if condResult {
				return e.evalExpr(when.Result, schema, row)
			}
		}
	}

	// No WHEN matched, return ELSE or NULL
	if caseExpr.Else != nil {
		return e.evalExpr(caseExpr.Else, schema, row)
	}

	// No ELSE clause - return NULL
	return catalog.Null(catalog.TypeUnknown), nil
}

// validateCheckConstraints validates all CHECK constraints for a row.
func (e *MVCCExecutor) validateCheckConstraints(schema *catalog.Schema, values []catalog.Value) error {
	for i, col := range schema.Columns {
		if col.CheckExpr == "" {
			continue
		}

		// Parse the CHECK expression directly
		parser := NewParser(col.CheckExpr)
		expr, err := parser.parseExpression()
		if err != nil {
			return fmt.Errorf("invalid CHECK expression for column %s: %w", col.Name, err)
		}

		// Evaluate the CHECK expression with the row values
		result, err := e.evalCondition(expr, schema, values)
		if err != nil {
			return fmt.Errorf("error evaluating CHECK constraint for column %s: %w", col.Name, err)
		}

		if !result {
			return fmt.Errorf("CHECK constraint violated for column %s (value: %v)", col.Name, values[i])
		}
	}

	return nil
}

// evalCastExpr evaluates a CAST expression for MVCC executor.
func (e *MVCCExecutor) evalCastExpr(cast *CastExpr, schema *catalog.Schema, row []catalog.Value) (catalog.Value, error) {
	val, err := e.evalExpr(cast.Expr, schema, row)
	if err != nil {
		return catalog.Value{}, err
	}

	if val.IsNull {
		return catalog.Null(cast.TargetType), nil
	}

	// Handle CAST type conversions
	switch cast.TargetType {
	case catalog.TypeInt32:
		switch val.Type {
		case catalog.TypeInt32:
			return val, nil
		case catalog.TypeInt64:
			if val.Int64 >= -2147483648 && val.Int64 <= 2147483647 {
				return catalog.NewInt32(int32(val.Int64)), nil
			}
			return catalog.Value{}, fmt.Errorf("value %d out of range for INT", val.Int64)
		case catalog.TypeText:
			i, err := strconv.ParseInt(val.Text, 10, 32)
			if err != nil {
				return catalog.Value{}, fmt.Errorf("cannot cast '%s' to INT", val.Text)
			}
			return catalog.NewInt32(int32(i)), nil
		case catalog.TypeBool:
			if val.Bool {
				return catalog.NewInt32(1), nil
			}
			return catalog.NewInt32(0), nil
		}

	case catalog.TypeInt64:
		switch val.Type {
		case catalog.TypeInt32:
			return catalog.NewInt64(int64(val.Int32)), nil
		case catalog.TypeInt64:
			return val, nil
		case catalog.TypeText:
			i, err := strconv.ParseInt(val.Text, 10, 64)
			if err != nil {
				return catalog.Value{}, fmt.Errorf("cannot cast '%s' to BIGINT", val.Text)
			}
			return catalog.NewInt64(i), nil
		case catalog.TypeBool:
			if val.Bool {
				return catalog.NewInt64(1), nil
			}
			return catalog.NewInt64(0), nil
		}

	case catalog.TypeText:
		switch val.Type {
		case catalog.TypeInt32:
			return catalog.NewText(strconv.FormatInt(int64(val.Int32), 10)), nil
		case catalog.TypeInt64:
			return catalog.NewText(strconv.FormatInt(val.Int64, 10)), nil
		case catalog.TypeText:
			return val, nil
		case catalog.TypeBool:
			if val.Bool {
				return catalog.NewText("true"), nil
			}
			return catalog.NewText("false"), nil
		case catalog.TypeTimestamp:
			return catalog.NewText(val.Timestamp.Format(time.RFC3339)), nil
		}

	case catalog.TypeBool:
		switch val.Type {
		case catalog.TypeInt32:
			return catalog.NewBool(val.Int32 != 0), nil
		case catalog.TypeInt64:
			return catalog.NewBool(val.Int64 != 0), nil
		case catalog.TypeText:
			lower := strings.ToLower(val.Text)
			if lower == "true" || lower == "1" || lower == "yes" {
				return catalog.NewBool(true), nil
			}
			if lower == "false" || lower == "0" || lower == "no" {
				return catalog.NewBool(false), nil
			}
			return catalog.Value{}, fmt.Errorf("cannot cast '%s' to BOOL", val.Text)
		case catalog.TypeBool:
			return val, nil
		}

	case catalog.TypeTimestamp:
		switch val.Type {
		case catalog.TypeText:
			formats := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
			for _, format := range formats {
				if t, err := time.Parse(format, val.Text); err == nil {
					return catalog.NewTimestamp(t), nil
				}
			}
			return catalog.Value{}, fmt.Errorf("cannot cast '%s' to TIMESTAMP", val.Text)
		case catalog.TypeTimestamp:
			return val, nil
		}
	}

	return catalog.Value{}, fmt.Errorf("cannot cast %v to %v", val.Type, cast.TargetType)
}

// executeCreateView creates a new view (MVCC version).
func (e *MVCCExecutor) executeCreateView(stmt *CreateViewStmt) (*Result, error) {
	return nil, fmt.Errorf("CREATE VIEW is not yet fully implemented (view definition parsed successfully)")
}

// executeDropView drops a view (MVCC version).
func (e *MVCCExecutor) executeDropView(stmt *DropViewStmt) (*Result, error) {
	if stmt.IfExists {
		return &Result{Message: fmt.Sprintf("View '%s' does not exist (IF EXISTS specified).", stmt.ViewName)}, nil
	}
	return nil, fmt.Errorf("DROP VIEW is not yet fully implemented")
}

// executeUnion executes a UNION/INTERSECT/EXCEPT operation (MVCC version).
func (e *MVCCExecutor) executeUnion(stmt *UnionStmt, tx *txn.Transaction) (*Result, error) {
	// Execute left SELECT
	leftResult, err := e.executeSelect(stmt.Left, tx)
	if err != nil {
		return nil, fmt.Errorf("error executing left side of %s: %w", stmt.Op, err)
	}

	// Execute right SELECT
	rightResult, err := e.executeSelect(stmt.Right, tx)
	if err != nil {
		return nil, fmt.Errorf("error executing right side of %s: %w", stmt.Op, err)
	}

	// Verify column counts match
	if len(leftResult.Columns) != len(rightResult.Columns) {
		return nil, fmt.Errorf("%s requires same number of columns on both sides (got %d and %d)",
			stmt.Op, len(leftResult.Columns), len(rightResult.Columns))
	}

	// Use left side's column names
	result := &Result{Columns: leftResult.Columns}

	switch stmt.Op {
	case "UNION":
		if stmt.All {
			// UNION ALL - just concatenate
			result.Rows = append(leftResult.Rows, rightResult.Rows...)
		} else {
			// UNION - concatenate and remove duplicates
			seen := make(map[string]bool)
			for _, row := range leftResult.Rows {
				key := rowKey(row)
				if !seen[key] {
					seen[key] = true
					result.Rows = append(result.Rows, row)
				}
			}
			for _, row := range rightResult.Rows {
				key := rowKey(row)
				if !seen[key] {
					seen[key] = true
					result.Rows = append(result.Rows, row)
				}
			}
		}

	case "INTERSECT":
		// Find rows that appear in both
		rightSet := make(map[string]bool)
		for _, row := range rightResult.Rows {
			rightSet[rowKey(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftResult.Rows {
			key := rowKey(row)
			if rightSet[key] && (stmt.All || !seen[key]) {
				seen[key] = true
				result.Rows = append(result.Rows, row)
			}
		}

	case "EXCEPT":
		// Find rows in left that don't appear in right
		rightSet := make(map[string]bool)
		for _, row := range rightResult.Rows {
			rightSet[rowKey(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftResult.Rows {
			key := rowKey(row)
			if !rightSet[key] && (stmt.All || !seen[key]) {
				seen[key] = true
				result.Rows = append(result.Rows, row)
			}
		}
	}

	// Apply ORDER BY if specified
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			found := false
			for j, colName := range result.Columns {
				if strings.EqualFold(colName, ob.Column) {
					orderByIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("ORDER BY column %s not found in result", ob.Column)
			}
		}
		sortRowsMVCC(result.Rows, stmt.OrderBy, orderByIndices)
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		if int(*stmt.Offset) >= len(result.Rows) {
			result.Rows = nil
		} else {
			result.Rows = result.Rows[*stmt.Offset:]
		}
	}
	if stmt.Limit != nil {
		if int(*stmt.Limit) < len(result.Rows) {
			result.Rows = result.Rows[:*stmt.Limit]
		}
	}

	return result, nil
}

// sortRowsMVCC sorts rows for MVCC executor based on ORDER BY clauses.
func sortRowsMVCC(rows [][]catalog.Value, orderBy []OrderByClause, orderByIndices []int) {
	if len(rows) <= 1 || len(orderBy) == 0 {
		return
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for k, ob := range orderBy {
			idx := orderByIndices[k]
			left := rows[i][idx]
			right := rows[j][idx]

			cmp := compareValuesForSortMVCC(left, right)
			if cmp == 0 {
				continue
			}

			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

// compareValuesForSortMVCC compares two values for sorting.
func compareValuesForSortMVCC(left, right catalog.Value) int {
	if left.IsNull && right.IsNull {
		return 0
	}
	if left.IsNull {
		return 1
	}
	if right.IsNull {
		return -1
	}

	switch left.Type {
	case catalog.TypeInt32:
		if right.Type == catalog.TypeInt64 {
			if int64(left.Int32) < right.Int64 {
				return -1
			} else if int64(left.Int32) > right.Int64 {
				return 1
			}
			return 0
		}
		if left.Int32 < right.Int32 {
			return -1
		} else if left.Int32 > right.Int32 {
			return 1
		}
		return 0

	case catalog.TypeInt64:
		if right.Type == catalog.TypeInt32 {
			if left.Int64 < int64(right.Int32) {
				return -1
			} else if left.Int64 > int64(right.Int32) {
				return 1
			}
			return 0
		}
		if left.Int64 < right.Int64 {
			return -1
		} else if left.Int64 > right.Int64 {
			return 1
		}
		return 0

	case catalog.TypeText:
		return strings.Compare(left.Text, right.Text)

	case catalog.TypeBool:
		if left.Bool == right.Bool {
			return 0
		}
		if !left.Bool {
			return -1
		}
		return 1

	case catalog.TypeTimestamp:
		if left.Timestamp.Before(right.Timestamp) {
			return -1
		} else if left.Timestamp.After(right.Timestamp) {
			return 1
		}
		return 0
	}
	return 0
}

// executeSelectWithWindowFunctions handles SELECT with window functions using MVCC.
func (e *MVCCExecutor) executeSelectWithWindowFunctions(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// First, scan all rows that match WHERE clause
	var allRows [][]catalog.Value

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, evalErr := e.evalCondition(stmt.Where, meta.Schema, row.Values)
			if evalErr != nil {
				return false, evalErr
			}
			if !match {
				return true, nil // continue scanning
			}
		}

		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		allRows = append(allRows, rowCopy)
		return true, nil // continue
	})
	if err != nil {
		return nil, err
	}

	// Determine output columns
	var outputCols []string
	for _, col := range stmt.Columns {
		if col.Star {
			for _, c := range meta.Schema.Columns {
				outputCols = append(outputCols, c.Name)
			}
		} else if col.Alias != "" {
			outputCols = append(outputCols, col.Alias)
		} else if col.Name != "" {
			outputCols = append(outputCols, col.Name)
		} else if col.Expression != nil {
			if wf, ok := col.Expression.(*WindowFuncExpr); ok {
				outputCols = append(outputCols, wf.Function)
			} else {
				outputCols = append(outputCols, "expr")
			}
		}
	}

	// Build output rows with window function results
	outputRows := make([][]catalog.Value, len(allRows))
	for i := range outputRows {
		outputRows[i] = make([]catalog.Value, len(stmt.Columns))
	}

	// Process each column
	for colIdx, col := range stmt.Columns {
		if col.Star {
			continue // handled separately
		}

		if wf, ok := col.Expression.(*WindowFuncExpr); ok {
			// This is a window function - compute values for all rows
			windowValues, err := e.computeWindowFunction(wf, allRows, meta.Schema)
			if err != nil {
				return nil, err
			}
			for rowIdx, val := range windowValues {
				outputRows[rowIdx][colIdx] = val
			}
		} else if col.Name != "" {
			// Regular column
			_, idx := meta.Schema.ColumnByName(col.Name)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column: %s", col.Name)
			}
			for rowIdx, row := range allRows {
				outputRows[rowIdx][colIdx] = row[idx]
			}
		} else if col.Expression != nil {
			// Other expression
			for rowIdx, row := range allRows {
				val, err := e.evalExpr(col.Expression, meta.Schema, row)
				if err != nil {
					return nil, err
				}
				outputRows[rowIdx][colIdx] = val
			}
		}
	}

	// Handle SELECT * expansion
	starOffset := 0
	for colIdx, col := range stmt.Columns {
		if col.Star {
			for rowIdx, row := range allRows {
				for i := range meta.Schema.Columns {
					if colIdx+starOffset+i < len(outputRows[rowIdx]) {
						outputRows[rowIdx][colIdx+starOffset+i] = row[i]
					}
				}
			}
			starOffset += len(meta.Schema.Columns) - 1
		}
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			_, idx := meta.Schema.ColumnByName(ob.Column)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
			}
			orderByIndices[i] = idx
		}

		type indexedRow struct {
			origIdx   int
			outputRow []catalog.Value
			sortRow   []catalog.Value
		}
		indexed := make([]indexedRow, len(allRows))
		for i := range allRows {
			indexed[i] = indexedRow{origIdx: i, outputRow: outputRows[i], sortRow: allRows[i]}
		}

		sort.SliceStable(indexed, func(i, j int) bool {
			for k, ob := range stmt.OrderBy {
				idx := orderByIndices[k]
				cmp := compareValuesForSortMVCC(indexed[i].sortRow[idx], indexed[j].sortRow[idx])
				if cmp != 0 {
					if ob.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})

		for i, ir := range indexed {
			outputRows[i] = ir.outputRow
		}
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		if int(*stmt.Offset) >= len(outputRows) {
			outputRows = nil
		} else {
			outputRows = outputRows[*stmt.Offset:]
		}
	}
	if stmt.Limit != nil && *stmt.Limit >= 0 {
		if int(*stmt.Limit) < len(outputRows) {
			outputRows = outputRows[:*stmt.Limit]
		}
	}

	return &Result{
		Columns: outputCols,
		Rows:    outputRows,
	}, nil
}

// computeWindowFunction computes window function values for all rows (MVCC version).
func (e *MVCCExecutor) computeWindowFunction(wf *WindowFuncExpr, rows [][]catalog.Value, schema *catalog.Schema) ([]catalog.Value, error) {
	result := make([]catalog.Value, len(rows))

	// Get partition column indices
	partitionIndices := make([]int, len(wf.Over.PartitionBy))
	for i, colName := range wf.Over.PartitionBy {
		_, idx := schema.ColumnByName(colName)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in PARTITION BY: %s", colName)
		}
		partitionIndices[i] = idx
	}

	// Get order by column indices
	orderByIndices := make([]int, len(wf.Over.OrderBy))
	for i, ob := range wf.Over.OrderBy {
		_, idx := schema.ColumnByName(ob.Column)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in window ORDER BY: %s", ob.Column)
		}
		orderByIndices[i] = idx
	}

	// Group rows by partition
	partitions := make(map[string][]int)
	partitionOrder := []string{}

	for i, row := range rows {
		key := mvccMakePartitionKey(row, partitionIndices)
		if _, exists := partitions[key]; !exists {
			partitionOrder = append(partitionOrder, key)
		}
		partitions[key] = append(partitions[key], i)
	}

	// Process each partition
	for _, partKey := range partitionOrder {
		rowIndices := partitions[partKey]

		// Sort within partition
		if len(wf.Over.OrderBy) > 0 {
			sort.SliceStable(rowIndices, func(i, j int) bool {
				for k, ob := range wf.Over.OrderBy {
					idx := orderByIndices[k]
					cmp := compareValuesForSortMVCC(rows[rowIndices[i]][idx], rows[rowIndices[j]][idx])
					if cmp != 0 {
						if ob.Desc {
							return cmp > 0
						}
						return cmp < 0
					}
				}
				return false
			})
		}

		// Compute function
		switch strings.ToUpper(wf.Function) {
		case "ROW_NUMBER":
			for rank, rowIdx := range rowIndices {
				result[rowIdx] = catalog.NewInt64(int64(rank + 1))
			}

		case "RANK":
			var prevVals []catalog.Value
			rank := 1
			for i, rowIdx := range rowIndices {
				currVals := mvccGetOrderByValues(rows[rowIdx], orderByIndices)
				if i == 0 || !mvccWindowValuesEqual(currVals, prevVals) {
					rank = i + 1
				}
				result[rowIdx] = catalog.NewInt64(int64(rank))
				prevVals = currVals
			}

		case "DENSE_RANK":
			var prevVals []catalog.Value
			rank := 0
			for i, rowIdx := range rowIndices {
				currVals := mvccGetOrderByValues(rows[rowIdx], orderByIndices)
				if i == 0 || !mvccWindowValuesEqual(currVals, prevVals) {
					rank++
				}
				result[rowIdx] = catalog.NewInt64(int64(rank))
				prevVals = currVals
			}

		case "SUM":
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("SUM requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("SUM argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			var runningSum int64
			for _, rowIdx := range rowIndices {
				val := rows[rowIdx][colIdx]
				if !val.IsNull {
					if val.Type == catalog.TypeInt32 {
						runningSum += int64(val.Int32)
					} else if val.Type == catalog.TypeInt64 {
						runningSum += val.Int64
					}
				}
				result[rowIdx] = catalog.NewInt64(runningSum)
			}

		case "COUNT":
			for i, rowIdx := range rowIndices {
				result[rowIdx] = catalog.NewInt64(int64(i + 1))
			}

		case "AVG":
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("AVG requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("AVG argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			var runningSum int64
			count := 0
			for _, rowIdx := range rowIndices {
				val := rows[rowIdx][colIdx]
				if !val.IsNull {
					if val.Type == catalog.TypeInt32 {
						runningSum += int64(val.Int32)
					} else if val.Type == catalog.TypeInt64 {
						runningSum += val.Int64
					}
					count++
				}
				if count > 0 {
					result[rowIdx] = catalog.NewInt64(runningSum / int64(count))
				} else {
					result[rowIdx] = catalog.Null(catalog.TypeInt64)
				}
			}

		default:
			return nil, fmt.Errorf("unsupported window function: %s", wf.Function)
		}
	}

	return result, nil
}

// Helper functions for MVCC window function processing
func mvccMakePartitionKey(row []catalog.Value, indices []int) string {
	if len(indices) == 0 {
		return ""
	}
	var parts []string
	for _, idx := range indices {
		parts = append(parts, mvccValueToString(row[idx]))
	}
	return strings.Join(parts, "|")
}

func mvccGetOrderByValues(row []catalog.Value, indices []int) []catalog.Value {
	vals := make([]catalog.Value, len(indices))
	for i, idx := range indices {
		vals[i] = row[idx]
	}
	return vals
}

func mvccWindowValuesEqual(a, b []catalog.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if compareValuesForSortMVCC(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

func mvccValueToString(v catalog.Value) string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case catalog.TypeInt32:
		return fmt.Sprintf("%d", v.Int32)
	case catalog.TypeInt64:
		return fmt.Sprintf("%d", v.Int64)
	case catalog.TypeText:
		return v.Text
	case catalog.TypeBool:
		return fmt.Sprintf("%t", v.Bool)
	case catalog.TypeTimestamp:
		return v.Timestamp.String()
	default:
		return "?"
	}
}

// executeSelectWithLateralJoin handles SELECT with a LATERAL join for MVCC.
func (e *MVCCExecutor) executeSelectWithLateralJoin(stmt *SelectStmt, tx *txn.Transaction, leftMeta *catalog.TableMeta, join JoinClause) (*Result, error) {
	var joinedRows [][]catalog.Value
	var combinedSchema *catalog.Schema
	var colTableMap map[string]int
	leftLen := len(leftMeta.Schema.Columns)
	var rightLen int
	schemaInitialized := false

	// Collect all left rows first
	var leftRows [][]catalog.Value
	err := e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		leftRows = append(leftRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Process each left row with the lateral subquery
	for _, leftRow := range leftRows {
		// Create a correlated subquery with substituted values
		correlatedQuery := e.substituteCorrelatedColumnsMVCC(join.Subquery, leftMeta, leftRow, stmt.TableName, stmt.TableAlias)

		// Execute the lateral subquery
		subResult, err := e.Execute(correlatedQuery, tx)
		if err != nil {
			return nil, fmt.Errorf("error executing LATERAL subquery: %w", err)
		}

		// Initialize schema on first successful subquery execution
		if !schemaInitialized && subResult != nil {
			rightLen = len(subResult.Columns)

			combinedSchema = &catalog.Schema{
				Columns: make([]catalog.Column, 0, leftLen+rightLen),
			}
			colTableMap = make(map[string]int)

			// Add left table columns
			for i, col := range leftMeta.Schema.Columns {
				combinedSchema.Columns = append(combinedSchema.Columns, col)
				colTableMap[stmt.TableName+"."+col.Name] = i
				if stmt.TableAlias != "" {
					colTableMap[stmt.TableAlias+"."+col.Name] = i
				}
				colTableMap[col.Name] = i
			}

			// Add right (subquery) columns
			alias := join.TableAlias
			for i, colName := range subResult.Columns {
				newCol := catalog.Column{Name: colName, Type: catalog.TypeText}
				combinedSchema.Columns = append(combinedSchema.Columns, newCol)
				if alias != "" {
					colTableMap[alias+"."+colName] = leftLen + i
				}
				if _, exists := colTableMap[colName]; !exists {
					colTableMap[colName] = leftLen + i
				}
			}

			schemaInitialized = true
		}

		if subResult == nil || len(subResult.Rows) == 0 {
			if join.JoinType == "LEFT" || join.JoinType == "CROSS" {
				if schemaInitialized {
					nullRightRow := make([]catalog.Value, rightLen)
					for i := 0; i < rightLen; i++ {
						nullRightRow[i] = catalog.Null(catalog.TypeText)
					}
					combinedRow := make([]catalog.Value, leftLen+rightLen)
					copy(combinedRow, leftRow)
					copy(combinedRow[leftLen:], nullRightRow)
					joinedRows = append(joinedRows, combinedRow)
				}
			}
			continue
		}

		for _, rightRow := range subResult.Rows {
			combinedRow := make([]catalog.Value, leftLen+rightLen)
			copy(combinedRow, leftRow)
			copy(combinedRow[leftLen:], rightRow)

			if join.Condition != nil {
				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			joinedRows = append(joinedRows, combinedRow)
		}
	}

	if combinedSchema == nil {
		combinedSchema = &catalog.Schema{
			Columns: make([]catalog.Column, 0, leftLen),
		}
		colTableMap = make(map[string]int)
		for i, col := range leftMeta.Schema.Columns {
			combinedSchema.Columns = append(combinedSchema.Columns, col)
			colTableMap[col.Name] = i
		}
	}

	// Apply WHERE filter
	var filteredRows [][]catalog.Value
	for _, row := range joinedRows {
		if stmt.Where != nil {
			match, err := e.evalJoinConditionMVCC(stmt.Where, combinedSchema, row, colTableMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// Determine output columns
	var outputCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for i, c := range combinedSchema.Columns {
			outputCols = append(outputCols, c.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			idx, ok := colTableMap[sc.Name]
			if !ok {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			if sc.Alias != "" {
				outputCols = append(outputCols, sc.Alias)
			} else {
				outputCols = append(outputCols, sc.Name)
			}
			colIndices = append(colIndices, idx)
		}
	}

	// Project columns
	resultRows := make([][]catalog.Value, len(filteredRows))
	for i, row := range filteredRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			projectedRow[j] = row[idx]
		}
		resultRows[i] = projectedRow
	}

	// Apply ORDER BY, LIMIT, OFFSET, DISTINCT as in regular executor
	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if limit < len(resultRows) {
			resultRows = resultRows[:limit]
		}
	}

	if stmt.Distinct {
		resultRows = deduplicateRows(resultRows)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// substituteCorrelatedColumnsMVCC replaces column references from the left table with actual values.
func (e *MVCCExecutor) substituteCorrelatedColumnsMVCC(subquery *SelectStmt, leftMeta *catalog.TableMeta, leftRow []catalog.Value, leftTableName string, leftTableAlias string) *SelectStmt {
	newQuery := &SelectStmt{
		Distinct:   subquery.Distinct,
		DistinctOn: append([]string{}, subquery.DistinctOn...),
		TableName:  subquery.TableName,
		TableAlias: subquery.TableAlias,
		GroupBy:    append([]string{}, subquery.GroupBy...),
		Limit:      subquery.Limit,
		Offset:     subquery.Offset,
	}

	newQuery.Columns = make([]SelectColumn, len(subquery.Columns))
	copy(newQuery.Columns, subquery.Columns)

	newQuery.OrderBy = make([]OrderByClause, len(subquery.OrderBy))
	copy(newQuery.OrderBy, subquery.OrderBy)

	if subquery.Where != nil {
		newQuery.Where = e.substituteExpressionValuesMVCC(subquery.Where, leftMeta, leftRow, leftTableName, leftTableAlias)
	}

	if subquery.Having != nil {
		newQuery.Having = e.substituteExpressionValuesMVCC(subquery.Having, leftMeta, leftRow, leftTableName, leftTableAlias)
	}

	return newQuery
}

// substituteExpressionValuesMVCC substitutes column references from the left table with actual values.
func (e *MVCCExecutor) substituteExpressionValuesMVCC(expr Expression, leftMeta *catalog.TableMeta, leftRow []catalog.Value, leftTableName string, leftTableAlias string) Expression {
	switch ex := expr.(type) {
	case *ColumnRef:
		colName := ex.Name
		parts := splitQualifiedName(colName)
		var lookupName string
		if len(parts) == 2 {
			tablePart := parts[0]
			columnPart := parts[1]
			if tablePart == leftTableName || tablePart == leftTableAlias {
				lookupName = columnPart
			}
		} else {
			lookupName = colName
		}

		for i, col := range leftMeta.Schema.Columns {
			if col.Name == lookupName {
				return &LiteralExpr{Value: leftRow[i]}
			}
		}
		return ex

	case *BinaryExpr:
		return &BinaryExpr{
			Left:  e.substituteExpressionValuesMVCC(ex.Left, leftMeta, leftRow, leftTableName, leftTableAlias),
			Op:    ex.Op,
			Right: e.substituteExpressionValuesMVCC(ex.Right, leftMeta, leftRow, leftTableName, leftTableAlias),
		}

	case *UnaryExpr:
		return &UnaryExpr{
			Op:   ex.Op,
			Expr: e.substituteExpressionValuesMVCC(ex.Expr, leftMeta, leftRow, leftTableName, leftTableAlias),
		}

	case *InExpr:
		newValues := make([]Expression, len(ex.Values))
		for i, v := range ex.Values {
			newValues[i] = e.substituteExpressionValuesMVCC(v, leftMeta, leftRow, leftTableName, leftTableAlias)
		}
		return &InExpr{
			Left:   e.substituteExpressionValuesMVCC(ex.Left, leftMeta, leftRow, leftTableName, leftTableAlias),
			Values: newValues,
			Not:    ex.Not,
		}

	default:
		return expr
	}
}

// executeMerge handles MERGE statements for MVCC executor.
func (e *MVCCExecutor) executeMerge(stmt *MergeStmt, tx *txn.Transaction) (*Result, error) {
	cat := e.mtm.Catalog()

	targetMeta, err := cat.GetTable(stmt.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("target table %q does not exist: %w", stmt.TargetTable, err)
	}

	// Get source data
	var sourceRows [][]catalog.Value
	var sourceCols []string
	var sourceSchema *catalog.Schema

	if stmt.SourceQuery != nil {
		result, err := e.Execute(stmt.SourceQuery, tx)
		if err != nil {
			return nil, fmt.Errorf("error executing source query: %w", err)
		}
		sourceRows = result.Rows
		sourceCols = result.Columns
		sourceSchema = &catalog.Schema{
			Columns: make([]catalog.Column, len(sourceCols)),
		}
		for i, col := range sourceCols {
			sourceSchema.Columns[i] = catalog.Column{Name: col, Type: catalog.TypeText}
		}
	} else {
		sourceMeta, err := cat.GetTable(stmt.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("source table %q does not exist: %w", stmt.SourceTable, err)
		}
		sourceSchema = sourceMeta.Schema
		for _, col := range sourceSchema.Columns {
			sourceCols = append(sourceCols, col.Name)
		}
		err = e.mtm.Scan(stmt.SourceTable, tx, func(row *catalog.MVCCRow) (bool, error) {
			rowCopy := make([]catalog.Value, len(row.Values))
			copy(rowCopy, row.Values)
			sourceRows = append(sourceRows, rowCopy)
			return true, nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Build combined schema
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(targetMeta.Schema.Columns)+len(sourceSchema.Columns)),
	}
	colMap := make(map[string]int)

	targetAlias := stmt.TargetAlias
	if targetAlias == "" {
		targetAlias = stmt.TargetTable
	}
	for i, col := range targetMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colMap[targetAlias+"."+col.Name] = i
		colMap[col.Name] = i
	}

	targetLen := len(targetMeta.Schema.Columns)
	sourceAlias := stmt.SourceAlias
	if sourceAlias == "" {
		sourceAlias = stmt.SourceTable
	}
	for i, col := range sourceSchema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colMap[sourceAlias+"."+col.Name] = targetLen + i
		if _, exists := colMap[col.Name]; !exists {
			colMap[col.Name] = targetLen + i
		}
	}

	var rowsInserted, rowsUpdated, rowsDeleted int

	for _, sourceRow := range sourceRows {
		// Collect target rows with their RIDs
		type targetRowInfo struct {
			rid    storage.RID
			values []catalog.Value
		}
		var targetRowsInfo []targetRowInfo

		err := e.mtm.Scan(stmt.TargetTable, tx, func(row *catalog.MVCCRow) (bool, error) {
			rowCopy := make([]catalog.Value, len(row.Values))
			copy(rowCopy, row.Values)
			targetRowsInfo = append(targetRowsInfo, targetRowInfo{rid: row.RID, values: rowCopy})
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		foundMatch := false
		for _, targetInfo := range targetRowsInfo {
			combinedRow := make([]catalog.Value, targetLen+len(sourceRow))
			copy(combinedRow, targetInfo.values)
			copy(combinedRow[targetLen:], sourceRow)

			match, err := e.evalJoinConditionMVCC(stmt.Condition, combinedSchema, combinedRow, colMap)
			if err != nil {
				return nil, fmt.Errorf("error evaluating MERGE condition: %w", err)
			}

			if match {
				foundMatch = true
				for _, when := range stmt.WhenClauses {
					if !when.Matched {
						continue
					}

					if when.Condition != nil {
						condMatch, err := e.evalJoinConditionMVCC(when.Condition, combinedSchema, combinedRow, colMap)
						if err != nil {
							return nil, err
						}
						if !condMatch {
							continue
						}
					}

					switch when.Action.ActionType {
					case "UPDATE":
						newRow := make([]catalog.Value, len(targetInfo.values))
						copy(newRow, targetInfo.values)

						for _, assign := range when.Action.Assignments {
							colIdx := -1
							for j, col := range targetMeta.Schema.Columns {
								if strings.EqualFold(col.Name, assign.Column) {
									colIdx = j
									break
								}
							}
							if colIdx == -1 {
								return nil, fmt.Errorf("unknown column in UPDATE: %s", assign.Column)
							}

							val, err := e.evalMergeExprMVCC(assign.Value, combinedSchema, combinedRow, colMap)
							if err != nil {
								return nil, err
							}
							newRow[colIdx] = val
						}

						if err := e.mtm.MarkDeleted(targetInfo.rid, tx); err != nil {
							return nil, err
						}
						if _, err := e.mtm.Insert(stmt.TargetTable, newRow, tx); err != nil {
							return nil, err
						}
						rowsUpdated++

					case "DELETE":
						if err := e.mtm.MarkDeleted(targetInfo.rid, tx); err != nil {
							return nil, err
						}
						rowsDeleted++

					case "DO NOTHING":
						// Do nothing
					}
					break
				}
				break
			}
		}

		if !foundMatch {
			for _, when := range stmt.WhenClauses {
				if when.Matched {
					continue
				}

				if when.Condition != nil {
					combinedRow := make([]catalog.Value, targetLen+len(sourceRow))
					for j := 0; j < targetLen; j++ {
						combinedRow[j] = catalog.Null(targetMeta.Schema.Columns[j].Type)
					}
					copy(combinedRow[targetLen:], sourceRow)

					condMatch, err := e.evalJoinConditionMVCC(when.Condition, combinedSchema, combinedRow, colMap)
					if err != nil {
						return nil, err
					}
					if !condMatch {
						continue
					}
				}

				switch when.Action.ActionType {
				case "INSERT":
					newRow := make([]catalog.Value, len(targetMeta.Schema.Columns))

					evalRow := make([]catalog.Value, targetLen+len(sourceRow))
					for j := 0; j < targetLen; j++ {
						evalRow[j] = catalog.Null(targetMeta.Schema.Columns[j].Type)
					}
					copy(evalRow[targetLen:], sourceRow)

					if len(when.Action.Columns) > 0 {
						if len(when.Action.Columns) != len(when.Action.Values) {
							return nil, fmt.Errorf("INSERT column count does not match value count")
						}

						for j, col := range targetMeta.Schema.Columns {
							if col.HasDefault && col.DefaultValue != nil {
								newRow[j] = *col.DefaultValue
							} else {
								newRow[j] = catalog.Null(col.Type)
							}
						}

						for j, colName := range when.Action.Columns {
							colIdx := -1
							for k, col := range targetMeta.Schema.Columns {
								if strings.EqualFold(col.Name, colName) {
									colIdx = k
									break
								}
							}
							if colIdx == -1 {
								return nil, fmt.Errorf("unknown column in INSERT: %s", colName)
							}

							val, err := e.evalMergeExprMVCC(when.Action.Values[j], combinedSchema, evalRow, colMap)
							if err != nil {
								return nil, err
							}
							newRow[colIdx] = val
						}
					} else {
						if len(when.Action.Values) != len(targetMeta.Schema.Columns) {
							return nil, fmt.Errorf("INSERT value count does not match target column count")
						}

						for j, valExpr := range when.Action.Values {
							val, err := e.evalMergeExprMVCC(valExpr, combinedSchema, evalRow, colMap)
							if err != nil {
								return nil, err
							}
							newRow[j] = val
						}
					}

					if _, err := e.mtm.Insert(stmt.TargetTable, newRow, tx); err != nil {
						return nil, err
					}
					rowsInserted++

				case "DO NOTHING":
					// Do nothing
				}
				break
			}
		}
	}

	return &Result{
		Message:      fmt.Sprintf("MERGE: %d row(s) inserted, %d row(s) updated, %d row(s) deleted.", rowsInserted, rowsUpdated, rowsDeleted),
		RowsAffected: rowsInserted + rowsUpdated + rowsDeleted,
	}, nil
}

// evalMergeExprMVCC evaluates an expression in MERGE context for MVCC.
func (e *MVCCExecutor) evalMergeExprMVCC(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		idx, ok := colMap[ex.Name]
		if !ok {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	case *BinaryExpr:
		left, err := e.evalMergeExprMVCC(ex.Left, schema, row, colMap)
		if err != nil {
			return catalog.Value{}, err
		}
		right, err := e.evalMergeExprMVCC(ex.Right, schema, row, colMap)
		if err != nil {
			return catalog.Value{}, err
		}
		return evalBinaryExprValueMVCC(left, ex.Op, right)

	default:
		return catalog.Value{}, fmt.Errorf("unsupported expression type in MERGE: %T", expr)
	}
}

func evalBinaryExprValueMVCC(left catalog.Value, op TokenType, right catalog.Value) (catalog.Value, error) {
	leftNum := valueToInt64MVCC(left)
	rightNum := valueToInt64MVCC(right)

	switch op {
	case TOKEN_PLUS:
		return catalog.NewInt64(leftNum + rightNum), nil
	case TOKEN_MINUS:
		return catalog.NewInt64(leftNum - rightNum), nil
	case TOKEN_STAR:
		return catalog.NewInt64(leftNum * rightNum), nil
	case TOKEN_SLASH:
		if rightNum == 0 {
			return catalog.Value{}, fmt.Errorf("division by zero")
		}
		return catalog.NewInt64(leftNum / rightNum), nil
	default:
		return catalog.Value{}, fmt.Errorf("unsupported operator: %v", op)
	}
}

func valueToInt64MVCC(v catalog.Value) int64 {
	if v.IsNull {
		return 0
	}
	switch v.Type {
	case catalog.TypeInt32:
		return int64(v.Int32)
	case catalog.TypeInt64:
		return v.Int64
	default:
		return 0
	}
}
