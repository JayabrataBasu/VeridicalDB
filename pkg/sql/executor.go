package sql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Executor executes SQL statements against a TableManager.
type Executor struct {
	tm              *catalog.TableManager
	autoIncCounters map[string]int64 // table.column -> next value
	autoIncMu       sync.Mutex
	cteData         map[string]*Result  // CTE name -> result (for WITH clause)
	views           map[string]*ViewDef // view name -> view definition
	viewsMu         sync.RWMutex
}

// ViewDef stores a view definition.
type ViewDef struct {
	Name    string
	Columns []string    // optional column aliases
	Query   *SelectStmt // the SELECT that defines the view
}

// NewExecutor creates a new Executor.
func NewExecutor(tm *catalog.TableManager) *Executor {
	return &Executor{
		tm:              tm,
		autoIncCounters: make(map[string]int64),
		views:           make(map[string]*ViewDef),
	}
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
	case *CreateViewStmt:
		return e.executeCreateView(s)
	case *DropTableStmt:
		return e.executeDrop(s)
	case *DropViewStmt:
		return e.executeDropView(s)
	case *InsertStmt:
		return e.executeInsert(s)
	case *SelectStmt:
		return e.executeSelect(s)
	case *UnionStmt:
		return e.executeUnion(s)
	case *UpdateStmt:
		return e.executeUpdate(s)
	case *DeleteStmt:
		return e.executeDelete(s)
	case *AlterTableStmt:
		return e.executeAlter(s)
	case *TruncateTableStmt:
		return e.executeTruncate(s)
	case *ShowStmt:
		return e.executeShow(s)
	case *ExplainStmt:
		return e.executeExplain(s)
	case *MergeStmt:
		return e.executeMerge(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Executor) executeCreate(stmt *CreateTableStmt) (*Result, error) {
	cols := make([]catalog.Column, len(stmt.Columns))
	for i, c := range stmt.Columns {
		col := catalog.Column{
			Name:          c.Name,
			Type:          c.Type,
			NotNull:       c.NotNull,
			PrimaryKey:    c.PrimaryKey,
			HasDefault:    c.HasDefault,
			AutoIncrement: c.AutoIncrement,
		}

		// If there's a default value, evaluate it and store it
		if c.HasDefault && c.Default != nil {
			defaultVal, err := e.evalExpr(c.Default, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("invalid default value for column %s: %w", c.Name, err)
			}
			// Coerce to column type
			defaultVal, err = coerceValue(defaultVal, c.Type)
			if err != nil {
				return nil, fmt.Errorf("invalid default value for column %s: %w", c.Name, err)
			}
			col.DefaultValue = &defaultVal
		}

		// If there's a CHECK constraint, serialize it to string for storage
		if c.Check != nil {
			col.CheckExpr = exprToString(c.Check)
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
				refTable, err := e.tm.Catalog().GetTable(c.ReferencesTable)
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

	// Convert partition spec from AST to catalog format
	partSpec := convertPartitionSpec(stmt.PartitionSpec)

	if err := e.tm.CreateTableWithStorage(stmt.TableName, cols, foreignKeys, storageType, partSpec); err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Table '%s' created", stmt.TableName)
	if storageType == "column" {
		msg += " (columnar storage)"
	}
	if partSpec != nil {
		msg += fmt.Sprintf(" (partitioned by %s)", partSpec.Type)
	}
	return &Result{Message: msg + "."}, nil
}

func (e *Executor) executeDrop(stmt *DropTableStmt) (*Result, error) {
	if err := e.tm.Catalog().DropTable(stmt.TableName); err != nil {
		return nil, err
	}
	return &Result{Message: fmt.Sprintf("Table '%s' dropped.", stmt.TableName)}, nil
}

func (e *Executor) checkForeignKeys(meta *catalog.TableMeta, values []catalog.Value) error {
	for _, fk := range meta.Schema.ForeignKeys {
		// Get values for FK columns
		var fkValues []catalog.Value
		hasNull := false
		for _, colName := range fk.Columns {
			_, idx := meta.Schema.ColumnByName(colName)
			if idx == -1 {
				return fmt.Errorf("column %q not found in table %q", colName, meta.Name)
			}
			val := values[idx]
			if val.IsNull {
				hasNull = true
			}
			fkValues = append(fkValues, val)
		}

		if hasNull {
			continue
		}

		refTable, err := e.tm.Catalog().GetTable(fk.RefTable)
		if err != nil {
			return fmt.Errorf("referenced table %q not found", fk.RefTable)
		}

		found := false
		err = e.scanTable(fk.RefTable, refTable.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
			match := true
			for i, refColName := range fk.RefColumns {
				_, refIdx := refTable.Schema.ColumnByName(refColName)
				if refIdx == -1 {
					return false, fmt.Errorf("referenced column %q not found in table %q", refColName, fk.RefTable)
				}
				if row[refIdx].Compare(fkValues[i]) != 0 {
					match = false
					break
				}
			}
			if match {
				found = true
				return false, nil // Stop scanning
			}
			return true, nil // Continue
		})
		if err != nil {
			return err
		}

		if !found {
			return fmt.Errorf("insert or update on table %q violates foreign key constraint %q", meta.Name, fk.Name)
		}
	}
	return nil
}

func (e *Executor) checkReferencingForeignKeys(meta *catalog.TableMeta, oldRow []catalog.Value) error {
	// Iterate over all tables in catalog
	tables := e.tm.Catalog().ListTables()
	for _, tableName := range tables {
		table, err := e.tm.Catalog().GetTable(tableName)
		if err != nil {
			continue
		}

		for _, fk := range table.Schema.ForeignKeys {
			if fk.RefTable == meta.Name {
				// This table references the table being modified

				// Check if oldRow is referenced
				// Construct values for referenced columns from oldRow
				var refValues []catalog.Value
				for _, refColName := range fk.RefColumns {
					_, idx := meta.Schema.ColumnByName(refColName)
					if idx == -1 {
						return fmt.Errorf("column %q not found", refColName)
					}
					refValues = append(refValues, oldRow[idx])
				}

				// Check if any row in 'table' has these values in 'fk.Columns'
				found := false
				err = e.scanTable(tableName, table.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
					match := true
					for i, colName := range fk.Columns {
						_, idx := table.Schema.ColumnByName(colName)
						if idx == -1 {
							return false, fmt.Errorf("column %q not found", colName)
						}
						if row[idx].Compare(refValues[i]) != 0 {
							match = false
							break
						}
					}
					if match {
						found = true
						return false, nil
					}
					return true, nil
				})
				if err != nil {
					return err
				}

				if found {
					return fmt.Errorf("update or delete on table %q violates foreign key constraint %q on table %q", meta.Name, fk.Name, tableName)
				}
			}
		}
	}
	return nil
}

func (e *Executor) executeInsert(stmt *InsertStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	totalInserted := 0
	totalUpdated := 0

	// Process each row in ValuesList
	for rowIdx, rowValues := range stmt.ValuesList {
		// Build values array matching schema order
		values := make([]catalog.Value, len(meta.Schema.Columns))

		if len(stmt.Columns) == 0 {
			// No column list provided - values must match schema order
			if len(rowValues) != len(meta.Schema.Columns) {
				return nil, fmt.Errorf("row %d: expected %d values, got %d", rowIdx+1, len(meta.Schema.Columns), len(rowValues))
			}
			for i, expr := range rowValues {
				val, err := e.evalExpr(expr, meta.Schema, nil)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
				}
				// Coerce type if needed
				val, err = coerceValue(val, meta.Schema.Columns[i].Type)
				if err != nil {
					return nil, fmt.Errorf("row %d, column %s: %w", rowIdx+1, meta.Schema.Columns[i].Name, err)
				}
				values[i] = val
			}
		} else {
			// Column list provided - match values to columns
			if len(stmt.Columns) != len(rowValues) {
				return nil, fmt.Errorf("row %d: column count (%d) doesn't match value count (%d)", rowIdx+1, len(stmt.Columns), len(rowValues))
			}

			// Initialize with defaults or nulls
			for i, col := range meta.Schema.Columns {
				if col.HasDefault && col.DefaultValue != nil {
					values[i] = *col.DefaultValue
				} else {
					values[i] = catalog.Null(col.Type)
				}
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
				val, err = coerceValue(val, col.Type)
				if err != nil {
					return nil, fmt.Errorf("row %d, column %s: %w", rowIdx+1, colName, err)
				}
				values[idx] = val
			}
		}

		// Apply auto-increment for NULL auto-increment columns
		for i, col := range meta.Schema.Columns {
			if col.AutoIncrement && values[i].IsNull {
				// Get next auto-increment value
				nextVal := e.getNextAutoIncrement(stmt.TableName, col.Name, meta.Schema, i)
				if col.Type == catalog.TypeInt32 {
					values[i] = catalog.NewInt32(int32(nextVal))
				} else {
					values[i] = catalog.NewInt64(nextVal)
				}
			}
		}

		// Check for conflicts on primary key or specified conflict columns
		conflictRID, conflictRow, hasConflict := e.findConflictingRow(stmt, meta.Schema, values)

		if hasConflict {
			// Handle ON CONFLICT
			if stmt.OnConflict == nil {
				// No ON CONFLICT clause - error on duplicate
				return nil, fmt.Errorf("row %d: duplicate key value violates unique constraint", rowIdx+1)
			}

			if stmt.OnConflict.DoNothing {
				// ON CONFLICT DO NOTHING - skip this row
				continue
			}

			// ON CONFLICT DO UPDATE SET ...
			// Create a combined row context for EXCLUDED references
			newRow := make([]catalog.Value, len(conflictRow))
			copy(newRow, conflictRow)

			for _, assign := range stmt.OnConflict.UpdateSet {
				col, idx := meta.Schema.ColumnByName(assign.Column)
				if col == nil {
					return nil, fmt.Errorf("unknown column in ON CONFLICT UPDATE: %s", assign.Column)
				}
				// Evaluate expression with EXCLUDED context (the new values)
				val, err := e.evalExprWithExcluded(assign.Value, meta.Schema, conflictRow, values)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
				}
				val, err = coerceValue(val, col.Type)
				if err != nil {
					return nil, fmt.Errorf("row %d, column %s: %w", rowIdx+1, assign.Column, err)
				}
				newRow[idx] = val
			}

			// Validate CHECK constraints on updated row
			if err := e.validateCheckConstraints(meta.Schema, newRow); err != nil {
				return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
			}

			// Check Foreign Key constraints
			if err := e.checkForeignKeys(meta, newRow); err != nil {
				return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
			}

			// Delete old row and insert updated row
			if err := e.tm.Delete(conflictRID); err != nil {
				return nil, fmt.Errorf("row %d: update delete failed: %w", rowIdx+1, err)
			}
			_, err = e.tm.Insert(stmt.TableName, newRow)
			if err != nil {
				return nil, fmt.Errorf("row %d: update insert failed: %w", rowIdx+1, err)
			}
			totalUpdated++
			continue
		}

		// No conflict - check primary key constraints (for non-conflict columns)
		for i, col := range meta.Schema.Columns {
			if col.PrimaryKey {
				pkValue := values[i]
				if pkValue.IsNull {
					return nil, fmt.Errorf("row %d: primary key column %q cannot be NULL", rowIdx+1, col.Name)
				}
			}
		}

		// Validate CHECK constraints
		if err := e.validateCheckConstraints(meta.Schema, values); err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		// Check Foreign Key constraints
		if err := e.checkForeignKeys(meta, values); err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		_, err = e.tm.Insert(stmt.TableName, values)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		totalInserted++
	}

	// Build result message
	if totalUpdated > 0 {
		if totalInserted == 0 {
			return &Result{Message: fmt.Sprintf("%d row(s) updated.", totalUpdated), RowsAffected: totalUpdated}, nil
		}
		return &Result{Message: fmt.Sprintf("%d row(s) inserted, %d row(s) updated.", totalInserted, totalUpdated), RowsAffected: totalInserted + totalUpdated}, nil
	}
	if totalInserted == 1 {
		return &Result{Message: "1 row inserted.", RowsAffected: totalInserted}, nil
	}
	return &Result{Message: fmt.Sprintf("%d rows inserted.", totalInserted), RowsAffected: totalInserted}, nil
}

// findConflictingRow checks if the values conflict with an existing row.
// It returns the RID, row data, and whether a conflict exists.
func (e *Executor) findConflictingRow(stmt *InsertStmt, schema *catalog.Schema, values []catalog.Value) (storage.RID, []catalog.Value, bool) {
	var conflictRID storage.RID
	var conflictRow []catalog.Value
	hasConflict := false

	// Determine which columns to check for conflicts
	var conflictColIndices []int

	if stmt.OnConflict != nil && len(stmt.OnConflict.ConflictColumns) > 0 {
		// Use specified conflict columns
		for _, colName := range stmt.OnConflict.ConflictColumns {
			_, idx := schema.ColumnByName(colName)
			if idx >= 0 {
				conflictColIndices = append(conflictColIndices, idx)
			}
		}
	} else {
		// Default to primary key columns
		for i, col := range schema.Columns {
			if col.PrimaryKey {
				conflictColIndices = append(conflictColIndices, i)
			}
		}
	}

	if len(conflictColIndices) == 0 {
		return conflictRID, conflictRow, false
	}

	_ = e.scanTable(stmt.TableName, schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		// Check if all conflict columns match
		allMatch := true
		for _, idx := range conflictColIndices {
			if !valuesEqual(values[idx], row[idx]) {
				allMatch = false
				break
			}
		}
		if allMatch {
			conflictRID = rid
			conflictRow = make([]catalog.Value, len(row))
			copy(conflictRow, row)
			hasConflict = true
			return false, nil // stop scanning
		}
		return true, nil // continue
	})

	return conflictRID, conflictRow, hasConflict
}

// evalExprWithExcluded evaluates an expression with EXCLUDED context for ON CONFLICT.
// EXCLUDED refers to the values that would have been inserted.
func (e *Executor) evalExprWithExcluded(expr Expression, schema *catalog.Schema, currentRow, excludedRow []catalog.Value) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *ColumnRef:
		// Check for EXCLUDED.column reference (stored as "EXCLUDED.colname" or "excluded.colname")
		colName := ex.Name
		if strings.HasPrefix(strings.ToUpper(colName), "EXCLUDED.") {
			// Extract the actual column name after "EXCLUDED."
			actualColName := colName[9:] // len("EXCLUDED.") = 9
			_, idx := schema.ColumnByName(actualColName)
			if idx < 0 {
				return catalog.Value{}, fmt.Errorf("unknown column in EXCLUDED: %s", actualColName)
			}
			return excludedRow[idx], nil
		}
		// Regular column reference - use current row
		_, idx := schema.ColumnByName(colName)
		if idx < 0 {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", colName)
		}
		return currentRow[idx], nil

	case *LiteralExpr:
		return ex.Value, nil

	case *BinaryExpr:
		left, err := e.evalExprWithExcluded(ex.Left, schema, currentRow, excludedRow)
		if err != nil {
			return catalog.Value{}, err
		}
		right, err := e.evalExprWithExcluded(ex.Right, schema, currentRow, excludedRow)
		if err != nil {
			return catalog.Value{}, err
		}
		return e.evalBinaryExprValue(left, ex.Op, right)

	default:
		// Fall back to regular eval for other expression types
		return e.evalExpr(expr, schema, currentRow)
	}
}

// primaryKeyExists checks if a value already exists for a primary key column.
// nolint:unused // kept for future use in primary key constraint enforcement
func (e *Executor) primaryKeyExists(tableName string, schema *catalog.Schema, colIdx int, value catalog.Value) (bool, error) {
	exists := false

	err := e.scanTable(tableName, schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		existingValue := row[colIdx]
		if valuesEqual(value, existingValue) {
			exists = true
			return false, nil // stop scanning
		}
		return true, nil // continue scanning
	})

	if err != nil {
		return false, err
	}
	return exists, nil
}

// valuesEqual compares two catalog values for equality.
func valuesEqual(a, b catalog.Value) bool {
	if a.IsNull || b.IsNull {
		return false // NULL != NULL for uniqueness
	}
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case catalog.TypeInt32:
		return a.Int32 == b.Int32
	case catalog.TypeInt64:
		return a.Int64 == b.Int64
	case catalog.TypeText:
		return a.Text == b.Text
	case catalog.TypeBool:
		return a.Bool == b.Bool
	case catalog.TypeTimestamp:
		return a.Timestamp.Equal(b.Timestamp)
	default:
		return false
	}
}

// getNextAutoIncrement returns the next auto-increment value for a column.
// It scans existing rows to find the maximum value if counter isn't initialized.
func (e *Executor) getNextAutoIncrement(tableName, colName string, schema *catalog.Schema, colIdx int) int64 {
	e.autoIncMu.Lock()
	defer e.autoIncMu.Unlock()

	key := tableName + "." + colName

	// Check if we already have a counter
	if next, ok := e.autoIncCounters[key]; ok {
		e.autoIncCounters[key] = next + 1
		return next
	}

	// Initialize by scanning existing rows to find max value
	var maxVal int64 = 0
	_ = e.scanTable(tableName, schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		val := row[colIdx]
		if !val.IsNull {
			var v int64
			if val.Type == catalog.TypeInt32 {
				v = int64(val.Int32)
			} else {
				v = val.Int64
			}
			if v > maxVal {
				maxVal = v
			}
		}
		return true, nil // continue scanning
	})

	// Start from max+1
	next := maxVal + 1
	e.autoIncCounters[key] = next + 1
	return next
}

func (e *Executor) executeSelect(stmt *SelectStmt) (*Result, error) {
	// Handle CTEs (WITH clause)
	if stmt.With != nil {
		return e.executeSelectWithCTEs(stmt)
	}

	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		// Check if it's an information_schema table
		if infoMeta, ok := e.getInformationSchemaTable(stmt.TableName); ok {
			meta = infoMeta
		} else {
			// Check if it's a CTE reference
			if e.cteData != nil {
				if cteResult, ok := e.cteData[stmt.TableName]; ok {
					return e.executeSelectFromCTE(stmt, cteResult)
				}
			}

			// Check if it's a view reference
			e.viewsMu.RLock()
			viewDef, isView := e.views[stmt.TableName]
			e.viewsMu.RUnlock()
			if isView {
				return e.executeSelectFromView(stmt, viewDef)
			}

			return nil, err
		}
	}

	// Check if query contains JOINs
	if len(stmt.Joins) > 0 {
		return e.executeSelectWithJoins(stmt, meta)
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
		return e.executeSelectWithWindowFunctions(stmt, meta)
	}

	// Check if query contains aggregate functions or GROUP BY
	hasAggregates := false
	for _, col := range stmt.Columns {
		if col.Aggregate != nil {
			hasAggregates = true
			break
		}
	}

	// Check for GROUPING SETS / CUBE / ROLLUP
	if len(stmt.GroupingSets) > 0 {
		return e.executeSelectWithGroupingSets(stmt, meta)
	}

	if hasAggregates || len(stmt.GroupBy) > 0 {
		return e.executeSelectWithAggregates(stmt, meta)
	}

	return e.executeSelectNormal(stmt, meta)
}

// executeSelectWithCTEs handles SELECT with WITH clause (Common Table Expressions).
func (e *Executor) executeSelectWithCTEs(stmt *SelectStmt) (*Result, error) {
	// Save any existing CTE data (for nested CTEs)
	oldCTEData := e.cteData

	// Initialize CTE data map
	e.cteData = make(map[string]*Result)

	// Execute each CTE and store results
	for _, cte := range stmt.With.CTEs {
		var cteResult *Result
		var err error

		// Check if this is a recursive CTE
		if cte.Recursive && cte.UnionQuery != nil {
			cteResult, err = e.executeRecursiveCTE(&cte)
		} else if cte.UnionQuery != nil {
			// Non-recursive UNION in CTE
			cteResult, err = e.executeUnion(cte.UnionQuery)
		} else if cte.Query != nil {
			// Simple SELECT CTE
			cteResult, err = e.executeSelect(cte.Query)
		} else {
			err = fmt.Errorf("CTE '%s' has no query defined", cte.Name)
		}

		if err != nil {
			e.cteData = oldCTEData // Restore on error
			return nil, fmt.Errorf("error executing CTE '%s': %w", cte.Name, err)
		}

		// If CTE has explicit column aliases, rename the result columns
		if len(cte.Columns) > 0 {
			if len(cte.Columns) != len(cteResult.Columns) {
				e.cteData = oldCTEData
				return nil, fmt.Errorf("CTE '%s' column count mismatch: expected %d, got %d",
					cte.Name, len(cte.Columns), len(cteResult.Columns))
			}
			cteResult.Columns = cte.Columns
		}

		e.cteData[cte.Name] = cteResult
	}

	// Execute the main query (without the WITH clause)
	mainStmt := *stmt
	mainStmt.With = nil
	result, err := e.executeSelect(&mainStmt)

	// Restore previous CTE data
	e.cteData = oldCTEData

	return result, err
}

// executeRecursiveCTE executes a recursive CTE with iterative fixed-point evaluation.
// The CTE must have a UNION (or UNION ALL) structure:
//   - Left side: base case (anchor query)
//   - Right side: recursive case (references the CTE name)
func (e *Executor) executeRecursiveCTE(cte *CTE) (*Result, error) {
	const maxIterations = 1000 // Safety limit to prevent infinite loops

	// Execute the base case (anchor query - left side of UNION)
	baseResult, err := e.executeSelect(cte.UnionQuery.Left)
	if err != nil {
		return nil, fmt.Errorf("error executing base case of recursive CTE: %w", err)
	}

	// Initialize CTE result with base case
	result := &Result{
		Columns: make([]string, len(baseResult.Columns)),
		Rows:    make([][]catalog.Value, len(baseResult.Rows)),
	}
	copy(result.Columns, baseResult.Columns)
	for i, row := range baseResult.Rows {
		result.Rows[i] = make([]catalog.Value, len(row))
		copy(result.Rows[i], row)
	}

	// Store current CTE result for recursive queries to reference
	e.cteData[cte.Name] = result

	// Track working table (rows from previous iteration)
	workingRows := baseResult.Rows

	// Iteratively execute the recursive part
	for iteration := 0; iteration < maxIterations; iteration++ {
		// Update the CTE data with only the working rows for this iteration
		workingResult := &Result{
			Columns: result.Columns,
			Rows:    workingRows,
		}
		e.cteData[cte.Name] = workingResult

		// Execute the recursive query (right side of UNION)
		recursiveResult, err := e.executeSelect(cte.UnionQuery.Right)
		if err != nil {
			return nil, fmt.Errorf("error executing recursive part of CTE (iteration %d): %w", iteration, err)
		}

		// Check if we got any new rows
		if len(recursiveResult.Rows) == 0 {
			// Fixed point reached - no more new rows
			break
		}

		// Add new rows to result
		newRows := recursiveResult.Rows

		// If UNION (not UNION ALL), remove duplicates
		if !cte.UnionQuery.All {
			newRows = e.removeDuplicateRows(newRows, result.Rows)
		}

		if len(newRows) == 0 {
			// No unique new rows - fixed point reached
			break
		}

		// Add new rows to result
		result.Rows = append(result.Rows, newRows...)

		// Working table for next iteration is the new rows
		workingRows = newRows
	}

	// Update final CTE result
	e.cteData[cte.Name] = result

	return result, nil
}

// removeDuplicateRows removes rows from newRows that already exist in existingRows.
func (e *Executor) removeDuplicateRows(newRows, existingRows [][]catalog.Value) [][]catalog.Value {
	var uniqueRows [][]catalog.Value

	for _, newRow := range newRows {
		isDuplicate := false
		for _, existingRow := range existingRows {
			if e.rowsEqual(newRow, existingRow) {
				isDuplicate = true
				break
			}
		}
		if !isDuplicate {
			uniqueRows = append(uniqueRows, newRow)
		}
	}

	return uniqueRows
}

// rowsEqual checks if two rows have equal values.
func (e *Executor) rowsEqual(row1, row2 []catalog.Value) bool {
	if len(row1) != len(row2) {
		return false
	}
	for i := range row1 {
		if compareValuesForSort(row1[i], row2[i]) != 0 {
			return false
		}
	}
	return true
}

// executeSelectFromCTE executes a SELECT against a CTE result set.
func (e *Executor) executeSelectFromCTE(stmt *SelectStmt, cteResult *Result) (*Result, error) {
	// Build a schema from the CTE result
	cteSchema := &catalog.Schema{
		Columns: make([]catalog.Column, len(cteResult.Columns)),
	}
	for i, colName := range cteResult.Columns {
		// Infer type from first row if possible, otherwise use Text as default
		colType := catalog.TypeText
		if len(cteResult.Rows) > 0 {
			colType = cteResult.Rows[0][i].Type
		}
		cteSchema.Columns[i] = catalog.Column{
			Name: colName,
			Type: colType,
		}
	}

	// Build column index map
	colIndexMap := make(map[string]int)
	for i, col := range cteResult.Columns {
		colIndexMap[col] = i
		if stmt.TableAlias != "" {
			colIndexMap[stmt.TableAlias+"."+col] = i
		}
		colIndexMap[stmt.TableName+"."+col] = i
	}

	// Determine output columns
	var outputCols []string
	var colIndices []int
	var colExpressions []Expression

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for i, col := range cteResult.Columns {
			outputCols = append(outputCols, col)
			colIndices = append(colIndices, i)
			colExpressions = append(colExpressions, nil)
		}
	} else {
		for _, sc := range stmt.Columns {
			if sc.Expression != nil {
				alias := sc.Alias
				if alias == "" {
					alias = "expr"
				}
				outputCols = append(outputCols, alias)
				colIndices = append(colIndices, -1) // expression
				colExpressions = append(colExpressions, sc.Expression)
			} else if sc.Aggregate != nil {
				alias := sc.Alias
				if alias == "" {
					alias = fmt.Sprintf("%s(%s)", sc.Aggregate.Function, sc.Aggregate.Arg)
				}
				outputCols = append(outputCols, alias)
				colIndices = append(colIndices, -2) // aggregate
				colExpressions = append(colExpressions, nil)
			} else {
				idx, ok := colIndexMap[sc.Name]
				if !ok {
					return nil, fmt.Errorf("column %q not found in CTE", sc.Name)
				}
				alias := sc.Alias
				if alias == "" {
					alias = sc.Name
				}
				outputCols = append(outputCols, alias)
				colIndices = append(colIndices, idx)
				colExpressions = append(colExpressions, nil)
			}
		}
	}

	// Filter rows based on WHERE clause
	var filteredRows [][]catalog.Value
	for _, row := range cteResult.Rows {
		// Evaluate WHERE clause
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, cteSchema, row)
			if err != nil {
				return nil, fmt.Errorf("error evaluating WHERE clause: %w", err)
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// Check for aggregates
	hasAggregates := false
	for _, sc := range stmt.Columns {
		if sc.Aggregate != nil {
			hasAggregates = true
			break
		}
	}

	// Handle GROUP BY and aggregates
	if hasAggregates || len(stmt.GroupBy) > 0 {
		return e.executeSelectFromCTEWithAggregates(stmt, cteResult, cteSchema, filteredRows, colIndexMap, outputCols)
	}

	// Build output rows
	var resultRows [][]catalog.Value
	for _, row := range filteredRows {
		var outRow []catalog.Value
		for i, idx := range colIndices {
			if idx == -1 {
				// Expression
				val, err := e.evalExpr(colExpressions[i], cteSchema, row)
				if err != nil {
					return nil, fmt.Errorf("error evaluating expression: %w", err)
				}
				outRow = append(outRow, val)
			} else {
				outRow = append(outRow, row[idx])
			}
		}
		resultRows = append(resultRows, outRow)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.sortRowsForCTE(resultRows, stmt.OrderBy, outputCols)
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset != nil {
		offset := int(*stmt.Offset)
		if offset > len(resultRows) {
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

	// Handle DISTINCT
	if stmt.Distinct {
		resultRows = deduplicateRows(resultRows)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// sortRowsForCTE sorts rows based on ORDER BY clause for CTE results.
func (e *Executor) sortRowsForCTE(rows [][]catalog.Value, orderBy []OrderByClause, cols []string) {
	colIndexMap := make(map[string]int)
	for i, col := range cols {
		colIndexMap[col] = i
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, ob := range orderBy {
			colIdx, ok := colIndexMap[ob.Column]
			if !ok {
				continue
			}
			cmp := compareValuesForSort(rows[i][colIdx], rows[j][colIdx])
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

// executeSelectFromCTEWithAggregates handles aggregates when selecting from a CTE.
func (e *Executor) executeSelectFromCTEWithAggregates(stmt *SelectStmt, cteResult *Result, cteSchema *catalog.Schema, rows [][]catalog.Value, colIndexMap map[string]int, outputCols []string) (*Result, error) {
	// Group rows by GROUP BY columns
	groups := make(map[string][][]catalog.Value)
	var groupOrder []string

	if len(stmt.GroupBy) == 0 {
		// No GROUP BY - all rows in one group
		groups[""] = rows
		groupOrder = []string{""}
	} else {
		for _, row := range rows {
			var keyParts []string
			for _, groupCol := range stmt.GroupBy {
				idx, ok := colIndexMap[groupCol]
				if !ok {
					return nil, fmt.Errorf("GROUP BY column %q not found", groupCol)
				}
				keyParts = append(keyParts, fmt.Sprintf("%v", row[idx]))
			}
			key := strings.Join(keyParts, "\x00")
			if _, exists := groups[key]; !exists {
				groupOrder = append(groupOrder, key)
			}
			groups[key] = append(groups[key], row)
		}
	}

	// Process each group
	var resultRows [][]catalog.Value
	for _, key := range groupOrder {
		groupRows := groups[key]
		if len(groupRows) == 0 {
			continue
		}

		var outRow []catalog.Value
		for _, sc := range stmt.Columns {
			if sc.Aggregate != nil {
				val, err := e.computeAggregateForCTE(sc.Aggregate, groupRows, cteResult.Columns, colIndexMap)
				if err != nil {
					return nil, err
				}
				outRow = append(outRow, val)
			} else if sc.Expression != nil {
				val, err := e.evalExpr(sc.Expression, cteSchema, groupRows[0])
				if err != nil {
					return nil, err
				}
				outRow = append(outRow, val)
			} else {
				idx, ok := colIndexMap[sc.Name]
				if !ok {
					return nil, fmt.Errorf("column %q not found", sc.Name)
				}
				outRow = append(outRow, groupRows[0][idx])
			}
		}
		resultRows = append(resultRows, outRow)
	}

	// Apply HAVING - build schema for aggregate results
	if stmt.Having != nil {
		havingSchema := &catalog.Schema{
			Columns: make([]catalog.Column, len(outputCols)),
		}
		for i, col := range outputCols {
			colType := catalog.TypeInt64 // default for aggregates
			if len(resultRows) > 0 {
				colType = resultRows[0][i].Type
			}
			havingSchema.Columns[i] = catalog.Column{Name: col, Type: colType}
		}

		var filteredRows [][]catalog.Value
		for _, row := range resultRows {
			match, err := e.evalCondition(stmt.Having, havingSchema, row)
			if err != nil {
				return nil, fmt.Errorf("error evaluating HAVING: %w", err)
			}
			if match {
				filteredRows = append(filteredRows, row)
			}
		}
		resultRows = filteredRows
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		e.sortRowsForCTE(resultRows, stmt.OrderBy, outputCols)
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset != nil {
		offset := int(*stmt.Offset)
		if offset > len(resultRows) {
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

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// computeAggregateForCTE computes an aggregate function over CTE rows.
func (e *Executor) computeAggregateForCTE(agg *AggregateFunc, rows [][]catalog.Value, _ []string, colIndexMap map[string]int) (catalog.Value, error) {
	switch strings.ToUpper(agg.Function) {
	case "COUNT":
		if agg.Arg == "*" {
			return catalog.NewInt64(int64(len(rows))), nil
		}
		idx, ok := colIndexMap[agg.Arg]
		if !ok {
			return catalog.Null(catalog.TypeInt64), fmt.Errorf("column %q not found", agg.Arg)
		}
		count := int64(0)
		for _, row := range rows {
			if !row[idx].IsNull {
				count++
			}
		}
		return catalog.NewInt64(count), nil

	case "SUM":
		idx, ok := colIndexMap[agg.Arg]
		if !ok {
			return catalog.Null(catalog.TypeInt64), fmt.Errorf("column %q not found", agg.Arg)
		}
		var sum int64
		hasValue := false
		for _, row := range rows {
			if !row[idx].IsNull {
				switch row[idx].Type {
				case catalog.TypeInt32:
					sum += int64(row[idx].Int32)
				case catalog.TypeInt64:
					sum += row[idx].Int64
				}
				hasValue = true
			}
		}
		if !hasValue {
			return catalog.Null(catalog.TypeInt64), nil
		}
		return catalog.NewInt64(sum), nil

	case "AVG":
		idx, ok := colIndexMap[agg.Arg]
		if !ok {
			return catalog.Null(catalog.TypeInt64), fmt.Errorf("column %q not found", agg.Arg)
		}
		var sum float64
		count := 0
		for _, row := range rows {
			if !row[idx].IsNull {
				switch row[idx].Type {
				case catalog.TypeInt32:
					sum += float64(row[idx].Int32)
				case catalog.TypeInt64:
					sum += float64(row[idx].Int64)
				}
				count++
			}
		}
		if count == 0 {
			return catalog.Null(catalog.TypeInt64), nil
		}
		return catalog.NewInt64(int64(sum / float64(count))), nil

	case "MIN":
		idx, ok := colIndexMap[agg.Arg]
		if !ok {
			return catalog.Null(catalog.TypeInt64), fmt.Errorf("column %q not found", agg.Arg)
		}
		var minVal catalog.Value
		first := true
		for _, row := range rows {
			if !row[idx].IsNull {
				if first {
					minVal = row[idx]
					first = false
				} else if compareValuesForSort(row[idx], minVal) < 0 {
					minVal = row[idx]
				}
			}
		}
		if first {
			return catalog.Null(catalog.TypeInt64), nil
		}
		return minVal, nil

	case "MAX":
		idx, ok := colIndexMap[agg.Arg]
		if !ok {
			return catalog.Null(catalog.TypeInt64), fmt.Errorf("column %q not found", agg.Arg)
		}
		var maxVal catalog.Value
		first := true
		for _, row := range rows {
			if !row[idx].IsNull {
				if first {
					maxVal = row[idx]
					first = false
				} else if compareValuesForSort(row[idx], maxVal) > 0 {
					maxVal = row[idx]
				}
			}
		}
		if first {
			return catalog.Null(catalog.TypeInt64), nil
		}
		return maxVal, nil

	default:
		return catalog.Null(catalog.TypeInt64), fmt.Errorf("unsupported aggregate function: %s", agg.Function)
	}
}

// executeSelectNormal handles regular SELECT without aggregates.
func (e *Executor) executeSelectNormal(stmt *SelectStmt, meta *catalog.TableMeta) (*Result, error) {
	// Determine which columns to return
	var outputCols []string
	var colIndices []int
	var colExpressions []Expression // For expressions like CASE

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for i, c := range meta.Schema.Columns {
			outputCols = append(outputCols, c.Name)
			colIndices = append(colIndices, i)
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
				outputCols = append(outputCols, alias)
				colIndices = append(colIndices, -1) // -1 means this is an expression
				colExpressions = append(colExpressions, sc.Expression)
			} else {
				col, idx := meta.Schema.ColumnByName(sc.Name)
				if col == nil {
					return nil, fmt.Errorf("unknown column: %s", sc.Name)
				}
				outputCols = append(outputCols, col.Name)
				colIndices = append(colIndices, idx)
				colExpressions = append(colExpressions, nil)
			}
		}
	}

	// Validate ORDER BY columns exist in schema
	orderByIndices := make([]int, len(stmt.OrderBy))
	for i, ob := range stmt.OrderBy {
		_, idx := meta.Schema.ColumnByName(ob.Column)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
		}
		orderByIndices[i] = idx
	}

	// Scan all rows (sequential scan) - keep full rows for sorting
	var fullRows [][]catalog.Value
	err := e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
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

		// Keep full row for ORDER BY
		rowCopy := make([]catalog.Value, len(row))
		copy(rowCopy, row)
		fullRows = append(fullRows, rowCopy)
		return true, nil // continue
	})

	if err != nil {
		return nil, err
	}

	// Apply ORDER BY sorting
	if len(stmt.OrderBy) > 0 {
		e.sortRows(fullRows, stmt.OrderBy, orderByIndices)
	}

	// Apply OFFSET
	startIdx := 0
	if stmt.Offset != nil {
		startIdx = int(*stmt.Offset)
		if startIdx > len(fullRows) {
			startIdx = len(fullRows)
		}
	}

	// Apply LIMIT
	endIdx := len(fullRows)
	if stmt.Limit != nil {
		endIdx = startIdx + int(*stmt.Limit)
		if endIdx > len(fullRows) {
			endIdx = len(fullRows)
		}
	} else if stmt.LimitExpr != nil {
		// Evaluate LIMIT expression (e.g., subquery)
		limitVal, err := e.evalLimitExpr(stmt.LimitExpr)
		if err != nil {
			return nil, fmt.Errorf("error evaluating LIMIT expression: %v", err)
		}
		endIdx = startIdx + limitVal
		if endIdx > len(fullRows) {
			endIdx = len(fullRows)
		}
	}

	// Slice rows based on OFFSET and LIMIT
	fullRows = fullRows[startIdx:endIdx]

	// Project columns for final output
	resultRows := make([][]catalog.Value, len(fullRows))
	for i, row := range fullRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			if idx >= 0 {
				// Regular column
				projectedRow[j] = row[idx]
			} else {
				// Expression (like CASE)
				val, err := e.evalExpr(colExpressions[j], meta.Schema, row)
				if err != nil {
					return nil, fmt.Errorf("error evaluating expression: %w", err)
				}
				projectedRow[j] = val
			}
		}
		resultRows[i] = projectedRow
	}

	// Apply DISTINCT or DISTINCT ON - deduplicate rows
	if stmt.Distinct {
		if len(stmt.DistinctOn) > 0 {
			// DISTINCT ON: keep first row for each unique combination of specified columns
			resultRows = deduplicateRowsOn(resultRows, stmt.DistinctOn, outputCols)
		} else {
			// Regular DISTINCT: deduplicate based on all columns
			resultRows = deduplicateRows(resultRows)
		}
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// executeSelectWithJoins handles SELECT with JOIN clauses.
func (e *Executor) executeSelectWithJoins(stmt *SelectStmt, leftMeta *catalog.TableMeta) (*Result, error) {
	// Currently only supporting single JOIN
	if len(stmt.Joins) != 1 {
		return nil, fmt.Errorf("only single JOIN is currently supported")
	}

	join := stmt.Joins[0]
	if join.JoinType != "INNER" && join.JoinType != "LEFT" && join.JoinType != "RIGHT" && join.JoinType != "FULL" && join.JoinType != "CROSS" {
		return nil, fmt.Errorf("unsupported JOIN type: %s", join.JoinType)
	}

	// Handle LATERAL joins with subquery
	if join.Lateral && join.Subquery != nil {
		return e.executeSelectWithLateralJoin(stmt, leftMeta, join)
	}

	// Check if the right table is a CTE
	if e.cteData != nil {
		if cteResult, isCTE := e.cteData[join.TableName]; isCTE {
			return e.executeSelectWithJoinCTE(stmt, leftMeta, join, cteResult)
		}
	}

	// Get the right table metadata
	rightMeta, err := e.tm.Catalog().GetTable(join.TableName)
	if err != nil {
		return nil, err
	}

	// Build combined schema with table prefixes
	// leftTable.col1, leftTable.col2, ..., rightTable.col1, rightTable.col2, ...
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(leftMeta.Schema.Columns)+len(rightMeta.Schema.Columns)),
	}
	colTableMap := make(map[string]int) // col name -> combined schema index

	for i, col := range leftMeta.Schema.Columns {
		newCol := col
		combinedSchema.Columns = append(combinedSchema.Columns, newCol)
		colTableMap[stmt.TableName+"."+col.Name] = i
		colTableMap[col.Name] = i // also allow unqualified access
	}

	leftLen := len(leftMeta.Schema.Columns)
	rightLen := len(rightMeta.Schema.Columns)
	for i, col := range rightMeta.Schema.Columns {
		newCol := col
		combinedSchema.Columns = append(combinedSchema.Columns, newCol)
		colTableMap[join.TableName+"."+col.Name] = leftLen + i
		// Only add unqualified if not already present
		if _, exists := colTableMap[col.Name]; !exists {
			colTableMap[col.Name] = leftLen + i
		}
	}

	// Create NULL row for outer joins
	nullRightRow := make([]catalog.Value, rightLen)
	for i, col := range rightMeta.Schema.Columns {
		nullRightRow[i] = catalog.Null(col.Type)
	}
	nullLeftRow := make([]catalog.Value, leftLen)
	for i, col := range leftMeta.Schema.Columns {
		nullLeftRow[i] = catalog.Null(col.Type)
	}

	// Perform nested loop join based on join type
	var joinedRows [][]catalog.Value

	switch join.JoinType {
	case "INNER":
		err = e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
			return true, e.scanTable(join.TableName, rightMeta.Schema, func(rightRID storage.RID, rightRow []catalog.Value) (bool, error) {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return false, err
				}
				if match {
					joinedRows = append(joinedRows, combinedRow)
				}
				return true, nil
			})
		})

	case "LEFT":
		err = e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
			foundMatch := false
			scanErr := e.scanTable(join.TableName, rightMeta.Schema, func(rightRID storage.RID, rightRow []catalog.Value) (bool, error) {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return false, err
				}
				if match {
					foundMatch = true
					joinedRows = append(joinedRows, combinedRow)
				}
				return true, nil
			})
			if scanErr != nil {
				return false, scanErr
			}
			// If no match found, emit left row with NULLs for right side
			if !foundMatch {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], nullRightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
			return true, nil
		})

	case "RIGHT":
		// Track which right rows have been matched
		rightRowMatches := make(map[int]bool)
		var rightRows [][]catalog.Value

		// First collect all right rows
		err = e.scanTable(join.TableName, rightMeta.Schema, func(rightRID storage.RID, rightRow []catalog.Value) (bool, error) {
			rowCopy := make([]catalog.Value, len(rightRow))
			copy(rowCopy, rightRow)
			rightRows = append(rightRows, rowCopy)
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// Scan left and match with right
		err = e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
			for i, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return false, err
				}
				if match {
					rightRowMatches[i] = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// Add unmatched right rows with NULLs for left side
		for i, rightRow := range rightRows {
			if !rightRowMatches[i] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, nullLeftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}
		err = nil // Reset err since we handled it above

	case "FULL":
		// Track which left and right rows have been matched
		leftRowMatches := make(map[int]bool)
		rightRowMatches := make(map[int]bool)
		var leftRows [][]catalog.Value
		var rightRows [][]catalog.Value

		// First collect all left rows
		err = e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
			rowCopy := make([]catalog.Value, len(leftRow))
			copy(rowCopy, leftRow)
			leftRows = append(leftRows, rowCopy)
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// Collect all right rows
		err = e.scanTable(join.TableName, rightMeta.Schema, func(rightRID storage.RID, rightRow []catalog.Value) (bool, error) {
			rowCopy := make([]catalog.Value, len(rightRow))
			copy(rowCopy, rightRow)
			rightRows = append(rightRows, rowCopy)
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// Match left with right
		for i, leftRow := range leftRows {
			for j, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
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
		err = nil // Reset err since we handled it above

	case "CROSS":
		// CROSS JOIN: Cartesian product of both tables (no condition)
		err = e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
			return true, e.scanTable(join.TableName, rightMeta.Schema, func(rightRID storage.RID, rightRow []catalog.Value) (bool, error) {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
				return true, nil
			})
		})
	}

	if err != nil {
		return nil, err
	}

	// Apply WHERE filter
	var filteredRows [][]catalog.Value
	for _, row := range joinedRows {
		if stmt.Where != nil {
			match, err := e.evalJoinCondition(stmt.Where, combinedSchema, row, colTableMap)
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
		// SELECT *
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
			outputCols = append(outputCols, sc.Name)
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

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			idx, ok := colTableMap[ob.Column]
			if !ok {
				return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
			}
			orderByIndices[i] = idx
		}
		sortJoinedRows(filteredRows, orderByIndices, stmt.OrderBy)
		// Re-project after sorting
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
	} else if stmt.LimitExpr != nil {
		limitVal, err := e.evalLimitExpr(stmt.LimitExpr)
		if err != nil {
			return nil, fmt.Errorf("error evaluating LIMIT expression: %v", err)
		}
		if limitVal < len(resultRows) {
			resultRows = resultRows[:limitVal]
		}
	}

	// Apply DISTINCT or DISTINCT ON
	if stmt.Distinct {
		if len(stmt.DistinctOn) > 0 {
			resultRows = deduplicateRowsOn(resultRows, stmt.DistinctOn, outputCols)
		} else {
			resultRows = deduplicateRows(resultRows)
		}
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// executeSelectWithJoinCTE handles JOIN where the right side is a CTE.
func (e *Executor) executeSelectWithJoinCTE(stmt *SelectStmt, leftMeta *catalog.TableMeta, join JoinClause, cteResult *Result) (*Result, error) {
	// Build CTE schema from result
	cteSchema := &catalog.Schema{
		Columns: make([]catalog.Column, len(cteResult.Columns)),
	}
	for i, colName := range cteResult.Columns {
		colType := catalog.TypeText
		if len(cteResult.Rows) > 0 {
			colType = cteResult.Rows[0][i].Type
		}
		cteSchema.Columns[i] = catalog.Column{Name: colName, Type: colType}
	}

	// Build combined schema with table prefixes
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(leftMeta.Schema.Columns)+len(cteSchema.Columns)),
	}
	colTableMap := make(map[string]int)

	// Left table columns
	leftLen := len(leftMeta.Schema.Columns)
	for i, col := range leftMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colTableMap[stmt.TableName+"."+col.Name] = i
		if stmt.TableAlias != "" {
			colTableMap[stmt.TableAlias+"."+col.Name] = i
		}
		colTableMap[col.Name] = i
	}

	// CTE columns (right side)
	cteLen := len(cteSchema.Columns)
	for i, col := range cteSchema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colTableMap[join.TableName+"."+col.Name] = leftLen + i
		if join.TableAlias != "" {
			colTableMap[join.TableAlias+"."+col.Name] = leftLen + i
		}
		if _, exists := colTableMap[col.Name]; !exists {
			colTableMap[col.Name] = leftLen + i
		}
	}

	// Perform nested loop join
	var joinedRows [][]catalog.Value

	err := e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
		for _, cteRow := range cteResult.Rows {
			combinedRow := make([]catalog.Value, leftLen+cteLen)
			copy(combinedRow, leftRow)
			copy(combinedRow[leftLen:], cteRow)

			match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
			if err != nil {
				return false, err
			}
			if match {
				joinedRows = append(joinedRows, combinedRow)
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
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
	resultRows := make([][]catalog.Value, len(joinedRows))
	for i, row := range joinedRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			projectedRow[j] = row[idx]
		}
		resultRows[i] = projectedRow
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// executeSelectWithLateralJoin handles SELECT with a LATERAL join.
// LATERAL allows the subquery to reference columns from the left side.
func (e *Executor) executeSelectWithLateralJoin(stmt *SelectStmt, leftMeta *catalog.TableMeta, join JoinClause) (*Result, error) {
	var joinedRows [][]catalog.Value
	var combinedSchema *catalog.Schema
	var colTableMap map[string]int
	leftLen := len(leftMeta.Schema.Columns)
	var rightLen int
	schemaInitialized := false

	// Collect all left rows first
	var leftRows [][]catalog.Value
	err := e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
		rowCopy := make([]catalog.Value, len(leftRow))
		copy(rowCopy, leftRow)
		leftRows = append(leftRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Process each left row with the lateral subquery
	for _, leftRow := range leftRows {
		// Create a correlated subquery with substituted values
		correlatedQuery := e.substituteCorrelatedColumns(join.Subquery, leftMeta, leftRow, stmt.TableName, stmt.TableAlias)

		// Execute the lateral subquery
		subResult, err := e.Execute(correlatedQuery)
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
				newCol := catalog.Column{Name: colName, Type: catalog.TypeText} // Default type
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
			// No results from lateral subquery
			if join.JoinType == "LEFT" || join.JoinType == "CROSS" {
				// For LEFT LATERAL or CROSS LATERAL with no results, emit left row with NULLs
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

		// Combine left row with each row from lateral subquery
		for _, rightRow := range subResult.Rows {
			combinedRow := make([]catalog.Value, leftLen+rightLen)
			copy(combinedRow, leftRow)
			copy(combinedRow[leftLen:], rightRow)

			// For INNER LATERAL, all matches are included
			// For CROSS LATERAL, all combinations are included (no condition)
			if join.Condition != nil {
				match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
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

	// If no rows produced (empty left table or all filtered out)
	if combinedSchema == nil {
		// Create empty schema
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
			match, err := e.evalJoinCondition(stmt.Where, combinedSchema, row, colTableMap)
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
		// SELECT *
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

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			idx, ok := colTableMap[ob.Column]
			if !ok {
				return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
			}
			orderByIndices[i] = idx
		}
		sortJoinedRows(filteredRows, orderByIndices, stmt.OrderBy)

		// Re-project after sorting
		resultRows = make([][]catalog.Value, len(filteredRows))
		for i, row := range filteredRows {
			projectedRow := make([]catalog.Value, len(colIndices))
			for j, idx := range colIndices {
				projectedRow[j] = row[idx]
			}
			resultRows[i] = projectedRow
		}
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil {
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
		if len(stmt.DistinctOn) > 0 {
			resultRows = deduplicateRowsOn(resultRows, stmt.DistinctOn, outputCols)
		} else {
			resultRows = deduplicateRows(resultRows)
		}
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// substituteCorrelatedColumns replaces column references in the subquery with
// the actual values from the current left row (for LATERAL).
func (e *Executor) substituteCorrelatedColumns(subquery *SelectStmt, leftMeta *catalog.TableMeta, leftRow []catalog.Value, leftTableName string, leftTableAlias string) *SelectStmt {
	// Create a deep copy of the subquery to avoid modifying the original
	newQuery := &SelectStmt{
		Distinct:   subquery.Distinct,
		DistinctOn: append([]string{}, subquery.DistinctOn...),
		TableName:  subquery.TableName,
		TableAlias: subquery.TableAlias,
		GroupBy:    append([]string{}, subquery.GroupBy...),
		Limit:      subquery.Limit,
		Offset:     subquery.Offset,
	}

	// Copy columns
	newQuery.Columns = make([]SelectColumn, len(subquery.Columns))
	copy(newQuery.Columns, subquery.Columns)

	// Copy OrderBy
	newQuery.OrderBy = make([]OrderByClause, len(subquery.OrderBy))
	copy(newQuery.OrderBy, subquery.OrderBy)

	// Copy and substitute WHERE clause
	if subquery.Where != nil {
		newQuery.Where = e.substituteExpressionValues(subquery.Where, leftMeta, leftRow, leftTableName, leftTableAlias)
	}

	// Copy and substitute HAVING clause
	if subquery.Having != nil {
		newQuery.Having = e.substituteExpressionValues(subquery.Having, leftMeta, leftRow, leftTableName, leftTableAlias)
	}

	return newQuery
}

// substituteExpressionValues substitutes column references from the left table with actual values.
func (e *Executor) substituteExpressionValues(expr Expression, leftMeta *catalog.TableMeta, leftRow []catalog.Value, leftTableName string, leftTableAlias string) Expression {
	switch ex := expr.(type) {
	case *ColumnRef:
		// Check if this column reference is from the left table
		colName := ex.Name
		// Handle qualified names like "t.col" or "table.col"
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

		// Look up the column in the left table schema
		for i, col := range leftMeta.Schema.Columns {
			if col.Name == lookupName {
				// Found a matching column from left table, substitute with literal value
				return &LiteralExpr{Value: leftRow[i]}
			}
		}
		return ex

	case *BinaryExpr:
		return &BinaryExpr{
			Left:  e.substituteExpressionValues(ex.Left, leftMeta, leftRow, leftTableName, leftTableAlias),
			Op:    ex.Op,
			Right: e.substituteExpressionValues(ex.Right, leftMeta, leftRow, leftTableName, leftTableAlias),
		}

	case *UnaryExpr:
		return &UnaryExpr{
			Op:   ex.Op,
			Expr: e.substituteExpressionValues(ex.Expr, leftMeta, leftRow, leftTableName, leftTableAlias),
		}

	case *InExpr:
		newValues := make([]Expression, len(ex.Values))
		for i, v := range ex.Values {
			newValues[i] = e.substituteExpressionValues(v, leftMeta, leftRow, leftTableName, leftTableAlias)
		}
		return &InExpr{
			Left:   e.substituteExpressionValues(ex.Left, leftMeta, leftRow, leftTableName, leftTableAlias),
			Values: newValues,
			Not:    ex.Not,
		}

	case *BetweenExpr:
		return &BetweenExpr{
			Expr: e.substituteExpressionValues(ex.Expr, leftMeta, leftRow, leftTableName, leftTableAlias),
			Low:  e.substituteExpressionValues(ex.Low, leftMeta, leftRow, leftTableName, leftTableAlias),
			High: e.substituteExpressionValues(ex.High, leftMeta, leftRow, leftTableName, leftTableAlias),
			Not:  ex.Not,
		}

	case *FunctionExpr:
		newArgs := make([]Expression, len(ex.Args))
		for i, arg := range ex.Args {
			newArgs[i] = e.substituteExpressionValues(arg, leftMeta, leftRow, leftTableName, leftTableAlias)
		}
		return &FunctionExpr{
			Name: ex.Name,
			Args: newArgs,
		}

	case *IsNullExpr:
		return &IsNullExpr{
			Expr: e.substituteExpressionValues(ex.Expr, leftMeta, leftRow, leftTableName, leftTableAlias),
			Not:  ex.Not,
		}

	case *CaseExpr:
		var newOperand Expression
		if ex.Operand != nil {
			newOperand = e.substituteExpressionValues(ex.Operand, leftMeta, leftRow, leftTableName, leftTableAlias)
		}
		newWhens := make([]WhenClause, len(ex.Whens))
		for i, w := range ex.Whens {
			newWhens[i] = WhenClause{
				Condition: e.substituteExpressionValues(w.Condition, leftMeta, leftRow, leftTableName, leftTableAlias),
				Result:    e.substituteExpressionValues(w.Result, leftMeta, leftRow, leftTableName, leftTableAlias),
			}
		}
		var newElse Expression
		if ex.Else != nil {
			newElse = e.substituteExpressionValues(ex.Else, leftMeta, leftRow, leftTableName, leftTableAlias)
		}
		return &CaseExpr{
			Operand: newOperand,
			Whens:   newWhens,
			Else:    newElse,
		}

	default:
		return expr
	}
}

// splitQualifiedName splits "table.column" into ["table", "column"]
func splitQualifiedName(name string) []string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return []string{name[:i], name[i+1:]}
		}
	}
	return []string{name}
}

// evalJoinCondition evaluates a join condition against a combined row.
func (e *Executor) evalJoinCondition(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		// Handle literal boolean values like TRUE or FALSE
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
		// For other literals, treat non-null/non-zero as true
		return !ex.Value.IsNull, nil

	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalJoinCondition(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalJoinCondition(ex.Right, schema, row, colMap)

		case TOKEN_OR:
			left, err := e.evalJoinCondition(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalJoinCondition(ex.Right, schema, row, colMap)

		case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
			leftVal, err := e.evalJoinExpr(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			rightVal, err := e.evalJoinExpr(ex.Right, schema, row, colMap)
			if err != nil {
				return false, err
			}
			return compareValues(leftVal, rightVal, ex.Op)

		default:
			return false, fmt.Errorf("unsupported operator in join condition: %v", ex.Op)
		}

	default:
		return false, fmt.Errorf("unsupported expression in join condition: %T", expr)
	}
}

// evalJoinExpr evaluates an expression in the context of a joined row.
func (e *Executor) evalJoinExpr(expr Expression, _ *catalog.Schema, row []catalog.Value, colMap map[string]int) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		idx, ok := colMap[ex.Name]
		if !ok {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	default:
		return catalog.Value{}, fmt.Errorf("unsupported expression type in join: %T", expr)
	}
}

// sortJoinedRows sorts joined rows by ORDER BY columns.
func sortJoinedRows(rows [][]catalog.Value, orderByIndices []int, orderBy []OrderByClause) {
	sort.SliceStable(rows, func(i, j int) bool {
		for k, idx := range orderByIndices {
			cmp := compareValuesForSort(rows[i][idx], rows[j][idx])
			if cmp == 0 {
				continue
			}
			if orderBy[k].Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

// aggregatorState holds aggregation state for a single aggregate function.
type aggregatorState struct {
	count    int64
	sum      int64
	sumFloat float64
	min      catalog.Value
	max      catalog.Value
	hasValue bool
}

// groupState holds the state for a single group in GROUP BY.
type groupState struct {
	groupKey    []catalog.Value   // values of GROUP BY columns
	aggregators []aggregatorState // aggregation state per output column
}

// executeSelectWithAggregates handles SELECT with aggregate functions and/or GROUP BY.
func (e *Executor) executeSelectWithAggregates(stmt *SelectStmt, meta *catalog.TableMeta) (*Result, error) {
	// Resolve GROUP BY column indices
	groupByIndices := make([]int, len(stmt.GroupBy))
	for i, colName := range stmt.GroupBy {
		_, idx := meta.Schema.ColumnByName(colName)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in GROUP BY: %s", colName)
		}
		groupByIndices[i] = idx
	}

	// Validate that non-aggregate columns appear in GROUP BY
	type columnInfo struct {
		isAggregate bool
		aggregate   *AggregateFunc
		colName     string
		colIdx      int // schema column index for regular columns
	}
	columnInfos := make([]columnInfo, len(stmt.Columns))
	outputCols := make([]string, len(stmt.Columns))

	for i, col := range stmt.Columns {
		if col.Aggregate != nil {
			agg := col.Aggregate
			columnInfos[i].isAggregate = true
			columnInfos[i].aggregate = agg
			// Use alias if provided, otherwise generate name from function
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = fmt.Sprintf("%s(%s)", agg.Function, agg.Arg)
			}

			if agg.Arg != "*" {
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx < 0 {
					return nil, fmt.Errorf("unknown column: %s", agg.Arg)
				}
			}
		} else if col.Name != "" {
			columnInfos[i].colName = col.Name
			// Use alias if provided
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = col.Name
			}

			// Verify column is in GROUP BY
			found := false
			for j, gbCol := range stmt.GroupBy {
				if gbCol == col.Name {
					found = true
					columnInfos[i].colIdx = groupByIndices[j]
					break
				}
			}
			if !found && len(stmt.GroupBy) > 0 {
				return nil, fmt.Errorf("column %s must appear in GROUP BY clause or be used in an aggregate function", col.Name)
			}
			if len(stmt.GroupBy) == 0 {
				return nil, fmt.Errorf("column %s must appear in GROUP BY clause or be used in an aggregate function", col.Name)
			}
		}
	}

	// Use a map with string key for grouping (serialize group key values)
	groups := make(map[string]*groupState)
	var groupOrder []string // preserve insertion order for stable output

	// Scan all rows
	err := e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
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

		// Build group key
		var groupKey []catalog.Value
		var keyStr string
		if len(groupByIndices) > 0 {
			groupKey = make([]catalog.Value, len(groupByIndices))
			for i, idx := range groupByIndices {
				groupKey[i] = row[idx]
			}
			keyStr = groupKeyString(groupKey)
		} else {
			// No GROUP BY - single group
			keyStr = ""
		}

		// Get or create group
		grp, exists := groups[keyStr]
		if !exists {
			grp = &groupState{
				groupKey:    groupKey,
				aggregators: make([]aggregatorState, len(stmt.Columns)),
			}
			groups[keyStr] = grp
			groupOrder = append(groupOrder, keyStr)
		}

		// Update aggregators for each column
		for i, colInfo := range columnInfos {
			if !colInfo.isAggregate {
				continue
			}
			agg := colInfo.aggregate
			aggState := &grp.aggregators[i]

			switch agg.Function {
			case "COUNT":
				if agg.Arg == "*" {
					aggState.count++
				} else {
					// COUNT(column) - count non-null values
					_, idx := meta.Schema.ColumnByName(agg.Arg)
					if idx >= 0 && !row[idx].IsNull {
						aggState.count++
					}
				}

			case "SUM":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row[idx]
					if !val.IsNull {
						aggState.hasValue = true
						switch val.Type {
						case catalog.TypeInt32:
							aggState.sum += int64(val.Int32)
						case catalog.TypeInt64:
							aggState.sum += val.Int64
						}
					}
				}

			case "AVG":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row[idx]
					if !val.IsNull {
						aggState.count++
						aggState.hasValue = true
						switch val.Type {
						case catalog.TypeInt32:
							aggState.sumFloat += float64(val.Int32)
						case catalog.TypeInt64:
							aggState.sumFloat += float64(val.Int64)
						}
					}
				}

			case "MIN":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row[idx]
					if !val.IsNull {
						if !aggState.hasValue {
							aggState.min = val
							aggState.hasValue = true
						} else if compareValuesForSort(val, aggState.min) < 0 {
							aggState.min = val
						}
					}
				}

			case "MAX":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row[idx]
					if !val.IsNull {
						if !aggState.hasValue {
							aggState.max = val
							aggState.hasValue = true
						} else if compareValuesForSort(val, aggState.max) > 0 {
							aggState.max = val
						}
					}
				}
			}
		}

		return true, nil // continue
	})

	if err != nil {
		return nil, err
	}

	// Build result rows from groups
	var resultRows [][]catalog.Value
	for _, keyStr := range groupOrder {
		grp := groups[keyStr]

		// Apply HAVING filter if present
		if stmt.Having != nil {
			match, err := e.evalHavingCondition(stmt.Having, grp, stmt.Columns, meta.Schema)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Build result row
		resultRow := make([]catalog.Value, len(stmt.Columns))
		for i, colInfo := range columnInfos {
			if colInfo.isAggregate {
				aggState := grp.aggregators[i]
				switch colInfo.aggregate.Function {
				case "COUNT":
					resultRow[i] = catalog.NewInt64(aggState.count)
				case "SUM":
					if !aggState.hasValue {
						resultRow[i] = catalog.Null(catalog.TypeInt64)
					} else {
						resultRow[i] = catalog.NewInt64(aggState.sum)
					}
				case "AVG":
					if !aggState.hasValue || aggState.count == 0 {
						resultRow[i] = catalog.Null(catalog.TypeInt64)
					} else {
						avg := aggState.sumFloat / float64(aggState.count)
						resultRow[i] = catalog.NewInt64(int64(avg))
					}
				case "MIN":
					if !aggState.hasValue {
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					} else {
						resultRow[i] = aggState.min
					}
				case "MAX":
					if !aggState.hasValue {
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					} else {
						resultRow[i] = aggState.max
					}
				}
			} else {
				// Regular column from GROUP BY
				for j, gbCol := range stmt.GroupBy {
					if gbCol == colInfo.colName {
						resultRow[i] = grp.groupKey[j]
						break
					}
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	}

	// If no groups (empty table), still return one row for aggregates without GROUP BY
	if len(resultRows) == 0 && len(stmt.GroupBy) == 0 {
		resultRow := make([]catalog.Value, len(stmt.Columns))
		for i, colInfo := range columnInfos {
			if colInfo.isAggregate {
				switch colInfo.aggregate.Function {
				case "COUNT":
					resultRow[i] = catalog.NewInt64(0)
				default:
					resultRow[i] = catalog.Null(catalog.TypeUnknown)
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// groupKeyString serializes a group key to a string for map lookup.
func groupKeyString(values []catalog.Value) string {
	var parts []string
	for _, v := range values {
		if v.IsNull {
			parts = append(parts, "NULL")
		} else {
			parts = append(parts, fmt.Sprintf("%v", v.String()))
		}
	}
	return strings.Join(parts, "|")
}

// executeSelectWithGroupingSets handles SELECT with GROUPING SETS, CUBE, or ROLLUP.
func (e *Executor) executeSelectWithGroupingSets(stmt *SelectStmt, meta *catalog.TableMeta) (*Result, error) {
	// Collect all columns that appear in any grouping set
	groupingCols := make(map[string]int) // column name -> schema index
	for _, gs := range stmt.GroupingSets {
		for _, col := range gs.Columns {
			if _, exists := groupingCols[col]; !exists {
				_, idx := meta.Schema.ColumnByName(col)
				if idx < 0 {
					return nil, fmt.Errorf("unknown column in GROUPING SETS: %s", col)
				}
				groupingCols[col] = idx
			}
		}
	}

	// Also add simple GROUP BY columns
	for _, col := range stmt.GroupBy {
		if _, exists := groupingCols[col]; !exists {
			_, idx := meta.Schema.ColumnByName(col)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column in GROUP BY: %s", col)
			}
			groupingCols[col] = idx
		}
	}

	// Determine output columns and aggregate info
	type columnInfo struct {
		isAggregate bool
		aggregate   *AggregateFunc
		colName     string
		isGrouping  bool   // GROUPING() function
		groupingArg string // column name for GROUPING()
	}
	columnInfos := make([]columnInfo, len(stmt.Columns))
	outputCols := make([]string, len(stmt.Columns))

	for i, col := range stmt.Columns {
		if col.Aggregate != nil {
			agg := col.Aggregate
			columnInfos[i].isAggregate = true
			columnInfos[i].aggregate = agg
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = fmt.Sprintf("%s(%s)", agg.Function, agg.Arg)
			}
		} else if col.Expression != nil {
			// Check if it's a GROUPING() function call
			if funcExpr, ok := col.Expression.(*FunctionExpr); ok && strings.ToUpper(funcExpr.Name) == "GROUPING" {
				columnInfos[i].isGrouping = true
				if len(funcExpr.Args) == 1 {
					if colRef, ok := funcExpr.Args[0].(*ColumnRef); ok {
						columnInfos[i].groupingArg = colRef.Name
					}
				}
				if col.Alias != "" {
					outputCols[i] = col.Alias
				} else {
					outputCols[i] = "GROUPING"
				}
			} else {
				// Other expression
				if col.Alias != "" {
					outputCols[i] = col.Alias
				} else {
					outputCols[i] = "expr"
				}
			}
		} else if col.Name != "" {
			columnInfos[i].colName = col.Name
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = col.Name
			}
		}
	}

	// Collect all matching rows first
	var allRows [][]catalog.Value
	err := e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
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
		rowCopy := make([]catalog.Value, len(row))
		copy(rowCopy, row)
		allRows = append(allRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Process each grouping set separately and combine results
	var resultRows [][]catalog.Value

	for _, gs := range stmt.GroupingSets {
		// Build index map for this grouping set
		groupIndices := make([]int, len(gs.Columns))
		groupColSet := make(map[string]bool)
		for i, col := range gs.Columns {
			groupIndices[i] = groupingCols[col]
			groupColSet[col] = true
		}

		// Group rows by this grouping set's columns
		groups := make(map[string]*groupingSetState)
		var groupOrder []string

		for _, row := range allRows {
			// Build group key
			var keyParts []string
			for _, idx := range groupIndices {
				if row[idx].IsNull {
					keyParts = append(keyParts, "NULL")
				} else {
					keyParts = append(keyParts, row[idx].String())
				}
			}
			keyStr := strings.Join(keyParts, "|")

			grp, exists := groups[keyStr]
			if !exists {
				// Store the group key values
				groupKey := make([]catalog.Value, len(groupIndices))
				for i, idx := range groupIndices {
					groupKey[i] = row[idx]
				}
				grp = &groupingSetState{
					groupKey:    groupKey,
					aggregators: make([]aggregatorState, len(stmt.Columns)),
				}
				groups[keyStr] = grp
				groupOrder = append(groupOrder, keyStr)
			}

			// Update aggregators
			for i, colInfo := range columnInfos {
				if colInfo.isAggregate {
					agg := colInfo.aggregate
					var val catalog.Value
					if agg.Arg == "*" {
						// COUNT(*) - just count rows
						grp.aggregators[i].count++
						grp.aggregators[i].hasValue = true
						continue
					}

					// Get column value
					_, idx := meta.Schema.ColumnByName(agg.Arg)
					if idx >= 0 {
						val = row[idx]
					} else {
						continue
					}

					if val.IsNull {
						continue
					}

					grp.aggregators[i].hasValue = true
					grp.aggregators[i].count++

					switch agg.Function {
					case "SUM", "AVG":
						switch val.Type {
						case catalog.TypeInt32:
							grp.aggregators[i].sum += int64(val.Int32)
						case catalog.TypeInt64:
							grp.aggregators[i].sum += val.Int64
						}
					case "MIN":
						if !grp.aggregators[i].hasValue || compareValuesForSort(val, grp.aggregators[i].min) < 0 {
							grp.aggregators[i].min = val
						}
					case "MAX":
						if !grp.aggregators[i].hasValue || compareValuesForSort(val, grp.aggregators[i].max) > 0 {
							grp.aggregators[i].max = val
						}
					}
				}
			}
		}

		// Build result rows for this grouping set
		for _, keyStr := range groupOrder {
			grp := groups[keyStr]
			resultRow := make([]catalog.Value, len(stmt.Columns))

			for i, colInfo := range columnInfos {
				if colInfo.isAggregate {
					agg := colInfo.aggregate
					aggState := grp.aggregators[i]

					switch agg.Function {
					case "COUNT":
						resultRow[i] = catalog.NewInt64(aggState.count)
					case "SUM":
						if aggState.hasValue {
							resultRow[i] = catalog.NewInt64(aggState.sum)
						} else {
							resultRow[i] = catalog.Null(catalog.TypeInt64)
						}
					case "AVG":
						if aggState.count > 0 {
							avg := aggState.sum / aggState.count
							resultRow[i] = catalog.NewInt64(avg)
						} else {
							resultRow[i] = catalog.Null(catalog.TypeInt64)
						}
					case "MIN":
						if aggState.hasValue {
							resultRow[i] = aggState.min
						} else {
							resultRow[i] = catalog.Null(catalog.TypeUnknown)
						}
					case "MAX":
						if aggState.hasValue {
							resultRow[i] = aggState.max
						} else {
							resultRow[i] = catalog.Null(catalog.TypeUnknown)
						}
					}
				} else if colInfo.isGrouping {
					// GROUPING() function: 1 if column is rolled up (NULL), 0 otherwise
					colInSet := groupColSet[colInfo.groupingArg]
					if colInSet {
						resultRow[i] = catalog.NewInt64(0) // Column is in grouping set
					} else {
						resultRow[i] = catalog.NewInt64(1) // Column is rolled up (super-aggregate)
					}
				} else if colInfo.colName != "" {
					// Regular column - get from group key if in this grouping set
					if _, ok := groupColSet[colInfo.colName]; ok {
						// Find the position in group key
						for j, col := range gs.Columns {
							if col == colInfo.colName {
								resultRow[i] = grp.groupKey[j]
								break
							}
						}
					} else {
						// Column not in this grouping set - return NULL
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					}
				}
			}

			resultRows = append(resultRows, resultRow)
		}

		// Handle empty group (grand total when grouping set is empty)
		// Only add if no rows were produced from the regular loop (which means no data)
		if len(gs.Columns) == 0 && len(allRows) > 0 && len(groupOrder) == 0 {
			resultRow := make([]catalog.Value, len(stmt.Columns))

			// Calculate aggregates over all rows
			var grandTotalAggs []aggregatorState
			grandTotalAggs = make([]aggregatorState, len(stmt.Columns))

			for _, row := range allRows {
				for i, colInfo := range columnInfos {
					if colInfo.isAggregate {
						agg := colInfo.aggregate
						if agg.Arg == "*" {
							grandTotalAggs[i].count++
							grandTotalAggs[i].hasValue = true
							continue
						}

						_, idx := meta.Schema.ColumnByName(agg.Arg)
						if idx < 0 {
							continue
						}
						val := row[idx]
						if val.IsNull {
							continue
						}

						grandTotalAggs[i].hasValue = true
						grandTotalAggs[i].count++

						switch agg.Function {
						case "SUM", "AVG":
							switch val.Type {
							case catalog.TypeInt32:
								grandTotalAggs[i].sum += int64(val.Int32)
							case catalog.TypeInt64:
								grandTotalAggs[i].sum += val.Int64
							}
						case "MIN":
							if !grandTotalAggs[i].hasValue || compareValuesForSort(val, grandTotalAggs[i].min) < 0 {
								grandTotalAggs[i].min = val
							}
						case "MAX":
							if !grandTotalAggs[i].hasValue || compareValuesForSort(val, grandTotalAggs[i].max) > 0 {
								grandTotalAggs[i].max = val
							}
						}
					}
				}
			}

			for i, colInfo := range columnInfos {
				if colInfo.isAggregate {
					agg := colInfo.aggregate
					aggState := grandTotalAggs[i]

					switch agg.Function {
					case "COUNT":
						resultRow[i] = catalog.NewInt64(aggState.count)
					case "SUM":
						if aggState.hasValue {
							resultRow[i] = catalog.NewInt64(aggState.sum)
						} else {
							resultRow[i] = catalog.Null(catalog.TypeInt64)
						}
					case "AVG":
						if aggState.count > 0 {
							resultRow[i] = catalog.NewInt64(aggState.sum / aggState.count)
						} else {
							resultRow[i] = catalog.Null(catalog.TypeInt64)
						}
					case "MIN":
						if aggState.hasValue {
							resultRow[i] = aggState.min
						} else {
							resultRow[i] = catalog.Null(catalog.TypeUnknown)
						}
					case "MAX":
						if aggState.hasValue {
							resultRow[i] = aggState.max
						} else {
							resultRow[i] = catalog.Null(catalog.TypeUnknown)
						}
					}
				} else if colInfo.isGrouping {
					// All columns are rolled up in grand total
					resultRow[i] = catalog.NewInt32(1)
				} else {
					// Regular columns are NULL in grand total
					resultRow[i] = catalog.Null(catalog.TypeUnknown)
				}
			}

			resultRows = append(resultRows, resultRow)
		}
	}

	// Handle empty result (no rows matched WHERE)
	if len(resultRows) == 0 && len(stmt.GroupingSets) > 0 {
		// Check if there's a grand total grouping set
		for _, gs := range stmt.GroupingSets {
			if len(gs.Columns) == 0 {
				resultRow := make([]catalog.Value, len(stmt.Columns))
				for i, colInfo := range columnInfos {
					if colInfo.isAggregate {
						switch colInfo.aggregate.Function {
						case "COUNT":
							resultRow[i] = catalog.NewInt64(0)
						default:
							resultRow[i] = catalog.Null(catalog.TypeUnknown)
						}
					} else if colInfo.isGrouping {
						resultRow[i] = catalog.NewInt32(1)
					} else {
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					}
				}
				resultRows = append(resultRows, resultRow)
				break
			}
		}
	}

	// Apply HAVING filter if present
	if stmt.Having != nil {
		var filteredRows [][]catalog.Value
		for _, row := range resultRows {
			// Build a schema for evaluation
			havingSchema := &catalog.Schema{
				Columns: make([]catalog.Column, len(outputCols)),
			}
			for i, name := range outputCols {
				havingSchema.Columns[i] = catalog.Column{Name: name}
			}
			match, err := e.evalCondition(stmt.Having, havingSchema, row)
			if err != nil {
				return nil, fmt.Errorf("error evaluating HAVING: %w", err)
			}
			if match {
				filteredRows = append(filteredRows, row)
			}
		}
		resultRows = filteredRows
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			found := false
			for j, col := range outputCols {
				if strings.EqualFold(col, ob.Column) {
					orderByIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("ORDER BY column not in result: %s", ob.Column)
			}
		}
		sortJoinedRows(resultRows, orderByIndices, stmt.OrderBy)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// groupingSetState holds state for a single group in GROUPING SETS processing.
type groupingSetState struct {
	groupKey    []catalog.Value
	aggregators []aggregatorState
}

// deduplicateRows removes duplicate rows from the result set.
func deduplicateRows(rows [][]catalog.Value) [][]catalog.Value {
	if len(rows) <= 1 {
		return rows
	}

	seen := make(map[string]bool)
	result := make([][]catalog.Value, 0, len(rows))

	for _, row := range rows {
		key := rowKeyString(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// rowKeyString creates a string key for deduplication.
func rowKeyString(row []catalog.Value) string {
	var parts []string
	for _, v := range row {
		if v.IsNull {
			parts = append(parts, "NULL")
		} else {
			parts = append(parts, v.String())
		}
	}
	return strings.Join(parts, "|")
}

// deduplicateRowsOn removes duplicates based on specific columns (DISTINCT ON).
// It keeps the first row encountered for each unique combination of the specified columns.
func deduplicateRowsOn(rows [][]catalog.Value, distinctCols []string, outputCols []string) [][]catalog.Value {
	if len(rows) <= 1 || len(distinctCols) == 0 {
		return rows
	}

	// Find indices of DISTINCT ON columns in the output
	colIndices := make([]int, 0, len(distinctCols))
	for _, dc := range distinctCols {
		for i, oc := range outputCols {
			if oc == dc {
				colIndices = append(colIndices, i)
				break
			}
		}
	}

	// If no matching columns found, return all rows
	if len(colIndices) == 0 {
		return rows
	}

	seen := make(map[string]bool)
	result := make([][]catalog.Value, 0, len(rows))

	for _, row := range rows {
		// Build key from only the DISTINCT ON columns
		var keyParts []string
		for _, idx := range colIndices {
			if idx < len(row) {
				if row[idx].IsNull {
					keyParts = append(keyParts, "NULL")
				} else {
					keyParts = append(keyParts, row[idx].String())
				}
			}
		}
		key := strings.Join(keyParts, "|")

		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// evalLimitExpr evaluates a LIMIT expression and returns an integer value.
func (e *Executor) evalLimitExpr(expr Expression) (int, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		// Literal integer
		switch ex.Value.Type {
		case catalog.TypeInt32:
			return int(ex.Value.Int32), nil
		case catalog.TypeInt64:
			return int(ex.Value.Int64), nil
		default:
			return 0, fmt.Errorf("LIMIT must be an integer, got %v", ex.Value.Type)
		}

	case *SubqueryExpr:
		// Execute the subquery
		result, err := e.executeSelect(ex.Query)
		if err != nil {
			return 0, fmt.Errorf("error executing LIMIT subquery: %v", err)
		}
		// Subquery must return single row with single column
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
			return 0, fmt.Errorf("LIMIT subquery must return exactly one value")
		}
		val := result.Rows[0][0]
		switch val.Type {
		case catalog.TypeInt32:
			return int(val.Int32), nil
		case catalog.TypeInt64:
			return int(val.Int64), nil
		default:
			return 0, fmt.Errorf("LIMIT subquery must return an integer, got %v", val.Type)
		}

	default:
		return 0, fmt.Errorf("unsupported LIMIT expression type: %T", expr)
	}
}

// evalHavingCondition evaluates a HAVING condition against group aggregates.
func (e *Executor) evalHavingCondition(expr Expression, grp *groupState, columns []SelectColumn, schema *catalog.Schema) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalHavingCondition(ex.Left, grp, columns, schema)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalHavingCondition(ex.Right, grp, columns, schema)

		case TOKEN_OR:
			left, err := e.evalHavingCondition(ex.Left, grp, columns, schema)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalHavingCondition(ex.Right, grp, columns, schema)

		case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
			left, err := e.evalHavingExpr(ex.Left, grp, columns, schema)
			if err != nil {
				return false, err
			}
			right, err := e.evalHavingExpr(ex.Right, grp, columns, schema)
			if err != nil {
				return false, err
			}
			return compareValues(left, right, ex.Op)
		}

	case *LiteralExpr:
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
	}

	return false, fmt.Errorf("cannot evaluate HAVING expression: %T", expr)
}

// evalHavingExpr evaluates an expression in HAVING context (can reference aggregates).
func (e *Executor) evalHavingExpr(expr Expression, grp *groupState, columns []SelectColumn, _ *catalog.Schema) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		// Look for this column in GROUP BY columns (grp.groupKey)
		for i, gbVal := range grp.groupKey {
			// We need to match column names. Look in columns for GROUP BY entries.
			// This is tricky - for now assume groupKey order matches groupBy columns
			_ = i     // placeholder
			_ = gbVal // placeholder
		}
		// Look in SELECT columns for non-aggregate columns
		for i, col := range columns {
			if col.Name == ex.Name && col.Aggregate == nil {
				if i < len(grp.groupKey) {
					return grp.groupKey[i], nil
				}
			}
		}
		return catalog.Value{}, fmt.Errorf("column %s not found in GROUP BY", ex.Name)

	case *FunctionExpr:
		// Handle aggregate functions in HAVING clause
		funcName := strings.ToUpper(ex.Name)

		// Find matching aggregate in SELECT columns
		for i, col := range columns {
			if col.Aggregate != nil && col.Aggregate.Function == funcName {
				// Found a matching aggregate, use its computed value
				if i < len(grp.aggregators) {
					aggState := grp.aggregators[i]
					switch funcName {
					case "COUNT":
						return catalog.NewInt64(aggState.count), nil
					case "SUM":
						if !aggState.hasValue {
							return catalog.Null(catalog.TypeInt64), nil
						}
						return catalog.NewInt64(aggState.sum), nil
					case "AVG":
						if aggState.count == 0 {
							return catalog.Null(catalog.TypeInt64), nil
						}
						// AVG uses sumFloat
						avg := int64(aggState.sumFloat / float64(aggState.count))
						return catalog.NewInt64(avg), nil
					case "MIN":
						if !aggState.hasValue {
							return catalog.Null(catalog.TypeUnknown), nil
						}
						return aggState.min, nil
					case "MAX":
						if !aggState.hasValue {
							return catalog.Null(catalog.TypeUnknown), nil
						}
						return aggState.max, nil
					}
				}
			}
		}
		return catalog.Value{}, fmt.Errorf("aggregate %s not found in SELECT columns", funcName)
	}

	return catalog.Value{}, fmt.Errorf("unsupported HAVING expression: %T", expr)
}

func (e *Executor) executeUpdate(stmt *UpdateStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Handle UPDATE with FROM clause (PostgreSQL-style join update)
	if stmt.FromTable != "" {
		return e.executeUpdateWithFrom(stmt, meta)
	}

	// Simple UPDATE without FROM
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

		// Validate CHECK constraints on the updated row
		if err := e.validateCheckConstraints(meta.Schema, newRow); err != nil {
			return false, err
		}

		ridsToUpdate = append(ridsToUpdate, rid)
		newRows = append(newRows, newRow)
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Delete old rows and insert new rows
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

// executeUpdateWithFrom handles UPDATE ... FROM ... WHERE syntax (PostgreSQL style).
func (e *Executor) executeUpdateWithFrom(stmt *UpdateStmt, targetMeta *catalog.TableMeta) (*Result, error) {
	// Get the FROM table metadata
	fromMeta, err := e.tm.Catalog().GetTable(stmt.FromTable)
	if err != nil {
		return nil, fmt.Errorf("FROM table %q: %w", stmt.FromTable, err)
	}

	// Build combined schema for expression evaluation
	combinedSchema := e.buildCombinedSchema(
		stmt.TableName, stmt.TableAlias, targetMeta.Schema,
		stmt.FromTable, stmt.FromAlias, fromMeta.Schema,
	)

	// Collect rows from FROM table
	var fromRows [][]catalog.Value
	err = e.scanTable(stmt.FromTable, fromMeta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		rowCopy := make([]catalog.Value, len(row))
		copy(rowCopy, row)
		fromRows = append(fromRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	var ridsToUpdate []storage.RID
	var newRows [][]catalog.Value

	// Scan target table
	err = e.scanTable(stmt.TableName, targetMeta.Schema, func(rid storage.RID, targetRow []catalog.Value) (bool, error) {
		// For each target row, find matching FROM rows
		for _, fromRow := range fromRows {
			// Combine rows for expression evaluation
			combinedRow := append(targetRow, fromRow...)

			// Apply WHERE filter on combined row
			if stmt.Where != nil {
				match, err := e.evalConditionCombined(stmt.Where, combinedSchema, combinedRow)
				if err != nil {
					return false, err
				}
				if !match {
					continue
				}
			}

			// Apply updates
			newRow := make([]catalog.Value, len(targetRow))
			copy(newRow, targetRow)

			for _, assign := range stmt.Assignments {
				col, idx := targetMeta.Schema.ColumnByName(assign.Column)
				if col == nil {
					return false, fmt.Errorf("unknown column: %s", assign.Column)
				}
				val, err := e.evalExprCombined(assign.Value, combinedSchema, combinedRow)
				if err != nil {
					return false, err
				}
				val, err = coerceValue(val, col.Type)
				if err != nil {
					return false, fmt.Errorf("column %s: %w", assign.Column, err)
				}
				newRow[idx] = val
			}

			// Validate CHECK constraints
			if err := e.validateCheckConstraints(targetMeta.Schema, newRow); err != nil {
				return false, err
			}

			ridsToUpdate = append(ridsToUpdate, rid)
			newRows = append(newRows, newRow)
			break // Only update each target row once (first match)
		}
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Delete old rows and insert new rows
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

// buildCombinedSchema creates a schema that combines two tables for join operations.
// Both qualified (table.column) and unqualified column names are supported.
// The combined schema maps both forms to the correct indices in combined rows.
func (e *Executor) buildCombinedSchema(table1, alias1 string, schema1 *catalog.Schema, table2, alias2 string, schema2 *catalog.Schema) *combinedSchema {
	combined := &combinedSchema{
		Schema: &catalog.Schema{
			Columns: make([]catalog.Column, len(schema1.Columns)+len(schema2.Columns)),
		},
		columnMap: make(map[string]int),
	}

	// Add columns from first table
	prefix1 := table1
	if alias1 != "" {
		prefix1 = alias1
	}
	for i, col := range schema1.Columns {
		newCol := col
		newCol.Name = prefix1 + "." + col.Name
		combined.Schema.Columns[i] = newCol
		// Map qualified name to index
		combined.columnMap[strings.ToUpper(prefix1+"."+col.Name)] = i
		// Also map unqualified name (may be overwritten if ambiguous)
		combined.columnMap[strings.ToUpper(col.Name)] = i
	}

	// Add columns from second table
	prefix2 := table2
	if alias2 != "" {
		prefix2 = alias2
	}
	offset := len(schema1.Columns)
	for i, col := range schema2.Columns {
		newCol := col
		newCol.Name = prefix2 + "." + col.Name
		combined.Schema.Columns[offset+i] = newCol
		// Map qualified name to index
		combined.columnMap[strings.ToUpper(prefix2+"."+col.Name)] = offset + i
		// Map unqualified name only if not ambiguous (not in schema1)
		if _, idx := schema1.ColumnByName(col.Name); idx < 0 {
			combined.columnMap[strings.ToUpper(col.Name)] = offset + i
		}
	}

	return combined
}

// combinedSchema wraps a schema with a column name to index map for efficient lookups.
type combinedSchema struct {
	Schema    *catalog.Schema
	columnMap map[string]int
}

// ColumnByName finds a column by name (supports both qualified and unqualified names).
func (cs *combinedSchema) ColumnByName(name string) (*catalog.Column, int) {
	if idx, ok := cs.columnMap[strings.ToUpper(name)]; ok {
		return &cs.Schema.Columns[idx], idx
	}
	return nil, -1
}

// evalExprCombined evaluates an expression using a combined schema.
func (e *Executor) evalExprCombined(expr Expression, cs *combinedSchema, row []catalog.Value) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		if row == nil {
			return catalog.Value{}, fmt.Errorf("cannot reference column %s without a row context", ex.Name)
		}
		col, idx := cs.ColumnByName(ex.Name)
		if col == nil {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_PLUS, TOKEN_MINUS, TOKEN_STAR, TOKEN_SLASH:
			left, err := e.evalExprCombined(ex.Left, cs, row)
			if err != nil {
				return catalog.Value{}, err
			}
			right, err := e.evalExprCombined(ex.Right, cs, row)
			if err != nil {
				return catalog.Value{}, err
			}
			return evalArithmetic(left, right, ex.Op)
		}
		return catalog.Value{}, fmt.Errorf("unsupported binary operator in expression: %v", ex.Op)

	case *FunctionExpr:
		args := make([]catalog.Value, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := e.evalExprCombined(arg, cs, row)
			if err != nil {
				return catalog.Value{}, err
			}
			args[i] = val
		}
		return evalFunction(ex.Name, args)

	default:
		// Fall back to regular evalExpr for other expression types
		return e.evalExpr(expr, cs.Schema, row)
	}
}

// evalConditionCombined evaluates a boolean expression using a combined schema.
func (e *Executor) evalConditionCombined(expr Expression, cs *combinedSchema, row []catalog.Value) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalConditionCombined(ex.Left, cs, row)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalConditionCombined(ex.Right, cs, row)

		case TOKEN_OR:
			left, err := e.evalConditionCombined(ex.Left, cs, row)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalConditionCombined(ex.Right, cs, row)

		case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
			left, err := e.evalExprCombined(ex.Left, cs, row)
			if err != nil {
				return false, err
			}
			right, err := e.evalExprCombined(ex.Right, cs, row)
			if err != nil {
				return false, err
			}
			return compareValues(left, right, ex.Op)
		}

	case *UnaryExpr:
		if ex.Op == TOKEN_NOT {
			val, err := e.evalConditionCombined(ex.Expr, cs, row)
			if err != nil {
				return false, err
			}
			return !val, nil
		}

	case *IsNullExpr:
		val, err := e.evalExprCombined(ex.Expr, cs, row)
		if err != nil {
			return false, err
		}
		isNull := val.IsNull
		if ex.Not {
			return !isNull, nil
		}
		return isNull, nil
	}

	return false, fmt.Errorf("unsupported condition type: %T", expr)
}

func (e *Executor) executeDelete(stmt *DeleteStmt) (*Result, error) {
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Handle DELETE with USING clause (PostgreSQL-style join delete)
	if stmt.UsingTable != "" {
		return e.executeDeleteWithUsing(stmt, meta)
	}

	// Simple DELETE without USING
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
		// Fetch row to check FKs
		row, err := e.tm.Fetch(stmt.TableName, rid)
		if err != nil {
			return nil, err
		}

		if err := e.checkReferencingForeignKeys(meta, row); err != nil {
			return nil, err
		}

		if err := e.tm.Delete(rid); err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) deleted.", len(ridsToDelete)),
		RowsAffected: len(ridsToDelete),
	}, nil
}

// executeDeleteWithUsing handles DELETE ... USING ... WHERE syntax (PostgreSQL style).
func (e *Executor) executeDeleteWithUsing(stmt *DeleteStmt, targetMeta *catalog.TableMeta) (*Result, error) {
	// Get the USING table metadata
	usingMeta, err := e.tm.Catalog().GetTable(stmt.UsingTable)
	if err != nil {
		return nil, fmt.Errorf("USING table %q: %w", stmt.UsingTable, err)
	}

	// Build combined schema for expression evaluation
	combinedSchema := e.buildCombinedSchema(
		stmt.TableName, stmt.TableAlias, targetMeta.Schema,
		stmt.UsingTable, stmt.UsingAlias, usingMeta.Schema,
	)

	// Collect rows from USING table
	var usingRows [][]catalog.Value
	err = e.scanTable(stmt.UsingTable, usingMeta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		rowCopy := make([]catalog.Value, len(row))
		copy(rowCopy, row)
		usingRows = append(usingRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	var ridsToDelete []storage.RID

	// Scan target table
	err = e.scanTable(stmt.TableName, targetMeta.Schema, func(rid storage.RID, targetRow []catalog.Value) (bool, error) {
		// For each target row, check against USING rows
		for _, usingRow := range usingRows {
			// Combine rows for expression evaluation
			combinedRow := append(targetRow, usingRow...)

			// Apply WHERE filter on combined row
			if stmt.Where != nil {
				match, err := e.evalConditionCombined(stmt.Where, combinedSchema, combinedRow)
				if err != nil {
					return false, err
				}
				if !match {
					continue
				}
			}

			// Match found - mark for deletion
			ridsToDelete = append(ridsToDelete, rid)
			break // Only need one match to delete the row
		}
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Actually delete the rows
	for _, rid := range ridsToDelete {
		// Fetch row to check FKs
		row, err := e.tm.Fetch(stmt.TableName, rid)
		if err != nil {
			return nil, err
		}

		if err := e.checkReferencingForeignKeys(targetMeta, row); err != nil {
			return nil, err
		}

		if err := e.tm.Delete(rid); err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) deleted.", len(ridsToDelete)),
		RowsAffected: len(ridsToDelete),
	}, nil
}

// executeMerge handles MERGE statements (SQL:2008).
// MERGE INTO target USING source ON condition
// WHEN MATCHED THEN UPDATE/DELETE
// WHEN NOT MATCHED THEN INSERT
func (e *Executor) executeMerge(stmt *MergeStmt) (*Result, error) {
	// Get target table metadata
	targetMeta, err := e.tm.Catalog().GetTable(stmt.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("target table %q does not exist: %w", stmt.TargetTable, err)
	}

	// Get source data (either from table or subquery)
	var sourceRows [][]catalog.Value
	var sourceCols []string
	var sourceSchema *catalog.Schema

	if stmt.SourceQuery != nil {
		// Execute the source query
		result, err := e.Execute(stmt.SourceQuery)
		if err != nil {
			return nil, fmt.Errorf("error executing source query: %w", err)
		}
		sourceRows = result.Rows
		sourceCols = result.Columns
		// Create a schema for the source query results
		sourceSchema = &catalog.Schema{
			Columns: make([]catalog.Column, len(sourceCols)),
		}
		for i, col := range sourceCols {
			sourceSchema.Columns[i] = catalog.Column{Name: col, Type: catalog.TypeText}
		}
	} else {
		// Get source table metadata
		sourceMeta, err := e.tm.Catalog().GetTable(stmt.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("source table %q does not exist: %w", stmt.SourceTable, err)
		}
		sourceSchema = sourceMeta.Schema
		for _, col := range sourceSchema.Columns {
			sourceCols = append(sourceCols, col.Name)
		}
		// Collect all source rows
		err = e.scanTable(stmt.SourceTable, sourceSchema, func(rid storage.RID, row []catalog.Value) (bool, error) {
			rowCopy := make([]catalog.Value, len(row))
			copy(rowCopy, row)
			sourceRows = append(sourceRows, rowCopy)
			return true, nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Build combined schema for evaluating the ON condition
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(targetMeta.Schema.Columns)+len(sourceSchema.Columns)),
	}
	colMap := make(map[string]int)

	// Add target columns
	targetAlias := stmt.TargetAlias
	if targetAlias == "" {
		targetAlias = stmt.TargetTable
	}
	for i, col := range targetMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colMap[targetAlias+"."+col.Name] = i
		colMap[col.Name] = i
	}

	// Add source columns
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

	// Track statistics
	var rowsInserted, rowsUpdated, rowsDeleted int

	// For each source row, check if it matches any target row
	for _, sourceRow := range sourceRows {
		// Collect target rows and their RIDs
		var targetRows [][]catalog.Value
		var targetRIDs []storage.RID

		err := e.scanTable(stmt.TargetTable, targetMeta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
			rowCopy := make([]catalog.Value, len(row))
			copy(rowCopy, row)
			targetRows = append(targetRows, rowCopy)
			targetRIDs = append(targetRIDs, rid)
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		foundMatch := false
		for i, targetRow := range targetRows {
			// Combine target row with source row for condition evaluation
			combinedRow := make([]catalog.Value, targetLen+len(sourceRow))
			copy(combinedRow, targetRow)
			copy(combinedRow[targetLen:], sourceRow)

			// Evaluate the ON condition
			match, err := e.evalJoinCondition(stmt.Condition, combinedSchema, combinedRow, colMap)
			if err != nil {
				return nil, fmt.Errorf("error evaluating MERGE condition: %w", err)
			}

			if match {
				foundMatch = true
				// Find and execute the appropriate WHEN MATCHED clause
				for _, when := range stmt.WhenClauses {
					if !when.Matched {
						continue
					}

					// Check optional AND condition
					if when.Condition != nil {
						condMatch, err := e.evalJoinCondition(when.Condition, combinedSchema, combinedRow, colMap)
						if err != nil {
							return nil, fmt.Errorf("error evaluating WHEN condition: %w", err)
						}
						if !condMatch {
							continue
						}
					}

					// Execute the action
					switch when.Action.ActionType {
					case "UPDATE":
						// Build new row with updated values
						newRow := make([]catalog.Value, len(targetRow))
						copy(newRow, targetRow)

						for _, assign := range when.Action.Assignments {
							// Find the column index
							colIdx := -1
							var targetCol catalog.Column
							for j, col := range targetMeta.Schema.Columns {
								if strings.EqualFold(col.Name, assign.Column) {
									colIdx = j
									targetCol = col
									break
								}
							}
							if colIdx == -1 {
								return nil, fmt.Errorf("unknown column in UPDATE: %s", assign.Column)
							}

							// Evaluate the value expression
							val, err := e.evalMergeExpr(assign.Value, combinedSchema, combinedRow, colMap)
							if err != nil {
								return nil, fmt.Errorf("error evaluating UPDATE value: %w", err)
							}
							// Coerce value to target column type
							val = e.coerceValue(val, targetCol.Type)
							newRow[colIdx] = val
						}

						// Delete old row and insert new one
						if err := e.tm.Delete(targetRIDs[i]); err != nil {
							return nil, fmt.Errorf("error deleting during MERGE UPDATE: %w", err)
						}
						if _, err := e.tm.Insert(stmt.TargetTable, newRow); err != nil {
							return nil, fmt.Errorf("error inserting during MERGE UPDATE: %w", err)
						}
						rowsUpdated++

					case "DELETE":
						if err := e.tm.Delete(targetRIDs[i]); err != nil {
							return nil, fmt.Errorf("error deleting during MERGE DELETE: %w", err)
						}
						rowsDeleted++

					case "DO NOTHING":
						// Do nothing
					}
					break // Only execute first matching WHEN clause
				}
				break // Only match first matching target row
			}
		}

		if !foundMatch {
			// Find and execute the appropriate WHEN NOT MATCHED clause
			for _, when := range stmt.WhenClauses {
				if when.Matched {
					continue
				}

				// For WHEN NOT MATCHED, evaluate condition against source row only
				if when.Condition != nil {
					// Create a row with NULL target values
					combinedRow := make([]catalog.Value, targetLen+len(sourceRow))
					for j := 0; j < targetLen; j++ {
						combinedRow[j] = catalog.Null(targetMeta.Schema.Columns[j].Type)
					}
					copy(combinedRow[targetLen:], sourceRow)

					condMatch, err := e.evalJoinCondition(when.Condition, combinedSchema, combinedRow, colMap)
					if err != nil {
						return nil, fmt.Errorf("error evaluating WHEN NOT MATCHED condition: %w", err)
					}
					if !condMatch {
						continue
					}
				}

				// Execute the action
				switch when.Action.ActionType {
				case "INSERT":
					// Build the new row
					newRow := make([]catalog.Value, len(targetMeta.Schema.Columns))

					// Create evaluation context with source values
					evalRow := make([]catalog.Value, targetLen+len(sourceRow))
					for j := 0; j < targetLen; j++ {
						evalRow[j] = catalog.Null(targetMeta.Schema.Columns[j].Type)
					}
					copy(evalRow[targetLen:], sourceRow)

					if len(when.Action.Columns) > 0 {
						// Specific columns specified
						if len(when.Action.Columns) != len(when.Action.Values) {
							return nil, fmt.Errorf("INSERT column count (%d) does not match value count (%d)",
								len(when.Action.Columns), len(when.Action.Values))
						}

						// Initialize with NULL/defaults
						for j, col := range targetMeta.Schema.Columns {
							if col.HasDefault && col.DefaultValue != nil {
								newRow[j] = *col.DefaultValue
							} else {
								newRow[j] = catalog.Null(col.Type)
							}
						}

						// Set specified columns
						for j, colName := range when.Action.Columns {
							colIdx := -1
							var targetCol catalog.Column
							for k, col := range targetMeta.Schema.Columns {
								if strings.EqualFold(col.Name, colName) {
									colIdx = k
									targetCol = col
									break
								}
							}
							if colIdx == -1 {
								return nil, fmt.Errorf("unknown column in INSERT: %s", colName)
							}

							val, err := e.evalMergeExpr(when.Action.Values[j], combinedSchema, evalRow, colMap)
							if err != nil {
								return nil, fmt.Errorf("error evaluating INSERT value: %w", err)
							}
							// Coerce value to target column type
							val = e.coerceValue(val, targetCol.Type)
							newRow[colIdx] = val
						}
					} else {
						// All columns (values must match target column count)
						if len(when.Action.Values) != len(targetMeta.Schema.Columns) {
							return nil, fmt.Errorf("INSERT value count (%d) does not match target column count (%d)",
								len(when.Action.Values), len(targetMeta.Schema.Columns))
						}

						for j, valExpr := range when.Action.Values {
							val, err := e.evalMergeExpr(valExpr, combinedSchema, evalRow, colMap)
							if err != nil {
								return nil, fmt.Errorf("error evaluating INSERT value: %w", err)
							}
							// Coerce value to target column type
							val = e.coerceValue(val, targetMeta.Schema.Columns[j].Type)
							newRow[j] = val
						}
					}

					if _, err := e.tm.Insert(stmt.TargetTable, newRow); err != nil {
						return nil, fmt.Errorf("error inserting during MERGE INSERT: %w", err)
					}
					rowsInserted++

				case "DO NOTHING":
					// Do nothing
				}
				break // Only execute first matching WHEN clause
			}
		}
	}

	return &Result{
		Message:      fmt.Sprintf("MERGE: %d row(s) inserted, %d row(s) updated, %d row(s) deleted.", rowsInserted, rowsUpdated, rowsDeleted),
		RowsAffected: rowsInserted + rowsUpdated + rowsDeleted,
	}, nil
}

// evalMergeExpr evaluates an expression in the context of a MERGE statement.
func (e *Executor) evalMergeExpr(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (catalog.Value, error) {
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
		left, err := e.evalMergeExpr(ex.Left, schema, row, colMap)
		if err != nil {
			return catalog.Value{}, err
		}
		right, err := e.evalMergeExpr(ex.Right, schema, row, colMap)
		if err != nil {
			return catalog.Value{}, err
		}
		return e.evalBinaryExprValue(left, ex.Op, right)

	case *FunctionExpr:
		// Evaluate function arguments
		args := make([]catalog.Value, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := e.evalMergeExpr(arg, schema, row, colMap)
			if err != nil {
				return catalog.Value{}, err
			}
			args[i] = val
		}
		return e.evalFunctionCallValue(ex.Name, args)

	default:
		return catalog.Value{}, fmt.Errorf("unsupported expression type in MERGE: %T", expr)
	}
}

// evalBinaryExprValue evaluates a binary expression with values.
func (e *Executor) evalBinaryExprValue(left catalog.Value, op TokenType, right catalog.Value) (catalog.Value, error) {
	// Get numeric values
	leftNum := e.valueToInt64(left)
	rightNum := e.valueToInt64(right)

	var result int64
	switch op {
	case TOKEN_PLUS:
		result = leftNum + rightNum

	case TOKEN_MINUS:
		result = leftNum - rightNum

	case TOKEN_STAR:
		result = leftNum * rightNum

	case TOKEN_SLASH:
		if rightNum == 0 {
			return catalog.Value{}, fmt.Errorf("division by zero")
		}
		result = leftNum / rightNum

	default:
		return catalog.Value{}, fmt.Errorf("unsupported binary operator in MERGE: %v", op)
	}

	// If both operands are INT32 and the result fits, return INT32
	if left.Type == catalog.TypeInt32 && right.Type == catalog.TypeInt32 &&
		result >= -2147483648 && result <= 2147483647 {
		return catalog.NewInt32(int32(result)), nil
	}

	return catalog.NewInt64(result), nil
}

// valueToInt64 extracts an int64 from a Value.
func (e *Executor) valueToInt64(v catalog.Value) int64 {
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

// coerceValue attempts to convert a value to the target type.
func (e *Executor) coerceValue(v catalog.Value, targetType catalog.DataType) catalog.Value {
	if v.IsNull {
		return catalog.Null(targetType)
	}

	// If already the same type, return as-is
	if v.Type == targetType {
		return v
	}

	// Handle numeric conversions
	switch targetType {
	case catalog.TypeInt32:
		// Convert Int64 to Int32 if it fits
		if v.Type == catalog.TypeInt64 {
			if v.Int64 >= -2147483648 && v.Int64 <= 2147483647 {
				return catalog.NewInt32(int32(v.Int64))
			}
			// Truncate if doesn't fit (could also error here)
			return catalog.NewInt32(int32(v.Int64))
		}
	case catalog.TypeInt64:
		// Convert Int32 to Int64
		if v.Type == catalog.TypeInt32 {
			return catalog.NewInt64(int64(v.Int32))
		}
	}

	// Return original value if no conversion applies
	return v
}

// evalFunctionCallValue evaluates a function call with provided arguments.
func (e *Executor) evalFunctionCallValue(name string, args []catalog.Value) (catalog.Value, error) {
	switch strings.ToUpper(name) {
	case "COALESCE":
		for _, arg := range args {
			if !arg.IsNull {
				return arg, nil
			}
		}
		return catalog.Null(catalog.TypeText), nil

	case "UPPER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("UPPER requires 1 argument")
		}
		if args[0].Type == catalog.TypeText {
			return catalog.NewText(strings.ToUpper(args[0].Text)), nil
		}
		return args[0], nil

	case "LOWER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LOWER requires 1 argument")
		}
		if args[0].Type == catalog.TypeText {
			return catalog.NewText(strings.ToLower(args[0].Text)), nil
		}
		return args[0], nil

	default:
		return catalog.Value{}, fmt.Errorf("unsupported function in MERGE: %s", name)
	}
}

// executeAlter handles ALTER TABLE statements.
func (e *Executor) executeAlter(stmt *AlterTableStmt) (*Result, error) {
	meta, err := e.tm.GetTableMeta(stmt.TableName)
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
		// Also update meta.Columns since that's what gets persisted
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.tm.UpdateTableMeta(meta); err != nil {
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
		// Persist updated metadata
		if err := e.tm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q dropped from table %q.", stmt.ColumnName, stmt.TableName)}, nil

	case "RENAME TO":
		// Rename the table
		if err := e.tm.RenameTable(stmt.TableName, stmt.NewName); err != nil {
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
		// Persist updated metadata
		if err := e.tm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q renamed to %q in table %q.", stmt.ColumnName, stmt.NewName, stmt.TableName)}, nil

	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action: %s", stmt.Action)
	}
}

// executeTruncate handles TRUNCATE TABLE statements.
func (e *Executor) executeTruncate(stmt *TruncateTableStmt) (*Result, error) {
	// Verify table exists
	_, err := e.tm.GetTableMeta(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	// Truncate the table (delete all rows)
	count, err := e.tm.TruncateTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate table: %w", err)
	}

	return &Result{
		Message:      fmt.Sprintf("Table %q truncated. %d row(s) deleted.", stmt.TableName, count),
		RowsAffected: count,
	}, nil
}

// executeShow handles SHOW statements.
func (e *Executor) executeShow(stmt *ShowStmt) (*Result, error) {
	switch stmt.ShowType {
	case "TABLES":
		// List all tables
		tables := e.tm.ListTables()
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
		meta, err := e.tm.GetTableMeta(stmt.TableName)
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

// generateCreateTableDDL generates a CREATE TABLE statement from table metadata.
func generateCreateTableDDL(meta *catalog.TableMeta) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(meta.Name)
	sb.WriteString(" (\n")

	for i, col := range meta.Schema.Columns {
		sb.WriteString("  ")
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(col.Type.String())
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
		if col.HasDefault && col.DefaultValue != nil {
			sb.WriteString(" DEFAULT ")
			if col.DefaultValue.IsNull {
				sb.WriteString("NULL")
			} else {
				switch col.DefaultValue.Type {
				case catalog.TypeText:
					sb.WriteString("'")
					sb.WriteString(col.DefaultValue.Text)
					sb.WriteString("'")
				case catalog.TypeInt32:
					sb.WriteString(fmt.Sprintf("%d", col.DefaultValue.Int32))
				case catalog.TypeInt64:
					sb.WriteString(fmt.Sprintf("%d", col.DefaultValue.Int64))
				case catalog.TypeBool:
					if col.DefaultValue.Bool {
						sb.WriteString("TRUE")
					} else {
						sb.WriteString("FALSE")
					}
				default:
					sb.WriteString(fmt.Sprintf("%v", col.DefaultValue))
				}
			}
		}
		if col.AutoIncrement {
			sb.WriteString(" AUTO_INCREMENT")
		}
		if col.PrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		if i < len(meta.Schema.Columns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	sb.WriteString(")")
	if meta.StorageType == "COLUMN" || meta.StorageType == "column" {
		sb.WriteString(" USING COLUMN")
	}
	sb.WriteString(";")
	return sb.String()
}

// scanTable scans all rows in a table and calls fn for each.
// This is a simple sequential scan implementation.
func (e *Executor) scanTable(tableName string, _ *catalog.Schema, fn func(rid storage.RID, row []catalog.Value) (bool, error)) error {
	// Check for information_schema tables
	if strings.HasPrefix(strings.ToLower(tableName), "information_schema.") {
		return e.scanInformationSchema(tableName, fn)
	}

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

	case *SubqueryExpr:
		// Scalar subquery - must return exactly one row with one column
		result, err := e.executeSelect(ex.Query)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("scalar subquery error: %v", err)
		}
		if len(result.Rows) == 0 {
			return catalog.Null(catalog.TypeUnknown), nil // Empty result returns NULL
		}
		if len(result.Rows) > 1 {
			return catalog.Value{}, fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) != 1 {
			return catalog.Value{}, fmt.Errorf("scalar subquery must return exactly one column, got %d", len(result.Rows[0]))
		}
		return result.Rows[0][0], nil

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
			// Execute the subquery
			result, err := e.executeSelect(ex.Subquery)
			if err != nil {
				return false, fmt.Errorf("IN subquery error: %v", err)
			}
			// Subquery must return single column
			if len(result.Columns) != 1 {
				return false, fmt.Errorf("IN subquery must return exactly one column, got %d", len(result.Columns))
			}
			// Check if leftVal is in the subquery results
			for _, subRow := range result.Rows {
				if len(subRow) > 0 && !subRow[0].IsNull {
					eq, err := compareValues(leftVal, subRow[0], TOKEN_EQ)
					if err != nil {
						return false, err
					}
					if eq {
						found = true
						break
					}
				}
			}
		} else {
			// Value list IN
			for _, valExpr := range ex.Values {
				rightVal, err := e.evalExpr(valExpr, schema, row)
				if err != nil {
					return false, err
				}
				if rightVal.IsNull {
					continue // Skip NULL values in the list
				}
				eq, err := compareValues(leftVal, rightVal, TOKEN_EQ)
				if err != nil {
					return false, err
				}
				if eq {
					found = true
					break
				}
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
		geLow, err := compareValues(val, lowVal, TOKEN_GE)
		if err != nil {
			return false, err
		}
		leHigh, err := compareValues(val, highVal, TOKEN_LE)
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
		// Execute the subquery and check if it returns exactly one scalar value
		result, err := e.executeSelect(ex.Query)
		if err != nil {
			return false, fmt.Errorf("subquery error: %v", err)
		}
		// For scalar subqueries in boolean context, check if result is non-empty
		if len(result.Rows) == 0 {
			return false, nil
		}
		// If single column, single row, try to interpret as boolean
		if len(result.Rows) == 1 && len(result.Rows[0]) == 1 {
			val := result.Rows[0][0]
			if val.Type == catalog.TypeBool {
				return val.Bool, nil
			}
		}
		// Non-empty result in boolean context is true
		return true, nil

	case *ExistsExpr:
		// Execute the subquery and check if it returns any rows
		result, err := e.executeSelect(ex.Query)
		if err != nil {
			return false, fmt.Errorf("EXISTS subquery error: %v", err)
		}
		exists := len(result.Rows) > 0
		if ex.Not {
			return !exists, nil
		}
		return exists, nil

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
		// NULL compared to anything is always false (use IS NULL for null checks)
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

// sortRows sorts rows in place based on ORDER BY clauses.
func (e *Executor) sortRows(rows [][]catalog.Value, orderBy []OrderByClause, orderByIndices []int) {
	if len(rows) <= 1 || len(orderBy) == 0 {
		return
	}

	// Use sort.Slice for stable multi-column sorting
	sort.SliceStable(rows, func(i, j int) bool {
		for k, ob := range orderBy {
			idx := orderByIndices[k]
			left := rows[i][idx]
			right := rows[j][idx]

			cmp := compareValuesForSort(left, right)
			if cmp == 0 {
				continue // equal, check next column
			}

			if ob.Desc {
				return cmp > 0 // descending
			}
			return cmp < 0 // ascending
		}
		return false // all columns equal
	})
}

// compareValuesForSort compares two values for sorting.
// Returns -1 if left < right, 0 if equal, 1 if left > right.
// NULLs are sorted last (greater than any non-NULL value).
func compareValuesForSort(left, right catalog.Value) int {
	// Handle NULLs - NULLs sort last
	if left.IsNull && right.IsNull {
		return 0
	}
	if left.IsNull {
		return 1 // NULL is greater (sorted last)
	}
	if right.IsNull {
		return -1
	}

	// Compare based on type
	switch left.Type {
	case catalog.TypeInt32:
		r := right
		if right.Type == catalog.TypeInt64 {
			// Compare as int64
			if int64(left.Int32) < right.Int64 {
				return -1
			} else if int64(left.Int32) > right.Int64 {
				return 1
			}
			return 0
		}
		if left.Int32 < r.Int32 {
			return -1
		} else if left.Int32 > r.Int32 {
			return 1
		}
		return 0

	case catalog.TypeInt64:
		r := right
		if right.Type == catalog.TypeInt32 {
			// Compare as int64
			if left.Int64 < int64(right.Int32) {
				return -1
			} else if left.Int64 > int64(right.Int32) {
				return 1
			}
			return 0
		}
		if left.Int64 < r.Int64 {
			return -1
		} else if left.Int64 > r.Int64 {
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

// matchLikePattern matches a string against a SQL LIKE pattern.
// % matches zero or more characters, _ matches exactly one character.
// If caseInsensitive is true, the match is case-insensitive (ILIKE).
func matchLikePattern(s, pattern string, caseInsensitive bool) bool {
	if caseInsensitive {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}

	// Convert SQL LIKE pattern to a simple state machine match
	// % = match any sequence of characters (including empty)
	// _ = match exactly one character
	return matchLikeHelper(s, pattern)
}

// matchLikeHelper performs the actual LIKE pattern matching.
func matchLikeHelper(s, pattern string) bool {
	si, pi := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == s[si]) {
			// Single character match or _ wildcard
			si++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			// % wildcard - remember position and try to match 0 characters first
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			// No match, but we have a previous % - backtrack
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			// No match and no % to backtrack to
			return false
		}
	}

	// Check remaining pattern (should only be % characters)
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}

	return pi == len(pattern)
}

// evalArithmetic evaluates an arithmetic operation on two values.
func evalArithmetic(left, right catalog.Value, op TokenType) (catalog.Value, error) {
	if left.IsNull || right.IsNull {
		return catalog.Null(catalog.TypeInt64), nil // NULL arithmetic returns NULL
	}

	// Coerce to the larger numeric type
	var leftInt, rightInt int64
	var isInt bool

	switch left.Type {
	case catalog.TypeInt32:
		leftInt = int64(left.Int32)
		isInt = true
	case catalog.TypeInt64:
		leftInt = left.Int64
		isInt = true
	default:
		return catalog.Value{}, fmt.Errorf("arithmetic not supported for type: %v", left.Type)
	}

	switch right.Type {
	case catalog.TypeInt32:
		rightInt = int64(right.Int32)
	case catalog.TypeInt64:
		rightInt = right.Int64
	default:
		return catalog.Value{}, fmt.Errorf("arithmetic not supported for type: %v", right.Type)
	}

	if !isInt {
		return catalog.Value{}, fmt.Errorf("arithmetic requires numeric operands")
	}

	var result int64
	switch op {
	case TOKEN_PLUS:
		result = leftInt + rightInt
	case TOKEN_MINUS:
		result = leftInt - rightInt
	case TOKEN_STAR:
		result = leftInt * rightInt
	case TOKEN_SLASH:
		if rightInt == 0 {
			return catalog.Value{}, fmt.Errorf("division by zero")
		}
		result = leftInt / rightInt
	default:
		return catalog.Value{}, fmt.Errorf("unknown arithmetic operator: %v", op)
	}

	return catalog.NewInt64(result), nil
}

// evalFunction evaluates a function call expression.
func evalFunction(name string, args []catalog.Value) (catalog.Value, error) {
	switch strings.ToUpper(name) {
	case "COALESCE":
		// COALESCE returns the first non-NULL argument
		for _, arg := range args {
			if !arg.IsNull {
				return arg, nil
			}
		}
		return catalog.Null(catalog.TypeUnknown), nil

	case "NULLIF":
		// NULLIF(a, b) returns NULL if a = b, otherwise returns a
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("NULLIF requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return args[0], nil
		}
		// Compare the values
		eq, _ := compareValues(args[0], args[1], TOKEN_EQ)
		if eq {
			return catalog.Null(args[0].Type), nil
		}
		return args[0], nil

	case "UPPER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("UPPER requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("UPPER requires text argument")
		}
		return catalog.NewText(strings.ToUpper(args[0].Text)), nil

	case "LOWER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LOWER requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LOWER requires text argument")
		}
		return catalog.NewText(strings.ToLower(args[0].Text)), nil

	case "LENGTH", "LEN":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LENGTH requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LENGTH requires text argument")
		}
		return catalog.NewInt64(int64(len(args[0].Text))), nil

	case "CONCAT":
		var result strings.Builder
		for _, arg := range args {
			if arg.IsNull {
				continue // CONCAT skips NULLs (like MySQL, unlike standard SQL)
			}
			if arg.Type != catalog.TypeText {
				return catalog.Value{}, fmt.Errorf("CONCAT requires text arguments")
			}
			result.WriteString(arg.Text)
		}
		return catalog.NewText(result.String()), nil

	case "SUBSTR", "SUBSTRING":
		// SUBSTR(str, start) or SUBSTR(str, start, length)
		if len(args) < 2 || len(args) > 3 {
			return catalog.Value{}, fmt.Errorf("SUBSTR requires 2 or 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("SUBSTR first argument must be text")
		}

		str := args[0].Text
		var start int64
		switch args[1].Type {
		case catalog.TypeInt32:
			start = int64(args[1].Int32)
		case catalog.TypeInt64:
			start = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("SUBSTR start must be integer")
		}

		// SQL uses 1-based indexing
		start-- // convert to 0-based
		if start < 0 {
			start = 0
		}
		if start >= int64(len(str)) {
			return catalog.NewText(""), nil
		}

		if len(args) == 3 {
			var length int64
			switch args[2].Type {
			case catalog.TypeInt32:
				length = int64(args[2].Int32)
			case catalog.TypeInt64:
				length = args[2].Int64
			default:
				return catalog.Value{}, fmt.Errorf("SUBSTR length must be integer")
			}
			if length < 0 {
				length = 0
			}
			end := start + length
			if end > int64(len(str)) {
				end = int64(len(str))
			}
			return catalog.NewText(str[start:end]), nil
		}

		return catalog.NewText(str[start:]), nil

	case "NOW", "CURRENT_TIMESTAMP":
		// Return current timestamp
		now := time.Now()
		return catalog.NewTimestamp(now), nil

	case "CURRENT_DATE":
		// Return current date (timestamp with time set to midnight)
		now := time.Now()
		date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return catalog.NewTimestamp(date), nil

	case "YEAR":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("YEAR requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("YEAR requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Year())), nil

	case "MONTH":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("MONTH requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("MONTH requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Month())), nil

	case "DAY":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("DAY requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("DAY requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Day())), nil

	case "HOUR":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("HOUR requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("HOUR requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Hour())), nil

	case "MINUTE":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("MINUTE requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("MINUTE requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Minute())), nil

	case "SECOND":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("SECOND requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("SECOND requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Second())), nil

	case "DATE_ADD":
		// DATE_ADD(date, interval_value, interval_unit)
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("DATE_ADD requires 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeTimestamp), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("DATE_ADD first argument must be timestamp")
		}
		if args[1].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("DATE_ADD interval arguments must be text")
		}
		interval, err := strconv.ParseInt(args[1].Text, 10, 64)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("DATE_ADD interval value must be integer: %w", err)
		}
		ts := args[0].Timestamp
		switch strings.ToUpper(args[2].Text) {
		case "YEAR":
			ts = ts.AddDate(int(interval), 0, 0)
		case "MONTH":
			ts = ts.AddDate(0, int(interval), 0)
		case "DAY":
			ts = ts.AddDate(0, 0, int(interval))
		case "HOUR":
			ts = ts.Add(time.Duration(interval) * time.Hour)
		case "MINUTE":
			ts = ts.Add(time.Duration(interval) * time.Minute)
		case "SECOND":
			ts = ts.Add(time.Duration(interval) * time.Second)
		default:
			return catalog.Value{}, fmt.Errorf("DATE_ADD unknown interval unit: %s", args[2].Text)
		}
		return catalog.NewTimestamp(ts), nil

	case "DATE_SUB":
		// DATE_SUB(date, interval_value, interval_unit) - same as DATE_ADD but subtract
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("DATE_SUB requires 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeTimestamp), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("DATE_SUB first argument must be timestamp")
		}
		if args[1].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("DATE_SUB interval arguments must be text")
		}
		interval, err := strconv.ParseInt(args[1].Text, 10, 64)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("DATE_SUB interval value must be integer: %w", err)
		}
		ts := args[0].Timestamp
		switch strings.ToUpper(args[2].Text) {
		case "YEAR":
			ts = ts.AddDate(-int(interval), 0, 0)
		case "MONTH":
			ts = ts.AddDate(0, -int(interval), 0)
		case "DAY":
			ts = ts.AddDate(0, 0, -int(interval))
		case "HOUR":
			ts = ts.Add(-time.Duration(interval) * time.Hour)
		case "MINUTE":
			ts = ts.Add(-time.Duration(interval) * time.Minute)
		case "SECOND":
			ts = ts.Add(-time.Duration(interval) * time.Second)
		default:
			return catalog.Value{}, fmt.Errorf("DATE_SUB unknown interval unit: %s", args[2].Text)
		}
		return catalog.NewTimestamp(ts), nil

	case "EXTRACT":
		// EXTRACT(part, date) - part is a string like "YEAR", "MONTH", etc.
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("EXTRACT requires 2 arguments")
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("EXTRACT first argument must be text (part name)")
		}
		if args[1].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[1].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("EXTRACT second argument must be timestamp")
		}
		ts := args[1].Timestamp
		switch strings.ToUpper(args[0].Text) {
		case "YEAR":
			return catalog.NewInt32(int32(ts.Year())), nil
		case "MONTH":
			return catalog.NewInt32(int32(ts.Month())), nil
		case "DAY":
			return catalog.NewInt32(int32(ts.Day())), nil
		case "HOUR":
			return catalog.NewInt32(int32(ts.Hour())), nil
		case "MINUTE":
			return catalog.NewInt32(int32(ts.Minute())), nil
		case "SECOND":
			return catalog.NewInt32(int32(ts.Second())), nil
		default:
			return catalog.Value{}, fmt.Errorf("EXTRACT unknown part: %s", args[0].Text)
		}

	// Math functions
	case "ABS":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("ABS requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		switch args[0].Type {
		case catalog.TypeInt32:
			v := args[0].Int32
			if v < 0 {
				v = -v
			}
			return catalog.NewInt32(v), nil
		case catalog.TypeInt64:
			v := args[0].Int64
			if v < 0 {
				v = -v
			}
			return catalog.NewInt64(v), nil
		default:
			return catalog.Value{}, fmt.Errorf("ABS requires numeric argument")
		}

	case "ROUND":
		if len(args) < 1 || len(args) > 2 {
			return catalog.Value{}, fmt.Errorf("ROUND requires 1 or 2 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		// For integers, ROUND just returns the value
		switch args[0].Type {
		case catalog.TypeInt32:
			return args[0], nil
		case catalog.TypeInt64:
			return args[0], nil
		default:
			return catalog.Value{}, fmt.Errorf("ROUND requires numeric argument")
		}

	case "FLOOR":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("FLOOR requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		// For integers, FLOOR just returns the value
		switch args[0].Type {
		case catalog.TypeInt32:
			return args[0], nil
		case catalog.TypeInt64:
			return args[0], nil
		default:
			return catalog.Value{}, fmt.Errorf("FLOOR requires numeric argument")
		}

	case "CEIL", "CEILING":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("CEIL requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		// For integers, CEIL just returns the value
		switch args[0].Type {
		case catalog.TypeInt32:
			return args[0], nil
		case catalog.TypeInt64:
			return args[0], nil
		default:
			return catalog.Value{}, fmt.Errorf("CEIL requires numeric argument")
		}

	case "MOD":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("MOD requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		var a, b int64
		switch args[0].Type {
		case catalog.TypeInt32:
			a = int64(args[0].Int32)
		case catalog.TypeInt64:
			a = args[0].Int64
		default:
			return catalog.Value{}, fmt.Errorf("MOD requires numeric arguments")
		}
		switch args[1].Type {
		case catalog.TypeInt32:
			b = int64(args[1].Int32)
		case catalog.TypeInt64:
			b = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("MOD requires numeric arguments")
		}
		if b == 0 {
			return catalog.Value{}, fmt.Errorf("MOD division by zero")
		}
		return catalog.NewInt64(a % b), nil

	case "POWER", "POW":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("POWER requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		var base, exp int64
		switch args[0].Type {
		case catalog.TypeInt32:
			base = int64(args[0].Int32)
		case catalog.TypeInt64:
			base = args[0].Int64
		default:
			return catalog.Value{}, fmt.Errorf("POWER requires numeric arguments")
		}
		switch args[1].Type {
		case catalog.TypeInt32:
			exp = int64(args[1].Int32)
		case catalog.TypeInt64:
			exp = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("POWER requires numeric arguments")
		}
		result := int64(1)
		for i := int64(0); i < exp; i++ {
			result *= base
		}
		return catalog.NewInt64(result), nil

	case "SQRT":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("SQRT requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		var val int64
		switch args[0].Type {
		case catalog.TypeInt32:
			val = int64(args[0].Int32)
		case catalog.TypeInt64:
			val = args[0].Int64
		default:
			return catalog.Value{}, fmt.Errorf("SQRT requires numeric argument")
		}
		if val < 0 {
			return catalog.Value{}, fmt.Errorf("SQRT of negative number")
		}
		// Integer square root
		result := int64(0)
		for result*result <= val {
			result++
		}
		return catalog.NewInt64(result - 1), nil

	// String functions
	case "TRIM":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("TRIM requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("TRIM requires text argument")
		}
		return catalog.NewText(strings.TrimSpace(args[0].Text)), nil

	case "LTRIM":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LTRIM requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LTRIM requires text argument")
		}
		return catalog.NewText(strings.TrimLeft(args[0].Text, " \t\n\r")), nil

	case "RTRIM":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("RTRIM requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("RTRIM requires text argument")
		}
		return catalog.NewText(strings.TrimRight(args[0].Text, " \t\n\r")), nil

	case "REPLACE":
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("REPLACE requires exactly 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText || args[1].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("REPLACE requires text arguments")
		}
		return catalog.NewText(strings.ReplaceAll(args[0].Text, args[1].Text, args[2].Text)), nil

	case "POSITION":
		// POSITION(substr, str) returns 1-based position, 0 if not found
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("POSITION requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeText || args[1].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("POSITION requires text arguments")
		}
		idx := strings.Index(args[1].Text, args[0].Text)
		if idx == -1 {
			return catalog.NewInt32(0), nil
		}
		return catalog.NewInt32(int32(idx + 1)), nil

	case "REVERSE":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("REVERSE requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("REVERSE requires text argument")
		}
		runes := []rune(args[0].Text)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return catalog.NewText(string(runes)), nil

	case "REPEAT":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("REPEAT requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("REPEAT first argument must be text")
		}
		var count int64
		switch args[1].Type {
		case catalog.TypeInt32:
			count = int64(args[1].Int32)
		case catalog.TypeInt64:
			count = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("REPEAT second argument must be integer")
		}
		if count < 0 {
			count = 0
		}
		return catalog.NewText(strings.Repeat(args[0].Text, int(count))), nil

	case "LPAD":
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("LPAD requires exactly 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LPAD requires text arguments")
		}
		var length int64
		switch args[1].Type {
		case catalog.TypeInt32:
			length = int64(args[1].Int32)
		case catalog.TypeInt64:
			length = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("LPAD length must be integer")
		}
		str := args[0].Text
		pad := args[2].Text
		if len(pad) == 0 {
			return catalog.NewText(str), nil
		}
		for int64(len(str)) < length {
			str = pad + str
		}
		if int64(len(str)) > length {
			str = str[int64(len(str))-length:]
		}
		return catalog.NewText(str), nil

	case "RPAD":
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("RPAD requires exactly 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("RPAD requires text arguments")
		}
		var length int64
		switch args[1].Type {
		case catalog.TypeInt32:
			length = int64(args[1].Int32)
		case catalog.TypeInt64:
			length = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("RPAD length must be integer")
		}
		str := args[0].Text
		pad := args[2].Text
		if len(pad) == 0 {
			return catalog.NewText(str), nil
		}
		for int64(len(str)) < length {
			str = str + pad
		}
		if int64(len(str)) > length {
			str = str[:length]
		}
		return catalog.NewText(str), nil

	default:
		return catalog.Value{}, fmt.Errorf("unknown function: %s", name)
	}
}

// evalCastExpr evaluates a CAST expression.
func (e *Executor) evalCastExpr(cast *CastExpr, schema *catalog.Schema, row []catalog.Value) (catalog.Value, error) {
	val, err := e.evalExpr(cast.Expr, schema, row)
	if err != nil {
		return catalog.Value{}, err
	}

	if val.IsNull {
		return catalog.Null(cast.TargetType), nil
	}

	// Attempt to coerce to target type
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
			// Try common formats
			formats := []string{
				time.RFC3339,
				"2006-01-02 15:04:05",
				"2006-01-02",
			}
			for _, f := range formats {
				if t, err := time.Parse(f, val.Text); err == nil {
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

// executeExplain executes an EXPLAIN statement
func (e *Executor) executeExplain(stmt *ExplainStmt) (*Result, error) {
	selectStmt, ok := stmt.Statement.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("EXPLAIN only supports SELECT statements")
	}

	// Get table metadata to validate the table exists
	meta, err := e.tm.Catalog().GetTable(selectStmt.TableName)
	if err != nil {
		return nil, err
	}

	// Create a planner with nil index manager (basic planning only)
	planner := NewPlanner(nil)
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
		result, err := e.executeSelect(selectStmt)
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
func (e *Executor) evalCaseExpr(caseExpr *CaseExpr, schema *catalog.Schema, row []catalog.Value) (catalog.Value, error) {
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
			equal, err := compareValues(operandVal, whenVal, TOKEN_EQ)
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

// exprToString converts an Expression AST node to a SQL string representation.
// This is used to serialize CHECK constraints for storage.
func exprToString(expr Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *LiteralExpr:
		switch e.Value.Type {
		case catalog.TypeText:
			return fmt.Sprintf("'%s'", e.Value.Text)
		case catalog.TypeInt32:
			return fmt.Sprintf("%d", e.Value.Int32)
		case catalog.TypeInt64:
			return fmt.Sprintf("%d", e.Value.Int64)
		case catalog.TypeBool:
			if e.Value.Bool {
				return "TRUE"
			}
			return "FALSE"
		default:
			if e.Value.IsNull {
				return "NULL"
			}
			return fmt.Sprintf("%v", e.Value)
		}

	case *ColumnRef:
		return e.Name

	case *BinaryExpr:
		left := exprToString(e.Left)
		right := exprToString(e.Right)
		op := tokenToOperator(e.Op)
		return fmt.Sprintf("(%s %s %s)", left, op, right)

	case *UnaryExpr:
		operand := exprToString(e.Expr)
		if e.Op == TOKEN_NOT {
			return fmt.Sprintf("NOT %s", operand)
		}
		return fmt.Sprintf("-%s", operand)

	case *InExpr:
		left := exprToString(e.Left)
		values := make([]string, len(e.Values))
		for i, v := range e.Values {
			values[i] = exprToString(v)
		}
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("%s %sIN (%s)", left, not, strings.Join(values, ", "))

	case *BetweenExpr:
		val := exprToString(e.Expr)
		low := exprToString(e.Low)
		high := exprToString(e.High)
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("%s %sBETWEEN %s AND %s", val, not, low, high)

	case *LikeExpr:
		left := exprToString(e.Expr)
		pattern := exprToString(e.Pattern)
		op := "LIKE"
		if e.CaseInsensitive {
			op = "ILIKE"
		}
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("%s %s%s %s", left, not, op, pattern)

	case *FunctionExpr:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToString(arg)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))

	case *CaseExpr:
		var sb strings.Builder
		sb.WriteString("CASE")
		if e.Operand != nil {
			sb.WriteString(" ")
			sb.WriteString(exprToString(e.Operand))
		}
		for _, when := range e.Whens {
			sb.WriteString(" WHEN ")
			sb.WriteString(exprToString(when.Condition))
			sb.WriteString(" THEN ")
			sb.WriteString(exprToString(when.Result))
		}
		if e.Else != nil {
			sb.WriteString(" ELSE ")
			sb.WriteString(exprToString(e.Else))
		}
		sb.WriteString(" END")
		return sb.String()

	default:
		return "?"
	}
}

// tokenToOperator converts a token type to its SQL string representation.
func tokenToOperator(op TokenType) string {
	switch op {
	case TOKEN_EQ:
		return "="
	case TOKEN_NE:
		return "<>"
	case TOKEN_LT:
		return "<"
	case TOKEN_LE:
		return "<="
	case TOKEN_GT:
		return ">"
	case TOKEN_GE:
		return ">="
	case TOKEN_AND:
		return "AND"
	case TOKEN_OR:
		return "OR"
	case TOKEN_PLUS:
		return "+"
	case TOKEN_MINUS:
		return "-"
	case TOKEN_STAR:
		return "*"
	case TOKEN_SLASH:
		return "/"
	default:
		return "?"
	}
}

// validateCheckConstraints validates all CHECK constraints for a row.
func (e *Executor) validateCheckConstraints(schema *catalog.Schema, values []catalog.Value) error {
	for i, col := range schema.Columns {
		if col.CheckExpr == "" {
			continue
		}

		// Parse the CHECK expression directly as a condition
		// We wrap it in a dummy SELECT to reuse the parser, but with a real column name
		// Actually, let's just parse the expression directly
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

// executeCreateView creates a new view.
func (e *Executor) executeCreateView(stmt *CreateViewStmt) (*Result, error) {
	e.viewsMu.Lock()
	defer e.viewsMu.Unlock()

	// Check if view already exists
	if _, exists := e.views[stmt.ViewName]; exists {
		if stmt.OrReplace {
			// Replace existing view
			delete(e.views, stmt.ViewName)
		} else {
			return nil, fmt.Errorf("view '%s' already exists", stmt.ViewName)
		}
	}

	// Validate the SELECT query by executing it (dry run to check for errors)
	// We don't actually need the result, just validation
	_, err := e.executeSelect(stmt.Query)
	if err != nil {
		return nil, fmt.Errorf("invalid view query: %w", err)
	}

	// Store the view definition
	e.views[stmt.ViewName] = &ViewDef{
		Name:    stmt.ViewName,
		Columns: stmt.Columns,
		Query:   stmt.Query,
	}

	return &Result{Message: fmt.Sprintf("View '%s' created.", stmt.ViewName)}, nil
}

// executeDropView drops a view.
func (e *Executor) executeDropView(stmt *DropViewStmt) (*Result, error) {
	e.viewsMu.Lock()
	defer e.viewsMu.Unlock()

	if _, exists := e.views[stmt.ViewName]; !exists {
		if stmt.IfExists {
			return &Result{Message: fmt.Sprintf("View '%s' does not exist (IF EXISTS specified).", stmt.ViewName)}, nil
		}
		return nil, fmt.Errorf("view '%s' does not exist", stmt.ViewName)
	}

	delete(e.views, stmt.ViewName)
	return &Result{Message: fmt.Sprintf("View '%s' dropped.", stmt.ViewName)}, nil
}

// executeSelectFromView executes a SELECT from a view by expanding the view definition.
func (e *Executor) executeSelectFromView(outerStmt *SelectStmt, viewDef *ViewDef) (*Result, error) {
	// First, execute the view's underlying query
	viewResult, err := e.executeSelect(viewDef.Query)
	if err != nil {
		return nil, fmt.Errorf("error executing view '%s': %w", viewDef.Name, err)
	}

	// If view has column aliases, apply them
	if len(viewDef.Columns) > 0 {
		if len(viewDef.Columns) != len(viewResult.Columns) {
			return nil, fmt.Errorf("view '%s' column count mismatch: %d aliases for %d columns",
				viewDef.Name, len(viewDef.Columns), len(viewResult.Columns))
		}
		copy(viewResult.Columns, viewDef.Columns)
	}

	// If the outer query is SELECT * FROM view, just return the view result
	if len(outerStmt.Columns) == 1 && outerStmt.Columns[0].Star &&
		outerStmt.Where == nil && len(outerStmt.OrderBy) == 0 &&
		outerStmt.Limit == nil {
		return viewResult, nil
	}

	// Build a schema from the view result
	viewSchema := &catalog.Schema{
		Columns: make([]catalog.Column, len(viewResult.Columns)),
	}
	colIndexMap := make(map[string]int)
	for i, colName := range viewResult.Columns {
		// Infer type from first row, or default to TypeText
		colType := catalog.TypeText
		if len(viewResult.Rows) > 0 {
			colType = viewResult.Rows[0][i].Type
		}
		viewSchema.Columns[i] = catalog.Column{Name: colName, Type: colType}
		colIndexMap[colName] = i
	}

	// Apply WHERE filter if present
	filteredRows := viewResult.Rows
	if outerStmt.Where != nil {
		filteredRows = nil
		for _, row := range viewResult.Rows {
			match, err := e.evaluateExpressionWithRow(outerStmt.Where, row, viewSchema)
			if err != nil {
				return nil, err
			}
			if match.Type == catalog.TypeBool && match.Bool {
				filteredRows = append(filteredRows, row)
			}
		}
	}

	// Build output columns
	var resultCols []string
	var resultRows [][]catalog.Value

	if len(outerStmt.Columns) == 1 && outerStmt.Columns[0].Star {
		// SELECT * - return all columns
		resultCols = viewResult.Columns
		resultRows = filteredRows
	} else {
		// Build specific columns
		for _, col := range outerStmt.Columns {
			if col.Alias != "" {
				resultCols = append(resultCols, col.Alias)
			} else if col.Name != "" {
				resultCols = append(resultCols, col.Name)
			} else if col.Expression != nil {
				// Expression column - use a generated name
				resultCols = append(resultCols, "expr")
			}
		}

		// Project columns
		for _, row := range filteredRows {
			resultRow := make([]catalog.Value, len(outerStmt.Columns))
			for i, col := range outerStmt.Columns {
				if col.Name != "" {
					idx, ok := colIndexMap[col.Name]
					if !ok {
						return nil, fmt.Errorf("unknown column '%s' in view '%s'", col.Name, viewDef.Name)
					}
					resultRow[i] = row[idx]
				} else if col.Expression != nil {
					val, err := e.evaluateExpressionWithRow(col.Expression, row, viewSchema)
					if err != nil {
						return nil, err
					}
					resultRow[i] = val
				}
			}
			resultRows = append(resultRows, resultRow)
		}
	}

	// Apply ORDER BY if present
	if len(outerStmt.OrderBy) > 0 {
		sort.Slice(resultRows, func(i, j int) bool {
			for _, ob := range outerStmt.OrderBy {
				idx, ok := colIndexMap[ob.Column]
				if !ok {
					// Try result columns
					for ci, cn := range resultCols {
						if cn == ob.Column {
							idx = ci
							break
						}
					}
				}
				if idx < 0 || idx >= len(resultRows[i]) {
					continue
				}
				cmp := compareValuesForSort(resultRows[i][idx], resultRows[j][idx])
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

	// Apply LIMIT if present
	if outerStmt.Limit != nil {
		limit := *outerStmt.Limit
		if limit < int64(len(resultRows)) {
			resultRows = resultRows[:limit]
		}
	}

	return &Result{
		Columns: resultCols,
		Rows:    resultRows,
	}, nil
}

// evaluateExpressionWithRow evaluates an expression given a row and schema.
func (e *Executor) evaluateExpressionWithRow(expr Expression, row []catalog.Value, schema *catalog.Schema) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		_, idx := schema.ColumnByName(ex.Name)
		if idx < 0 {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	case *BinaryExpr:
		left, err := e.evaluateExpressionWithRow(ex.Left, row, schema)
		if err != nil {
			return catalog.Value{}, err
		}
		right, err := e.evaluateExpressionWithRow(ex.Right, row, schema)
		if err != nil {
			return catalog.Value{}, err
		}

		switch ex.Op {
		case TOKEN_EQ:
			return catalog.Value{Type: catalog.TypeBool, Bool: compareValuesForSort(left, right) == 0}, nil
		case TOKEN_NE:
			return catalog.Value{Type: catalog.TypeBool, Bool: compareValuesForSort(left, right) != 0}, nil
		case TOKEN_LT:
			return catalog.Value{Type: catalog.TypeBool, Bool: compareValuesForSort(left, right) < 0}, nil
		case TOKEN_GT:
			return catalog.Value{Type: catalog.TypeBool, Bool: compareValuesForSort(left, right) > 0}, nil
		case TOKEN_LE:
			return catalog.Value{Type: catalog.TypeBool, Bool: compareValuesForSort(left, right) <= 0}, nil
		case TOKEN_GE:
			return catalog.Value{Type: catalog.TypeBool, Bool: compareValuesForSort(left, right) >= 0}, nil
		case TOKEN_AND:
			return catalog.Value{Type: catalog.TypeBool, Bool: left.Bool && right.Bool}, nil
		case TOKEN_OR:
			return catalog.Value{Type: catalog.TypeBool, Bool: left.Bool || right.Bool}, nil
		case TOKEN_PLUS:
			if left.Type == catalog.TypeInt64 && right.Type == catalog.TypeInt64 {
				return catalog.Value{Type: catalog.TypeInt64, Int64: left.Int64 + right.Int64}, nil
			}
			if left.Type == catalog.TypeInt32 && right.Type == catalog.TypeInt32 {
				return catalog.Value{Type: catalog.TypeInt32, Int32: left.Int32 + right.Int32}, nil
			}
		case TOKEN_MINUS:
			if left.Type == catalog.TypeInt64 && right.Type == catalog.TypeInt64 {
				return catalog.Value{Type: catalog.TypeInt64, Int64: left.Int64 - right.Int64}, nil
			}
			if left.Type == catalog.TypeInt32 && right.Type == catalog.TypeInt32 {
				return catalog.Value{Type: catalog.TypeInt32, Int32: left.Int32 - right.Int32}, nil
			}
		case TOKEN_STAR:
			if left.Type == catalog.TypeInt64 && right.Type == catalog.TypeInt64 {
				return catalog.Value{Type: catalog.TypeInt64, Int64: left.Int64 * right.Int64}, nil
			}
			if left.Type == catalog.TypeInt32 && right.Type == catalog.TypeInt32 {
				return catalog.Value{Type: catalog.TypeInt32, Int32: left.Int32 * right.Int32}, nil
			}
		case TOKEN_SLASH:
			if left.Type == catalog.TypeInt64 && right.Type == catalog.TypeInt64 && right.Int64 != 0 {
				return catalog.Value{Type: catalog.TypeInt64, Int64: left.Int64 / right.Int64}, nil
			}
			if left.Type == catalog.TypeInt32 && right.Type == catalog.TypeInt32 && right.Int32 != 0 {
				return catalog.Value{Type: catalog.TypeInt32, Int32: left.Int32 / right.Int32}, nil
			}
		}
		return catalog.Value{}, fmt.Errorf("unsupported binary operation: %v", ex.Op)

	case *UnaryExpr:
		operand, err := e.evaluateExpressionWithRow(ex.Expr, row, schema)
		if err != nil {
			return catalog.Value{}, err
		}
		if ex.Op == TOKEN_NOT {
			return catalog.Value{Type: catalog.TypeBool, Bool: !operand.Bool}, nil
		}
		return catalog.Value{}, fmt.Errorf("unsupported unary operation: %v", ex.Op)
	}

	return catalog.Value{}, fmt.Errorf("unsupported expression type in view: %T", expr)
}

// executeUnion executes a UNION/INTERSECT/EXCEPT operation.
func (e *Executor) executeUnion(stmt *UnionStmt) (*Result, error) {
	// Handle CTEs (WITH clause)
	if stmt.With != nil {
		return e.executeUnionWithCTEs(stmt)
	}

	// Execute left SELECT
	leftResult, err := e.executeSelect(stmt.Left)
	if err != nil {
		return nil, fmt.Errorf("error executing left side of %s: %w", stmt.Op, err)
	}

	// Execute right SELECT
	rightResult, err := e.executeSelect(stmt.Right)
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
		// Build order by indices
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
		e.sortRows(result.Rows, stmt.OrderBy, orderByIndices)
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

// executeUnionWithCTEs handles UNION with WITH clause.
func (e *Executor) executeUnionWithCTEs(stmt *UnionStmt) (*Result, error) {
	// Save any existing CTE data
	oldCTEData := e.cteData

	// Initialize CTE data map
	e.cteData = make(map[string]*Result)

	// Execute each CTE and store results
	for _, cte := range stmt.With.CTEs {
		cteResult, err := e.executeSelect(cte.Query)
		if err != nil {
			e.cteData = oldCTEData
			return nil, fmt.Errorf("error executing CTE '%s': %w", cte.Name, err)
		}

		if len(cte.Columns) > 0 {
			if len(cte.Columns) != len(cteResult.Columns) {
				e.cteData = oldCTEData
				return nil, fmt.Errorf("CTE '%s' column count mismatch", cte.Name)
			}
			cteResult.Columns = cte.Columns
		}

		e.cteData[cte.Name] = cteResult
	}

	// Execute the UNION without WITH clause
	mainStmt := *stmt
	mainStmt.With = nil
	result, err := e.executeUnion(&mainStmt)

	// Restore previous CTE data
	e.cteData = oldCTEData

	return result, err
}

// rowKey creates a unique string key for a row (used for duplicate detection in UNION)
func rowKey(row []catalog.Value) string {
	var parts []string
	for _, v := range row {
		if v.IsNull {
			parts = append(parts, "NULL")
		} else {
			switch v.Type {
			case catalog.TypeInt32:
				parts = append(parts, fmt.Sprintf("%d", v.Int32))
			case catalog.TypeInt64:
				parts = append(parts, fmt.Sprintf("%d", v.Int64))
			case catalog.TypeText:
				parts = append(parts, v.Text)
			case catalog.TypeBool:
				parts = append(parts, fmt.Sprintf("%t", v.Bool))
			case catalog.TypeTimestamp:
				parts = append(parts, v.Timestamp.String())
			default:
				parts = append(parts, "?")
			}
		}
	}
	return strings.Join(parts, "|")
}

// executeSelectWithWindowFunctions handles SELECT with window functions.
func (e *Executor) executeSelectWithWindowFunctions(stmt *SelectStmt, meta *catalog.TableMeta) (*Result, error) {
	// First, scan all rows that match WHERE clause
	var allRows [][]catalog.Value

	err := e.scanTable(stmt.TableName, meta.Schema, func(rid storage.RID, row []catalog.Value) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, evalErr := e.evalCondition(stmt.Where, meta.Schema, row)
			if evalErr != nil {
				return false, evalErr
			}
			if !match {
				return true, nil // continue scanning
			}
		}

		rowCopy := make([]catalog.Value, len(row))
		copy(rowCopy, row)
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
			// Copy all columns for this position (handled separately)
			continue
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
			// Expand star at this position
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

		// Sort using the original rows for ordering
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
				cmp := compareValuesForSort(indexed[i].sortRow[idx], indexed[j].sortRow[idx])
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

// computeWindowFunction computes window function values for all rows.
func (e *Executor) computeWindowFunction(wf *WindowFuncExpr, rows [][]catalog.Value, schema *catalog.Schema) ([]catalog.Value, error) {
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

	// Get order by column indices for window ordering
	orderByIndices := make([]int, len(wf.Over.OrderBy))
	for i, ob := range wf.Over.OrderBy {
		_, idx := schema.ColumnByName(ob.Column)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in window ORDER BY: %s", ob.Column)
		}
		orderByIndices[i] = idx
	}

	// Group rows by partition
	partitions := make(map[string][]int) // partition key -> row indices
	partitionOrder := []string{}         // to maintain order

	for i, row := range rows {
		key := makePartitionKey(row, partitionIndices)
		if _, exists := partitions[key]; !exists {
			partitionOrder = append(partitionOrder, key)
		}
		partitions[key] = append(partitions[key], i)
	}

	// Process each partition
	for _, partKey := range partitionOrder {
		rowIndices := partitions[partKey]

		// Sort rows within partition by ORDER BY columns
		if len(wf.Over.OrderBy) > 0 {
			sort.SliceStable(rowIndices, func(i, j int) bool {
				for k, ob := range wf.Over.OrderBy {
					idx := orderByIndices[k]
					cmp := compareValuesForSort(rows[rowIndices[i]][idx], rows[rowIndices[j]][idx])
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

		// Compute window function for this partition
		switch strings.ToUpper(wf.Function) {
		case "ROW_NUMBER":
			for rank, rowIdx := range rowIndices {
				result[rowIdx] = catalog.NewInt64(int64(rank + 1))
			}

		case "RANK":
			// RANK: same values get same rank, gaps after ties
			var prevVals []catalog.Value
			rank := 1
			for i, rowIdx := range rowIndices {
				currVals := getOrderByValues(rows[rowIdx], orderByIndices)
				if i == 0 || !windowValuesEqual(currVals, prevVals) {
					rank = i + 1
				}
				result[rowIdx] = catalog.NewInt64(int64(rank))
				prevVals = currVals
			}

		case "DENSE_RANK":
			// DENSE_RANK: same values get same rank, no gaps
			var prevVals []catalog.Value
			rank := 0
			for i, rowIdx := range rowIndices {
				currVals := getOrderByValues(rows[rowIdx], orderByIndices)
				if i == 0 || !windowValuesEqual(currVals, prevVals) {
					rank++
				}
				result[rowIdx] = catalog.NewInt64(int64(rank))
				prevVals = currVals
			}

		case "NTILE":
			// NTILE(n): divide rows into n buckets
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("NTILE requires exactly 1 argument")
			}
			nVal, err := e.evalExpr(wf.Args[0], schema, nil)
			if err != nil {
				return nil, err
			}
			n := int64(1)
			if nVal.Type == catalog.TypeInt32 {
				n = int64(nVal.Int32)
			} else if nVal.Type == catalog.TypeInt64 {
				n = nVal.Int64
			}
			if n < 1 {
				n = 1
			}

			totalRows := int64(len(rowIndices))
			for i, rowIdx := range rowIndices {
				bucket := (int64(i) * n / totalRows) + 1
				result[rowIdx] = catalog.NewInt64(bucket)
			}

		case "SUM":
			// SUM with frame support
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

			// Use frame bounds for each row
			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 {
					result[rowIdx] = catalog.Null(catalog.TypeInt64)
				} else {
					sum := computeFrameSum(rows, rowIndices, colIdx, startIdx, endIdx)
					result[rowIdx] = catalog.NewInt64(sum)
				}
			}

		case "COUNT":
			// COUNT with frame support
			colIdx := -1 // -1 means COUNT(*)
			if len(wf.Args) == 1 {
				if colRef, ok := wf.Args[0].(*ColumnRef); ok {
					_, colIdx = schema.ColumnByName(colRef.Name)
				}
			}

			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 {
					result[rowIdx] = catalog.NewInt64(0)
				} else {
					count := computeFrameCount(rows, rowIndices, colIdx, startIdx, endIdx, colIdx < 0)
					result[rowIdx] = catalog.NewInt64(count)
				}
			}

		case "AVG":
			// AVG with frame support
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

			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 {
					result[rowIdx] = catalog.Null(catalog.TypeInt64)
				} else {
					sum := computeFrameSum(rows, rowIndices, colIdx, startIdx, endIdx)
					count := computeFrameCount(rows, rowIndices, colIdx, startIdx, endIdx, false)
					if count > 0 {
						result[rowIdx] = catalog.NewInt64(sum / count)
					} else {
						result[rowIdx] = catalog.Null(catalog.TypeInt64)
					}
				}
			}

		case "MIN":
			// MIN with frame support
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("MIN requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("MIN argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 {
					result[rowIdx] = catalog.Null(catalog.TypeInt64)
				} else {
					result[rowIdx] = computeFrameMin(rows, rowIndices, colIdx, startIdx, endIdx)
				}
			}

		case "MAX":
			// MAX with frame support
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("MAX requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("MAX argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 {
					result[rowIdx] = catalog.Null(catalog.TypeInt64)
				} else {
					result[rowIdx] = computeFrameMax(rows, rowIndices, colIdx, startIdx, endIdx)
				}
			}

		case "LAG":
			// LAG(col, offset, default)
			if len(wf.Args) < 1 {
				return nil, fmt.Errorf("LAG requires at least 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("LAG first argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			offset := int64(1)
			if len(wf.Args) >= 2 {
				offsetVal, err := e.evalExpr(wf.Args[1], schema, nil)
				if err != nil {
					return nil, err
				}
				if offsetVal.Type == catalog.TypeInt32 {
					offset = int64(offsetVal.Int32)
				} else if offsetVal.Type == catalog.TypeInt64 {
					offset = offsetVal.Int64
				}
			}

			defaultVal := catalog.Null(catalog.TypeUnknown)
			if len(wf.Args) >= 3 {
				var err error
				defaultVal, err = e.evalExpr(wf.Args[2], schema, nil)
				if err != nil {
					return nil, err
				}
			}

			for i, rowIdx := range rowIndices {
				lagIdx := i - int(offset)
				if lagIdx >= 0 && lagIdx < len(rowIndices) {
					result[rowIdx] = rows[rowIndices[lagIdx]][colIdx]
				} else {
					result[rowIdx] = defaultVal
				}
			}

		case "LEAD":
			// LEAD(col, offset, default)
			if len(wf.Args) < 1 {
				return nil, fmt.Errorf("LEAD requires at least 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("LEAD first argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			offset := int64(1)
			if len(wf.Args) >= 2 {
				offsetVal, err := e.evalExpr(wf.Args[1], schema, nil)
				if err != nil {
					return nil, err
				}
				if offsetVal.Type == catalog.TypeInt32 {
					offset = int64(offsetVal.Int32)
				} else if offsetVal.Type == catalog.TypeInt64 {
					offset = offsetVal.Int64
				}
			}

			defaultVal := catalog.Null(catalog.TypeUnknown)
			if len(wf.Args) >= 3 {
				var err error
				defaultVal, err = e.evalExpr(wf.Args[2], schema, nil)
				if err != nil {
					return nil, err
				}
			}

			for i, rowIdx := range rowIndices {
				leadIdx := i + int(offset)
				if leadIdx >= 0 && leadIdx < len(rowIndices) {
					result[rowIdx] = rows[rowIndices[leadIdx]][colIdx]
				} else {
					result[rowIdx] = defaultVal
				}
			}

		case "FIRST_VALUE":
			// FIRST_VALUE with frame support
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("FIRST_VALUE requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("FIRST_VALUE argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 || startIdx > endIdx || startIdx >= len(rowIndices) {
					result[rowIdx] = catalog.Null(schema.Columns[colIdx].Type)
				} else {
					result[rowIdx] = rows[rowIndices[startIdx]][colIdx]
				}
			}

		case "LAST_VALUE":
			// LAST_VALUE with frame support
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("LAST_VALUE requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("LAST_VALUE argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 || startIdx > endIdx || endIdx >= len(rowIndices) {
					if endIdx >= len(rowIndices) {
						endIdx = len(rowIndices) - 1
					}
					if endIdx < 0 || startIdx > endIdx {
						result[rowIdx] = catalog.Null(schema.Columns[colIdx].Type)
					} else {
						result[rowIdx] = rows[rowIndices[endIdx]][colIdx]
					}
				} else {
					result[rowIdx] = rows[rowIndices[endIdx]][colIdx]
				}
			}

		case "NTH_VALUE":
			// NTH_VALUE with frame support - returns value from nth row of the window frame
			if len(wf.Args) != 2 {
				return nil, fmt.Errorf("NTH_VALUE requires exactly 2 arguments")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("NTH_VALUE first argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			// Get the N value (1-based index)
			var nVal int64
			switch n := wf.Args[1].(type) {
			case *LiteralExpr:
				if n.Value.Type == catalog.TypeInt64 {
					nVal = n.Value.Int64
				} else if n.Value.Type == catalog.TypeInt32 {
					nVal = int64(n.Value.Int32)
				} else {
					return nil, fmt.Errorf("NTH_VALUE second argument must be an integer")
				}
			default:
				return nil, fmt.Errorf("NTH_VALUE second argument must be an integer literal")
			}

			if nVal < 1 {
				return nil, fmt.Errorf("NTH_VALUE second argument must be a positive integer")
			}

			// Return the nth value within the frame (1-based)
			for i, rowIdx := range rowIndices {
				startIdx, endIdx := getFrameBounds(i, len(rowIndices), wf.Over.FrameType, wf.Over.FrameStart, wf.Over.FrameEnd)
				if startIdx < 0 {
					result[rowIdx] = catalog.Null(schema.Columns[colIdx].Type)
					continue
				}
				// Calculate the index within the frame
				nIdx := startIdx + int(nVal) - 1 // Convert 1-based to absolute index within partition
				if nIdx <= endIdx && nIdx < len(rowIndices) {
					result[rowIdx] = rows[rowIndices[nIdx]][colIdx]
				} else {
					// If n is beyond frame size, return NULL
					result[rowIdx] = catalog.Null(schema.Columns[colIdx].Type)
				}
			}

		default:
			return nil, fmt.Errorf("unsupported window function: %s", wf.Function)
		}
	}

	return result, nil
}

// makePartitionKey creates a key for grouping rows by partition columns.
func makePartitionKey(row []catalog.Value, indices []int) string {
	if len(indices) == 0 {
		return "" // All rows in same partition
	}
	var parts []string
	for _, idx := range indices {
		parts = append(parts, valueToString(row[idx]))
	}
	return strings.Join(parts, "|")
}

// getOrderByValues extracts the values used for ordering.
func getOrderByValues(row []catalog.Value, indices []int) []catalog.Value {
	vals := make([]catalog.Value, len(indices))
	for i, idx := range indices {
		vals[i] = row[idx]
	}
	return vals
}

// windowValuesEqual checks if two slices of values are equal (for window function comparisons).
func windowValuesEqual(a, b []catalog.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if compareValuesForSort(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

// valueToString converts a value to string for partition key.
func valueToString(v catalog.Value) string {
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

// getFrameBounds calculates the start and end indices for a window frame.
// Parameters:
//   - currentIdx: index of current row within the partition (0-based)
//   - partitionSize: total number of rows in the partition
//   - frameType: "ROWS" or "RANGE" (empty means default)
//   - frameStart: "UNBOUNDED PRECEDING", "CURRENT ROW", "n PRECEDING", "n FOLLOWING"
//   - frameEnd: same as frameStart, or empty for single-bound frame
//
// Returns: startIdx, endIdx (inclusive) within the partition
func getFrameBounds(currentIdx, partitionSize int, frameType, frameStart, frameEnd string) (int, int) {
	// Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	if frameType == "" || frameStart == "" {
		return 0, currentIdx
	}

	startIdx := parseFrameBoundIndex(frameStart, currentIdx, partitionSize, true)

	// If no end bound specified, use same as start for single-bound frame
	var endIdx int
	if frameEnd == "" {
		// Single bound - frame goes from start to current row
		endIdx = currentIdx
	} else {
		endIdx = parseFrameBoundIndex(frameEnd, currentIdx, partitionSize, false)
	}

	// Clamp to valid bounds
	if startIdx < 0 {
		startIdx = 0
	}
	if endIdx >= partitionSize {
		endIdx = partitionSize - 1
	}
	if startIdx > endIdx {
		// Empty frame
		return -1, -1
	}

	return startIdx, endIdx
}

// parseFrameBoundIndex converts a frame bound string to an index.
func parseFrameBoundIndex(bound string, currentIdx, partitionSize int, _ bool) int {
	switch bound {
	case "UNBOUNDED PRECEDING":
		return 0
	case "UNBOUNDED FOLLOWING":
		return partitionSize - 1
	case "CURRENT ROW":
		return currentIdx
	default:
		// Parse "n PRECEDING" or "n FOLLOWING"
		parts := strings.Split(bound, " ")
		if len(parts) == 2 {
			n, err := strconv.Atoi(parts[0])
			if err == nil {
				if parts[1] == "PRECEDING" {
					return currentIdx - n
				} else if parts[1] == "FOLLOWING" {
					return currentIdx + n
				}
			}
		}
		// Default: current row
		return currentIdx
	}
}

// computeFrameSum calculates the sum of values within a frame.
func computeFrameSum(rows [][]catalog.Value, rowIndices []int, colIdx int, startIdx, endIdx int) int64 {
	var sum int64
	for i := startIdx; i <= endIdx && i < len(rowIndices); i++ {
		if i < 0 {
			continue
		}
		val := rows[rowIndices[i]][colIdx]
		if !val.IsNull {
			if val.Type == catalog.TypeInt32 {
				sum += int64(val.Int32)
			} else if val.Type == catalog.TypeInt64 {
				sum += val.Int64
			}
		}
	}
	return sum
}

// computeFrameCount calculates the count of non-null values within a frame.
func computeFrameCount(rows [][]catalog.Value, rowIndices []int, colIdx int, startIdx, endIdx int, countStar bool) int64 {
	var count int64
	for i := startIdx; i <= endIdx && i < len(rowIndices); i++ {
		if i < 0 {
			continue
		}
		if countStar {
			count++
		} else {
			val := rows[rowIndices[i]][colIdx]
			if !val.IsNull {
				count++
			}
		}
	}
	return count
}

// computeFrameMin finds the minimum value within a frame.
func computeFrameMin(rows [][]catalog.Value, rowIndices []int, colIdx int, startIdx, endIdx int) catalog.Value {
	minVal := catalog.Value{IsNull: true}
	for i := startIdx; i <= endIdx && i < len(rowIndices); i++ {
		if i < 0 {
			continue
		}
		val := rows[rowIndices[i]][colIdx]
		if !val.IsNull {
			if minVal.IsNull || compareValuesForSort(val, minVal) < 0 {
				minVal = val
			}
		}
	}
	return minVal
}

// computeFrameMax finds the maximum value within a frame.
func computeFrameMax(rows [][]catalog.Value, rowIndices []int, colIdx int, startIdx, endIdx int) catalog.Value {
	maxVal := catalog.Value{IsNull: true}
	for i := startIdx; i <= endIdx && i < len(rowIndices); i++ {
		if i < 0 {
			continue
		}
		val := rows[rowIndices[i]][colIdx]
		if !val.IsNull {
			if maxVal.IsNull || compareValuesForSort(val, maxVal) > 0 {
				maxVal = val
			}
		}
	}
	return maxVal
}
