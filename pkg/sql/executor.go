package sql

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Executor executes SQL statements against a TableManager.
type Executor struct {
	tm              *catalog.TableManager
	autoIncCounters map[string]int64 // table.column -> next value
	autoIncMu       sync.Mutex
}

// NewExecutor creates a new Executor.
func NewExecutor(tm *catalog.TableManager) *Executor {
	return &Executor{
		tm:              tm,
		autoIncCounters: make(map[string]int64),
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

		cols[i] = col
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

	// Check primary key uniqueness before insert
	for i, col := range meta.Schema.Columns {
		if col.PrimaryKey {
			// Get the value for this primary key column
			pkValue := values[i]
			if pkValue.IsNull {
				return nil, fmt.Errorf("primary key column %q cannot be NULL", col.Name)
			}

			// Scan existing rows to check for duplicates
			exists, err := e.primaryKeyExists(stmt.TableName, meta.Schema, i, pkValue)
			if err != nil {
				return nil, err
			}
			if exists {
				return nil, fmt.Errorf("duplicate key value violates primary key constraint on column %q", col.Name)
			}
		}
	}

	_, err = e.tm.Insert(stmt.TableName, values)
	if err != nil {
		return nil, err
	}

	return &Result{Message: "1 row inserted.", RowsAffected: 1}, nil
}

// primaryKeyExists checks if a value already exists for a primary key column.
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
	meta, err := e.tm.Catalog().GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Check if query contains JOINs
	if len(stmt.Joins) > 0 {
		return e.executeSelectWithJoins(stmt, meta)
	}

	// Check if query contains aggregate functions or GROUP BY
	hasAggregates := false
	for _, col := range stmt.Columns {
		if col.Aggregate != nil {
			hasAggregates = true
			break
		}
	}

	if hasAggregates || len(stmt.GroupBy) > 0 {
		return e.executeSelectWithAggregates(stmt, meta)
	}

	return e.executeSelectNormal(stmt, meta)
}

// executeSelectNormal handles regular SELECT without aggregates.
func (e *Executor) executeSelectNormal(stmt *SelectStmt, meta *catalog.TableMeta) (*Result, error) {
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
	}

	// Slice rows based on OFFSET and LIMIT
	fullRows = fullRows[startIdx:endIdx]

	// Project columns for final output
	resultRows := make([][]catalog.Value, len(fullRows))
	for i, row := range fullRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			projectedRow[j] = row[idx]
		}
		resultRows[i] = projectedRow
	}

	// Apply DISTINCT - deduplicate rows
	if stmt.Distinct {
		resultRows = deduplicateRows(resultRows)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// executeSelectWithJoins handles SELECT with JOIN clauses.
func (e *Executor) executeSelectWithJoins(stmt *SelectStmt, leftMeta *catalog.TableMeta) (*Result, error) {
	// Currently only supporting single INNER JOIN
	if len(stmt.Joins) != 1 {
		return nil, fmt.Errorf("only single JOIN is currently supported")
	}

	join := stmt.Joins[0]
	if join.JoinType != "INNER" {
		return nil, fmt.Errorf("only INNER JOIN is currently supported")
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
	for i, col := range rightMeta.Schema.Columns {
		newCol := col
		combinedSchema.Columns = append(combinedSchema.Columns, newCol)
		colTableMap[join.TableName+"."+col.Name] = leftLen + i
		// Only add unqualified if not already present
		if _, exists := colTableMap[col.Name]; !exists {
			colTableMap[col.Name] = leftLen + i
		}
	}

	// Perform nested loop join
	var joinedRows [][]catalog.Value

	err = e.scanTable(stmt.TableName, leftMeta.Schema, func(leftRID storage.RID, leftRow []catalog.Value) (bool, error) {
		return true, e.scanTable(join.TableName, rightMeta.Schema, func(rightRID storage.RID, rightRow []catalog.Value) (bool, error) {
			// Combine rows
			combinedRow := make([]catalog.Value, len(leftRow)+len(rightRow))
			copy(combinedRow, leftRow)
			copy(combinedRow[len(leftRow):], rightRow)

			// Evaluate join condition
			match, err := e.evalJoinCondition(join.Condition, combinedSchema, combinedRow, colTableMap)
			if err != nil {
				return false, err
			}

			if match {
				joinedRows = append(joinedRows, combinedRow)
			}

			return true, nil // continue scanning
		})
	})

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

// evalJoinCondition evaluates a join condition against a combined row.
func (e *Executor) evalJoinCondition(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
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
			outputCols[i] = fmt.Sprintf("%s(%s)", agg.Function, agg.Arg)

			if agg.Arg != "*" {
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx < 0 {
					return nil, fmt.Errorf("unknown column: %s", agg.Arg)
				}
			}
		} else if col.Name != "" {
			columnInfos[i].colName = col.Name
			outputCols[i] = col.Name

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
		// Look for this column in GROUP BY columns
		for i, col := range columns {
			if col.Name == ex.Name {
				return grp.groupKey[i], nil
			}
		}
		return catalog.Value{}, fmt.Errorf("column %s not found in GROUP BY", ex.Name)

		// Note: For now, aggregate functions in HAVING would require more complex parsing.
		// This is a simplified implementation that compares against literals.
	}

	return catalog.Value{}, fmt.Errorf("unsupported HAVING expression: %T", expr)
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
func (e *Executor) scanTable(tableName string, _ *catalog.Schema, fn func(rid storage.RID, row []catalog.Value) (bool, error)) error {
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
