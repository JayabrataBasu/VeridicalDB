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

	// Validate CHECK constraints
	if err := e.validateCheckConstraints(meta.Schema, values); err != nil {
		return nil, err
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
// Note: This is a simplified implementation that stores view definitions in memory.
// A production implementation would persist view metadata to the catalog.
func (e *Executor) executeCreateView(stmt *CreateViewStmt) (*Result, error) {
	// For now, views are not fully implemented - we return an error
	// A real implementation would:
	// 1. Validate the SELECT query
	// 2. Store the view definition in the catalog
	// 3. Allow SELECT from the view by expanding it
	return nil, fmt.Errorf("CREATE VIEW is not yet fully implemented (view definition parsed successfully)")
}

// executeDropView drops a view.
func (e *Executor) executeDropView(stmt *DropViewStmt) (*Result, error) {
	// For now, views are not fully implemented
	if stmt.IfExists {
		return &Result{Message: fmt.Sprintf("View '%s' does not exist (IF EXISTS specified).", stmt.ViewName)}, nil
	}
	return nil, fmt.Errorf("DROP VIEW is not yet fully implemented")
}

// executeUnion executes a UNION/INTERSECT/EXCEPT operation.
func (e *Executor) executeUnion(stmt *UnionStmt) (*Result, error) {
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
