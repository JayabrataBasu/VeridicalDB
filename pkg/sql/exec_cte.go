package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
)

// cteResultSchema builds a synthetic schema for a materialised CTE result,
// inferring column types from the first row.
func cteResultSchema(cteResult *Result) *catalog.Schema {
	cols := make([]catalog.Column, len(cteResult.Columns))
	for i, name := range cteResult.Columns {
		t := catalog.TypeText
		if len(cteResult.Rows) > 0 {
			t = cteResult.Rows[0][i].Type
		}
		cols[i] = catalog.Column{ID: i, Name: name, Type: t}
	}
	return &catalog.Schema{Columns: cols}
}

// cteResultMeta wraps a CTE result as a TableMeta so the join machinery can
// treat it like a base table.
func cteResultMeta(name string, cteResult *Result) *catalog.TableMeta {
	sch := cteResultSchema(cteResult)
	return &catalog.TableMeta{Name: name, Columns: sch.Columns, Schema: sch}
}

// executeSelectFromCTE executes a SELECT against a CTE result set.
func selectFromCTE(stmt *ast.SelectStmt, cteResult *Result,
	evalExpr func(ast.Expression, *catalog.Schema, []catalog.Value) (catalog.Value, error),
	evalCond func(ast.Expression, *catalog.Schema, []catalog.Value) (bool, error),
) (*Result, error) {
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
	var colExpressions []ast.Expression

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
			match, err := evalCond(stmt.Where, cteSchema, row)
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
		return selectFromCTEWithAggregates(stmt, cteResult, cteSchema, filteredRows, colIndexMap, outputCols, evalExpr, evalCond)
	}

	// Build output rows
	var resultRows [][]catalog.Value
	for _, row := range filteredRows {
		var outRow []catalog.Value
		for i, idx := range colIndices {
			if idx == -1 {
				// Expression
				val, err := evalExpr(colExpressions[i], cteSchema, row)
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
		sortRowsForCTE(resultRows, stmt.OrderBy, outputCols)
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
func sortRowsForCTE(rows [][]catalog.Value, orderBy []ast.OrderByClause, cols []string) {
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
func selectFromCTEWithAggregates(stmt *ast.SelectStmt, cteResult *Result, cteSchema *catalog.Schema, rows [][]catalog.Value, colIndexMap map[string]int, outputCols []string,
	evalExpr func(ast.Expression, *catalog.Schema, []catalog.Value) (catalog.Value, error),
	evalCond func(ast.Expression, *catalog.Schema, []catalog.Value) (bool, error),
) (*Result, error) {
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
				val, err := computeAggregateForCTE(sc.Aggregate, groupRows, cteResult.Columns, colIndexMap)
				if err != nil {
					return nil, err
				}
				outRow = append(outRow, val)
			} else if sc.Expression != nil {
				val, err := evalExpr(sc.Expression, cteSchema, groupRows[0])
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
			match, err := evalCond(stmt.Having, havingSchema, row)
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
		sortRowsForCTE(resultRows, stmt.OrderBy, outputCols)
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
func computeAggregateForCTE(agg *ast.AggregateFunc, rows [][]catalog.Value, _ []string, colIndexMap map[string]int) (catalog.Value, error) {
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
