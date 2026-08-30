package sql

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
)

// groupingSetState holds the aggregate state for one group within one grouping set.
type groupingSetState struct {
	groupKey    []catalog.Value
	aggregators []aggregatorState
}

// computeGroupingSets evaluates a SELECT with GROUP BY GROUPING SETS / CUBE /
// ROLLUP over rows that have already been scanned and WHERE-filtered. It is
// shared by both execution paths: the base scan is MVCC-independent, so the
// grouping / aggregation / GROUPING() logic lives here once.
//
// evalHaving evaluates the HAVING clause against a synthetic schema whose columns
// are the output columns; pass the caller's condition evaluator.
func computeGroupingSets(
	allRows [][]catalog.Value,
	schema *catalog.Schema,
	stmt *ast.SelectStmt,
	evalHaving func(expr ast.Expression, resultSchema *catalog.Schema, row []catalog.Value) (bool, error),
) (*Result, error) {
	// Columns that appear in any grouping set (plus any plain GROUP BY columns).
	groupingCols := make(map[string]int)
	for _, gs := range stmt.GroupingSets {
		for _, col := range gs.Columns {
			if _, ok := groupingCols[col]; !ok {
				if _, idx := schema.ColumnByName(col); idx >= 0 {
					groupingCols[col] = idx
				} else {
					return nil, fmt.Errorf("unknown column in GROUPING SETS: %s", col)
				}
			}
		}
	}
	for _, col := range stmt.GroupBy {
		if _, ok := groupingCols[col]; !ok {
			if _, idx := schema.ColumnByName(col); idx >= 0 {
				groupingCols[col] = idx
			} else {
				return nil, fmt.Errorf("unknown column in GROUP BY: %s", col)
			}
		}
	}

	type columnInfo struct {
		isAggregate bool
		aggregate   *ast.AggregateFunc
		colName     string
		isGrouping  bool
		groupingArg string
	}
	columnInfos := make([]columnInfo, len(stmt.Columns))
	outputCols := make([]string, len(stmt.Columns))

	for i, col := range stmt.Columns {
		switch {
		case col.Aggregate != nil:
			columnInfos[i].isAggregate = true
			columnInfos[i].aggregate = col.Aggregate
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = fmt.Sprintf("%s(%s)", col.Aggregate.Function, col.Aggregate.Arg)
			}
		case col.Expression != nil:
			if fe, ok := col.Expression.(*ast.FunctionExpr); ok && strings.ToUpper(fe.Name) == "GROUPING" {
				columnInfos[i].isGrouping = true
				if len(fe.Args) == 1 {
					if cr, ok := fe.Args[0].(*ast.ColumnRef); ok {
						columnInfos[i].groupingArg = cr.Name
					}
				}
				if col.Alias != "" {
					outputCols[i] = col.Alias
				} else {
					outputCols[i] = "GROUPING"
				}
			} else if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = "expr"
			}
		case col.Name != "":
			columnInfos[i].colName = col.Name
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = col.Name
			}
		}
	}

	accumulate := func(aggs []aggregatorState, row []catalog.Value) {
		for i, ci := range columnInfos {
			if !ci.isAggregate {
				continue
			}
			agg := ci.aggregate
			if agg.Arg == "*" {
				aggs[i].count++
				aggs[i].hasValue = true
				continue
			}
			_, idx := schema.ColumnByName(agg.Arg)
			if idx < 0 {
				continue
			}
			val := row[idx]
			if val.IsNull {
				continue
			}
			aggs[i].hasValue = true
			aggs[i].count++
			switch agg.Function {
			case "SUM", "AVG":
				switch val.Type {
				case catalog.TypeInt32:
					aggs[i].sum += int64(val.Int32)
				case catalog.TypeInt64:
					aggs[i].sum += val.Int64
				}
			case "MIN":
				if compareValuesForSort(val, aggs[i].min) < 0 || aggs[i].min.Type == catalog.TypeUnknown {
					aggs[i].min = val
				}
			case "MAX":
				if compareValuesForSort(val, aggs[i].max) > 0 || aggs[i].max.Type == catalog.TypeUnknown {
					aggs[i].max = val
				}
			}
		}
	}

	finishAgg := func(aggState aggregatorState, fn string) catalog.Value {
		switch fn {
		case "COUNT":
			return catalog.NewInt64(aggState.count)
		case "SUM":
			if aggState.hasValue {
				return catalog.NewInt64(aggState.sum)
			}
			return catalog.Null(catalog.TypeInt64)
		case "AVG":
			if aggState.count > 0 {
				return catalog.NewInt64(aggState.sum / aggState.count)
			}
			return catalog.Null(catalog.TypeInt64)
		case "MIN":
			if aggState.hasValue {
				return aggState.min
			}
			return catalog.Null(catalog.TypeUnknown)
		case "MAX":
			if aggState.hasValue {
				return aggState.max
			}
			return catalog.Null(catalog.TypeUnknown)
		}
		return catalog.Null(catalog.TypeUnknown)
	}

	var resultRows [][]catalog.Value

	for _, gs := range stmt.GroupingSets {
		groupIndices := make([]int, len(gs.Columns))
		groupColSet := make(map[string]bool)
		for i, col := range gs.Columns {
			groupIndices[i] = groupingCols[col]
			groupColSet[col] = true
		}

		groups := make(map[string]*groupingSetState)
		var groupOrder []string

		for _, row := range allRows {
			var keyParts []string
			for _, idx := range groupIndices {
				if row[idx].IsNull {
					keyParts = append(keyParts, "NULL")
				} else {
					keyParts = append(keyParts, row[idx].String())
				}
			}
			key := strings.Join(keyParts, "|")

			grp, ok := groups[key]
			if !ok {
				groupKey := make([]catalog.Value, len(groupIndices))
				for i, idx := range groupIndices {
					groupKey[i] = row[idx]
				}
				grp = &groupingSetState{groupKey: groupKey, aggregators: make([]aggregatorState, len(stmt.Columns))}
				groups[key] = grp
				groupOrder = append(groupOrder, key)
			}
			accumulate(grp.aggregators, row)
		}

		for _, key := range groupOrder {
			grp := groups[key]
			resultRow := make([]catalog.Value, len(stmt.Columns))
			for i, ci := range columnInfos {
				switch {
				case ci.isAggregate:
					resultRow[i] = finishAgg(grp.aggregators[i], ci.aggregate.Function)
				case ci.isGrouping:
					if groupColSet[ci.groupingArg] {
						resultRow[i] = catalog.NewInt64(0)
					} else {
						resultRow[i] = catalog.NewInt64(1)
					}
				case ci.colName != "":
					if groupColSet[ci.colName] {
						for j, col := range gs.Columns {
							if col == ci.colName {
								resultRow[i] = grp.groupKey[j]
								break
							}
						}
					} else {
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					}
				}
			}
			resultRows = append(resultRows, resultRow)
		}

		// Empty grouping set () with data but no group produced above → grand total.
		if len(gs.Columns) == 0 && len(allRows) > 0 && len(groupOrder) == 0 {
			grand := make([]aggregatorState, len(stmt.Columns))
			for _, row := range allRows {
				accumulate(grand, row)
			}
			resultRow := make([]catalog.Value, len(stmt.Columns))
			for i, ci := range columnInfos {
				switch {
				case ci.isAggregate:
					resultRow[i] = finishAgg(grand[i], ci.aggregate.Function)
				case ci.isGrouping:
					resultRow[i] = catalog.NewInt64(1)
				default:
					resultRow[i] = catalog.Null(catalog.TypeUnknown)
				}
			}
			resultRows = append(resultRows, resultRow)
		}
	}

	// No rows matched: still emit the grand-total row if there is an empty set.
	if len(resultRows) == 0 {
		for _, gs := range stmt.GroupingSets {
			if len(gs.Columns) != 0 {
				continue
			}
			resultRow := make([]catalog.Value, len(stmt.Columns))
			for i, ci := range columnInfos {
				switch {
				case ci.isAggregate && ci.aggregate.Function == "COUNT":
					resultRow[i] = catalog.NewInt64(0)
				case ci.isGrouping:
					resultRow[i] = catalog.NewInt64(1)
				default:
					resultRow[i] = catalog.Null(catalog.TypeUnknown)
				}
			}
			resultRows = append(resultRows, resultRow)
			break
		}
	}

	// HAVING over the result rows.
	if stmt.Having != nil {
		havingSchema := &catalog.Schema{Columns: make([]catalog.Column, len(outputCols))}
		for i, name := range outputCols {
			havingSchema.Columns[i] = catalog.Column{Name: name}
		}
		var kept [][]catalog.Value
		for _, row := range resultRows {
			match, err := evalHaving(stmt.Having, havingSchema, row)
			if err != nil {
				return nil, fmt.Errorf("error evaluating HAVING: %w", err)
			}
			if match {
				kept = append(kept, row)
			}
		}
		resultRows = kept
	}

	// ORDER BY over the result rows.
	if len(stmt.OrderBy) > 0 {
		idxs := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			found := false
			for j, col := range outputCols {
				if strings.EqualFold(col, ob.Column) {
					idxs[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("ORDER BY column not in result: %s", ob.Column)
			}
		}
		sortRowsMVCC(resultRows, stmt.OrderBy, idxs)
	}

	return &Result{Columns: outputCols, Rows: resultRows}, nil
}
