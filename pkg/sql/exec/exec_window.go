package exec

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
)

// computeWindowFunction computes a window function over already-materialised,
// WHERE-filtered rows. evalArg evaluates a scalar argument expression (used by
// NTILE / LAG / LEAD). Shared by both execution paths.
func computeWindowFunction(wf *ast.WindowFuncExpr, rows [][]catalog.Value, schema *catalog.Schema, evalArg func(ast.Expression) (catalog.Value, error)) ([]catalog.Value, error) {
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
			nVal, err := evalArg(wf.Args[0])
			if err != nil {
				return nil, err
			}
			n := int64(1)
			switch nVal.Type {
			case catalog.TypeInt32:
				n = int64(nVal.Int32)
			case catalog.TypeInt64:
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
				if colRef, ok := wf.Args[0].(*ast.ColumnRef); ok {
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
			if !ok {
				return nil, fmt.Errorf("LAG first argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			offset := int64(1)
			if len(wf.Args) >= 2 {
				offsetVal, err := evalArg(wf.Args[1])
				if err != nil {
					return nil, err
				}
				switch offsetVal.Type {
				case catalog.TypeInt32:
					offset = int64(offsetVal.Int32)
				case catalog.TypeInt64:
					offset = offsetVal.Int64
				}
			}

			defaultVal := catalog.Null(catalog.TypeUnknown)
			if len(wf.Args) >= 3 {
				var err error
				defaultVal, err = evalArg(wf.Args[2])
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
			if !ok {
				return nil, fmt.Errorf("LEAD first argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			offset := int64(1)
			if len(wf.Args) >= 2 {
				offsetVal, err := evalArg(wf.Args[1])
				if err != nil {
					return nil, err
				}
				switch offsetVal.Type {
				case catalog.TypeInt32:
					offset = int64(offsetVal.Int32)
				case catalog.TypeInt64:
					offset = offsetVal.Int64
				}
			}

			defaultVal := catalog.Null(catalog.TypeUnknown)
			if len(wf.Args) >= 3 {
				var err error
				defaultVal, err = evalArg(wf.Args[2])
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
			colRef, ok := wf.Args[0].(*ast.ColumnRef)
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
			case *ast.LiteralExpr:
				switch n.Value.Type {
				case catalog.TypeInt64:
					nVal = n.Value.Int64
				case catalog.TypeInt32:
					nVal = int64(n.Value.Int32)
				default:
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
				switch parts[1] {
				case "PRECEDING":
					return currentIdx - n
				case "FOLLOWING":
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
			switch val.Type {
			case catalog.TypeInt32:
				sum += int64(val.Int32)
			case catalog.TypeInt64:
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
