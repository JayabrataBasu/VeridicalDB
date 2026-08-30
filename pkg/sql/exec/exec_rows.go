package exec

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

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
