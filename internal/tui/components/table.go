// Package components provides reusable data models and managers for TUI components.
package components

import (
	"fmt"
	"sort"
	"strings"
)

// SortDirection indicates ascending or descending sort order.
type SortDirection int

const (
	SortNone SortDirection = iota
	SortAscending
	SortDescending
)

// ColumnSort tracks sorting configuration for a column.
type ColumnSort struct {
	ColumnIndex int
	Direction   SortDirection
}

// TableData manages query results with sorting and selection.
type TableData struct {
	Columns      []string                       // Column names
	Rows         [][]interface{}                // Data rows
	SelectedRows map[int]bool                   // Selected row indices
	SortConfig   *ColumnSort                    // Current sort configuration
	RowMetadata  map[int]map[string]interface{} // Metadata per row (for tags, states)
}

// NewTableData creates a new table data manager.
func NewTableData(columns []string, rows [][]interface{}) *TableData {
	return &TableData{
		Columns:      columns,
		Rows:         rows,
		SelectedRows: make(map[int]bool),
		SortConfig:   nil,
		RowMetadata:  make(map[int]map[string]interface{}),
	}
}

// Sort sorts the table data by the specified column.
// If already sorted by this column, toggles the sort direction.
func (t *TableData) Sort(columnIndex int) error {
	if columnIndex < 0 || columnIndex >= len(t.Columns) {
		return ErrInvalidColumnIndex
	}

	// Determine sort direction
	direction := SortAscending
	if t.SortConfig != nil && t.SortConfig.ColumnIndex == columnIndex {
		if t.SortConfig.Direction == SortAscending {
			direction = SortDescending
		} else {
			direction = SortNone
		}
	}

	// If toggling to None, clear sort
	if direction == SortNone {
		t.SortConfig = nil
		return nil
	}

	// Perform sort
	rowIndices := make([]int, len(t.Rows))
	for i := range rowIndices {
		rowIndices[i] = i
	}

	sort.Slice(rowIndices, func(i, j int) bool {
		iRow := t.Rows[rowIndices[i]]
		jRow := t.Rows[rowIndices[j]]

		if columnIndex >= len(iRow) || columnIndex >= len(jRow) {
			return false
		}

		iVal := toString(iRow[columnIndex])
		jVal := toString(jRow[columnIndex])

		if direction == SortDescending {
			return iVal > jVal
		}
		return iVal < jVal
	})

	// Reorder rows and update selected indices mapping
	newRows := make([][]interface{}, len(t.Rows))
	indexMap := make(map[int]int) // old index -> new index
	for newIdx, oldIdx := range rowIndices {
		newRows[newIdx] = t.Rows[oldIdx]
		indexMap[oldIdx] = newIdx
	}

	t.Rows = newRows
	t.SortConfig = &ColumnSort{
		ColumnIndex: columnIndex,
		Direction:   direction,
	}

	// Update selected rows with new indices
	newSelected := make(map[int]bool)
	for oldIdx, selected := range t.SelectedRows {
		if newIdx, ok := indexMap[oldIdx]; ok && selected {
			newSelected[newIdx] = true
		}
	}
	t.SelectedRows = newSelected

	return nil
}

// SelectRow marks a row as selected.
func (t *TableData) SelectRow(rowIndex int) {
	if rowIndex >= 0 && rowIndex < len(t.Rows) {
		t.SelectedRows[rowIndex] = true
	}
}

// DeselectRow marks a row as not selected.
func (t *TableData) DeselectRow(rowIndex int) {
	delete(t.SelectedRows, rowIndex)
}

// ToggleRow toggles selection status of a row.
func (t *TableData) ToggleRow(rowIndex int) {
	if rowIndex < 0 || rowIndex >= len(t.Rows) {
		return
	}

	if t.SelectedRows[rowIndex] {
		t.DeselectRow(rowIndex)
	} else {
		t.SelectRow(rowIndex)
	}
}

// SelectAll selects all rows.
func (t *TableData) SelectAll() {
	for i := 0; i < len(t.Rows); i++ {
		t.SelectedRows[i] = true
	}
}

// DeselectAll deselects all rows.
func (t *TableData) DeselectAll() {
	t.SelectedRows = make(map[int]bool)
}

// IsSelected returns whether a row is selected.
func (t *TableData) IsSelected(rowIndex int) bool {
	return t.SelectedRows[rowIndex]
}

// GetSelectedCount returns the number of selected rows.
func (t *TableData) GetSelectedCount() int {
	return len(t.SelectedRows)
}

// GetSelectedRows returns indices of all selected rows.
func (t *TableData) GetSelectedRows() []int {
	indices := make([]int, 0, len(t.SelectedRows))
	for idx := range t.SelectedRows {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	return indices
}

// GetRow returns a single row by index.
func (t *TableData) GetRow(rowIndex int) []interface{} {
	if rowIndex < 0 || rowIndex >= len(t.Rows) {
		return nil
	}
	return t.Rows[rowIndex]
}

// GetRowCount returns the total number of rows.
func (t *TableData) GetRowCount() int {
	return len(t.Rows)
}

// GetColumnCount returns the total number of columns.
func (t *TableData) GetColumnCount() int {
	return len(t.Columns)
}

// SetRowMetadata sets metadata for a row.
func (t *TableData) SetRowMetadata(rowIndex int, key string, value interface{}) {
	if _, ok := t.RowMetadata[rowIndex]; !ok {
		t.RowMetadata[rowIndex] = make(map[string]interface{})
	}
	t.RowMetadata[rowIndex][key] = value
}

// GetRowMetadata retrieves metadata for a row.
func (t *TableData) GetRowMetadata(rowIndex int, key string) (interface{}, bool) {
	if meta, ok := t.RowMetadata[rowIndex]; ok {
		val, exists := meta[key]
		return val, exists
	}
	return nil, false
}

// Filter filters rows based on a predicate function.
// Returns a new TableData with filtered rows, preserving column structure.
func (t *TableData) Filter(predicate func([]interface{}) bool) *TableData {
	filtered := make([][]interface{}, 0)
	for _, row := range t.Rows {
		if predicate(row) {
			filtered = append(filtered, row)
		}
	}
	return NewTableData(t.Columns, filtered)
}

// GetVisibleRows returns a slice of rows for the given range.
func (t *TableData) GetVisibleRows(startRow, endRow int) [][]interface{} {
	if startRow < 0 {
		startRow = 0
	}
	if endRow > len(t.Rows) {
		endRow = len(t.Rows)
	}
	if startRow >= endRow {
		return [][]interface{}{}
	}

	result := make([][]interface{}, endRow-startRow)
	copy(result, t.Rows[startRow:endRow])
	return result
}

// SearchRows finds rows matching a search term in any column.
func (t *TableData) SearchRows(searchTerm string) []int {
	var results []int
	searchLower := strings.ToLower(searchTerm)

	for rowIdx, row := range t.Rows {
		for _, cell := range row {
			if strings.Contains(strings.ToLower(toString(cell)), searchLower) {
				results = append(results, rowIdx)
				break
			}
		}
	}
	return results
}

// GetSortedIndices returns the indices of rows in their current sorted order.
func (t *TableData) GetSortedIndices() []int {
	indices := make([]int, len(t.Rows))
	for i := range indices {
		indices[i] = i
	}
	return indices
}

// Helper functions

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case nil:
		return ""
	default:
		return strings.TrimSpace(strings.ToLower(String(v)))
	}
}

// String converts a value to string safely.
func String(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	return strings.TrimSpace(fmt.Sprintf("%v", v))
}
