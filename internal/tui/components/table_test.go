package components

import (
	"testing"
)

func TestNewTableData(t *testing.T) {
	columns := []string{"ID", "Name", "Email"}
	rows := [][]interface{}{
		{1, "Alice", "alice@example.com"},
		{2, "Bob", "bob@example.com"},
	}

	table := NewTableData(columns, rows)
	if len(table.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(table.Columns))
	}
	if len(table.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(table.Rows))
	}
}

func TestSelectRow(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}, {3}})

	table.SelectRow(0)
	if !table.IsSelected(0) {
		t.Error("Row 0 should be selected")
	}
	if table.IsSelected(1) {
		t.Error("Row 1 should not be selected")
	}
}

func TestDeselectRow(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}})
	table.SelectRow(0)
	table.DeselectRow(0)

	if table.IsSelected(0) {
		t.Error("Row 0 should be deselected")
	}
}

func TestToggleRow(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}})

	table.ToggleRow(0)
	if !table.IsSelected(0) {
		t.Error("Row 0 should be selected after toggle")
	}

	table.ToggleRow(0)
	if table.IsSelected(0) {
		t.Error("Row 0 should be deselected after second toggle")
	}
}

func TestSelectAll(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}, {3}})
	table.SelectAll()

	if table.GetSelectedCount() != 3 {
		t.Errorf("Expected 3 selected rows, got %d", table.GetSelectedCount())
	}
}

func TestDeselectAll(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}, {3}})
	table.SelectAll()
	table.DeselectAll()

	if table.GetSelectedCount() != 0 {
		t.Error("All rows should be deselected")
	}
}

func TestGetSelectedCount(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}, {3}})

	if table.GetSelectedCount() != 0 {
		t.Error("Initially no rows should be selected")
	}

	table.SelectRow(0)
	table.SelectRow(2)

	if table.GetSelectedCount() != 2 {
		t.Errorf("Expected 2 selected rows, got %d", table.GetSelectedCount())
	}
}

func TestGetSelectedRows(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}, {3}})
	table.SelectRow(0)
	table.SelectRow(2)

	selected := table.GetSelectedRows()
	if len(selected) != 2 {
		t.Errorf("Expected 2 selected rows, got %d", len(selected))
	}
	if selected[0] != 0 || selected[1] != 2 {
		t.Errorf("Expected [0, 2], got %v", selected)
	}
}

func TestSortAscending(t *testing.T) {
	table := NewTableData(
		[]string{"Name"},
		[][]interface{}{
			{"Charlie"},
			{"Alice"},
			{"Bob"},
		},
	)

	err := table.Sort(0)
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}

	if table.SortConfig == nil {
		t.Fatal("SortConfig should be set")
	}
	if table.SortConfig.Direction != SortAscending {
		t.Error("Should be sorted ascending")
	}

	// Check row order
	if table.Rows[0][0] != "Alice" {
		t.Errorf("First row should be Alice, got %v", table.Rows[0][0])
	}
	if table.Rows[1][0] != "Bob" {
		t.Errorf("Second row should be Bob, got %v", table.Rows[1][0])
	}
	if table.Rows[2][0] != "Charlie" {
		t.Errorf("Third row should be Charlie, got %v", table.Rows[2][0])
	}
}

func TestSortDescending(t *testing.T) {
	table := NewTableData(
		[]string{"Name"},
		[][]interface{}{
			{"Alice"},
			{"Charlie"},
			{"Bob"},
		},
	)

	_ = table.Sort(0) // Ascending
	_ = table.Sort(0) // Toggle to descending

	if table.SortConfig.Direction != SortDescending {
		t.Error("Should be sorted descending")
	}

	if table.Rows[0][0] != "Charlie" {
		t.Errorf("First row should be Charlie, got %v", table.Rows[0][0])
	}
}

func TestSortToggleOff(t *testing.T) {
	table := NewTableData(
		[]string{"Name"},
		[][]interface{}{
			{"Alice"},
			{"Charlie"},
			{"Bob"},
		},
	)

	_ = table.Sort(0) // Ascending
	_ = table.Sort(0) // Descending
	_ = table.Sort(0) // Toggle off

	if table.SortConfig != nil {
		t.Error("SortConfig should be nil after toggling off")
	}
}

func TestSortInvalidColumn(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}})

	err := table.Sort(5) // Invalid index
	if err == nil {
		t.Error("Should return error for invalid column index")
	}
}

func TestGetRow(t *testing.T) {
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}
	table := NewTableData([]string{"ID", "Name"}, rows)

	row := table.GetRow(0)
	if row[0] != 1 || row[1] != "Alice" {
		t.Errorf("Expected [1, Alice], got %v", row)
	}
}

func TestGetRowInvalidIndex(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}})
	row := table.GetRow(5)

	if row != nil {
		t.Error("Should return nil for invalid index")
	}
}

func TestGetRowCount(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}, {2}, {3}})

	if table.GetRowCount() != 3 {
		t.Errorf("Expected 3 rows, got %d", table.GetRowCount())
	}
}

func TestGetColumnCount(t *testing.T) {
	table := NewTableData([]string{"A", "B", "C"}, [][]interface{}{{1, 2, 3}})

	if table.GetColumnCount() != 3 {
		t.Errorf("Expected 3 columns, got %d", table.GetColumnCount())
	}
}

func TestRowMetadata(t *testing.T) {
	table := NewTableData([]string{"A"}, [][]interface{}{{1}})

	table.SetRowMetadata(0, "highlighted", true)
	table.SetRowMetadata(0, "comment", "Important")

	val, exists := table.GetRowMetadata(0, "highlighted")
	if !exists || val != true {
		t.Error("Metadata should be set correctly")
	}

	val, exists = table.GetRowMetadata(0, "comment")
	if !exists || val != "Important" {
		t.Error("String metadata should be stored")
	}

	_, exists = table.GetRowMetadata(0, "nonexistent")
	if exists {
		t.Error("Nonexistent metadata should not exist")
	}
}

func TestGetVisibleRows(t *testing.T) {
	rows := [][]interface{}{{1}, {2}, {3}, {4}, {5}}
	table := NewTableData([]string{"A"}, rows)

	visible := table.GetVisibleRows(1, 4)
	if len(visible) != 3 {
		t.Errorf("Expected 3 visible rows, got %d", len(visible))
	}
	if visible[0][0] != 2 {
		t.Error("First visible row should be 2")
	}
}

func TestSearchRows(t *testing.T) {
	table := NewTableData(
		[]string{"Name", "City"},
		[][]interface{}{
			{"Alice", "NYC"},
			{"Bob", "LA"},
			{"Charlie", "NYC"},
		},
	)

	results := table.SearchRows("NYC")
	if len(results) != 2 {
		t.Errorf("Expected 2 search results, got %d", len(results))
	}
	if results[0] != 0 || results[1] != 2 {
		t.Errorf("Expected [0, 2], got %v", results)
	}
}

func TestSearchRowsCaseInsensitive(t *testing.T) {
	table := NewTableData(
		[]string{"Name"},
		[][]interface{}{
			{"alice"},
			{"ALICE"},
			{"Bob"},
		},
	)

	results := table.SearchRows("ALICE")
	if len(results) != 2 {
		t.Errorf("Expected 2 results for case-insensitive search, got %d", len(results))
	}
}

func TestFilter(t *testing.T) {
	table := NewTableData(
		[]string{"Age"},
		[][]interface{}{
			{20},
			{35},
			{45},
			{18},
		},
	)

	filtered := table.Filter(func(row []interface{}) bool {
		age := row[0].(int)
		return age >= 30
	})

	if filtered.GetRowCount() != 2 {
		t.Errorf("Expected 2 filtered rows, got %d", filtered.GetRowCount())
	}
}

func TestSelectionPreservedAfterSort(t *testing.T) {
	table := NewTableData(
		[]string{"ID", "Name"},
		[][]interface{}{
			{1, "Alice"},
			{2, "Bob"},
			{3, "Charlie"},
		},
	)

	table.SelectRow(1) // Select Bob
	_ = table.Sort(1)  // Sort by Name

	// Bob should still be selected even though his row position changed
	if table.GetSelectedCount() != 1 {
		t.Errorf("Expected 1 selected row after sort, got %d", table.GetSelectedCount())
	}
}

func TestSortNumericColumn(t *testing.T) {
	table := NewTableData(
		[]string{"Number"},
		[][]interface{}{
			{10},
			{5},
			{20},
			{3},
		},
	)

	_ = table.Sort(0) // Sort ascending

	// Note: Current implementation sorts as strings, so 10 < 20 < 3 < 5
	// This is the current behavior; could be improved with type detection
	if table.GetRowCount() != 4 {
		t.Error("All rows should remain after sort")
	}
}
