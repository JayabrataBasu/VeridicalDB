package components

import (
	"testing"
)

func TestNewRowDetail(t *testing.T) {
	columns := []string{"ID", "Name", "Email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	detail := NewRowDetail(0, columns, values)
	if len(detail.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(detail.Columns))
	}
	if detail.Index != 0 {
		t.Errorf("Expected index 0, got %d", detail.Index)
	}
}

func TestRowDetailGetValue(t *testing.T) {
	columns := []string{"ID", "Name", "Email"}
	values := []interface{}{1, "Alice", "alice@example.com"}

	detail := NewRowDetail(0, columns, values)

	val := detail.GetValue("Name")
	if val != "Alice" {
		t.Errorf("Expected 'Alice', got %v", val)
	}

	val = detail.GetValue("nonexistent")
	if val != nil {
		t.Errorf("Expected nil for nonexistent column, got %v", val)
	}
}

func TestRowDetailGetColumnCount(t *testing.T) {
	detail := NewRowDetail(0, []string{"A", "B", "C"}, []interface{}{1, 2, 3})

	if detail.GetColumnCount() != 3 {
		t.Errorf("Expected 3 columns, got %d", detail.GetColumnCount())
	}
}

func TestRowDetailMetadata(t *testing.T) {
	detail := NewRowDetail(0, []string{"A"}, []interface{}{1})

	detail.SetMetadata("viewed", true)
	detail.SetMetadata("timestamp", "2024-01-24")

	val, exists := detail.GetMetadata("viewed")
	if !exists || val != true {
		t.Error("Metadata not stored correctly")
	}

	_, exists = detail.GetMetadata("nonexistent")
	if exists {
		t.Error("Nonexistent metadata should not exist")
	}
}

func TestNewDetailViewManager(t *testing.T) {
	tableData := NewTableData([]string{"A"}, [][]interface{}{{1}})
	dvm := NewDetailViewManager(tableData)

	if dvm == nil {
		t.Fatal("NewDetailViewManager returned nil")
	}
	// Initially should not have detail
}

func TestShowDetail(t *testing.T) {
	tableData := NewTableData(
		[]string{"ID", "Name"},
		[][]interface{}{
			{1, "Alice"},
			{2, "Bob"},
		},
	)
	dvm := NewDetailViewManager(tableData)

	err := dvm.ShowDetail(0)
	if err != nil {
		t.Fatalf("ShowDetail failed: %v", err)
	}

	if !dvm.HasDetail() {
		t.Error("Should have detail view after ShowDetail")
	}
	if dvm.CurrentDetail.Index != 0 {
		t.Errorf("Expected index 0, got %d", dvm.CurrentDetail.Index)
	}
}

func TestShowDetailInvalidIndex(t *testing.T) {
	tableData := NewTableData([]string{"A"}, [][]interface{}{{1}})
	dvm := NewDetailViewManager(tableData)

	err := dvm.ShowDetail(5)
	if err == nil {
		t.Error("ShowDetail should fail for invalid index")
	}
}

func TestNextRow(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	err := dvm.NextRow()
	if err != nil {
		t.Fatalf("NextRow failed: %v", err)
	}

	if dvm.CurrentDetail.Index != 1 {
		t.Errorf("Expected index 1, got %d", dvm.CurrentDetail.Index)
	}

	// Go to last row
	_ = dvm.ShowDetail(2)
	err = dvm.NextRow()
	if err == nil {
		t.Error("NextRow should fail when at last row")
	}
}

func TestPreviousRow(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(1)
	err := dvm.PreviousRow()
	if err != nil {
		t.Fatalf("PreviousRow failed: %v", err)
	}

	if dvm.CurrentDetail.Index != 0 {
		t.Errorf("Expected index 0, got %d", dvm.CurrentDetail.Index)
	}

	// Try from first row
	err = dvm.PreviousRow()
	if err == nil {
		t.Error("PreviousRow should fail when at first row")
	}
}

func TestDetailNavigation(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}, {4}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	if dvm.CurrentDetail.Index != 0 {
		t.Error("Should start at row 0")
	}

	_ = dvm.NextRow()
	if dvm.CurrentDetail.Index != 1 {
		t.Error("Should move to row 1")
	}

	_ = dvm.NextRow()
	if dvm.CurrentDetail.Index != 2 {
		t.Error("Should move to row 2")
	}

	_ = dvm.PreviousRow()
	if dvm.CurrentDetail.Index != 1 {
		t.Error("Should move back to row 1")
	}
}

func TestCloseDetail(t *testing.T) {
	tableData := NewTableData([]string{"A"}, [][]interface{}{{1}})
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	if !dvm.HasDetail() {
		t.Error("Should have detail before close")
	}

	dvm.CloseDetail()
	if dvm.HasDetail() {
		t.Error("Should not have detail after close")
	}
}

func TestDetailHistory(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}, {4}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	_ = dvm.ShowDetail(2)
	_ = dvm.ShowDetail(1)

	if len(dvm.DetailHistory) != 3 {
		t.Errorf("Expected 3 items in history, got %d", len(dvm.DetailHistory))
	}

	if dvm.HistoryIndex != 2 {
		t.Errorf("Expected history index 2, got %d", dvm.HistoryIndex)
	}
}

func TestGoBack(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	_ = dvm.ShowDetail(1)
	_ = dvm.ShowDetail(2)

	err := dvm.GoBack()
	if err != nil {
		t.Fatalf("GoBack failed: %v", err)
	}
	if dvm.CurrentDetail.Index != 1 {
		t.Errorf("Expected index 1 after GoBack, got %d", dvm.CurrentDetail.Index)
	}

	err = dvm.GoBack()
	if err != nil {
		t.Fatalf("Second GoBack failed: %v", err)
	}
	if dvm.CurrentDetail.Index != 0 {
		t.Errorf("Expected index 0, got %d", dvm.CurrentDetail.Index)
	}

	err = dvm.GoBack()
	if err == nil {
		t.Error("GoBack should fail at start of history")
	}
}

func TestGoForward(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	_ = dvm.ShowDetail(1)
	_ = dvm.ShowDetail(2)

	_ = dvm.GoBack()
	_ = dvm.GoBack() // Now at index 0

	err := dvm.GoForward()
	if err != nil {
		t.Fatalf("GoForward failed: %v", err)
	}
	if dvm.CurrentDetail.Index != 1 {
		t.Errorf("Expected index 1 after GoForward, got %d", dvm.CurrentDetail.Index)
	}

	_ = dvm.GoForward() // Now at index 2
	err = dvm.GoForward()
	if err == nil {
		t.Error("GoForward should fail at end of history")
	}
}

func TestHistoryNavigationBoundaries(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}},
	)
	dvm := NewDetailViewManager(tableData)

	if dvm.CanGoBack() {
		t.Error("Should not be able to go back with no detail")
	}
	if dvm.CanGoForward() {
		t.Error("Should not be able to go forward with no detail")
	}

	_ = dvm.ShowDetail(0)
	_ = dvm.ShowDetail(1)

	if !dvm.CanGoBack() {
		t.Error("Should be able to go back")
	}
	if dvm.CanGoForward() {
		t.Error("Should not be able to go forward at end of history")
	}
}

func TestGetDetailFieldCount(t *testing.T) {
	tableData := NewTableData(
		[]string{"ID", "Name", "Email"},
		[][]interface{}{{1, "Alice", "alice@example.com"}},
	)
	dvm := NewDetailViewManager(tableData)

	if dvm.GetDetailFieldCount() != 0 {
		t.Error("Should have 0 fields before showing detail")
	}

	_ = dvm.ShowDetail(0)
	if dvm.GetDetailFieldCount() != 3 {
		t.Errorf("Expected 3 fields, got %d", dvm.GetDetailFieldCount())
	}
}

func TestGetDetailField(t *testing.T) {
	tableData := NewTableData(
		[]string{"ID", "Name"},
		[][]interface{}{{1, "Alice"}},
	)
	dvm := NewDetailViewManager(tableData)

	if dvm.GetDetailField("Name") != nil {
		t.Error("Should return nil when no detail is active")
	}

	_ = dvm.ShowDetail(0)
	val := dvm.GetDetailField("Name")
	if val != "Alice" {
		t.Errorf("Expected 'Alice', got %v", val)
	}
}

func TestGetDetailColumnNames(t *testing.T) {
	columns := []string{"ID", "Name"}
	tableData := NewTableData(
		columns,
		[][]interface{}{{1, "Alice"}},
	)
	dvm := NewDetailViewManager(tableData)

	cols := dvm.GetDetailColumnNames()
	if cols != nil {
		t.Error("Should return nil when no detail is active")
	}

	_ = dvm.ShowDetail(0)
	cols = dvm.GetDetailColumnNames()
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
}

func TestGetDetailValues(t *testing.T) {
	tableData := NewTableData(
		[]string{"ID", "Name"},
		[][]interface{}{{1, "Alice"}},
	)
	dvm := NewDetailViewManager(tableData)

	vals := dvm.GetDetailValues()
	if vals != nil {
		t.Error("Should return nil when no detail is active")
	}

	_ = dvm.ShowDetail(0)
	vals = dvm.GetDetailValues()
	if len(vals) != 2 {
		t.Errorf("Expected 2 values, got %d", len(vals))
	}
}

func TestGetDetailRowIndex(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}},
	)
	dvm := NewDetailViewManager(tableData)

	idx := dvm.GetDetailRowIndex()
	if idx != -1 {
		t.Errorf("Expected -1 when no detail, got %d", idx)
	}

	_ = dvm.ShowDetail(1)
	idx = dvm.GetDetailRowIndex()
	if idx != 1 {
		t.Errorf("Expected 1, got %d", idx)
	}
}

func TestClearHistory(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	_ = dvm.ShowDetail(1)

	if len(dvm.DetailHistory) != 2 {
		t.Error("History should have 2 items")
	}

	dvm.ClearHistory()
	if len(dvm.DetailHistory) != 0 {
		t.Error("History should be cleared")
	}
	if dvm.HistoryIndex != -1 {
		t.Error("History index should be -1")
	}
}

func TestHistoryPruning(t *testing.T) {
	tableData := NewTableData(
		[]string{"A"},
		[][]interface{}{{1}, {2}, {3}, {4}},
	)
	dvm := NewDetailViewManager(tableData)

	_ = dvm.ShowDetail(0)
	_ = dvm.ShowDetail(1)
	_ = dvm.ShowDetail(2)

	// Go back
	_ = dvm.GoBack()

	// Show new detail (should prune forward history)
	_ = dvm.ShowDetail(3)

	if len(dvm.DetailHistory) != 3 {
		t.Errorf("Expected 3 items in history after pruning, got %d", len(dvm.DetailHistory))
	}

	if dvm.DetailHistory[2] != 3 {
		t.Error("Last history item should be the new detail")
	}
}
