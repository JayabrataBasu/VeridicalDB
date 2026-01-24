// Package components provides reusable data models and managers for TUI components.
package components

import "fmt"

// RowDetail represents detailed view of a single row.
type RowDetail struct {
	Columns  []string               // Column names
	Values   []interface{}          // Row values corresponding to columns
	Index    int                    // Original row index in table
	Metadata map[string]interface{} // Additional metadata
}

// NewRowDetail creates a new row detail from a row.
func NewRowDetail(index int, columns []string, rowValues []interface{}) *RowDetail {
	return &RowDetail{
		Columns:  columns,
		Values:   rowValues,
		Index:    index,
		Metadata: make(map[string]interface{}),
	}
}

// GetValue returns the value of a column by name.
func (rd *RowDetail) GetValue(columnName string) interface{} {
	for i, col := range rd.Columns {
		if col == columnName {
			if i < len(rd.Values) {
				return rd.Values[i]
			}
		}
	}
	return nil
}

// GetColumnCount returns the number of columns.
func (rd *RowDetail) GetColumnCount() int {
	return len(rd.Columns)
}

// SetMetadata sets metadata for this row detail.
func (rd *RowDetail) SetMetadata(key string, value interface{}) {
	rd.Metadata[key] = value
}

// GetMetadata retrieves metadata for this row detail.
func (rd *RowDetail) GetMetadata(key string) (interface{}, bool) {
	val, exists := rd.Metadata[key]
	return val, exists
}

// DetailViewManager manages switching between detail and table views.
type DetailViewManager struct {
	TableData     *TableData // Reference to the table data
	CurrentDetail *RowDetail // Currently displayed row detail
	DetailHistory []int      // History of viewed row indices
	HistoryIndex  int        // Current position in history
}

// NewDetailViewManager creates a new detail view manager.
func NewDetailViewManager(tableData *TableData) *DetailViewManager {
	return &DetailViewManager{
		TableData:     tableData,
		CurrentDetail: nil,
		DetailHistory: make([]int, 0),
		HistoryIndex:  -1,
	}
}

// ShowDetail displays the detail view for a specific row.
func (dvm *DetailViewManager) ShowDetail(rowIndex int) error {
	if rowIndex < 0 || rowIndex >= dvm.TableData.GetRowCount() {
		return ErrInvalidRowIndex
	}

	row := dvm.TableData.GetRow(rowIndex)
	if row == nil {
		return ErrInvalidRowIndex
	}

	dvm.CurrentDetail = NewRowDetail(
		rowIndex,
		dvm.TableData.Columns,
		row,
	)

	// Add to history
	if dvm.HistoryIndex >= 0 && dvm.HistoryIndex < len(dvm.DetailHistory)-1 {
		// Remove forward history if we're not at the end
		dvm.DetailHistory = dvm.DetailHistory[:dvm.HistoryIndex+1]
	}

	dvm.DetailHistory = append(dvm.DetailHistory, rowIndex)
	dvm.HistoryIndex = len(dvm.DetailHistory) - 1

	return nil
}

// NextRow moves to the next row in detail view (if exists).
func (dvm *DetailViewManager) NextRow() error {
	if dvm.CurrentDetail == nil {
		return ErrNoDetailView
	}

	nextIndex := dvm.CurrentDetail.Index + 1
	return dvm.ShowDetail(nextIndex)
}

// PreviousRow moves to the previous row in detail view (if exists).
func (dvm *DetailViewManager) PreviousRow() error {
	if dvm.CurrentDetail == nil {
		return ErrNoDetailView
	}

	prevIndex := dvm.CurrentDetail.Index - 1
	return dvm.ShowDetail(prevIndex)
}

// GoBack navigates back in detail view history.
func (dvm *DetailViewManager) GoBack() error {
	if dvm.HistoryIndex <= 0 {
		return ErrCannotGoBack
	}

	dvm.HistoryIndex--
	rowIndex := dvm.DetailHistory[dvm.HistoryIndex]

	// Update CurrentDetail without adding to history
	row := dvm.TableData.GetRow(rowIndex)
	if row == nil {
		return ErrInvalidRowIndex
	}

	dvm.CurrentDetail = NewRowDetail(
		rowIndex,
		dvm.TableData.Columns,
		row,
	)
	return nil
}

// GoForward navigates forward in detail view history.
func (dvm *DetailViewManager) GoForward() error {
	if dvm.HistoryIndex >= len(dvm.DetailHistory)-1 {
		return ErrCannotGoForward
	}

	dvm.HistoryIndex++
	rowIndex := dvm.DetailHistory[dvm.HistoryIndex]

	// Update CurrentDetail without adding to history
	row := dvm.TableData.GetRow(rowIndex)
	if row == nil {
		return ErrInvalidRowIndex
	}

	dvm.CurrentDetail = NewRowDetail(
		rowIndex,
		dvm.TableData.Columns,
		row,
	)
	return nil
}

// HasDetail returns whether there's a current detail view.
func (dvm *DetailViewManager) HasDetail() bool {
	return dvm.CurrentDetail != nil
}

// CloseDetail closes the detail view.
func (dvm *DetailViewManager) CloseDetail() {
	dvm.CurrentDetail = nil
}

// CanGoBack returns whether back navigation is possible.
func (dvm *DetailViewManager) CanGoBack() bool {
	return dvm.HistoryIndex > 0
}

// CanGoForward returns whether forward navigation is possible.
func (dvm *DetailViewManager) CanGoForward() bool {
	return dvm.HistoryIndex < len(dvm.DetailHistory)-1
}

// GetDetailFieldCount returns the number of fields in the current detail.
func (dvm *DetailViewManager) GetDetailFieldCount() int {
	if dvm.CurrentDetail == nil {
		return 0
	}
	return dvm.CurrentDetail.GetColumnCount()
}

// GetDetailField returns a field from the current detail by name.
func (dvm *DetailViewManager) GetDetailField(fieldName string) interface{} {
	if dvm.CurrentDetail == nil {
		return nil
	}
	return dvm.CurrentDetail.GetValue(fieldName)
}

// GetDetailColumnNames returns column names from the current detail.
func (dvm *DetailViewManager) GetDetailColumnNames() []string {
	if dvm.CurrentDetail == nil {
		return nil
	}
	return dvm.CurrentDetail.Columns
}

// GetDetailValues returns all values from the current detail.
func (dvm *DetailViewManager) GetDetailValues() []interface{} {
	if dvm.CurrentDetail == nil {
		return nil
	}
	return dvm.CurrentDetail.Values
}

// GetDetailRowIndex returns the current row index being viewed.
func (dvm *DetailViewManager) GetDetailRowIndex() int {
	if dvm.CurrentDetail == nil {
		return -1
	}
	return dvm.CurrentDetail.Index
}

// ClearHistory clears the navigation history.
func (dvm *DetailViewManager) ClearHistory() {
	dvm.DetailHistory = make([]int, 0)
	dvm.HistoryIndex = -1
}

// Helper errors
var (
	ErrNoDetailView    = fmt.Errorf("detail view: no detail view active")
	ErrCannotGoBack    = fmt.Errorf("detail view: cannot go back in history")
	ErrCannotGoForward = fmt.Errorf("detail view: cannot go forward in history")
	ErrInvalidRowIndex = fmt.Errorf("detail view: invalid row index")
)

// NewError creates a formatted error message.
func NewError(msg string) error {
	return fmt.Errorf("detail view: %s", msg)
}
