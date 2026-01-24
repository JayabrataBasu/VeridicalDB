package components

// PaginationManager handles pagination logic with dynamic page sizing and auto-fetch capability.
type PaginationManager struct {
	TotalRows    int  // Total number of rows
	PageSize     int  // Rows per page
	CurrentPage  int  // Zero-indexed current page
	PrefetchSize int  // Number of rows to prefetch ahead
	AutoFetch    bool // Whether to auto-fetch next page
	FetchedTo    int  // How many rows have been fetched so far
}

// NewPaginationManager creates a new pagination manager.
func NewPaginationManager(totalRows int, pageSize int) *PaginationManager {
	if pageSize < 1 {
		pageSize = 10
	}
	return &PaginationManager{
		TotalRows:    totalRows,
		PageSize:     pageSize,
		CurrentPage:  0,
		PrefetchSize: pageSize * 2, // Prefetch 2 pages ahead
		AutoFetch:    false,
		FetchedTo:    totalRows, // Start with all rows fetched
	}
}

// SetPageSize updates the page size and recalculates pagination.
func (pm *PaginationManager) SetPageSize(pageSize int) error {
	if pageSize < 1 {
		return ErrInvalidPageSize
	}
	pm.PageSize = pageSize
	// Recalculate current page to ensure it's still valid
	if pm.CurrentPage >= pm.GetPageCount() && pm.GetPageCount() > 0 {
		pm.CurrentPage = pm.GetPageCount() - 1
	}
	return nil
}

// GetPageCount returns the total number of pages.
func (pm *PaginationManager) GetPageCount() int {
	if pm.PageSize == 0 {
		return 0
	}
	return (pm.TotalRows + pm.PageSize - 1) / pm.PageSize
}

// GetCurrentPageSize returns the number of rows on the current page.
func (pm *PaginationManager) GetCurrentPageSize() int {
	if pm.CurrentPage >= pm.GetPageCount() {
		return 0
	}

	startRow := pm.GetStartRow()
	endRow := pm.GetEndRow()
	return endRow - startRow
}

// GetStartRow returns the zero-indexed start row of the current page.
func (pm *PaginationManager) GetStartRow() int {
	return pm.CurrentPage * pm.PageSize
}

// GetEndRow returns the zero-indexed end row (exclusive) of the current page.
func (pm *PaginationManager) GetEndRow() int {
	endRow := (pm.CurrentPage + 1) * pm.PageSize
	if endRow > pm.TotalRows {
		endRow = pm.TotalRows
	}
	return endRow
}

// NextPage moves to the next page if available.
func (pm *PaginationManager) NextPage() bool {
	if pm.CurrentPage < pm.GetPageCount()-1 {
		pm.CurrentPage++
		pm.checkPrefetch()
		return true
	}
	return false
}

// PreviousPage moves to the previous page if available.
func (pm *PaginationManager) PreviousPage() bool {
	if pm.CurrentPage > 0 {
		pm.CurrentPage--
		return true
	}
	return false
}

// GoToPage navigates to a specific page (zero-indexed).
func (pm *PaginationManager) GoToPage(page int) bool {
	if page < 0 || page >= pm.GetPageCount() {
		return false
	}
	pm.CurrentPage = page
	pm.checkPrefetch()
	return true
}

// FirstPage navigates to the first page.
func (pm *PaginationManager) FirstPage() {
	pm.CurrentPage = 0
}

// LastPage navigates to the last page.
func (pm *PaginationManager) LastPage() {
	pageCount := pm.GetPageCount()
	if pageCount > 0 {
		pm.CurrentPage = pageCount - 1
	}
}

// SetTotalRows updates the total row count (used for auto-fetch scenarios).
func (pm *PaginationManager) SetTotalRows(totalRows int) {
	pm.TotalRows = totalRows
	// Recalculate current page to ensure it's still valid
	if pm.CurrentPage >= pm.GetPageCount() && pm.GetPageCount() > 0 {
		pm.CurrentPage = pm.GetPageCount() - 1
	}
}

// ShouldFetch returns whether more data should be fetched from the backend.
// This is used for lazy-loading scenarios. Returns true if there's more data
// to fetch and we've consumed enough of what's already fetched.
func (pm *PaginationManager) ShouldFetch() bool {
	if !pm.AutoFetch {
		return false
	}
	if pm.FetchedTo >= pm.TotalRows {
		return false
	}
	// Check if we need to fetch based on prefetch strategy
	// We should fetch if the end of the current page + prefetch is beyond what we've fetched
	targetRow := pm.GetEndRow() + pm.PrefetchSize
	return targetRow > pm.FetchedTo
}

// GetFetchSize returns how many rows should be fetched next.
// This is the difference between what's needed and what's already fetched.
func (pm *PaginationManager) GetFetchSize() int {
	if !pm.AutoFetch || pm.FetchedTo >= pm.TotalRows {
		return 0
	}

	// Fetch prefetch size ahead of current page
	targetRow := pm.GetEndRow() + pm.PrefetchSize
	if targetRow > pm.TotalRows {
		targetRow = pm.TotalRows
	}

	return targetRow - pm.FetchedTo
}

// MarkFetched marks that rows have been fetched up to a certain point.
func (pm *PaginationManager) MarkFetched(rowCount int) {
	if pm.FetchedTo+rowCount <= pm.TotalRows {
		pm.FetchedTo += rowCount
	} else {
		pm.FetchedTo = pm.TotalRows
	}
}

// checkPrefetch checks if prefetch is needed for the current page.
func (pm *PaginationManager) checkPrefetch() {
	if !pm.AutoFetch {
		return
	}

	// Check if we need to prefetch for the current page
	pageEndRow := pm.GetEndRow()
	_ = pageEndRow // Prefetch logic handled in ShouldFetch
}

// IsFirstPage returns whether we're on the first page.
func (pm *PaginationManager) IsFirstPage() bool {
	return pm.CurrentPage == 0
}

// IsLastPage returns whether we're on the last page.
func (pm *PaginationManager) IsLastPage() bool {
	pageCount := pm.GetPageCount()
	if pageCount == 0 {
		return true
	}
	return pm.CurrentPage == pageCount-1
}

// GetPaginationInfo returns a formatted string with pagination info.
func (pm *PaginationManager) GetPaginationInfo() string {
	pageCount := pm.GetPageCount()
	if pageCount == 0 {
		return "No data"
	}

	startRow := pm.GetStartRow()
	endRow := pm.GetEndRow()

	return formatPaginationInfo(pm.CurrentPage+1, pageCount, startRow+1, endRow, pm.TotalRows)
}

// Helper function
func formatPaginationInfo(currentPage, pageCount, startRow, endRow, totalRows int) string {
	return "Page " + String(currentPage) + "/" + String(pageCount) + " | Rows " + String(startRow) + "-" + String(endRow) + " of " + String(totalRows)
}

// EnableAutoFetch enables auto-fetch mode for lazy loading.
func (pm *PaginationManager) EnableAutoFetch() {
	pm.AutoFetch = true
	pm.FetchedTo = pm.PageSize * 2 // Start with first 2 pages
}

// DisableAutoFetch disables auto-fetch mode.
func (pm *PaginationManager) DisableAutoFetch() {
	pm.AutoFetch = false
}
