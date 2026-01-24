package components

import (
	"testing"
)

func TestNewPaginationManager(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if pm.TotalRows != 100 {
		t.Errorf("Expected 100 total rows, got %d", pm.TotalRows)
	}
	if pm.PageSize != 10 {
		t.Errorf("Expected page size 10, got %d", pm.PageSize)
	}
	if pm.CurrentPage != 0 {
		t.Errorf("Expected starting page 0, got %d", pm.CurrentPage)
	}
}

func TestNewPaginationManagerInvalidPageSize(t *testing.T) {
	pm := NewPaginationManager(100, 0)

	if pm.PageSize != 10 {
		t.Errorf("Expected default page size 10 for invalid input, got %d", pm.PageSize)
	}
}

func TestGetPageCount(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if pm.GetPageCount() != 10 {
		t.Errorf("Expected 10 pages, got %d", pm.GetPageCount())
	}

	pm.TotalRows = 95
	if pm.GetPageCount() != 10 {
		t.Errorf("Expected 10 pages for 95 rows with size 10, got %d", pm.GetPageCount())
	}

	pm.TotalRows = 0
	if pm.GetPageCount() != 0 {
		t.Errorf("Expected 0 pages for 0 rows, got %d", pm.GetPageCount())
	}
}

func TestGetStartRow(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if pm.GetStartRow() != 0 {
		t.Errorf("Expected start row 0 on page 0, got %d", pm.GetStartRow())
	}

	pm.CurrentPage = 2
	if pm.GetStartRow() != 20 {
		t.Errorf("Expected start row 20 on page 2, got %d", pm.GetStartRow())
	}
}

func TestGetEndRow(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if pm.GetEndRow() != 10 {
		t.Errorf("Expected end row 10 on page 0, got %d", pm.GetEndRow())
	}

	pm.CurrentPage = 9
	if pm.GetEndRow() != 100 {
		t.Errorf("Expected end row 100 on last page, got %d", pm.GetEndRow())
	}

	pm.CurrentPage = 5
	if pm.GetEndRow() != 60 {
		t.Errorf("Expected end row 60 on page 5, got %d", pm.GetEndRow())
	}
}

func TestGetCurrentPageSize(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if pm.GetCurrentPageSize() != 10 {
		t.Errorf("Expected current page size 10, got %d", pm.GetCurrentPageSize())
	}

	pm.CurrentPage = 9
	if pm.GetCurrentPageSize() != 10 {
		t.Errorf("Expected last page size 10, got %d", pm.GetCurrentPageSize())
	}

	pm.TotalRows = 95
	pm.CurrentPage = 9
	if pm.GetCurrentPageSize() != 5 {
		t.Errorf("Expected partial last page size 5, got %d", pm.GetCurrentPageSize())
	}
}

func TestNextPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	ok := pm.NextPage()
	if !ok {
		t.Error("NextPage should succeed on page 0")
	}
	if pm.CurrentPage != 1 {
		t.Errorf("Expected current page 1, got %d", pm.CurrentPage)
	}

	pm.CurrentPage = 9
	ok = pm.NextPage()
	if ok {
		t.Error("NextPage should fail on last page")
	}
}

func TestPreviousPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	ok := pm.PreviousPage()
	if ok {
		t.Error("PreviousPage should fail on page 0")
	}

	pm.CurrentPage = 5
	ok = pm.PreviousPage()
	if !ok {
		t.Error("PreviousPage should succeed on page 5")
	}
	if pm.CurrentPage != 4 {
		t.Errorf("Expected current page 4, got %d", pm.CurrentPage)
	}
}

func TestGoToPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	ok := pm.GoToPage(5)
	if !ok {
		t.Error("GoToPage should succeed for valid page")
	}
	if pm.CurrentPage != 5 {
		t.Errorf("Expected current page 5, got %d", pm.CurrentPage)
	}

	ok = pm.GoToPage(15)
	if ok {
		t.Error("GoToPage should fail for out-of-range page")
	}

	ok = pm.GoToPage(-1)
	if ok {
		t.Error("GoToPage should fail for negative page")
	}
}

func TestFirstPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)
	pm.CurrentPage = 5

	pm.FirstPage()
	if pm.CurrentPage != 0 {
		t.Errorf("Expected current page 0, got %d", pm.CurrentPage)
	}
}

func TestLastPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	pm.LastPage()
	if pm.CurrentPage != 9 {
		t.Errorf("Expected current page 9, got %d", pm.CurrentPage)
	}
}

func TestSetPageSize(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	err := pm.SetPageSize(20)
	if err != nil {
		t.Fatalf("SetPageSize failed: %v", err)
	}
	if pm.PageSize != 20 {
		t.Errorf("Expected page size 20, got %d", pm.PageSize)
	}

	err = pm.SetPageSize(0)
	if err == nil {
		t.Error("SetPageSize should fail for size 0")
	}
}

func TestSetPageSizeAdjustsCurrentPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)
	pm.CurrentPage = 9 // Last page with size 10

	_ = pm.SetPageSize(50) // Now there are only 2 pages
	if pm.CurrentPage >= pm.GetPageCount() {
		t.Error("CurrentPage should be adjusted after SetPageSize")
	}
}

func TestSetTotalRows(t *testing.T) {
	pm := NewPaginationManager(100, 10)
	pm.CurrentPage = 9

	pm.SetTotalRows(50) // Now only 5 pages
	if pm.CurrentPage >= pm.GetPageCount() && pm.GetPageCount() > 0 {
		t.Errorf("CurrentPage should be adjusted, now on page %d of %d", pm.CurrentPage, pm.GetPageCount())
	}
}

func TestIsFirstPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if !pm.IsFirstPage() {
		t.Error("Should be on first page initially")
	}

	pm.CurrentPage = 5
	if pm.IsFirstPage() {
		t.Error("Should not be on first page")
	}
}

func TestIsLastPage(t *testing.T) {
	pm := NewPaginationManager(100, 10)
	pm.CurrentPage = 9

	if !pm.IsLastPage() {
		t.Error("Should be on last page")
	}

	pm.CurrentPage = 5
	if pm.IsLastPage() {
		t.Error("Should not be on last page")
	}
}

func TestAutoFetch(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	if pm.ShouldFetch() {
		t.Error("Should not fetch when AutoFetch is disabled")
	}

	pm.EnableAutoFetch()
	if !pm.AutoFetch {
		t.Error("AutoFetch should be enabled")
	}

	// Initially should need to fetch (nothing fetched yet)
	if !pm.ShouldFetch() {
		t.Error("Should need to fetch initially")
	}

	// Mark that we've fetched all the way to the end of page 3 (30 items)
	// With current page 0 and prefetch size 20, this should be enough
	pm.MarkFetched(30)
	if pm.ShouldFetch() {
		t.Error("Should not need to fetch yet when we've prefetched ahead")
	}

	pm.DisableAutoFetch()
	if pm.AutoFetch {
		t.Error("AutoFetch should be disabled")
	}
}

func TestGetFetchSize(t *testing.T) {
	pm := NewPaginationManager(100, 10)
	pm.EnableAutoFetch()

	size := pm.GetFetchSize()
	if size <= 0 {
		t.Errorf("Should calculate fetch size, got %d", size)
	}

	pm.FetchedTo = 100
	size = pm.GetFetchSize()
	if size != 0 {
		t.Errorf("Should not fetch when all rows are fetched, got %d", size)
	}
}

func TestMarkFetched(t *testing.T) {
	pm := NewPaginationManager(100, 10)
	pm.EnableAutoFetch()

	pm.MarkFetched(20)
	if pm.FetchedTo != 20+20 { // Initial 20 + 20 marked
		t.Errorf("Expected FetchedTo 40, got %d", pm.FetchedTo)
	}

	pm.MarkFetched(100) // Try to fetch more than available
	if pm.FetchedTo != 100 {
		t.Errorf("FetchedTo should cap at TotalRows, got %d", pm.FetchedTo)
	}
}

func TestGetPaginationInfo(t *testing.T) {
	pm := NewPaginationManager(100, 10)

	info := pm.GetPaginationInfo()
	if len(info) == 0 {
		t.Error("GetPaginationInfo should return non-empty string")
	}

	// Verify it contains expected parts
	if !contains(info, "Page") {
		t.Error("Should contain 'Page' in info")
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestPaginationWithPartialLastPage(t *testing.T) {
	pm := NewPaginationManager(95, 10)

	if pm.GetPageCount() != 10 {
		t.Errorf("Expected 10 pages for 95 rows with size 10, got %d", pm.GetPageCount())
	}

	pm.LastPage()
	if pm.GetCurrentPageSize() != 5 {
		t.Errorf("Expected last page size 5, got %d", pm.GetCurrentPageSize())
	}
}

func TestEmptyPagination(t *testing.T) {
	pm := NewPaginationManager(0, 10)

	if pm.GetPageCount() != 0 {
		t.Errorf("Expected 0 pages for empty data, got %d", pm.GetPageCount())
	}
	if !pm.IsLastPage() {
		t.Error("Should be on last page with no data")
	}
}

func TestNavigationBoundaries(t *testing.T) {
	pm := NewPaginationManager(25, 10)

	// Page 0: rows 0-9
	if pm.GetStartRow() != 0 || pm.GetEndRow() != 10 {
		t.Error("Page 0 boundaries incorrect")
	}

	// Page 1: rows 10-19
	pm.NextPage()
	if pm.GetStartRow() != 10 || pm.GetEndRow() != 20 {
		t.Error("Page 1 boundaries incorrect")
	}

	// Page 2: rows 20-24 (partial)
	pm.NextPage()
	if pm.GetStartRow() != 20 || pm.GetEndRow() != 25 {
		t.Error("Page 2 boundaries incorrect")
	}
}
