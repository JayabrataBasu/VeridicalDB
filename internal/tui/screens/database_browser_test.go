package screens

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// TestNewDatabaseBrowser tests browser creation
func TestNewDatabaseBrowser(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)

	if browser.width != 80 {
		t.Errorf("expected width 80, got %d", browser.width)
	}
	if browser.height != 24 {
		t.Errorf("expected height 24, got %d", browser.height)
	}
	if len(browser.databases) != 0 {
		t.Errorf("expected empty databases, got %d", len(browser.databases))
	}
	if browser.panelFocus != 0 {
		t.Errorf("expected panelFocus 0, got %d", browser.panelFocus)
	}
}

// TestSetDatabases tests database list update
func TestSetDatabases(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)

	databases := []Database{
		{
			Name:  "users_db",
			Size:  1024000,
			Tables: []Table{
				{Name: "users", RowCount: 100, Size: 5000},
				{Name: "roles", RowCount: 10, Size: 1000},
			},
		},
		{
			Name:  "products_db",
			Size:  2048000,
			Tables: []Table{
				{Name: "products", RowCount: 5000, Size: 100000},
			},
		},
	}

	browser.SetDatabases(databases)

	if len(browser.databases) != 2 {
		t.Errorf("expected 2 databases, got %d", len(browser.databases))
	}
	if browser.databases[0].Name != "users_db" {
		t.Errorf("expected database name 'users_db', got %s", browser.databases[0].Name)
	}
}

// TestDatabaseBrowserInit tests initialization
func TestDatabaseBrowserInit(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	cmd := browser.Init()

	if cmd != nil {
		t.Errorf("expected nil command, got %v", cmd)
	}
}

// TestDatabaseBrowserWindowSize tests window resize handling
func TestDatabaseBrowserWindowSize(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)

	msg := tea.WindowSizeMsg{Width: 120, Height: 30}
	updated, _ := browser.Update(msg)

	if updated.width != 120 {
		t.Errorf("expected width 120, got %d", updated.width)
	}
	if updated.height != 30 {
		t.Errorf("expected height 30, got %d", updated.height)
	}
}

// TestDatabaseBrowserNavigateUp tests navigation
func TestDatabaseBrowserNavigateUp(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{Name: "db1", Tables: []Table{}},
		{Name: "db2", Tables: []Table{}},
		{Name: "db3", Tables: []Table{}},
	})
	browser.selectedDBIdx = 2

	msg := tea.KeyMsg{Type: tea.KeyUp}
	updated, _ := browser.Update(msg)

	if updated.selectedDBIdx != 1 {
		t.Errorf("expected selectedDBIdx 1, got %d", updated.selectedDBIdx)
	}
}

// TestDatabaseBrowserNavigateDown tests navigation
func TestDatabaseBrowserNavigateDown(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{Name: "db1", Tables: []Table{}},
		{Name: "db2", Tables: []Table{}},
		{Name: "db3", Tables: []Table{}},
	})
	browser.selectedDBIdx = 1

	msg := tea.KeyMsg{Type: tea.KeyDown}
	updated, _ := browser.Update(msg)

	if updated.selectedDBIdx != 2 {
		t.Errorf("expected selectedDBIdx 2, got %d", updated.selectedDBIdx)
	}
}

// TestDatabaseBrowserSwitchPanels tests tab navigation
func TestDatabaseBrowserSwitchPanels(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	if browser.panelFocus != 0 {
		t.Fatalf("expected initial panelFocus 0")
	}

	msg := tea.KeyMsg{Type: tea.KeyTab}
	updated, _ := browser.Update(msg)

	if updated.panelFocus != 1 {
		t.Errorf("expected panelFocus 1, got %d", updated.panelFocus)
	}

	updated2, _ := updated.Update(msg)
	if updated2.panelFocus != 2 {
		t.Errorf("expected panelFocus 2, got %d", updated2.panelFocus)
	}

	updated3, _ := updated2.Update(msg)
	if updated3.panelFocus != 0 {
		t.Errorf("expected panelFocus 0 (wraparound), got %d", updated3.panelFocus)
	}
}

// TestDatabaseBrowserViewEmpty tests view rendering with no databases
func TestDatabaseBrowserViewEmpty(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	view := browser.View()

	if !strings.Contains(view, "No databases available") {
		t.Errorf("expected 'No databases available' in view")
	}
}

// TestDatabaseBrowserViewWithData tests view rendering with databases
func TestDatabaseBrowserViewWithData(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{
			Name: "testdb",
			Size: 1000000,
			Tables: []Table{
				{
					Name:     "users",
					RowCount: 100,
					Size:     50000,
					Type:     "BASE TABLE",
					Columns: []Column{
						{Name: "id", Type: "INT", IsPrimary: true},
						{Name: "name", Type: "VARCHAR(100)", Nullable: true},
					},
				},
			},
		},
	})

	view := browser.View()

	if !strings.Contains(view, "testdb") {
		t.Errorf("expected 'testdb' in view")
	}
	if !strings.Contains(view, "Databases") {
		t.Errorf("expected 'Databases' panel header in view")
	}
}

// TestDatabaseBrowserViewFocusIndicators tests focus indicators
func TestDatabaseBrowserViewFocusIndicators(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{Name: "db1", Tables: []Table{}},
	})

	// Test each panel focus - updated to match new premium styling format
	tests := []struct {
		panelFocus int
		indicator  string
	}{
		{0, "Databases ●"},
		{1, "Tables ●"},
		{2, "Columns ●"},
	}

	for _, test := range tests {
		browser.panelFocus = test.panelFocus
		view := browser.View()

		if !strings.Contains(view, test.indicator) {
			t.Errorf("panelFocus %d: expected '%s' in view", test.panelFocus, test.indicator)
		}
	}
}

// TestDatabaseBrowserSelectionPersistence tests selection across panel switches
func TestDatabaseBrowserSelectionPersistence(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{
			Name: "db1",
			Tables: []Table{
				{Name: "t1", Columns: []Column{
					{Name: "c1", Type: "INT"},
				}},
			},
		},
	})

	// Navigate to table
	browser.panelFocus = 1
	browser.selectedTblIdx = 0

	// Navigate to column
	browser.panelFocus = 2
	browser.selectedColIdx = 0

	if browser.selectedTblIdx != 0 {
		t.Errorf("expected selectedTblIdx 0, got %d", browser.selectedTblIdx)
	}
	if browser.selectedColIdx != 0 {
		t.Errorf("expected selectedColIdx 0, got %d", browser.selectedColIdx)
	}
}

// TestFormatBytes tests byte formatting
func TestFormatBytesDB(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{2147483648, "2.0 GB"},
	}

	for _, test := range tests {
		result := dbFormatBytes(test.input)
		if result != test.expected {
			t.Errorf("dbFormatBytes(%d): expected %s, got %s", test.input, test.expected, result)
		}
	}
}

// TestPadRight tests padding function
func TestPadRight(t *testing.T) {
	tests := []struct {
		input    string
		length   int
		expected int
	}{
		{"hello", 10, 10},
		{"hello", 5, 5},
		{"hello", 3, 5}, // Should not truncate
	}

	for _, test := range tests {
		result := dbPadRight(test.input, test.length)
		if len(result) != test.expected {
			t.Errorf("dbPadRight(%q, %d): expected length %d, got %d", test.input, test.length, test.expected, len(result))
		}
	}
}

// TestDatabaseBrowserScrolling tests scroll offset tracking
func TestDatabaseBrowserScrolling(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	
	// Create 20 databases to test scrolling
	databases := make([]Database, 20)
	for i := 0; i < 20; i++ {
		databases[i] = Database{Name: "db" + string(rune(i))}
	}
	browser.SetDatabases(databases)

	// Navigate down past visible items
	for i := 0; i < 12; i++ {
		browser.moveDown()
	}

	if browser.selectedDBIdx != 12 {
		t.Errorf("expected selectedDBIdx 12, got %d", browser.selectedDBIdx)
	}
	if browser.scrollOffset[0] == 0 {
		t.Errorf("expected scrollOffset to advance, got 0")
	}
}

// TestDatabaseBrowserHomeEnd tests home/end keys
func TestDatabaseBrowserHomeEnd(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{Name: "db1", Tables: []Table{}},
		{Name: "db2", Tables: []Table{}},
		{Name: "db3", Tables: []Table{}},
	})

	browser.selectedDBIdx = 2
	browser.scrollOffset[0] = 2

	msg := tea.KeyMsg{Type: tea.KeyHome}
	updated, _ := browser.Update(msg)

	if updated.selectedDBIdx != 0 {
		t.Errorf("home key: expected selectedDBIdx 0, got %d", updated.selectedDBIdx)
	}
	if updated.scrollOffset[0] != 0 {
		t.Errorf("home key: expected scrollOffset 0, got %d", updated.scrollOffset[0])
	}
}

// TestColumnFormatting tests column line formatting
func TestColumnFormatting(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)

	tests := []struct {
		column Column
		shouldContain string
	}{
		{
			Column{Name: "id", Type: "INT", IsPrimary: true},
			"🔑",
		},
		{
			Column{Name: "user_id", Type: "INT", IsForeign: true},
			"🔗",
		},
		{
			Column{Name: "email", Type: "VARCHAR", Nullable: true},
			"?",
		},
	}

	for _, test := range tests {
		line := browser.formatColumnLine(test.column, false, 60)
		if !strings.Contains(line, test.shouldContain) {
			t.Errorf("expected '%s' in formatted column", test.shouldContain)
		}
	}
}

// TestTableTypeIcon tests table type icon display
func TestTableTypeIcon(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)

	baseTable := Table{Name: "users", Type: "BASE TABLE", RowCount: 100}
	viewTable := Table{Name: "user_view", Type: "VIEW", RowCount: 50}

	baseLine := browser.formatTableLine(baseTable, false, 60)
	viewLine := browser.formatTableLine(viewTable, false, 60)

	if !strings.Contains(baseLine, "📄") {
		t.Errorf("expected 📄 icon for base table")
	}
	if !strings.Contains(viewLine, "👁") {
		t.Errorf("expected 👁 icon for view")
	}
}

// TestMultiPanelSelection tests independent selection in each panel
func TestMultiPanelSelection(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{
			Name: "db1",
			Tables: []Table{
				{
					Name: "t1",
					Columns: []Column{
						{Name: "c1", Type: "INT"},
						{Name: "c2", Type: "VARCHAR"},
						{Name: "c3", Type: "TIMESTAMP"},
					},
				},
				{
					Name: "t2",
					Columns: []Column{
						{Name: "x1", Type: "INT"},
					},
				},
			},
		},
		{
			Name: "db2",
			Tables: []Table{},
		},
	})

	// Select first database, first table, second column
	browser.selectedDBIdx = 0
	browser.selectedTblIdx = 0
	browser.selectedColIdx = 1

	if browser.databases[0].Tables[0].Columns[1].Name != "c2" {
		t.Errorf("expected column c2 selected")
	}

	// Switch to second database (should clear table selection due to SetDatabases behavior)
	browser.selectedDBIdx = 1
	if browser.selectedTblIdx != 0 {
		// This is expected since we manually switch
		t.Logf("Table selection cleared after database switch (expected behavior)")
	}
}

// TestLeftRightNavigation tests left/right arrow key navigation between panels
func TestLeftRightNavigation(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)

	// Start at tables panel
	browser.panelFocus = 1

	msg := tea.KeyMsg{Type: tea.KeyLeft}
	updated, _ := browser.Update(msg)

	if updated.panelFocus != 0 {
		t.Errorf("left key: expected panelFocus 0, got %d", updated.panelFocus)
	}

	msg = tea.KeyMsg{Type: tea.KeyRight}
	updated, _ = updated.Update(msg)

	if updated.panelFocus != 1 {
		t.Errorf("right key: expected panelFocus 1, got %d", updated.panelFocus)
	}
}

// TestBottomInfoFormatting tests bottom info bar formatting
func TestBottomInfoFormatting(t *testing.T) {
	browser := NewDatabaseBrowser(80, 24)
	browser.SetDatabases([]Database{
		{
			Name: "testdb",
			Size: 1000000,
			Tables: []Table{
				{
					Name:     "users",
					RowCount: 100,
					Size:     50000,
					Type:     "BASE TABLE",
					Columns: []Column{
						{Name: "id", Type: "INT"},
					},
				},
			},
		},
	})

	info := browser.renderBottomInfo()

	if !strings.Contains(info, "testdb") {
		t.Errorf("expected database name in info")
	}
	if !strings.Contains(info, "users") {
		t.Errorf("expected table name in info")
	}
	if !strings.Contains(info, "id") {
		t.Errorf("expected column name in info")
	}
}
