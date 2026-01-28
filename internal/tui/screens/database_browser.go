package screens

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/observability"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// DatabaseBrowser represents a hierarchical database explorer
type DatabaseBrowser struct {
	width          int
	height         int
	databases      []Database
	selectedDBIdx  int
	selectedTblIdx int
	selectedColIdx int
	panelFocus     int                          // 0: databases, 1: tables, 2: columns
	scrollOffset   [3]int                       // scroll positions for each panel
	sysCatalog     *observability.SystemCatalog // Real schema data
}

// Database represents a database with its tables
type Database struct {
	Name   string
	Tables []Table
	Size   int64 // bytes
}

// Table represents a table with its columns
type Table struct {
	Name      string
	Columns   []Column
	RowCount  int64
	Size      int64  // bytes
	Type      string // "BASE TABLE", "VIEW", etc.
	CreatedAt string
}

// Column represents a table column
type Column struct {
	Name      string
	Type      string
	Nullable  bool
	Default   string
	IsPrimary bool
	IsForeign bool
	IndexName string
}

// NewDatabaseBrowser creates a new database browser
func NewDatabaseBrowser(width, height int) *DatabaseBrowser {
	return &DatabaseBrowser{
		width:          width,
		height:         height,
		databases:      make([]Database, 0),
		selectedDBIdx:  0,
		selectedTblIdx: 0,
		selectedColIdx: 0,
		panelFocus:     0,
		scrollOffset:   [3]int{0, 0, 0},
	}
}

// SetDatabases updates the database list
func (db *DatabaseBrowser) SetDatabases(databases []Database) {
	db.databases = databases
	if db.selectedDBIdx >= len(databases) {
		db.selectedDBIdx = 0
	}
	db.selectedTblIdx = 0
	db.selectedColIdx = 0
}

// SetSystemCatalog sets the system catalog for real schema data
func (db *DatabaseBrowser) SetSystemCatalog(sc *observability.SystemCatalog) {
	db.sysCatalog = sc
	// Load real tables from catalog
	db.refreshFromCatalog()
}

// refreshFromCatalog loads real table and column data from SystemCatalog
func (db *DatabaseBrowser) refreshFromCatalog() {
	if db.sysCatalog == nil {
		return
	}

	tableRows := db.sysCatalog.GetTables()
	realDatabase := Database{
		Name:   "default",
		Tables: make([]Table, 0, len(tableRows)),
		Size:   0,
	}

	for _, tableRow := range tableRows {
		var tableName string

		// Extract table name and column count from SystemTableRow
		if len(tableRow.Values) >= 1 {
			tableName = fmt.Sprintf("%v", tableRow.Values[0])
		}

		columnRows := db.sysCatalog.GetColumns(tableName)
		realTable := Table{
			Name:     tableName,
			Type:     "BASE TABLE",
			RowCount: 0,
			Size:     0,
			Columns:  make([]Column, 0, len(columnRows)),
		}

		for _, colRow := range columnRows {
			var colName, colType string
			var notNull bool

			if len(colRow.Values) >= 2 {
				colName = fmt.Sprintf("%v", colRow.Values[1])
			}
			if len(colRow.Values) >= 3 {
				colType = fmt.Sprintf("%v", colRow.Values[2])
			}
			if len(colRow.Values) >= 4 {
				if nn, ok := colRow.Values[3].(bool); ok {
					notNull = nn
				}
			}

			realTable.Columns = append(realTable.Columns, Column{
				Name:      colName,
				Type:      colType,
				Nullable:  !notNull,
				Default:   "",
				IsPrimary: false,
				IsForeign: false,
			})
		}

		realDatabase.Tables = append(realDatabase.Tables, realTable)
	}

	db.SetDatabases([]Database{realDatabase})
}

// Init initializes the database browser (required by tea.Model interface)
func (db *DatabaseBrowser) Init() tea.Cmd {
	return nil
}

// Update handles user input and state changes
func (db *DatabaseBrowser) Update(msg tea.Msg) (*DatabaseBrowser, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "tab":
			db.panelFocus = (db.panelFocus + 1) % 3
			return db, nil
		case "shift+tab":
			db.panelFocus = (db.panelFocus + 3 - 1) % 3
			return db, nil
		case "up":
			db.moveUp()
			return db, nil
		case "down":
			db.moveDown()
			return db, nil
		case "left":
			if db.panelFocus > 0 {
				db.panelFocus--
			}
			return db, nil
		case "right":
			if db.panelFocus < 2 {
				db.panelFocus++
			}
			return db, nil
		case "home":
			db.scrollOffset[db.panelFocus] = 0
			db.resetSelection()
			return db, nil
		case "end":
			db.moveToEnd()
			return db, nil
		}
	case tea.WindowSizeMsg:
		db.width = msg.Width
		db.height = msg.Height
		return db, nil
	}
	return db, nil
}

// View renders the database browser
func (db *DatabaseBrowser) View() string {
	// Premium header
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 3).
		MarginBottom(2)

	header := headerStyle.Render(types.Icons.Browser + "  Database Browser")

	if len(db.databases) == 0 {
		emptyStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#888888")).
			Padding(2, 4)
		return lipgloss.JoinVertical(lipgloss.Left,
			header,
			"",
			emptyStyle.Render("No databases available\n\nPress 'q' to go back"),
		)
	}

	// Calculate panel widths (33% each)
	panelWidth := (db.width - 4) / 3 // 4 spaces for separators

	// Render three panels
	dbPanel := db.renderDatabasesPanel(panelWidth)
	tblPanel := db.renderTablesPanel(panelWidth)
	colPanel := db.renderColumnsPanel(panelWidth)

	// Combine panels horizontally
	panels := lipgloss.JoinHorizontal(
		lipgloss.Top,
		dbPanel,
		"  ",
		tblPanel,
		"  ",
		colPanel,
	)

	// Add bottom info
	info := db.renderBottomInfo()

	// Combine with header
	remaining := db.height - lipgloss.Height(header) - lipgloss.Height(info) - 4
	if remaining > 0 {
		panels = lipgloss.NewStyle().Height(remaining).Render(panels)
	}

	return lipgloss.JoinVertical(
		lipgloss.Top,
		header,
		"",
		panels,
		"",
		info,
	)
}

// renderDatabasesPanel renders the databases panel with premium styling
func (db *DatabaseBrowser) renderDatabasesPanel(width int) string {
	// Title styling
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF"))

	activeTitleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#50FA7B"))

	separatorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#3a3a5c"))

	title := titleStyle.Render(types.Icons.Database + " Databases")
	if db.panelFocus == 0 {
		title = activeTitleStyle.Render(types.Icons.Database + " Databases ●")
	}

	var lines []string
	lines = append(lines, title)
	lines = append(lines, separatorStyle.Render(strings.Repeat("─", width-2)))

	panelHeight := 10
	for i := db.scrollOffset[0]; i < len(db.databases) && i < db.scrollOffset[0]+panelHeight; i++ {
		database := db.databases[i]
		selected := (i == db.selectedDBIdx && db.panelFocus == 0)
		line := db.formatDatabaseLine(database, selected, width-2)
		lines = append(lines, line)
	}

	// Pad to fill height
	for len(lines) < panelHeight+2 {
		lines = append(lines, strings.Repeat(" ", width-2))
	}

	content := strings.Join(lines, "\n")
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Width(width).
		Padding(0, 1)

	if db.panelFocus == 0 {
		style = style.BorderForeground(lipgloss.Color("#00D9FF"))
	}

	return style.Render(content)
}

// renderTablesPanel renders the tables panel with premium styling
func (db *DatabaseBrowser) renderTablesPanel(width int) string {
	// Title styling
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FFB86C"))

	activeTitleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#50FA7B"))

	separatorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#3a3a5c"))

	title := titleStyle.Render(types.Icons.Table + " Tables")
	if db.panelFocus == 1 {
		title = activeTitleStyle.Render(types.Icons.Table + " Tables ●")
	}

	var lines []string
	lines = append(lines, title)
	lines = append(lines, separatorStyle.Render(strings.Repeat("─", width-2)))

	if len(db.databases) == 0 || db.selectedDBIdx >= len(db.databases) {
		emptyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#666666"))
		lines = append(lines, emptyStyle.Render("(No database selected)"))
		for len(lines) < 12 {
			lines = append(lines, "")
		}
	} else {
		tables := db.databases[db.selectedDBIdx].Tables
		panelHeight := 10
		for i := db.scrollOffset[1]; i < len(tables) && i < db.scrollOffset[1]+panelHeight; i++ {
			table := tables[i]
			selected := (i == db.selectedTblIdx && db.panelFocus == 1)
			line := db.formatTableLine(table, selected, width-2)
			lines = append(lines, line)
		}

		// Pad to fill height
		for len(lines) < panelHeight+2 {
			lines = append(lines, strings.Repeat(" ", width-2))
		}
	}

	content := strings.Join(lines, "\n")
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Width(width).
		Padding(0, 1)

	if db.panelFocus == 1 {
		style = style.BorderForeground(lipgloss.Color("#00D9FF"))
	}

	return style.Render(content)
}

// renderColumnsPanel renders the columns panel with premium styling
func (db *DatabaseBrowser) renderColumnsPanel(width int) string {
	// Title styling
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#BD93F9"))

	activeTitleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#50FA7B"))

	separatorStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#3a3a5c"))

	title := titleStyle.Render(types.Icons.Column + " Columns")
	if db.panelFocus == 2 {
		title = activeTitleStyle.Render(types.Icons.Column + " Columns ●")
	}

	var lines []string
	lines = append(lines, title)
	lines = append(lines, separatorStyle.Render(strings.Repeat("─", width-2)))

	emptyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#666666"))

	if len(db.databases) == 0 || db.selectedDBIdx >= len(db.databases) {
		lines = append(lines, emptyStyle.Render("(No table selected)"))
		for len(lines) < 12 {
			lines = append(lines, "")
		}
	} else {
		tables := db.databases[db.selectedDBIdx].Tables
		if len(tables) == 0 || db.selectedTblIdx >= len(tables) {
			lines = append(lines, emptyStyle.Render("(No columns)"))
			for len(lines) < 12 {
				lines = append(lines, "")
			}
		} else {
			columns := tables[db.selectedTblIdx].Columns
			panelHeight := 10
			for i := db.scrollOffset[2]; i < len(columns) && i < db.scrollOffset[2]+panelHeight; i++ {
				column := columns[i]
				selected := (i == db.selectedColIdx && db.panelFocus == 2)
				line := db.formatColumnLine(column, selected, width-2)
				lines = append(lines, line)
			}

			// Pad to fill height
			for len(lines) < panelHeight+2 {
				lines = append(lines, strings.Repeat(" ", width-2))
			}
		}
	}

	content := strings.Join(lines, "\n")
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Width(width).
		Padding(0, 1)

	if db.panelFocus == 2 {
		style = style.BorderForeground(lipgloss.Color("#00D9FF"))
	}

	return style.Render(content)
}

// renderBottomInfo renders information about selected items with premium styling
func (db *DatabaseBrowser) renderBottomInfo() string {
	if len(db.databases) == 0 {
		return ""
	}

	var info strings.Builder

	// Define styles
	labelStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888"))

	valueStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#00D9FF")).
		Bold(true)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		MarginTop(1)

	// Database info
	info.WriteString(labelStyle.Render(types.Icons.Database + " Database: "))
	info.WriteString(valueStyle.Render(db.databases[db.selectedDBIdx].Name))
	info.WriteString(labelStyle.Render(fmt.Sprintf(" (%s)", dbFormatBytes(db.databases[db.selectedDBIdx].Size))))

	// Table info
	if db.selectedDBIdx < len(db.databases) {
		tables := db.databases[db.selectedDBIdx].Tables
		if db.selectedTblIdx < len(tables) {
			table := tables[db.selectedTblIdx]
			info.WriteString(labelStyle.Render("  │  " + types.Icons.Table + " Table: "))
			info.WriteString(valueStyle.Render(table.Name))
			info.WriteString(labelStyle.Render(fmt.Sprintf(" (%d rows, %s)",
				table.RowCount,
				dbFormatBytes(table.Size))))

			// Column info
			if db.selectedColIdx < len(table.Columns) {
				col := table.Columns[db.selectedColIdx]
				nullable := "NOT NULL"
				if col.Nullable {
					nullable = "NULL"
				}
				info.WriteString(labelStyle.Render("  │  " + types.Icons.Column + " Column: "))
				info.WriteString(valueStyle.Render(col.Name))
				info.WriteString(labelStyle.Render(fmt.Sprintf(" (%s, %s)", col.Type, nullable)))
			}
		}
	}

	// Help bar
	info.WriteString("\n")
	info.WriteString(helpStyle.Render("  ↑↓ Navigate  │  ←→/Tab Switch Panel  │  Home/End First/Last  │  q/Esc Back"))

	return info.String()
}

// formatDatabaseLine formats a database line with selection highlight
func (db *DatabaseBrowser) formatDatabaseLine(database Database, selected bool, width int) string {
	line := fmt.Sprintf("  %s", database.Name)
	if len(line) > width {
		line = line[:width-1] + "…"
	}
	line = dbPadRight(line, width)

	if selected {
		return lipgloss.NewStyle().
			Background(lipgloss.Color("#00D9FF")).
			Foreground(lipgloss.Color("#1a1a2e")).
			Bold(true).
			Render(line)
	}
	return lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		Render(line)
}

// formatTableLine formats a table line with selection highlight
func (db *DatabaseBrowser) formatTableLine(table Table, selected bool, width int) string {
	icon := types.Icons.Table
	if table.Type == "VIEW" {
		icon = types.Icons.File
	}
	line := fmt.Sprintf("  %s %s (%d)", icon, table.Name, table.RowCount)
	if len(line) > width {
		line = line[:width-1] + "…"
	}
	line = dbPadRight(line, width)

	if selected {
		return lipgloss.NewStyle().
			Background(lipgloss.Color("#00D9FF")).
			Foreground(lipgloss.Color("#1a1a2e")).
			Bold(true).
			Render(line)
	}
	return lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		Render(line)
}

// formatColumnLine formats a column line with selection highlight
func (db *DatabaseBrowser) formatColumnLine(column Column, selected bool, width int) string {
	// Build column indicators
	indicators := ""
	if column.IsPrimary {
		indicators += types.Icons.Key + " "
	} else if column.IsForeign {
		indicators += types.Icons.ForeignKey + " "
	}

	// Build column line
	nullable := ""
	if column.Nullable {
		nullable = " ?"
	}
	line := fmt.Sprintf("  %s%s: %s%s", indicators, column.Name, column.Type, nullable)
	if len(line) > width {
		line = line[:width-1] + "…"
	}
	line = dbPadRight(line, width)

	if selected {
		return lipgloss.NewStyle().
			Background(lipgloss.Color("#00D9FF")).
			Foreground(lipgloss.Color("#1a1a2e")).
			Bold(true).
			Render(line)
	}
	return lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		Render(line)
}

// moveUp moves selection up in the current panel
func (db *DatabaseBrowser) moveUp() {
	switch db.panelFocus {
	case 0: // Databases
		if db.selectedDBIdx > 0 {
			db.selectedDBIdx--
			if db.selectedDBIdx < db.scrollOffset[0] {
				db.scrollOffset[0] = db.selectedDBIdx
			}
		}
	case 1: // Tables
		if db.selectedTblIdx > 0 {
			db.selectedTblIdx--
			if db.selectedTblIdx < db.scrollOffset[1] {
				db.scrollOffset[1] = db.selectedTblIdx
			}
		}
	case 2: // Columns
		if db.selectedColIdx > 0 {
			db.selectedColIdx--
			if db.selectedColIdx < db.scrollOffset[2] {
				db.scrollOffset[2] = db.selectedColIdx
			}
		}
	}
}

// moveDown moves selection down in the current panel
func (db *DatabaseBrowser) moveDown() {
	panelHeight := 10
	switch db.panelFocus {
	case 0: // Databases
		if db.selectedDBIdx < len(db.databases)-1 {
			db.selectedDBIdx++
			if db.selectedDBIdx >= db.scrollOffset[0]+panelHeight {
				db.scrollOffset[0] = db.selectedDBIdx - panelHeight + 1
			}
		}
	case 1: // Tables
		if len(db.databases) > 0 && db.selectedDBIdx < len(db.databases) {
			tables := db.databases[db.selectedDBIdx].Tables
			if db.selectedTblIdx < len(tables)-1 {
				db.selectedTblIdx++
				if db.selectedTblIdx >= db.scrollOffset[1]+panelHeight {
					db.scrollOffset[1] = db.selectedTblIdx - panelHeight + 1
				}
			}
		}
	case 2: // Columns
		if len(db.databases) > 0 && db.selectedDBIdx < len(db.databases) {
			tables := db.databases[db.selectedDBIdx].Tables
			if db.selectedTblIdx < len(tables) {
				columns := tables[db.selectedTblIdx].Columns
				if db.selectedColIdx < len(columns)-1 {
					db.selectedColIdx++
					if db.selectedColIdx >= db.scrollOffset[2]+panelHeight {
						db.scrollOffset[2] = db.selectedColIdx - panelHeight + 1
					}
				}
			}
		}
	}
}

// moveToEnd moves to the last item in the current panel
func (db *DatabaseBrowser) moveToEnd() {
	panelHeight := 10
	switch db.panelFocus {
	case 0: // Databases
		if len(db.databases) > 0 {
			db.selectedDBIdx = len(db.databases) - 1
			if db.selectedDBIdx >= panelHeight {
				db.scrollOffset[0] = db.selectedDBIdx - panelHeight + 1
			}
		}
	case 1: // Tables
		if len(db.databases) > 0 && db.selectedDBIdx < len(db.databases) {
			tables := db.databases[db.selectedDBIdx].Tables
			if len(tables) > 0 {
				db.selectedTblIdx = len(tables) - 1
				if db.selectedTblIdx >= panelHeight {
					db.scrollOffset[1] = db.selectedTblIdx - panelHeight + 1
				}
			}
		}
	case 2: // Columns
		if len(db.databases) > 0 && db.selectedDBIdx < len(db.databases) {
			tables := db.databases[db.selectedDBIdx].Tables
			if db.selectedTblIdx < len(tables) {
				columns := tables[db.selectedTblIdx].Columns
				if len(columns) > 0 {
					db.selectedColIdx = len(columns) - 1
					if db.selectedColIdx >= panelHeight {
						db.scrollOffset[2] = db.selectedColIdx - panelHeight + 1
					}
				}
			}
		}
	}
}

// resetSelection resets selection to 0
func (db *DatabaseBrowser) resetSelection() {
	db.selectedDBIdx = 0
	db.selectedTblIdx = 0
	db.selectedColIdx = 0
}

// Helper functions

func dbPadRight(s string, length int) string {
	if len(s) >= length {
		return s
	}
	return s + strings.Repeat(" ", length-len(s))
}

func dbFormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
