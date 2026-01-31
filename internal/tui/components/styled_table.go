// Package components provides reusable TUI components with brand styling.
package components

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// StyledTableColumn defines a column in the styled table.
type StyledTableColumn struct {
	Header string
	Width  int
	Align  lipgloss.Position
	Icon   string // Optional Nerd Font icon
}

// StyledTableRow represents a row in the styled table.
type StyledTableRow struct {
	Cells    []string
	ID       string // Unique identifier
	Metadata map[string]interface{}
}

// StyledTable provides a feature-rich table with brand styling.
type StyledTable struct {
	columns       []StyledTableColumn
	rows          []StyledTableRow
	selectedIndex int
	startIndex    int // For scrolling
	maxVisible    int // Maximum visible rows
	focused       bool
	width         int
	height        int
	theme         *theme.Theme

	// Callbacks
	OnSelect func(row StyledTableRow) tea.Msg
	OnFocus  func() tea.Msg
	OnBlur   func() tea.Msg

	// Style cache
	headerStyle      lipgloss.Style
	rowEvenStyle     lipgloss.Style
	rowOddStyle      lipgloss.Style
	selectedStyle    lipgloss.Style
	borderStyle      lipgloss.Style
	focusBorderStyle lipgloss.Style
}

// NewStyledTable creates a new styled table.
func NewStyledTable(columns []StyledTableColumn, t *theme.Theme) *StyledTable {
	st := &StyledTable{
		columns:       columns,
		rows:          make([]StyledTableRow, 0),
		selectedIndex: 0,
		maxVisible:    10,
		theme:         t,
	}
	st.updateStyles()
	return st
}

// updateStyles rebuilds the style cache from the theme.
func (st *StyledTable) updateStyles() {
	t := st.theme
	if t == nil {
		return
	}

	st.headerStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.BrandAccent)).
		Background(lipgloss.Color(t.Background)).
		Padding(0, 1)

	st.rowEvenStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(t.TableRowEven)).
		Padding(0, 1)

	st.rowOddStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(t.TableRowOdd)).
		Padding(0, 1)

	st.selectedStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(t.BrandSelection)).
		Foreground(lipgloss.Color(t.BrandAccent)).
		Bold(true).
		Padding(0, 1)

	st.borderStyle = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(t.Border))

	st.focusBorderStyle = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(t.BrandFocus))
}

// SetRows updates the table rows.
func (st *StyledTable) SetRows(rows []StyledTableRow) {
	st.rows = rows
	if st.selectedIndex >= len(rows) {
		st.selectedIndex = max(0, len(rows)-1)
	}
	st.adjustScroll()
}

// SetDimensions sets the table dimensions.
func (st *StyledTable) SetDimensions(width, height int) {
	st.width = width
	st.height = height
	st.maxVisible = height - 4 // Account for header and borders
	if st.maxVisible < 1 {
		st.maxVisible = 1
	}
	st.adjustScroll()
}

// Focus sets the table as focused.
func (st *StyledTable) Focus() tea.Cmd {
	st.focused = true
	if st.OnFocus != nil {
		return func() tea.Msg { return st.OnFocus() }
	}
	return nil
}

// Blur removes focus from the table.
func (st *StyledTable) Blur() tea.Cmd {
	st.focused = false
	if st.OnBlur != nil {
		return func() tea.Msg { return st.OnBlur() }
	}
	return nil
}

// IsFocused returns whether the table is focused.
func (st *StyledTable) IsFocused() bool {
	return st.focused
}

// SelectedRow returns the currently selected row.
func (st *StyledTable) SelectedRow() (StyledTableRow, bool) {
	if st.selectedIndex >= 0 && st.selectedIndex < len(st.rows) {
		return st.rows[st.selectedIndex], true
	}
	return StyledTableRow{}, false
}

// SelectedIndex returns the currently selected index.
func (st *StyledTable) SelectedIndex() int {
	return st.selectedIndex
}

// SetSelectedIndex sets the selected index.
func (st *StyledTable) SetSelectedIndex(idx int) {
	if idx >= 0 && idx < len(st.rows) {
		st.selectedIndex = idx
		st.adjustScroll()
	}
}

// MoveUp moves selection up.
func (st *StyledTable) MoveUp() {
	if st.selectedIndex > 0 {
		st.selectedIndex--
		st.adjustScroll()
	}
}

// MoveDown moves selection down.
func (st *StyledTable) MoveDown() {
	if st.selectedIndex < len(st.rows)-1 {
		st.selectedIndex++
		st.adjustScroll()
	}
}

// PageUp moves selection up by page.
func (st *StyledTable) PageUp() {
	st.selectedIndex -= st.maxVisible
	if st.selectedIndex < 0 {
		st.selectedIndex = 0
	}
	st.adjustScroll()
}

// PageDown moves selection down by page.
func (st *StyledTable) PageDown() {
	st.selectedIndex += st.maxVisible
	if st.selectedIndex >= len(st.rows) {
		st.selectedIndex = len(st.rows) - 1
	}
	if st.selectedIndex < 0 {
		st.selectedIndex = 0
	}
	st.adjustScroll()
}

// GoToTop moves selection to the first row.
func (st *StyledTable) GoToTop() {
	st.selectedIndex = 0
	st.adjustScroll()
}

// GoToBottom moves selection to the last row.
func (st *StyledTable) GoToBottom() {
	if len(st.rows) > 0 {
		st.selectedIndex = len(st.rows) - 1
	}
	st.adjustScroll()
}

// adjustScroll ensures the selected row is visible.
func (st *StyledTable) adjustScroll() {
	if st.selectedIndex < st.startIndex {
		st.startIndex = st.selectedIndex
	}
	if st.selectedIndex >= st.startIndex+st.maxVisible {
		st.startIndex = st.selectedIndex - st.maxVisible + 1
	}
	if st.startIndex < 0 {
		st.startIndex = 0
	}
}

// Update handles table input.
func (st *StyledTable) Update(msg tea.Msg) tea.Cmd {
	if !st.focused {
		return nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			st.MoveUp()
		case "down", "j":
			st.MoveDown()
		case "pgup", "ctrl+u":
			st.PageUp()
		case "pgdown", "ctrl+d":
			st.PageDown()
		case "home", "g":
			st.GoToTop()
		case "end", "G":
			st.GoToBottom()
		case "enter":
			if row, ok := st.SelectedRow(); ok && st.OnSelect != nil {
				return func() tea.Msg { return st.OnSelect(row) }
			}
		}
	}

	return nil
}

// View renders the styled table.
func (st *StyledTable) View() string {
	if len(st.columns) == 0 {
		return ""
	}

	var b strings.Builder
	t := st.theme

	// Calculate column widths
	totalWidth := st.width - 4 // Account for borders and padding
	if totalWidth < 20 {
		totalWidth = 20
	}

	// Auto-calculate widths if not specified
	colWidths := make([]int, len(st.columns))
	fixedWidth := 0
	flexCols := 0
	for i, col := range st.columns {
		if col.Width > 0 {
			colWidths[i] = col.Width
			fixedWidth += col.Width
		} else {
			flexCols++
		}
	}
	if flexCols > 0 {
		flexWidth := (totalWidth - fixedWidth) / flexCols
		for i, col := range st.columns {
			if col.Width == 0 {
				colWidths[i] = flexWidth
			}
		}
	}

	// Render header
	var headerCells []string
	for i, col := range st.columns {
		header := col.Header
		if col.Icon != "" {
			header = col.Icon + " " + header
		}
		cell := st.headerStyle.
			Width(colWidths[i]).
			Align(col.Align).
			Render(truncateStr(header, colWidths[i]))
		headerCells = append(headerCells, cell)
	}
	b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, headerCells...))
	b.WriteString("\n")

	// Render separator
	sepStyle := lipgloss.NewStyle()
	if t != nil {
		sepStyle = sepStyle.Foreground(lipgloss.Color(t.BrandAccent))
	}
	b.WriteString(sepStyle.Render(strings.Repeat("─", totalWidth)))
	b.WriteString("\n")

	// Render visible rows
	endIndex := st.startIndex + st.maxVisible
	if endIndex > len(st.rows) {
		endIndex = len(st.rows)
	}

	for i := st.startIndex; i < endIndex; i++ {
		row := st.rows[i]
		rowStyle := st.rowEvenStyle
		if i%2 == 1 {
			rowStyle = st.rowOddStyle
		}
		if i == st.selectedIndex {
			rowStyle = st.selectedStyle
		}

		var cells []string
		for j, col := range st.columns {
			cellContent := ""
			if j < len(row.Cells) {
				cellContent = row.Cells[j]
			}
			cell := rowStyle.
				Width(colWidths[j]).
				Align(col.Align).
				Render(truncateStr(cellContent, colWidths[j]))
			cells = append(cells, cell)
		}
		b.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, cells...))
		b.WriteString("\n")
	}

	// Add scrollbar indicator if needed
	if len(st.rows) > st.maxVisible {
		scrollInfo := lipgloss.NewStyle()
		if t != nil {
			scrollInfo = scrollInfo.Foreground(lipgloss.Color(t.Muted))
		}
		b.WriteString(scrollInfo.Render(
			"↑↓ " + string(rune('0'+st.startIndex+1)) + "-" +
				string(rune('0'+endIndex)) + "/" +
				string(rune('0'+len(st.rows)))))
	}

	// Apply border
	borderStyle := st.borderStyle
	if st.focused {
		borderStyle = st.focusBorderStyle
	}

	content := b.String()
	return borderStyle.Width(st.width).Render(content)
}

// Helper to truncate strings.
func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
