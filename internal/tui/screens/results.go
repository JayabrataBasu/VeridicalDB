package screens

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ResultsScreen displays query results with pagination
type ResultsScreen struct {
	app         types.StyleProvider
	result      *QueryResult
	page        int
	pageSize    int
	colOffset   int
	totalPages  int
	displayCols int // Number of columns to display at once
	width       int
	height      int
}

// NewResultsScreen creates a new results viewer screen
func NewResultsScreen(app types.StyleProvider) *ResultsScreen {
	SyncScreenIcons()
	return &ResultsScreen{
		app:         app,
		page:        0,
		pageSize:    50,
		colOffset:   0,
		displayCols: 10,
	}
}

// SetResult updates the results to display
func (r *ResultsScreen) SetResult(result *QueryResult) {
	r.result = result
	r.page = 0
	r.colOffset = 0
	if result != nil && len(result.Rows) > 0 {
		r.totalPages = (len(result.Rows) + r.pageSize - 1) / r.pageSize
	} else {
		r.totalPages = 0
	}
}

// Init initializes the results screen
func (r *ResultsScreen) Init() tea.Cmd {
	return nil
}

// Update handles messages for the results screen
func (r *ResultsScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc", "q":
			// Return to editor
			return r, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "editor"}
			}

		case "pgdown", "space":
			// Next page
			if r.page < r.totalPages-1 {
				r.page++
			}

		case "pgup":
			// Previous page
			if r.page > 0 {
				r.page--
			}

		case "right", "l":
			// Scroll columns right
			if r.result != nil && r.colOffset+r.displayCols < len(r.result.Columns) {
				r.colOffset++
			}

		case "left", "h":
			// Scroll columns left
			if r.colOffset > 0 {
				r.colOffset--
			}

		case "home":
			// First page
			r.page = 0

		case "end":
			// Last page
			if r.totalPages > 0 {
				r.page = r.totalPages - 1
			}

		case "ctrl+e":
			// Export results (future implementation)
			return r, func() tea.Msg {
				return StatusMsg{Message: "Export functionality coming soon"}
			}
		}

	case tea.WindowSizeMsg:
		r.width = msg.Width
		r.height = msg.Height
		// Adjust display columns based on window width
		r.displayCols = (msg.Width - 10) / 15 // Rough estimate
		if r.displayCols < 1 {
			r.displayCols = 1
		}
	}

	return r, nil
}

// View renders the results screen with premium 3-pane layout
func (r *ResultsScreen) View() string {
	width := r.width
	height := r.height
	if width <= 0 {
		width = 120
	}
	if height <= 0 {
		height = 40
	}

	// Brand palette colors
	brandAccent := "#00D9FF"
	brandWarning := "#FFB86C"
	brandSuccess := "#55FF55"
	brandMuted := "#44475A"
	brandBg := "#0A0E27"
	brandText := "#FFFFFF"
	if tp, ok := r.app.(interface{ GetThemeManager() *theme.Manager }); ok {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			brandAccent = t.BrandAccent
			brandWarning = t.BrandWarning
			brandSuccess = t.BrandSuccess
			brandMuted = t.BrandMuted
			brandBg = t.Background
			brandText = t.Foreground
		}
	}

	header := styles.FromHexBold(types.Icons.Table+"  Query Results", brandAccent)
	breadcrumb := styles.FromHex(types.Icons.Pointer+" Home › ", brandMuted) + styles.FromHexBold(types.Icons.Query+" Editor › Results", brandAccent)

	leftWidth := max(20, int(float64(width)*0.18))
	rightWidth := max(24, int(float64(width)*0.22))
	centerWidth := width - leftWidth - rightWidth - 6
	if centerWidth < 50 {
		centerWidth = 50
		rightWidth = max(20, width-leftWidth-centerWidth-6)
	}
	if rightWidth < 20 {
		rightWidth = 20
	}

	bodyHeight := max(10, height-9)

	leftPane := lipgloss.NewStyle().
		Width(leftWidth).
		Height(bodyHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(brandMuted)).
		Background(lipgloss.Color(brandBg)).
		Padding(0, 1).
		Render(r.renderMetadataPane(leftWidth-2, bodyHeight, brandWarning, brandMuted))

	centerPane := lipgloss.NewStyle().
		Width(centerWidth).
		Height(bodyHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(brandAccent)).
		Background(lipgloss.Color(brandBg)).
		Padding(0, 1).
		Render(r.renderTablePane(centerWidth-2, bodyHeight, brandAccent, brandMuted, brandText))

	rightPane := lipgloss.NewStyle().
		Width(rightWidth).
		Height(bodyHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(brandSuccess)).
		Background(lipgloss.Color(brandBg)).
		Padding(0, 1).
		Render(r.renderStatsPane(rightWidth-2, bodyHeight, brandSuccess, brandMuted))

	content := lipgloss.JoinHorizontal(lipgloss.Top, leftPane, centerPane, rightPane)

	helpText := styles.FromHexBold("PgUp/Dn", brandAccent) + styles.FromHex(" Navigate  ", brandMuted) +
		styles.FromHexBold("←→", brandAccent) + styles.FromHex(" Scroll  ", brandMuted) +
		styles.FromHexBold("Ctrl+E", brandAccent) + styles.FromHex(" Export  ", brandMuted) +
		styles.FromHexBold("Esc", brandAccent) + styles.FromHex(" Back", brandMuted)

	return strings.Join([]string{
		header,
		breadcrumb,
		"",
		content,
		"",
		helpText,
	}, "\n")
}

func (r *ResultsScreen) renderMetadataPane(width, height int, accent, muted string) string {
	if r.result == nil || len(r.result.Rows) == 0 {
		return styles.FromHex("No results", muted)
	}

	lines := []string{
		styles.FromHexBold(types.Icons.File+" Pagination", accent),
		"",
		fmt.Sprintf("Page: %d / %d", r.page+1, r.totalPages),
		fmt.Sprintf("Rows: %d total", len(r.result.Rows)),
		fmt.Sprintf("Per page: %d", r.pageSize),
		"",
		styles.FromHexBold(types.Icons.Column+" Columns", accent),
		"",
		fmt.Sprintf("Total: %d", len(r.result.Columns)),
		fmt.Sprintf("Shown: %d", min(r.displayCols, len(r.result.Columns))),
	}

	if len(lines) > height {
		lines = lines[:height]
	}
	content := strings.Join(lines, "\n")
	return lipgloss.NewStyle().Width(width).MaxHeight(height).Render(content)
}

func (r *ResultsScreen) renderTablePane(width, height int, accent, muted, text string) string {
	if r.result == nil {
		return styles.FromHex(types.Icons.Info+" No results", muted)
	}
	if r.result.Message != "" {
		return styles.FromHexBold(types.Icons.Success+" "+r.result.Message, accent)
	}
	if len(r.result.Rows) == 0 {
		return styles.FromHex(types.Icons.Info+" Empty result set", muted)
	}

	title := styles.FromHexBold(types.Icons.Table+" Results", accent)
	tableHeight := height - 2
	if tableHeight < 3 {
		tableHeight = 3
	}

	tableContent := r.renderCompactTable(width, tableHeight, accent, muted, text)
	tableView := lipgloss.NewStyle().Width(width).MaxHeight(tableHeight).Render(tableContent)
	return lipgloss.JoinVertical(lipgloss.Left, title, tableView)
}

func (r *ResultsScreen) renderStatsPane(width, height int, success, muted string) string {
	if r.result == nil {
		return styles.FromHex("No data", muted)
	}

	lines := []string{
		styles.FromHexBold(types.Icons.CPU+" Statistics", success),
		"",
		fmt.Sprintf("Cols: %d", len(r.result.Columns)),
		fmt.Sprintf("Rows: %d", len(r.result.Rows)),
	}

	if len(r.result.Columns) > 0 && len(r.result.Columns) <= 6 {
		lines = append(lines, "")
		lines = append(lines, styles.FromHexBold("Fields", success))
		for _, col := range r.result.Columns {
			if len(col) > width-4 {
				col = col[:width-7] + "..."
			}
			lines = append(lines, " ○ "+col)
		}
	}

	lines = append(lines, "")
	lines = append(lines, styles.FromHexBold("Export", success))
	lines = append(lines, "Ctrl+E to export")

	if len(lines) > height {
		lines = lines[:height]
	}
	content := strings.Join(lines, "\n")
	return lipgloss.NewStyle().Width(width).MaxHeight(height).Render(content)
}

func (r *ResultsScreen) renderCompactTable(width, height int, accent, muted, text string) string {
	if r.result == nil || len(r.result.Rows) == 0 {
		return ""
	}

	startCol := r.colOffset
	endCol := min(startCol+r.displayCols, len(r.result.Columns))
	if endCol <= startCol {
		endCol = startCol + 1
	}
	visibleCols := r.result.Columns[startCol:endCol]

	colWidths := make([]int, len(visibleCols))
	for i, col := range visibleCols {
		colWidths[i] = min(len(col), width/max(1, len(visibleCols)))
	}

	startRow := r.page * r.pageSize
	endRow := min(startRow+r.pageSize, len(r.result.Rows))

if endRow > len(r.result.Rows) {
		endRow = len(r.result.Rows)
	}
	if startRow >= endRow {
		startRow = 0
		endRow = min(r.pageSize, len(r.result.Rows))
	}

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(accent))

	cellStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(text))

	altStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted))

	var b strings.Builder

	// Header
	for i, col := range visibleCols {
		if i > 0 {
			b.WriteString(" ")
		}
		if len(col) > colWidths[i] {
			col = col[:colWidths[i]-1] + "…"
		}
		b.WriteString(headerStyle.Render(padRight(col, colWidths[i])))
	}
	b.WriteString("\n")

	sepWidth := 0
	for _, w := range colWidths {
		sepWidth += w
	}
	sepWidth += max(0, len(visibleCols)-1)
	if sepWidth > width {
		sepWidth = width
	}
	b.WriteString(strings.Repeat("─", sepWidth))
	b.WriteString("\n")

	// Rows
	for rowIdx := startRow; rowIdx < endRow && len(b.String()) < width*height; rowIdx++ {
		row := r.result.Rows[rowIdx]
		style := cellStyle
		if (rowIdx-startRow)%2 == 1 {
			style = altStyle
		}

		for i := 0; i < len(visibleCols); i++ {
			colIdx := startCol + i
			var valStr string
			if colIdx < len(row) {
				valStr = fmt.Sprintf("%v", row[colIdx])
			}
			if len(valStr) > colWidths[i] {
				valStr = valStr[:colWidths[i]-1] + "…"
			}
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString(style.Render(padRight(valStr, colWidths[i])))
		}
		b.WriteString("\n")
	}

	return b.String()
}

// Helper functions
func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
