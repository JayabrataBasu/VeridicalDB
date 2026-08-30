package screens

import (
	"fmt"
	"strings"
	"time"

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
	elapsed     time.Duration
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

// SetResult updates the results to display.
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

// SetElapsed records how long the query took, shown in the results header.
func (r *ResultsScreen) SetElapsed(d time.Duration) { r.elapsed = d }

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

// View renders the results screen: a summary line, a full-width table, a footer.
func (r *ResultsScreen) View() string {
	width, height := r.width, r.height
	if width <= 0 {
		width = 120
	}
	if height <= 0 {
		height = 40
	}

	accent, warn, ok, muted, border, text := "#00D9FF", "#FFB86C", "#55FF55", "#8A93A5", "#4A4E5A", "#E0E0E0"
	if tp, is := r.app.(interface{ GetThemeManager() *theme.Manager }); is {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			accent, warn, ok, muted, border, text = t.BrandAccent, t.BrandWarning, t.BrandSuccess, t.Muted, t.Border, t.Foreground
		}
	}

	var b strings.Builder

	// ---- summary line ----
	summary := r.summaryLine(accent, ok, warn, muted)
	b.WriteString(" " + summary + "\n")
	b.WriteString(styles.FromHex(strings.Repeat("─", width), border) + "\n")

	// ---- body ----
	bodyHeight := height - 4
	if bodyHeight < 3 {
		bodyHeight = 3
	}
	switch {
	case r.result == nil:
		b.WriteString(indentLines(r.emptyState(muted, accent), 1))
	case len(r.result.Rows) == 0:
		msg := r.result.Message
		if msg == "" {
			msg = "Statement completed. No rows returned."
		}
		b.WriteString("  " + styles.FromHex(types.Icons.Success+" ", ok) + styles.FromHex(msg, text))
	default:
		b.WriteString(indentLines(r.renderCompactTable(width-2, bodyHeight, accent, muted, text), 1))
	}

	// ---- footer ----
	b.WriteString("\n")
	hints := strings.Join([]string{
		styles.FromHexBold("PgUp/Dn", accent) + styles.FromHex(" page", muted),
		styles.FromHexBold("←→", accent) + styles.FromHex(" columns", muted),
		styles.FromHexBold("^E", accent) + styles.FromHex(" export", muted),
		styles.FromHexBold("esc", accent) + styles.FromHex(" editor", muted),
	}, styles.FromHex("  ", muted))
	b.WriteString(" " + hints)

	return b.String()
}

// summaryLine builds "N rows · P columns · page 1/3 · 8.2ms".
func (r *ResultsScreen) summaryLine(accent, ok, warn, muted string) string {
	if r.result == nil {
		return styles.FromHex("No query has been run yet", muted)
	}
	var seg []string
	if len(r.result.Rows) > 0 {
		unit := "rows"
		if len(r.result.Rows) == 1 {
			unit = "row"
		}
		seg = append(seg, styles.FromHexBold(fmt.Sprintf("%d %s", len(r.result.Rows), unit), ok))
		seg = append(seg, styles.FromHex(fmt.Sprintf("%d columns", len(r.result.Columns)), muted))
		if r.totalPages > 1 {
			seg = append(seg, styles.FromHex(fmt.Sprintf("page %d/%d", r.page+1, r.totalPages), muted))
		}
	} else {
		seg = append(seg, styles.FromHexBold("OK", ok))
	}
	if r.elapsed > 0 {
		c := muted
		if r.elapsed > 500*time.Millisecond {
			c = warn
		}
		seg = append(seg, styles.FromHex(fmtDuration(r.elapsed), c))
	}
	return strings.Join(seg, styles.FromHex("  ·  ", muted))
}

func (r *ResultsScreen) emptyState(muted, accent string) string {
	return styles.FromHex("Run a statement in the ", muted) +
		styles.FromHexBold("Editor", accent) +
		styles.FromHex(" (Ctrl+Enter) and its results appear here.", muted)
}

func fmtDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func (r *ResultsScreen) renderCompactTable(width, height int, accent, muted, text string) string {
	if r.result == nil || len(r.result.Rows) == 0 {
		return ""
	}
	if width < 12 {
		width = 12
	}
	if height < 3 {
		height = 3
	}
	if len(r.result.Columns) == 0 {
		return styles.FromHex(types.Icons.Info+" No columns", muted)
	}

	startCol := r.colOffset
	endCol := min(startCol+r.displayCols, len(r.result.Columns))
	if endCol <= startCol {
		endCol = startCol + 1
	}
	visibleCols := r.result.Columns[startCol:endCol]
	if len(visibleCols) == 0 {
		return styles.FromHex(types.Icons.Info+" No visible columns", muted)
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

	// Size each column to its widest value (header or a sampled cell), then
	// clamp so the whole table fits the available width.
	colWidths := make([]int, len(visibleCols))
	for i, col := range visibleCols {
		w := len(col)
		sample := endRow
		if sample > startRow+40 {
			sample = startRow + 40
		}
		for ri := startRow; ri < sample; ri++ {
			ci := startCol + i
			if ci < len(r.result.Rows[ri]) {
				if l := len(fmt.Sprintf("%v", r.result.Rows[ri][ci])); l > w {
					w = l
				}
			}
		}
		colWidths[i] = clampInt(w, 3, 40)
	}
	// Shrink proportionally if we overflow.
	total := len(visibleCols) - 1
	for _, w := range colWidths {
		total += w
	}
	for total > width && total > len(visibleCols) {
		widest := 0
		for i := range colWidths {
			if colWidths[i] > colWidths[widest] {
				widest = i
			}
		}
		if colWidths[widest] <= 3 {
			break
		}
		colWidths[widest]--
		total--
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
		col = truncateCell(col, colWidths[i])
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
			valStr = truncateCell(valStr, colWidths[i])
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
	if width <= 0 {
		return ""
	}
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func truncateCell(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if len(s) <= width {
		return s
	}
	if width == 1 {
		return "…"
	}
	return s[:width-1] + "…"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
