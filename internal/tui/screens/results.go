package screens

import (
	"fmt"
	"strings"

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
}

// NewResultsScreen creates a new results viewer screen
func NewResultsScreen(app types.StyleProvider) *ResultsScreen {
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
		// Adjust display columns based on window width
		r.displayCols = (msg.Width - 10) / 15 // Rough estimate
		if r.displayCols < 1 {
			r.displayCols = 1
		}
	}

	return r, nil
}

// View renders the results screen
func (r *ResultsScreen) View() string {
	palette := r.getStyles()

	var b strings.Builder

	// Header
	header := palette.Title.Render("Query Results")
	b.WriteString(header)
	b.WriteString("\n\n")

	// Display results
	if r.result == nil {
		b.WriteString(palette.Subtle.Render("No results to display"))
	} else if r.result.Message != "" {
		// DDL/Command result (no rows)
		b.WriteString(palette.Highlight.Render(r.result.Message))
	} else if len(r.result.Rows) == 0 {
		b.WriteString(palette.Subtle.Render("Query returned no rows"))
	} else {
		// Render table
		b.WriteString(r.renderTable())
	}

	b.WriteString("\n\n")

	// Pagination info
	if r.totalPages > 0 {
		paginationInfo := fmt.Sprintf(
			"Page %d/%d | Rows %d-%d of %d",
			r.page+1,
			r.totalPages,
			r.page*r.pageSize+1,
			min(r.page*r.pageSize+r.pageSize, len(r.result.Rows)),
			len(r.result.Rows),
		)
		b.WriteString(palette.Subtle.Render(paginationInfo))
		b.WriteString("\n\n")
	}

	// Help text
	helpText := palette.Help.Render(
		"PgUp/PgDn: Navigate | ←/→: Scroll columns | Home/End: First/Last | Ctrl+E: Export | Esc: Back",
	)
	b.WriteString(helpText)

	return b.String()
}

// renderTable renders the result table with current pagination
func (r *ResultsScreen) renderTable() string {
	if r.result == nil || len(r.result.Rows) == 0 {
		return ""
	}

	palette := r.getStyles()

	// Determine visible columns
	startCol := r.colOffset
	endCol := min(startCol+r.displayCols, len(r.result.Columns))
	visibleCols := r.result.Columns[startCol:endCol]

	// Calculate column widths
	colWidths := make([]int, len(visibleCols))
	for i, col := range visibleCols {
		colWidths[i] = len(col)
	}

	// Check row values for width
	startRow := r.page * r.pageSize
	endRow := min(startRow+r.pageSize, len(r.result.Rows))

	for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
		row := r.result.Rows[rowIdx]
		for i := 0; i < len(visibleCols); i++ {
			colIdx := startCol + i
			if colIdx < len(row) {
				valStr := fmt.Sprintf("%v", row[colIdx])
				if len(valStr) > colWidths[i] {
					colWidths[i] = len(valStr)
				}
			}
		}
	}

	// Cap maximum column width
	for i := range colWidths {
		if colWidths[i] > 40 {
			colWidths[i] = 40
		}
	}

	var b strings.Builder

	// Header row
	headerStyle := palette.Title.Foreground(lipgloss.Color("#00FF00"))
	for i, col := range visibleCols {
		b.WriteString(headerStyle.Render(padRight(col, colWidths[i])))
		b.WriteString("  ")
	}
	b.WriteString("\n")

	// Separator
	for i := range visibleCols {
		b.WriteString(strings.Repeat("─", colWidths[i]))
		b.WriteString("  ")
	}
	b.WriteString("\n")

	// Data rows
	for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
		row := r.result.Rows[rowIdx]
		for i := 0; i < len(visibleCols); i++ {
			colIdx := startCol + i
			var valStr string
			if colIdx < len(row) {
				valStr = fmt.Sprintf("%v", row[colIdx])
			}
			if len(valStr) > colWidths[i] {
				valStr = valStr[:colWidths[i]-3] + "..."
			}
			b.WriteString(padRight(valStr, colWidths[i]))
			b.WriteString("  ")
		}
		b.WriteString("\n")
	}

	return b.String()
}

// getStyles retrieves the shared style palette from the app.
func (r *ResultsScreen) getStyles() *types.StylePalette {
	if r.app == nil {
		return &types.StylePalette{}
	}
	return r.app.GetStyles()
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
