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

// View renders the results screen with premium styling
func (r *ResultsScreen) View() string {
	var b strings.Builder

	// Define premium styles with better spacing
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 3).
		MarginBottom(2)

	containerStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Padding(1, 2).
		MarginTop(1).
		MarginBottom(1)

	statusStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#50FA7B")).
		Bold(true)

	infoStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888"))

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		MarginTop(2).
		Padding(0, 1)

	// Header with icon
	header := headerStyle.Render(types.Icons.Dashboard + "  Query Results")
	b.WriteString(header)
	b.WriteString("\n\n")

	// Display results
	if r.result == nil {
		emptyMsg := containerStyle.Render(infoStyle.Render("No results to display"))
		b.WriteString(emptyMsg)
	} else if r.result.Message != "" {
		// DDL/Command result (no rows)
		successIcon := types.Icons.Success + " "
		msg := statusStyle.Render(successIcon + r.result.Message)
		b.WriteString(containerStyle.Render(msg))
	} else if len(r.result.Rows) == 0 {
		emptyMsg := containerStyle.Render(infoStyle.Render("Query returned no rows"))
		b.WriteString(emptyMsg)
	} else {
		// Render table with premium styling
		b.WriteString(r.renderPremiumTable())
	}

	b.WriteString("\n")

	// Pagination info with styling
	if r.totalPages > 0 {
		pageStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00D9FF")).
			Bold(true)

		rowStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFB86C"))

		paginationInfo := fmt.Sprintf(
			"%s Page %s | Rows %s",
			types.Icons.File,
			pageStyle.Render(fmt.Sprintf("%d/%d", r.page+1, r.totalPages)),
			rowStyle.Render(fmt.Sprintf("%d-%d of %d",
				r.page*r.pageSize+1,
				min(r.page*r.pageSize+r.pageSize, len(r.result.Rows)),
				len(r.result.Rows))),
		)
		b.WriteString(infoStyle.Render(paginationInfo))
		b.WriteString("\n")
	}

	// Help text with key icons - better spacing
	helpText := helpStyle.Render(
		"PgUp/PgDn Navigate  │  ←→ Scroll Columns  │  Home/End First/Last  │  Ctrl+E Export  │  Esc Back",
	)
	b.WriteString(helpText)

	return b.String()
}

// renderPremiumTable renders the result table with premium styling
func (r *ResultsScreen) renderPremiumTable() string {
	if r.result == nil || len(r.result.Rows) == 0 {
		return ""
	}

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

	// Define table styles
	headerCellStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 1)

	cellStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		Padding(0, 1)

	altCellStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		Background(lipgloss.Color("#252530")).
		Padding(0, 1)

	borderStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#3a3a5c"))

	var b strings.Builder

	// Top border
	topBorder := "╭"
	for i, w := range colWidths {
		topBorder += strings.Repeat("─", w+2)
		if i < len(colWidths)-1 {
			topBorder += "┬"
		}
	}
	topBorder += "╮"
	b.WriteString(borderStyle.Render(topBorder))
	b.WriteString("\n")

	// Header row
	b.WriteString(borderStyle.Render("│"))
	for i, col := range visibleCols {
		b.WriteString(headerCellStyle.Render(padRight(col, colWidths[i])))
		b.WriteString(borderStyle.Render("│"))
	}
	b.WriteString("\n")

	// Header separator
	sepBorder := "├"
	for i, w := range colWidths {
		sepBorder += strings.Repeat("─", w+2)
		if i < len(colWidths)-1 {
			sepBorder += "┼"
		}
	}
	sepBorder += "┤"
	b.WriteString(borderStyle.Render(sepBorder))
	b.WriteString("\n")

	// Data rows with alternating colors
	for rowIdx := startRow; rowIdx < endRow; rowIdx++ {
		row := r.result.Rows[rowIdx]
		b.WriteString(borderStyle.Render("│"))

		style := cellStyle
		if (rowIdx-startRow)%2 == 1 {
			style = altCellStyle
		}

		for i := 0; i < len(visibleCols); i++ {
			colIdx := startCol + i
			var valStr string
			if colIdx < len(row) {
				valStr = fmt.Sprintf("%v", row[colIdx])
			}
			if len(valStr) > colWidths[i] {
				valStr = valStr[:colWidths[i]-3] + "..."
			}
			b.WriteString(style.Render(padRight(valStr, colWidths[i])))
			b.WriteString(borderStyle.Render("│"))
		}
		b.WriteString("\n")
	}

	// Bottom border
	bottomBorder := "╰"
	for i, w := range colWidths {
		bottomBorder += strings.Repeat("─", w+2)
		if i < len(colWidths)-1 {
			bottomBorder += "┴"
		}
	}
	bottomBorder += "╯"
	b.WriteString(borderStyle.Render(bottomBorder))
	b.WriteString("\n")

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
