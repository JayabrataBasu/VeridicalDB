// Package display provides output formatting utilities for VeridicalDB.
package display

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
)

// TableFormatter formats query results as ASCII tables.
type TableFormatter struct {
	MaxColumnWidth int
	NullDisplay    string
	TruncationMark string
}

// NewTableFormatter creates a new TableFormatter with default settings.
func NewTableFormatter() *TableFormatter {
	return &TableFormatter{
		MaxColumnWidth: 40,
		NullDisplay:    "NULL",
		TruncationMark: "...",
	}
}

// FormatResult formats a SQL result as an ASCII table string.
// Returns the formatted table or a message-only result.
func (tf *TableFormatter) FormatResult(result *sql.Result) string {
	// If it's a message-only result (CREATE, INSERT, UPDATE, DELETE)
	if result.Message != "" && result.Columns == nil {
		return result.Message
	}

	// If no columns, return empty result
	if len(result.Columns) == 0 {
		return "(no columns)"
	}

	// Calculate column widths
	widths := tf.calculateWidths(result.Columns, result.Rows)

	var buf strings.Builder

	// Print header
	buf.WriteString("\n")
	for i, col := range result.Columns {
		buf.WriteString(" ")
		buf.WriteString(padCell(col, widths[i]))
		buf.WriteString(" ")
		if i < len(result.Columns)-1 {
			buf.WriteString("│")
		}
	}
	buf.WriteString("\n")

	// Print separator
	for i := range result.Columns {
		buf.WriteString(strings.Repeat("─", widths[i]+2))
		if i < len(result.Columns)-1 {
			buf.WriteString("┼")
		}
	}
	buf.WriteString("\n")

	// Print rows
	for _, row := range result.Rows {
		for i, val := range row {
			str := formatValue(val)
			buf.WriteString(" ")
			buf.WriteString(padCell(truncate(str, widths[i]), widths[i]))
			buf.WriteString(" ")
			if i < len(row)-1 {
				buf.WriteString("│")
			}
		}
		buf.WriteString("\n")
	}

	// Print row count
	buf.WriteString(fmt.Sprintf("\n(%d row(s))\n", len(result.Rows)))

	return buf.String()
}

// CalculateWidths determines optimal column widths for the result data.
func (tf *TableFormatter) calculateWidths(columns []string, rows [][]catalog.Value) []int {
	widths := make([]int, len(columns))

	// Initialize with header width
	for i, col := range columns {
		widths[i] = len(col)
	}

	// Expand to accommodate data
	for _, row := range rows {
		for i, val := range row {
			str := formatValue(val)
			if len(str) > widths[i] {
				widths[i] = len(str)
			}
		}
	}

	// Cap each column at MaxColumnWidth
	for i := range widths {
		if widths[i] > tf.MaxColumnWidth {
			widths[i] = tf.MaxColumnWidth
		}
	}

	return widths
}

// padCell pads a string to the specified width with trailing spaces.
func padCell(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// formatValue converts a catalog.Value to a display string.
// This is a wrapper around the ValueFormatter for convenience.
func formatValue(v catalog.Value) string {
	return NewValueFormatter().Format(v)
}

// truncate limits a string to maxLen characters, adding ellipsis if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen < 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
