// Package components provides reusable TUI components.
package components

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/syntax"
	"github.com/charmbracelet/lipgloss"
)

// HighlightedTextView renders SQL code with syntax highlighting and line numbers.
type HighlightedTextView struct {
	text        string
	highlighter *syntax.Highlighter
	width       int
	height      int
	lineNumbers bool
	startLine   int // For scrolling
}

// NewHighlightedTextView creates a new highlighted text view.
func NewHighlightedTextView(text string, highlighter *syntax.Highlighter) *HighlightedTextView {
	return &HighlightedTextView{
		text:        text,
		highlighter: highlighter,
		width:       80,
		height:      20,
		lineNumbers: true,
		startLine:   0,
	}
}

// SetDimensions sets the width and height of the view.
func (h *HighlightedTextView) SetDimensions(width, height int) {
	h.width = width
	h.height = height
}

// SetLineNumbers enables/disables line number display.
func (h *HighlightedTextView) SetLineNumbers(show bool) {
	h.lineNumbers = show
}

// SetText updates the text content.
func (h *HighlightedTextView) SetText(text string) {
	h.text = text
	h.startLine = 0
}

// Render returns the highlighted and formatted text view.
func (h *HighlightedTextView) Render() string {
	if h.width <= 0 || h.height <= 0 {
		return ""
	}

	if h.text == "" {
		empty := make([]string, h.height)
		for i := range empty {
			empty[i] = strings.Repeat(" ", h.width)
		}
		return strings.Join(empty, "\n")
	}

	// Apply syntax highlighting to raw text
	highlighted := h.text
	if h.highlighter != nil {
		highlighted = h.highlighter.Highlight(h.text)
	}
	lines := strings.Split(highlighted, "\n")

	// Calculate line number width if enabled
	lineNumWidth := 0
	if h.lineNumbers {
		lineNumWidth = len(strconv.Itoa(len(lines)))
		if lineNumWidth < 2 {
			lineNumWidth = 2
		}
	}

	// Available width for content
	contentWidth := h.width
	if h.lineNumbers {
		contentWidth -= (lineNumWidth + 2) // e.g. " 12 "
	}
	if contentWidth < 1 {
		contentWidth = 1
	}

	var result []string
	endLine := h.startLine + h.height
	if endLine > len(lines) {
		endLine = len(lines)
	}

	for i := h.startLine; i < endLine && i < len(lines); i++ {
		var line string
		if h.lineNumbers {
			lineNum := lipgloss.NewStyle().
				Foreground(lipgloss.Color("8")).
				Align(lipgloss.Right).
				Width(lineNumWidth).
				Render(strconv.Itoa(i + 1))

			// lines[i] already has ANSI codes from the Highlighter;
			// measure visible width without re-rendering through lipgloss
			content := lines[i]
			visWidth := lipgloss.Width(content)
			if visWidth < contentWidth {
				content += strings.Repeat(" ", contentWidth-visWidth)
			}

			line = fmt.Sprintf("%s  %s", lineNum, content)
		} else {
			content := lines[i]
			visWidth := lipgloss.Width(content)
			if visWidth < h.width {
				content += strings.Repeat(" ", h.width-visWidth)
			}
			line = content
		}

		result = append(result, line)
	}

	// Pad with empty lines if needed
	for len(result) < h.height {
		result = append(result, strings.Repeat(" ", h.width))
	}

	return strings.Join(result, "\n")
}

// ScrollUp scrolls the view up by n lines.
func (h *HighlightedTextView) ScrollUp(lines int) {
	h.startLine -= lines
	if h.startLine < 0 {
		h.startLine = 0
	}
}

// ScrollDown scrolls the view down by n lines.
func (h *HighlightedTextView) ScrollDown(lines int) {
	totalLines := len(strings.Split(h.text, "\n"))
	maxStart := totalLines - h.height
	if maxStart < 0 {
		maxStart = 0
	}

	h.startLine += lines
	if h.startLine > maxStart {
		h.startLine = maxStart
	}
}

// GetLineCount returns the total number of lines.
func (h *HighlightedTextView) GetLineCount() int {
	return len(strings.Split(h.text, "\n"))
}

// GetVisibleLines returns the range of visible lines.
func (h *HighlightedTextView) GetVisibleLines() (int, int) {
	start := h.startLine
	end := h.startLine + h.height
	totalLines := h.GetLineCount()
	if end > totalLines {
		end = totalLines
	}
	return start, end
}
