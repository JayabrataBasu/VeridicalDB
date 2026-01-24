package syntax

import "strings"

// LineOperations provides line-based text editing operations.
type LineOperations struct {
	content string
	lines   []string
	cursor  int // Current line
}

// NewLineOperations creates a new line operations handler.
func NewLineOperations(content string) *LineOperations {
	lines := strings.Split(content, "\n")
	return &LineOperations{
		content: content,
		lines:   lines,
		cursor:  0,
	}
}

// SetContent updates the content.
func (lo *LineOperations) SetContent(content string) {
	lo.content = content
	lo.lines = strings.Split(content, "\n")
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}
}

// GetContent returns the current content.
func (lo *LineOperations) GetContent() string {
	return strings.Join(lo.lines, "\n")
}

// SetCursor sets the current line cursor.
func (lo *LineOperations) SetCursor(line int) {
	if line >= 0 && line < len(lo.lines) {
		lo.cursor = line
	}
}

// GetCursor returns the current line cursor.
func (lo *LineOperations) GetCursor() int {
	return lo.cursor
}

// DeleteLine removes the line at the cursor position.
func (lo *LineOperations) DeleteLine() string {
	if len(lo.lines) == 0 {
		return ""
	}
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	deleted := lo.lines[lo.cursor]
	lo.lines = append(lo.lines[:lo.cursor], lo.lines[lo.cursor+1:]...)

	if len(lo.lines) == 0 {
		lo.lines = []string{""}
	}
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	return deleted
}

// DuplicateLine duplicates the line at the cursor position.
func (lo *LineOperations) DuplicateLine() {
	if len(lo.lines) == 0 {
		return
	}
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	line := lo.lines[lo.cursor]
	lo.lines = append(lo.lines[:lo.cursor+1], append([]string{line}, lo.lines[lo.cursor+1:]...)...)
}

// MoveLine moves a line up or down.
// direction: -1 for up, 1 for down.
func (lo *LineOperations) MoveLine(direction int) {
	if len(lo.lines) <= 1 {
		return
	}
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	if direction == -1 && lo.cursor > 0 {
		// Move up
		lo.lines[lo.cursor], lo.lines[lo.cursor-1] = lo.lines[lo.cursor-1], lo.lines[lo.cursor]
		lo.cursor--
	} else if direction == 1 && lo.cursor < len(lo.lines)-1 {
		// Move down
		lo.lines[lo.cursor], lo.lines[lo.cursor+1] = lo.lines[lo.cursor+1], lo.lines[lo.cursor]
		lo.cursor++
	}
}

// DeleteWord deletes a word from the specified position in a line.
// wordIndex: 0-based index of the word in the line.
func (lo *LineOperations) DeleteWord(wordIndex int) string {
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	line := lo.lines[lo.cursor]
	words := strings.Fields(line)

	if wordIndex < 0 || wordIndex >= len(words) {
		return ""
	}

	deleted := words[wordIndex]
	words = append(words[:wordIndex], words[wordIndex+1:]...)

	// Reconstruct line preserving some spacing
	lo.lines[lo.cursor] = strings.Join(words, " ")
	return deleted
}

// InsertLine inserts a new line at cursor position.
func (lo *LineOperations) InsertLine(content string) {
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	lo.lines = append(lo.lines[:lo.cursor+1], append([]string{content}, lo.lines[lo.cursor+1:]...)...)
}

// GetLine returns the line at the cursor position.
func (lo *LineOperations) GetLine() string {
	if lo.cursor >= len(lo.lines) {
		return ""
	}
	return lo.lines[lo.cursor]
}

// SetLine updates the line at the cursor position.
func (lo *LineOperations) SetLine(content string) {
	if lo.cursor < len(lo.lines) {
		lo.lines[lo.cursor] = content
	}
}

// CopyLine returns the line at the cursor (for copy operation).
func (lo *LineOperations) CopyLine() string {
	return lo.GetLine()
}

// CutLine removes and returns the line at the cursor (for cut operation).
func (lo *LineOperations) CutLine() string {
	return lo.DeleteLine()
}

// PasteLine inserts a line after the cursor.
func (lo *LineOperations) PasteLine(content string) {
	if lo.cursor+1 < len(lo.lines) {
		lo.lines = append(lo.lines[:lo.cursor+1], append([]string{content}, lo.lines[lo.cursor+1:]...)...)
	} else {
		lo.lines = append(lo.lines, content)
	}
	lo.cursor++
}

// JoinWithNext joins the current line with the next line.
// separator: the separator to use between lines (usually " " or empty).
func (lo *LineOperations) JoinWithNext(separator string) bool {
	if lo.cursor >= len(lo.lines)-1 {
		return false
	}

	current := lo.lines[lo.cursor]
	next := lo.lines[lo.cursor+1]
	lo.lines[lo.cursor] = current + separator + next
	lo.lines = append(lo.lines[:lo.cursor+1], lo.lines[lo.cursor+2:]...)
	return true
}

// SplitLine splits the current line at the specified position.
// position: character position to split at.
func (lo *LineOperations) SplitLine(position int) {
	if lo.cursor >= len(lo.lines) {
		lo.cursor = len(lo.lines) - 1
	}

	line := lo.lines[lo.cursor]
	if position < 0 || position > len(line) {
		return
	}

	first := line[:position]
	second := line[position:]

	lo.lines[lo.cursor] = first
	lo.InsertLine(second)
}

// IndentLine adds indentation to the line.
// spaces: number of spaces to add.
func (lo *LineOperations) IndentLine(spaces int) {
	if lo.cursor >= len(lo.lines) {
		return
	}

	indent := strings.Repeat(" ", spaces)
	lo.lines[lo.cursor] = indent + lo.lines[lo.cursor]
}

// DedentLine removes indentation from the line.
// spaces: number of spaces to remove.
func (lo *LineOperations) DedentLine(spaces int) {
	if lo.cursor >= len(lo.lines) {
		return
	}

	line := lo.lines[lo.cursor]
	spaceToRemove := strings.Repeat(" ", spaces)

	if strings.HasPrefix(line, spaceToRemove) {
		lo.lines[lo.cursor] = line[len(spaceToRemove):]
	}
}

// TrimLine removes leading and trailing whitespace.
func (lo *LineOperations) TrimLine() {
	if lo.cursor < len(lo.lines) {
		lo.lines[lo.cursor] = strings.TrimSpace(lo.lines[lo.cursor])
	}
}

// LineCount returns the number of lines.
func (lo *LineOperations) LineCount() int {
	return len(lo.lines)
}

// SelectLines returns lines between start and end (inclusive).
func (lo *LineOperations) SelectLines(start, end int) []string {
	if start < 0 || end >= len(lo.lines) || start > end {
		return nil
	}
	return lo.lines[start : end+1]
}

// DeleteSelectedLines removes lines from start to end (inclusive).
func (lo *LineOperations) DeleteSelectedLines(start, end int) {
	if start < 0 || end >= len(lo.lines) || start > end {
		return
	}

	lo.lines = append(lo.lines[:start], lo.lines[end+1:]...)

	if lo.cursor > start {
		lo.cursor = start
	}
	if lo.cursor >= len(lo.lines) && len(lo.lines) > 0 {
		lo.cursor = len(lo.lines) - 1
	}
}
