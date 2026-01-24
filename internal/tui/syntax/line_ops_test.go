package syntax

import (
	"strings"
	"testing"
)

func TestNewLineOperations(t *testing.T) {
	content := "line 1\nline 2\nline 3"
	lo := NewLineOperations(content)

	if lo == nil {
		t.Fatal("NewLineOperations returned nil")
	}
	if lo.GetCursor() != 0 {
		t.Errorf("Expected cursor at 0, got %d", lo.GetCursor())
	}
	if lo.LineCount() != 3 {
		t.Errorf("Expected 3 lines, got %d", lo.LineCount())
	}
}

func TestSetContent(t *testing.T) {
	lo := NewLineOperations("old content")
	lo.SetContent("line 1\nline 2")

	if lo.LineCount() != 2 {
		t.Errorf("Expected 2 lines, got %d", lo.LineCount())
	}
}

func TestGetLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)

	line := lo.GetLine()
	if line != "line 2" {
		t.Errorf("Expected 'line 2', got '%s'", line)
	}
}

func TestSetLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)
	lo.SetLine("modified line")

	if lo.GetLine() != "modified line" {
		t.Error("SetLine didn't update the line")
	}
}

func TestDeleteLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)

	deleted := lo.DeleteLine()
	if deleted != "line 2" {
		t.Errorf("Expected deleted 'line 2', got '%s'", deleted)
	}
	if lo.LineCount() != 2 {
		t.Errorf("Expected 2 lines after delete, got %d", lo.LineCount())
	}
}

func TestDuplicateLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)
	lo.DuplicateLine()

	if lo.LineCount() != 4 {
		t.Errorf("Expected 4 lines after duplicate, got %d", lo.LineCount())
	}
	if lo.GetLine() != "line 2" {
		t.Error("Cursor should stay on original line")
	}
}

func TestMoveLineUp(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(2)
	lo.MoveLine(-1)

	content := lo.GetContent()
	lines := strings.Split(content, "\n")
	if lines[1] != "line 3" {
		t.Errorf("Line 3 should be at position 1, got '%s'", lines[1])
	}
	if lo.GetCursor() != 1 {
		t.Errorf("Cursor should be at 1 after moving up, got %d", lo.GetCursor())
	}
}

func TestMoveLineDown(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)
	lo.MoveLine(1)

	content := lo.GetContent()
	lines := strings.Split(content, "\n")
	if lines[2] != "line 2" {
		t.Errorf("Line 2 should be at position 2, got '%s'", lines[2])
	}
	if lo.GetCursor() != 2 {
		t.Errorf("Cursor should be at 2 after moving down, got %d", lo.GetCursor())
	}
}

func TestCopyLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)

	copied := lo.CopyLine()
	if copied != "line 2" {
		t.Errorf("Expected 'line 2', got '%s'", copied)
	}
	// Content should not change
	if lo.LineCount() != 3 {
		t.Error("Copy should not modify content")
	}
}

func TestCutLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)

	cut := lo.CutLine()
	if cut != "line 2" {
		t.Errorf("Expected 'line 2', got '%s'", cut)
	}
	if lo.LineCount() != 2 {
		t.Error("Cut should remove the line")
	}
}

func TestPasteLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(1)
	lo.PasteLine("inserted line")

	if lo.LineCount() != 4 {
		t.Errorf("Expected 4 lines, got %d", lo.LineCount())
	}
	if lo.GetLine() != "inserted line" {
		t.Error("Cursor should be on pasted line")
	}
}

func TestJoinWithNext(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(0)

	ok := lo.JoinWithNext(" ")
	if !ok {
		t.Error("JoinWithNext should succeed")
	}
	if lo.LineCount() != 2 {
		t.Errorf("Expected 2 lines after join, got %d", lo.LineCount())
	}
	if lo.GetLine() != "line 1 line 2" {
		t.Errorf("Expected 'line 1 line 2', got '%s'", lo.GetLine())
	}
}

func TestJoinWithNextAtEnd(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2")
	lo.SetCursor(1)

	ok := lo.JoinWithNext(" ")
	if ok {
		t.Error("JoinWithNext at last line should fail")
	}
}

func TestSplitLine(t *testing.T) {
	lo := NewLineOperations("line 1 test")
	lo.SetCursor(0)
	lo.SplitLine(6)

	if lo.LineCount() != 2 {
		t.Errorf("Expected 2 lines after split, got %d", lo.LineCount())
	}
	lo.SetCursor(0)
	if lo.GetLine() != "line 1" {
		t.Errorf("Expected 'line 1', got '%s'", lo.GetLine())
	}
	lo.SetCursor(1)
	if lo.GetLine() != " test" {
		t.Errorf("Expected ' test', got '%s'", lo.GetLine())
	}
}

func TestIndentLine(t *testing.T) {
	lo := NewLineOperations("line 1")
	lo.SetCursor(0)
	lo.IndentLine(4)

	if !strings.HasPrefix(lo.GetLine(), "    ") {
		t.Error("Line should be indented with 4 spaces")
	}
}

func TestDedentLine(t *testing.T) {
	lo := NewLineOperations("    line 1")
	lo.SetCursor(0)
	lo.DedentLine(4)

	if strings.HasPrefix(lo.GetLine(), " ") {
		t.Error("Line should be dedented")
	}
	if !strings.HasPrefix(lo.GetLine(), "line") {
		t.Errorf("Expected 'line 1', got '%s'", lo.GetLine())
	}
}

func TestTrimLine(t *testing.T) {
	lo := NewLineOperations("  line 1  ")
	lo.SetCursor(0)
	lo.TrimLine()

	if lo.GetLine() != "line 1" {
		t.Errorf("Expected 'line 1', got '%s'", lo.GetLine())
	}
}

func TestSelectLines(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3\nline 4")

	selected := lo.SelectLines(1, 2)
	if len(selected) != 2 {
		t.Errorf("Expected 2 selected lines, got %d", len(selected))
	}
	if selected[0] != "line 2" || selected[1] != "line 3" {
		t.Error("Wrong lines selected")
	}
}

func TestDeleteSelectedLines(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3\nline 4")

	lo.DeleteSelectedLines(1, 2)
	if lo.LineCount() != 2 {
		t.Errorf("Expected 2 lines after delete, got %d", lo.LineCount())
	}

	lo.SetCursor(0)
	if lo.GetLine() != "line 1" {
		t.Error("First line should remain")
	}
}

func TestDeleteSelectedLinesAdjustsCursor(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(2)
	lo.DeleteSelectedLines(1, 2)

	if lo.GetCursor() != 0 {
		t.Errorf("Cursor should adjust to 0, got %d", lo.GetCursor())
	}
}

func TestLineCount(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	if lo.LineCount() != 3 {
		t.Errorf("Expected 3 lines, got %d", lo.LineCount())
	}
}

func TestEmptyContent(t *testing.T) {
	lo := NewLineOperations("")
	if lo.LineCount() != 1 {
		t.Errorf("Empty content should have 1 line, got %d", lo.LineCount())
	}
}

func TestDeleteWordFromLine(t *testing.T) {
	lo := NewLineOperations("SELECT * FROM users WHERE id = 1")
	lo.SetCursor(0)

	deleted := lo.DeleteWord(0)
	if deleted != "SELECT" {
		t.Errorf("Expected 'SELECT', got '%s'", deleted)
	}

	line := lo.GetLine()
	if !strings.Contains(line, "FROM") {
		t.Error("FROM should remain after deleting SELECT")
	}
}

func TestInsertLine(t *testing.T) {
	lo := NewLineOperations("line 1\nline 3")
	lo.SetCursor(0)
	lo.InsertLine("line 2")

	if lo.LineCount() != 3 {
		t.Errorf("Expected 3 lines, got %d", lo.LineCount())
	}

	lo.SetCursor(1)
	if lo.GetLine() != "line 2" {
		t.Errorf("Expected inserted line at position 1")
	}
}

func TestSetCursor(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2\nline 3")
	lo.SetCursor(2)

	if lo.GetCursor() != 2 {
		t.Errorf("Expected cursor at 2, got %d", lo.GetCursor())
	}
}

func TestSetCursorOutOfBounds(t *testing.T) {
	lo := NewLineOperations("line 1\nline 2")
	lo.SetCursor(5) // Out of bounds

	// Should stay at valid position (last line or similar)
	if lo.GetCursor() >= lo.LineCount() {
		t.Error("Cursor should be within bounds")
	}
}
