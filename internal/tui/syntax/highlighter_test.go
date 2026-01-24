package syntax

import (
	"strings"
	"testing"
)

func TestNewHighlighter(t *testing.T) {
	h := NewHighlighter()
	if h == nil {
		t.Fatal("NewHighlighter returned nil")
	}
}

func TestHighlightKeywords(t *testing.T) {
	h := NewHighlighter()
	code := "SELECT * FROM users WHERE id = 1"
	result := h.Highlight(code)

	// Should contain styled keywords
	if !strings.Contains(result, "SELECT") && !strings.Contains(result, "FROM") {
		t.Error("Highlighting should preserve keywords")
	}
}

func TestHighlightMultilineSQL(t *testing.T) {
	h := NewHighlighter()
	code := `SELECT id, name
FROM users
WHERE status = 'active'`

	result := h.Highlight(code)
	if result == "" {
		t.Error("Highlight should return non-empty result")
	}

	lines := strings.Split(result, "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines, got %d", len(lines))
	}
}

func TestHighlightDataTypes(t *testing.T) {
	h := NewHighlighter()
	code := "CREATE TABLE users (id INT, name VARCHAR(100))"
	result := h.Highlight(code)

	// Verify it doesn't crash with complex SQL
	if result == "" {
		t.Error("Should highlight data type declarations")
	}
}

func TestHighlightComments(t *testing.T) {
	h := NewHighlighter()
	code := "-- This is a comment\nSELECT * FROM users"
	result := h.Highlight(code)

	lines := strings.Split(result, "\n")
	if len(lines) != 2 {
		t.Errorf("Expected 2 lines, got %d", len(lines))
	}
}

func TestHighlightStrings(t *testing.T) {
	h := NewHighlighter()
	code := "SELECT * FROM users WHERE name = 'John Doe'"
	result := h.Highlight(code)

	if !strings.Contains(result, "John Doe") {
		t.Error("String content should be preserved")
	}
}

func TestHighlightNumbers(t *testing.T) {
	h := NewHighlighter()
	code := "SELECT * FROM users WHERE age > 18 AND balance >= 100.50"
	result := h.Highlight(code)

	if result == "" {
		t.Error("Should handle numeric literals")
	}
}

func TestHighlightFunctions(t *testing.T) {
	h := NewHighlighter()
	code := "SELECT COUNT(*), MAX(salary), UPPER(name) FROM employees"
	result := h.Highlight(code)

	if result == "" {
		t.Error("Should highlight SQL functions")
	}
}

func TestSetThemeDark(t *testing.T) {
	h := NewHighlighter()
	h.SetTheme("dark")
	// Should not crash
	code := "SELECT * FROM table1"
	result := h.Highlight(code)
	if result == "" {
		t.Error("Dark theme should work")
	}
}

func TestSetThemeLight(t *testing.T) {
	h := NewHighlighter()
	h.SetTheme("light")
	code := "SELECT * FROM table1"
	result := h.Highlight(code)
	if result == "" {
		t.Error("Light theme should work")
	}
}

func TestSetThemeHighContrast(t *testing.T) {
	h := NewHighlighter()
	h.SetTheme("high-contrast")
	code := "SELECT * FROM table1"
	result := h.Highlight(code)
	if result == "" {
		t.Error("High-contrast theme should work")
	}
}

func TestHighlightLeadingWhitespace(t *testing.T) {
	h := NewHighlighter()
	code := "    SELECT * FROM users"
	result := h.Highlight(code)

	if !strings.HasPrefix(result, "    ") {
		t.Error("Should preserve leading whitespace")
	}
}

func TestHighlightEmptyLine(t *testing.T) {
	h := NewHighlighter()
	code := "SELECT * FROM users\n\nWHERE id = 1"
	result := h.Highlight(code)

	lines := strings.Split(result, "\n")
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines including empty, got %d", len(lines))
	}
}

func TestHighlightComplexQuery(t *testing.T) {
	h := NewHighlighter()
	code := `SELECT u.id, u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 5
ORDER BY order_count DESC
LIMIT 10`

	result := h.Highlight(code)
	if result == "" {
		t.Error("Should handle complex multi-line query")
	}

	lines := strings.Split(result, "\n")
	if len(lines) != strings.Count(code, "\n")+1 {
		t.Error("Should maintain line structure")
	}
}

func TestHighlightSelection(t *testing.T) {
	h := NewHighlighter()
	code := `SELECT * FROM table1
SELECT * FROM table2
SELECT * FROM table3`

	result := h.HighlightSelection(code, 0, 1)
	// Should only highlight first line
	if result == "" {
		t.Error("HighlightSelection should work")
	}
}

func TestHighlightKeywordCaseInsensitive(t *testing.T) {
	h := NewHighlighter()
	code1 := "SELECT * FROM users"
	code2 := "select * from users"
	code3 := "SeLeCt * FrOm users"

	result1 := h.Highlight(code1)
	result2 := h.Highlight(code2)
	result3 := h.Highlight(code3)

	// All should produce results (case-insensitive matching)
	if result1 == "" || result2 == "" || result3 == "" {
		t.Error("Should handle keywords in any case")
	}
}

func TestStylesNotEmpty(t *testing.T) {
	h := NewHighlighter()

	// Verify styles were created by using them to render something
	if len(h.keywordStyle.Render("test")) == 0 {
		t.Error("keywordStyle should render")
	}
	if len(h.functionStyle.Render("test")) == 0 {
		t.Error("functionStyle should render")
	}
	if len(h.dataTypeStyle.Render("test")) == 0 {
		t.Error("dataTypeStyle should render")
	}
	if len(h.stringStyle.Render("test")) == 0 {
		t.Error("stringStyle should render")
	}
}
