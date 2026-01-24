// Package syntax provides SQL syntax highlighting utilities.
package syntax

import (
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// TokenType represents different SQL token types.
type TokenType int

const (
	TokenKeyword TokenType = iota
	TokenFunction
	TokenDataType
	TokenString
	TokenNumber
	TokenOperator
	TokenComment
	TokenIdentifier
	TokenDefault
)

// Highlighter provides SQL syntax highlighting.
type Highlighter struct {
	keywordStyle    lipgloss.Style
	functionStyle   lipgloss.Style
	dataTypeStyle   lipgloss.Style
	stringStyle     lipgloss.Style
	numberStyle     lipgloss.Style
	operatorStyle   lipgloss.Style
	commentStyle    lipgloss.Style
	identifierStyle lipgloss.Style
}

// NewHighlighter creates a new SQL syntax highlighter with default colors.
func NewHighlighter() *Highlighter {
	return &Highlighter{
		keywordStyle:    lipgloss.NewStyle().Foreground(lipgloss.Color("#569CD6")).Bold(true),
		functionStyle:   lipgloss.NewStyle().Foreground(lipgloss.Color("#DCDCAA")).Bold(true),
		dataTypeStyle:   lipgloss.NewStyle().Foreground(lipgloss.Color("#4EC9B0")),
		stringStyle:     lipgloss.NewStyle().Foreground(lipgloss.Color("#CE9178")),
		numberStyle:     lipgloss.NewStyle().Foreground(lipgloss.Color("#B5CEA8")),
		operatorStyle:   lipgloss.NewStyle().Foreground(lipgloss.Color("#D4D4D4")),
		commentStyle:    lipgloss.NewStyle().Foreground(lipgloss.Color("#6A9955")).Italic(true),
		identifierStyle: lipgloss.NewStyle().Foreground(lipgloss.Color("#9CDCFE")),
	}
}

// SetTheme sets the color scheme for syntax highlighting.
func (h *Highlighter) SetTheme(theme string) {
	switch theme {
	case "light":
		h.keywordStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#0000FF")).Bold(true)
		h.functionStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#795E26")).Bold(true)
		h.dataTypeStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#267F99"))
		h.stringStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#A31515"))
		h.numberStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#098658"))
		h.commentStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#008000")).Italic(true)
		h.identifierStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#001080"))
	case "high-contrast":
		h.keywordStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Bold(true).Background(lipgloss.Color("#0000FF"))
		h.functionStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFF00")).Bold(true)
		h.dataTypeStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#00FFFF"))
		h.stringStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF00FF"))
		h.numberStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00"))
		h.commentStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFFFF")).Italic(true)
	default: // dark theme
		h.keywordStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#569CD6")).Bold(true)
		h.functionStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#DCDCAA")).Bold(true)
		h.dataTypeStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#4EC9B0"))
		h.stringStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#CE9178"))
		h.numberStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#B5CEA8"))
		h.commentStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#6A9955")).Italic(true)
	}
}

// Highlight applies syntax highlighting to SQL code.
func (h *Highlighter) Highlight(code string) string {
	lines := strings.Split(code, "\n")
	result := make([]string, len(lines))

	for i, line := range lines {
		result[i] = h.highlightLine(line)
	}

	return strings.Join(result, "\n")
}

// highlightLine highlights a single line of SQL code.
func (h *Highlighter) highlightLine(line string) string {
	// Remove and preserve leading whitespace
	leadingSpace := len(line) - len(strings.TrimLeft(line, " \t"))
	trimmed := strings.TrimSpace(line)

	if trimmed == "" {
		return line
	}

	// Handle comments first (single-line)
	if strings.HasPrefix(trimmed, "--") {
		return strings.Repeat(" ", leadingSpace) + h.commentStyle.Render(trimmed)
	}

	// Handle multi-line comment markers
	if strings.HasPrefix(trimmed, "/*") || strings.HasPrefix(trimmed, "*/") {
		return strings.Repeat(" ", leadingSpace) + h.commentStyle.Render(trimmed)
	}

	// Process the line for various token types
	return strings.Repeat(" ", leadingSpace) + h.highlightTokens(trimmed)
}

// highlightTokens highlights tokens in a line.
func (h *Highlighter) highlightTokens(line string) string {
	// Replace strings first to avoid highlighting inside strings
	stringPattern := regexp.MustCompile(`'([^'\\]|\\.)*'|"([^"\\]|\\.)*"`)
	stringMatches := stringPattern.FindAllStringIndex(line, -1)

	// Track string positions to skip them during other highlighting
	inString := func(pos int) bool {
		for _, match := range stringMatches {
			if pos >= match[0] && pos < match[1] {
				return true
			}
		}
		return false
	}

	// Apply string highlighting
	result := line
	for i := len(stringMatches) - 1; i >= 0; i-- {
		match := stringMatches[i]
		str := line[match[0]:match[1]]
		result = result[:match[0]] + h.stringStyle.Render(str) + result[match[1]:]
	}

	// Highlight keywords
	keywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
		"CREATE", "DROP", "ALTER", "TABLE", "DATABASE", "INDEX",
		"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
		"GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
		"AND", "OR", "NOT", "IN", "EXISTS", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END",
		"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "WITH",
		"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT",
		"DISTINCT", "AS", "UNION", "ALL", "INTERSECT", "EXCEPT",
	}

	for _, keyword := range keywords {
		pattern := regexp.MustCompile(`(?i)\b` + keyword + `\b`)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			if inString(strings.Index(result, match)) {
				return match
			}
			return h.keywordStyle.Render(match)
		})
	}

	// Highlight data types
	dataTypes := []string{
		"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
		"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL",
		"CHAR", "VARCHAR", "TEXT", "BLOB", "CLOB",
		"DATE", "TIME", "TIMESTAMP", "DATETIME",
		"BOOLEAN", "BOOL", "BINARY",
		"JSON", "UUID", "ARRAY", "ENUM",
	}

	for _, dtype := range dataTypes {
		pattern := regexp.MustCompile(`(?i)\b` + dtype + `\b`)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			if inString(strings.Index(result, match)) {
				return match
			}
			return h.dataTypeStyle.Render(match)
		})
	}

	// Highlight functions
	functions := []string{
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"UPPER", "LOWER", "LENGTH", "SUBSTR", "CONCAT",
		"ROUND", "CEIL", "FLOOR", "ABS",
		"NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"CAST", "COALESCE", "NULLIF", "CASE",
		"ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD",
		"EXTRACT", "DATE_PART", "DATE_TRUNC",
	}

	for _, fn := range functions {
		pattern := regexp.MustCompile(`(?i)` + fn + `\s*\(`)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			if inString(strings.Index(result, match)) {
				return match
			}
			fnName := strings.TrimSpace(match[:len(match)-1])
			return h.functionStyle.Render(fnName) + "("
		})
	}

	// Highlight numbers
	numberPattern := regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	result = numberPattern.ReplaceAllStringFunc(result, func(match string) string {
		if inString(strings.Index(result, match)) {
			return match
		}
		return h.numberStyle.Render(match)
	})

	// Highlight operators
	operators := []string{"=", "!=", "<>", "<", ">", "<=", ">=", "\\+", "-", "\\*", "/", "%", "||", "&&"}
	for _, op := range operators {
		pattern := regexp.MustCompile(op)
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			if inString(strings.Index(result, match)) {
				return match
			}
			return h.operatorStyle.Render(match)
		})
	}

	return result
}

// HighlightSelection highlights a selection within code.
func (h *Highlighter) HighlightSelection(code string, startLine, endLine int) string {
	lines := strings.Split(code, "\n")
	for i := startLine; i < endLine && i < len(lines); i++ {
		lines[i] = h.highlightLine(lines[i])
	}
	return strings.Join(lines, "\n")
}
