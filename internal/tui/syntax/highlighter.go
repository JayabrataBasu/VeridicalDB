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

// highlightTokens highlights tokens in a line using a single-pass approach.
// All regex matching is done on the ORIGINAL text before any ANSI codes are inserted,
// then tokens are rendered in one pass from left to right.
func (h *Highlighter) highlightTokens(line string) string {
	// Collect string positions first (they take highest priority)
	stringPattern := regexp.MustCompile(`'([^'\\]|\\.)*'|"([^"\\]|\\.)*"`)
	stringMatches := stringPattern.FindAllStringIndex(line, -1)

	inString := func(pos int) bool {
		for _, m := range stringMatches {
			if pos >= m[0] && pos < m[1] {
				return true
			}
		}
		return false
	}

	var tokens []token

	// Add string tokens
	for _, m := range stringMatches {
		tokens = append(tokens, token{m[0], m[1], h.stringStyle})
	}

	// Collect keyword matches
	keywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
		"CREATE", "DROP", "ALTER", "TABLE", "DATABASE", "INDEX",
		"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
		"GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
		"AND", "OR", "NOT", "IN", "EXISTS", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END",
		"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "WITH",
		"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT",
		"DISTINCT", "AS", "UNION", "ALL", "INTERSECT", "EXCEPT",
		"USE", "SHOW", "SET", "VALUES", "INTO",
	}

	for _, kw := range keywords {
		pat := regexp.MustCompile(`(?i)\b` + kw + `\b`)
		for _, m := range pat.FindAllStringIndex(line, -1) {
			if !inString(m[0]) {
				tokens = append(tokens, token{m[0], m[1], h.keywordStyle})
			}
		}
	}

	// Data types
	dataTypes := []string{
		"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
		"DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL",
		"CHAR", "VARCHAR", "TEXT", "BLOB", "CLOB",
		"DATE", "TIME", "TIMESTAMP", "DATETIME",
		"BOOLEAN", "BOOL", "BINARY",
		"JSON", "UUID", "ARRAY", "ENUM",
	}
	for _, dt := range dataTypes {
		pat := regexp.MustCompile(`(?i)\b` + dt + `\b`)
		for _, m := range pat.FindAllStringIndex(line, -1) {
			if !inString(m[0]) {
				tokens = append(tokens, token{m[0], m[1], h.dataTypeStyle})
			}
		}
	}

	// Functions (word followed by paren)
	functions := []string{
		"COUNT", "SUM", "AVG", "MIN", "MAX",
		"UPPER", "LOWER", "LENGTH", "SUBSTR", "CONCAT",
		"ROUND", "CEIL", "FLOOR", "ABS",
		"NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"CAST", "COALESCE", "NULLIF",
		"ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD",
		"EXTRACT", "DATE_PART", "DATE_TRUNC",
	}
	for _, fn := range functions {
		pat := regexp.MustCompile(`(?i)\b` + fn + `\s*\(`)
		for _, m := range pat.FindAllStringIndex(line, -1) {
			if !inString(m[0]) {
				// Only highlight the function name, not the paren
				nameEnd := m[1] - 1 // exclude '('
				for nameEnd > m[0] && (line[nameEnd-1] == ' ' || line[nameEnd-1] == '\t') {
					nameEnd--
				}
				tokens = append(tokens, token{m[0], nameEnd, h.functionStyle})
			}
		}
	}

	// Numbers
	numPat := regexp.MustCompile(`\b\d+(\.\d+)?\b`)
	for _, m := range numPat.FindAllStringIndex(line, -1) {
		if !inString(m[0]) {
			tokens = append(tokens, token{m[0], m[1], h.numberStyle})
		}
	}

	// If no tokens found, return as-is
	if len(tokens) == 0 {
		return line
	}

	// Sort tokens by start position; for overlaps, keep the first (highest priority) one
	// Strings have highest priority since they were added first
	sortTokens(tokens)
	merged := mergeTokens(tokens)

	// Render in a single pass
	var b strings.Builder
	pos := 0
	for _, t := range merged {
		if t.start > pos {
			b.WriteString(line[pos:t.start])
		}
		b.WriteString(t.style.Render(line[t.start:t.end]))
		pos = t.end
	}
	if pos < len(line) {
		b.WriteString(line[pos:])
	}
	return b.String()
}

// sortTokens sorts by start position.
func sortTokens(tokens []token) {
	// Simple insertion sort — token lists are small.
	for i := 1; i < len(tokens); i++ {
		key := tokens[i]
		j := i - 1
		for j >= 0 && tokens[j].start > key.start {
			tokens[j+1] = tokens[j]
			j--
		}
		tokens[j+1] = key
	}
}

type token struct {
	start int
	end   int
	style lipgloss.Style
}

// mergeTokens removes overlapping tokens, keeping the first one encountered.
func mergeTokens(tokens []token) []token {
	if len(tokens) == 0 {
		return nil
	}
	result := []token{tokens[0]}
	for i := 1; i < len(tokens); i++ {
		last := result[len(result)-1]
		if tokens[i].start >= last.end {
			result = append(result, tokens[i])
		}
		// else: overlapping — skip (earlier token wins)
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
