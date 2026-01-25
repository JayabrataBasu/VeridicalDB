package components

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// AutocompleteManager handles autocomplete suggestions for SQL and commands
type AutocompleteManager struct {
	keywords    []string
	suggestions []string
	visible     bool
	selected    int
	maxVisible  int
	theme       AutocompleteTheme
}

// AutocompleteTheme provides styling for autocomplete
type AutocompleteTheme struct {
	Background  lipgloss.Color
	Foreground  lipgloss.Color
	Selection   lipgloss.Color
	SelectionFG lipgloss.Color
	Border      lipgloss.Color
}

// DefaultAutocompleteTheme returns default styling
func DefaultAutocompleteTheme() AutocompleteTheme {
	return AutocompleteTheme{
		Background:  lipgloss.Color("235"), // Dark background
		Foreground:  lipgloss.Color("252"), // Light gray text
		Selection:   lipgloss.Color("33"),  // Blue selection
		SelectionFG: lipgloss.Color("255"), // White text
		Border:      lipgloss.Color("238"), // Dark border
	}
}

// NewAutocompleteManager creates a new autocomplete manager
func NewAutocompleteManager() *AutocompleteManager {
	return &AutocompleteManager{
		keywords: []string{
			// SQL DDL
			"CREATE", "DROP", "ALTER", "TRUNCATE",
			"TABLE", "INDEX", "VIEW", "DATABASE", "SCHEMA",
			"PRIMARY", "KEY", "FOREIGN", "UNIQUE", "CHECK", "DEFAULT",

			// SQL DML
			"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE",
			"FROM", "WHERE", "ORDER", "BY", "GROUP", "LIMIT", "OFFSET",
			"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS",
			"ON", "USING", "DISTINCT", "ALL", "AS",
			"AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL",
			"UNION", "INTERSECT", "EXCEPT",
			"CASE", "WHEN", "THEN", "ELSE", "END",
			"AGGREGATE", "AVG", "COUNT", "MAX", "MIN", "SUM",

			// SQL TCL
			"BEGIN", "COMMIT", "ROLLBACK", "START", "TRANSACTION",
			"SAVEPOINT", "RELEASE",

			// SQL DCL
			"GRANT", "REVOKE", "PRIVILEGES",

			// Functions
			"CAST", "COALESCE", "NULLIF", "EXTRACT", "SUBSTRING", "LENGTH",
			"UPPER", "LOWER", "TRIM", "ROUND", "ABS", "CEIL", "FLOOR",

			// Commands
			"\\dt", "\\di", "\\d", "\\status", "\\config", "\\clear", "\\help",
			"HELP", "EXIT", "QUIT", "\\q",
		},
		maxVisible: 10,
		theme:      DefaultAutocompleteTheme(),
	}
}

// GetSuggestions returns autocomplete suggestions for partial input
func (am *AutocompleteManager) GetSuggestions(input string) []string {
	if input == "" {
		am.suggestions = nil
		am.visible = false
		am.selected = 0
		return nil
	}

	input = strings.ToUpper(strings.TrimSpace(input))
	suggestions := make([]string, 0)

	for _, kw := range am.keywords {
		if strings.HasPrefix(kw, input) {
			suggestions = append(suggestions, kw)
			if len(suggestions) >= am.maxVisible {
				break
			}
		}
	}

	am.suggestions = suggestions
	am.visible = len(suggestions) > 0
	am.selected = 0
	return suggestions
}

// SelectNext moves to the next suggestion
func (am *AutocompleteManager) SelectNext() {
	if am.selected < len(am.suggestions)-1 {
		am.selected++
	}
}

// SelectPrev moves to the previous suggestion
func (am *AutocompleteManager) SelectPrev() {
	if am.selected > 0 {
		am.selected--
	}
}

// GetSelected returns the currently selected suggestion
func (am *AutocompleteManager) GetSelected() string {
	if am.selected >= 0 && am.selected < len(am.suggestions) {
		return am.suggestions[am.selected]
	}
	return ""
}

// RenderSuggestions renders the suggestion dropdown
func (am *AutocompleteManager) RenderSuggestions() string {
	if !am.visible || len(am.suggestions) == 0 {
		return ""
	}

	var lines []string
	borderStyle := lipgloss.NewStyle().Foreground(am.theme.Border)
	lines = append(lines, borderStyle.Render("┌─ Suggestions ─"))

	for i, sugg := range am.suggestions {
		var line string
		if i == am.selected {
			// Selected item
			style := lipgloss.NewStyle().
				Background(am.theme.Selection).
				Foreground(am.theme.SelectionFG).
				Padding(0, 1)
			line = style.Render("→ " + sugg)
		} else {
			// Unselected item
			style := lipgloss.NewStyle().
				Foreground(am.theme.Foreground).
				Padding(0, 1)
			line = style.Render("  " + sugg)
		}
		lines = append(lines, borderStyle.Render("│")+line)
	}

	lines = append(lines, borderStyle.Render("└─────────────"))

	return strings.Join(lines, "\n")
}

// Hide hides the suggestions
func (am *AutocompleteManager) Hide() {
	am.visible = false
	am.suggestions = nil
	am.selected = 0
}

// Show displays the suggestions
func (am *AutocompleteManager) Show() {
	am.visible = true
}

// IsVisible returns whether suggestions are visible
func (am *AutocompleteManager) IsVisible() bool {
	return am.visible
}

// AddKeyword adds a custom keyword
func (am *AutocompleteManager) AddKeyword(keyword string) {
	keyword = strings.ToUpper(keyword)
	for _, kw := range am.keywords {
		if kw == keyword {
			return // Already exists
		}
	}
	am.keywords = append(am.keywords, keyword)
}

// SetTheme sets custom theme for autocomplete
func (am *AutocompleteManager) SetTheme(theme AutocompleteTheme) {
	am.theme = theme
}
