package screens

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// EditorScreen is the primary SQL editor interface
type EditorScreen struct {
	app        types.StyleProvider
	textarea   textarea.Model
	status     string
	executing  bool
	history    []string // Query history
	historyIdx int
}

// NewEditorScreen creates a new SQL editor screen
func NewEditorScreen(app types.StyleProvider) *EditorScreen {
	ta := textarea.New()
	ta.Placeholder = "Enter SQL query here... (Ctrl+Enter or F5 to execute)"
	ta.Focus()
	ta.CharLimit = 0 // No limit
	ta.SetWidth(100)
	ta.SetHeight(15)
	ta.ShowLineNumbers = true
	ta.KeyMap.InsertNewline.SetEnabled(true)

	return &EditorScreen{
		app:        app,
		textarea:   ta,
		status:     "Ready",
		history:    make([]string, 0),
		historyIdx: -1,
	}
}

// Init initializes the editor screen
func (e *EditorScreen) Init() tea.Cmd {
	return textarea.Blink
}

// Update handles messages for the editor screen
func (e *EditorScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			// Return to home screen
			return e, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "home"}
			}

		case "esc":
			// Return to home screen
			return e, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "home"}
			}

		case "f5", "ctrl+enter":
			// Execute query
			sql := strings.TrimSpace(e.textarea.Value())
			if sql != "" && !e.executing {
				e.executing = true
				e.status = "Executing query..."

				// Add to history
				e.history = append(e.history, sql)
				e.historyIdx = len(e.history)

				// Send execute message
				return e, func() tea.Msg {
					return ExecuteQueryMsg{SQL: sql}
				}
			}

		case "ctrl+k":
			// Clear editor
			e.textarea.Reset()
			e.status = "Editor cleared"
			return e, nil

		case "ctrl+l":
			// Clear editor (alternative)
			e.textarea.Reset()
			e.status = "Editor cleared"
			return e, nil

		case "ctrl+up":
			// Navigate history backwards
			if len(e.history) > 0 && e.historyIdx > 0 {
				e.historyIdx--
				e.textarea.SetValue(e.history[e.historyIdx])
				e.status = "History: " + string(rune(e.historyIdx+1)) + "/" + string(rune(len(e.history)))
			}
			return e, nil

		case "ctrl+down":
			// Navigate history forwards
			if len(e.history) > 0 && e.historyIdx < len(e.history)-1 {
				e.historyIdx++
				e.textarea.SetValue(e.history[e.historyIdx])
				e.status = "History: " + string(rune(e.historyIdx+1)) + "/" + string(rune(len(e.history)))
			} else if e.historyIdx == len(e.history)-1 {
				e.historyIdx = len(e.history)
				e.textarea.Reset()
				e.status = "Ready"
			}
			return e, nil
		}

	case QueryCompletedMsg:
		// Query execution completed
		e.executing = false
		if msg.Error != nil {
			e.status = "Error: " + msg.Error.Error()
		} else {
			e.status = "Query executed successfully"
			// Navigate to results screen
			return e, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "results"}
			}
		}
		return e, nil

	case tea.WindowSizeMsg:
		// Resize textarea to fit window
		e.textarea.SetWidth(msg.Width - 4)
		e.textarea.SetHeight(msg.Height - 10)
	}

	// Update textarea
	e.textarea, cmd = e.textarea.Update(msg)
	cmds = append(cmds, cmd)

	return e, tea.Batch(cmds...)
}

// View renders the editor screen
func (e *EditorScreen) View() string {
	// Get theme from app
	palette := e.getStyles()

	var b strings.Builder

	// Header
	header := palette.Title.Render("SQL Editor")
	b.WriteString(header)
	b.WriteString("\n\n")

	// Editor with syntax highlighting
	editorContent := e.renderWithSyntaxHighlight(e.textarea.View())
	b.WriteString(editorContent)
	b.WriteString("\n\n")

	// Status bar
	statusStyle := palette.Subtle
	if e.executing {
		statusStyle = palette.Highlight
	} else if strings.HasPrefix(e.status, "Error") {
		statusStyle = palette.Error
	}

	statusBar := statusStyle.Render("Status: " + e.status)
	b.WriteString(statusBar)
	b.WriteString("\n\n")

	// Help text
	helpText := palette.Help.Render(
		"F5/Ctrl+Enter: Execute | Ctrl+K: Clear | Ctrl+↑/↓: History | Esc: Back to Menu",
	)
	b.WriteString(helpText)

	return b.String()
}

// renderWithSyntaxHighlight applies basic SQL syntax highlighting
func (e *EditorScreen) renderWithSyntaxHighlight(text string) string {
	// SQL keywords to highlight
	keywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
		"CREATE", "DROP", "ALTER", "TABLE", "DATABASE", "INDEX",
		"JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
		"GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
		"AND", "OR", "NOT", "IN", "EXISTS", "LIKE",
		"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
		"PRIMARY KEY", "FOREIGN KEY", "REFERENCES",
		"INT", "INTEGER", "TEXT", "VARCHAR", "BOOLEAN", "TIMESTAMP",
		"NULL", "NOT NULL", "DEFAULT", "UNIQUE", "CHECK",
	}

	keywordStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#569CD6")).Bold(true)

	result := text
	lines := strings.Split(result, "\n")

	for i, line := range lines {
		for _, keyword := range keywords {
			// Case-insensitive replacement
			upperLine := strings.ToUpper(line)
			if strings.Contains(upperLine, keyword) {
				// Find and highlight the keyword
				words := strings.Fields(line)
				for j, word := range words {
					if strings.ToUpper(strings.Trim(word, "(),;")) == keyword {
						words[j] = keywordStyle.Render(word)
					}
				}
				lines[i] = strings.Join(words, " ")
			}
		}
	}

	return strings.Join(lines, "\n")
}

// getStyles retrieves the shared style palette from the app.
func (e *EditorScreen) getStyles() *types.StylePalette {
	if e.app == nil {
		return &types.StylePalette{}
	}
	return e.app.GetStyles()
}
