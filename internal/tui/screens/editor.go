package screens

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/syntax"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
)

// EditorScreen is the primary SQL editor interface
type EditorScreen struct {
	app         types.StyleProvider
	textarea    textarea.Model
	status      string
	executing   bool
	history     []string // Query history
	historyIdx  int
	highlighter *syntax.Highlighter    // Syntax highlighter for full coverage
	lineOps     *syntax.LineOperations // Line operations handler
}

// NewEditorScreen creates a new SQL editor screen
func NewEditorScreen(app types.StyleProvider) *EditorScreen {
	ta := textarea.New()
	ta.Placeholder = "Enter SQL query here... (F5 or Ctrl+Enter to execute)"
	ta.Focus()
	ta.CharLimit = 0 // No limit
	ta.SetWidth(100)
	ta.SetHeight(15)
	ta.ShowLineNumbers = true
	ta.KeyMap.InsertNewline.SetEnabled(true)

	return &EditorScreen{
		app:         app,
		textarea:    ta,
		status:      "Ready",
		history:     make([]string, 0),
		historyIdx:  -1,
		highlighter: syntax.NewHighlighter(),
		lineOps:     syntax.NewLineOperations(""),
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

		// Both F5 and Ctrl+Enter execute queries (fixed execution modes - Task 9)
		case "f5":
			sql := strings.TrimSpace(e.textarea.Value())
			if sql != "" && !e.executing {
				e.executing = true
				e.status = "Executing query..."
				e.history = append(e.history, sql)
				e.historyIdx = len(e.history)
				return e, func() tea.Msg {
					return ExecuteQueryMsg{SQL: sql}
				}
			}

		case "ctrl+enter":
			// Alternative execution trigger (fixed - was not working properly)
			sql := strings.TrimSpace(e.textarea.Value())
			if sql != "" && !e.executing {
				e.executing = true
				e.status = "Executing query..."
				e.history = append(e.history, sql)
				e.historyIdx = len(e.history)
				return e, func() tea.Msg {
					return ExecuteQueryMsg{SQL: sql}
				}
			}

		// Line operations (Task 10)
		case "ctrl+shift+k":
			// Delete current line
			content := e.textarea.Value()
			e.lineOps.SetContent(content)
			e.lineOps.DeleteLine()
			e.textarea.SetValue(e.lineOps.GetContent())
			e.status = "Line deleted"
			return e, nil

		case "alt+shift+down":
			// Move line down
			content := e.textarea.Value()
			e.lineOps.SetContent(content)
			e.lineOps.MoveLine(1)
			e.textarea.SetValue(e.lineOps.GetContent())
			e.status = "Line moved down"
			return e, nil

		case "alt+shift+up":
			// Move line up
			content := e.textarea.Value()
			e.lineOps.SetContent(content)
			e.lineOps.MoveLine(-1)
			e.textarea.SetValue(e.lineOps.GetContent())
			e.status = "Line moved up"
			return e, nil

		case "ctrl+d":
			// Duplicate line
			content := e.textarea.Value()
			e.lineOps.SetContent(content)
			e.lineOps.DuplicateLine()
			e.textarea.SetValue(e.lineOps.GetContent())
			e.status = "Line duplicated"
			return e, nil

		case "tab":
			// Indent line
			content := e.textarea.Value()
			e.lineOps.SetContent(content)
			e.lineOps.IndentLine(4)
			e.textarea.SetValue(e.lineOps.GetContent())
			return e, nil

		case "shift+tab":
			// Dedent line
			content := e.textarea.Value()
			e.lineOps.SetContent(content)
			e.lineOps.DedentLine(4)
			e.textarea.SetValue(e.lineOps.GetContent())
			return e, nil

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

	// Help text with Sprint-2 keybindings
	helpText := palette.Help.Render(
		"F5/Ctrl+Enter: Execute | Ctrl+D: Duplicate Line | Ctrl+Shift+K: Delete Line | Alt+Shift+↑↓: Move Line | Tab/Shift+Tab: Indent | Esc: Back",
	)
	b.WriteString(helpText)

	return b.String()
}

// renderWithSyntaxHighlight applies full SQL syntax highlighting (Task 7)
func (e *EditorScreen) renderWithSyntaxHighlight(text string) string {
	// Use the comprehensive highlighter for full coverage
	return e.highlighter.Highlight(text)
}

// getStyles retrieves the shared style palette from the app.
func (e *EditorScreen) getStyles() *types.StylePalette {
	if e.app == nil {
		return &types.StylePalette{}
	}
	return e.app.GetStyles()
}
