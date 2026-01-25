package screens

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/components"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/syntax"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var keywordSuggestions = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE",
	"CREATE", "CREATE TABLE", "CREATE INDEX",
	"DROP", "DROP TABLE", "DROP INDEX",
	"BEGIN", "COMMIT", "ROLLBACK",
	"HELP", "EXIT", "QUIT",
	"\\dt", "\\di", "\\d", "\\status", "\\config", "\\clear", "\\help", "\\q",
}

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

	// Autocomplete
	autocomplete *components.AutocompleteManager
	showHelp     bool
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
		app:          app,
		textarea:     ta,
		status:       "Ready",
		history:      make([]string, 0),
		historyIdx:   -1,
		highlighter:  syntax.NewHighlighter(),
		lineOps:      syntax.NewLineOperations(""),
		autocomplete: initializeAutocomplete(),
		showHelp:     false,
	}
}

// initializeAutocomplete creates and configures the autocomplete manager with custom keywords
func initializeAutocomplete() *components.AutocompleteManager {
	am := components.NewAutocompleteManager()

	// Add editor-specific keyword suggestions to enhance autocomplete
	for _, keyword := range keywordSuggestions {
		am.AddKeyword(keyword)
	}

	return am
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
		// Handle autocomplete when visible
		if e.autocomplete.IsVisible() {
			switch msg.String() {
			case "up":
				e.autocomplete.SelectPrev()
				return e, nil
			case "down":
				e.autocomplete.SelectNext()
				return e, nil
			case "enter", "tab":
				selected := e.autocomplete.GetSelected()
				if selected != "" {
					// Insert selected keyword into editor
					text := e.textarea.Value()
					row := e.textarea.Line()
					col := e.textarea.LineInfo().ColumnOffset
					lines := strings.Split(text, "\n")

					// Find the word being completed
					line := lines[row]
					wordStart := col
					for wordStart > 0 && (isWordChar(rune(line[wordStart-1])) || line[wordStart-1] == '_') {
						wordStart--
					}

					// Replace word with suggestion
					before := line[:wordStart]
					after := line[col:]
					lines[row] = before + selected + after

					e.textarea.SetValue(strings.Join(lines, "\n"))
					e.textarea.SetCursor(len(before) + len(selected))
					e.autocomplete.Hide()
				}
				return e, nil
			case "esc":
				e.autocomplete.Hide()
				return e, nil
			}
		}

		// Global key handlers
		switch msg.String() {
		case "f1":
			e.showHelp = !e.showHelp
			return e, nil

		case "ctrl+space":
			// Trigger autocomplete suggestions based on current text
			text := e.textarea.Value()
			row := e.textarea.Line()
			col := e.textarea.LineInfo().ColumnOffset
			lines := strings.Split(text, "\n")
			if row >= 0 && row < len(lines) {
				line := lines[row]
				// Extract word being typed
				wordStart := col
				for wordStart > 0 && (isWordChar(rune(line[wordStart-1])) || line[wordStart-1] == '_') {
					wordStart--
				}
				prefix := line[wordStart:col]
				if prefix != "" {
					e.autocomplete.GetSuggestions(prefix)
					e.autocomplete.Show()
				}
			}
			return e, nil

		case "up":
			if e.autocomplete.IsVisible() {
				e.autocomplete.SelectPrev()
				return e, nil
			}

		case "down":
			if e.autocomplete.IsVisible() {
				e.autocomplete.SelectNext()
				return e, nil
			}

		case "ctrl+c":
			// Return to home screen
			return e, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "home"}
			}

		case "esc":
			if e.autocomplete.IsVisible() {
				e.autocomplete.Hide()
				return e, nil
			}
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

// View renders the editor screen with premium styling
func (e *EditorScreen) View() string {
	var b strings.Builder

	// Premium header with icon and rounded border
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 2).
		MarginBottom(1)
	
	header := headerStyle.Render("🗂  SQL Editor")
	b.WriteString(header)
	b.WriteString("\n")

	// Editor container with border - shows textarea content
	editorStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#00D9FF")).
		Padding(1).
		MarginBottom(1)
	
	// Get raw text for syntax highlighting, then render the textarea
	editorContent := editorStyle.Render(e.textarea.View())
	b.WriteString(editorContent)
	b.WriteString("\n")

	// Status bar with icon
	var statusIcon string
	var statusStyle lipgloss.Style
	
	if e.executing {
		statusIcon = "⏳"
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFB86C")).
			Bold(true)
	} else if strings.HasPrefix(e.status, "Error") {
		statusIcon = "✗"
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF5555")).
			Bold(true)
	} else if strings.Contains(e.status, "success") {
		statusIcon = "✓"
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#50FA7B")).
			Bold(true)
	} else {
		statusIcon = "●"
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00D9FF"))
	}

	statusBar := statusStyle.Render(statusIcon + " " + e.status)
	b.WriteString(statusBar)
	b.WriteString("\n\n")

	// Autocomplete popup if visible
	if e.autocomplete.IsVisible() {
		autocompleteView := e.autocomplete.RenderSuggestions()
		if autocompleteView != "" {
			popupStyle := lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#FFB86C")).
				Padding(0, 1).
				MarginLeft(2)
			b.WriteString(popupStyle.Render(autocompleteView))
			b.WriteString("\n")
		}
	}

	// Help bar with keyboard shortcuts
	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6272A4")).
		MarginTop(1)
	
	helpText := helpStyle.Render(
		"⌨  F5/Ctrl+Enter: Execute │ Ctrl+D: Duplicate │ Ctrl+Space: Autocomplete │ Esc: Back",
	)
	b.WriteString(helpText)

	return b.String()
}

// isWordChar checks if a character can be part of a word for autocomplete
func isWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
