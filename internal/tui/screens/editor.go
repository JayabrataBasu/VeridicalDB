package screens

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/components"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/syntax"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// just a few of the available ones
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

// initializeAutocomplete creates and configures the autocomplete manager with custom keywords, does not seem very functional
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
		// Resize textarea to fit window with proper padding
		editorWidth := msg.Width - 8    // Account for padding and borders
		editorHeight := msg.Height - 12 // Account for header, status, help
		if editorWidth < 40 {           //adjusts minimum size same for 8
			editorWidth = 40
		}
		if editorHeight < 8 {
			editorHeight = 8
		}
		e.textarea.SetWidth(editorWidth)
		e.textarea.SetHeight(editorHeight)
	}

	// Update textarea
	e.textarea, cmd = e.textarea.Update(msg)
	cmds = append(cmds, cmd)

	return e, tea.Batch(cmds...)
}

// View renders the editor screen with premium styling and proper spacing
func (e *EditorScreen) View() string {
	var b strings.Builder

	// Brand palette colors - bold tech aesthetic (theme-aware with fallbacks). looks nice, don't ask me i don't have opinnions
	brandAccent := "#00D9FF"    // Neon Cyan
	brandHighlight := "#FF006E" // Neon Magenta
	brandWarning := "#FFB86C"   // Accent Orange
	brandSuccess := "#55FF55"   // Bright Green
	brandMuted := "#44475A"     // Steel Gray
	brandDark := "#0A0E27"      // Dark Charcoal
	brandPurple := "#BD00FF"    // Neon Purple
	if tp, ok := e.app.(interface{ GetThemeManager() *theme.Manager }); ok {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			brandAccent = t.BrandAccent
			brandHighlight = t.BrandHighlight
			brandWarning = t.BrandWarning
			brandSuccess = t.BrandSuccess
			brandMuted = t.BrandMuted
			brandDark = t.Background
			brandPurple = t.BrandGradientB
		}
	}

	// Premium header with Nerd Font icon and brand accent
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(brandAccent)).
		Padding(0, 2).
		MarginBottom(1)

	header := headerStyle.Render(NerdIcons.Query + "  SQL Editor")
	b.WriteString(header)
	b.WriteString("\n")

	// Breadcrumb navigation with brand muted styling
	bcStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(brandMuted))
	bcActiveStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(brandAccent)).Bold(true)
	b.WriteString(bcStyle.Render(NerdIcons.Home+" Home › ") + bcActiveStyle.Render("Editor"))
	b.WriteString("\n\n")

	// Editor container with rounded border and brand focus color
	editorBorderColor := brandAccent // Brand accent
	if e.executing {
		editorBorderColor = brandWarning // Brand warning when executing
	}

	editorStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(editorBorderColor)).
		Padding(1, 2).
		MarginBottom(1)

	// Render the textarea with syntax highlighting
	editorContent := editorStyle.Render(e.textarea.View())
	b.WriteString(editorContent)
	b.WriteString("\n")

	// Status bar with Nerd Font icon
	var statusIcon string
	var statusStyle lipgloss.Style

	if e.executing {
		statusIcon = NerdIcons.Pending + " "
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(brandWarning)).
			Bold(true)
	} else if strings.HasPrefix(e.status, "Error") {
		statusIcon = NerdIcons.Error + " "
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(brandHighlight)).
			Bold(true)
	} else if strings.Contains(e.status, "success") {
		statusIcon = NerdIcons.Success + " "
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(brandSuccess)).
			Bold(true)
	} else {
		statusIcon = NerdIcons.Running + " "
		statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(brandAccent))
	}

	statusBar := statusStyle.Render(statusIcon + e.status)
	b.WriteString(statusBar)
	b.WriteString("\n\n")

	// Autocomplete popup if visible
	if e.autocomplete.IsVisible() {
		autocompleteView := e.autocomplete.RenderSuggestions()
		if autocompleteView != "" {
			popupStyle := lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color(brandPurple)).
				Background(lipgloss.Color(brandDark)).
				Padding(0, 1).
				MarginLeft(2)
			b.WriteString(popupStyle.Render(autocompleteView))
			b.WriteString("\n")
		}
	}

	// Help bar with keyboard shortcuts - brand muted styling
	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(brandMuted)).
		MarginTop(1).
		Padding(0, 1)

	keyStyle := lipgloss.NewStyle().
		Background(lipgloss.Color(brandDark)).
		Foreground(lipgloss.Color(brandAccent)).
		Bold(true).
		Padding(0, 1)

	helpText := helpStyle.Render(
		keyStyle.Render("F5") + " Execute  " +
			keyStyle.Render("Ctrl+D") + " Duplicate  " +
			keyStyle.Render("Ctrl+Space") + " Autocomplete  " +
			keyStyle.Render("Esc") + " Back",
	)
	b.WriteString(helpText)

	return b.String()
}

// isWordChar checks if a character can be part of a word for autocomplete, how nice, the entire modern keyboard
func isWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
