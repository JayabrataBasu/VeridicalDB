package screens

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/components"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
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
	width        int
	height       int
}

// NewEditorScreen creates a new SQL editor screen
func NewEditorScreen(app types.StyleProvider) *EditorScreen {
	SyncScreenIcons()
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
		width:        120,
		height:       40,
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
				e.status = fmt.Sprintf("History: %d/%d", e.historyIdx+1, len(e.history))
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
		e.width = msg.Width
		e.height = msg.Height
		// Resize textarea to fit window with proper padding
		editorWidth := int(float64(msg.Width)*0.56) - 8
		editorHeight := msg.Height - 13 // Account for header, status, help, and pane borders
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
	width := e.width
	height := e.height
	if width <= 0 {
		width = 120
	}
	if height <= 0 {
		height = 40
	}

	// Brand palette colors - theme-aware with fallbacks
	brandAccent := "#00D9FF"
	brandHighlight := "#FF006E"
	brandWarning := "#FFB86C"
	brandSuccess := "#55FF55"
	brandMuted := "#44475A"
	brandBg := "#0A0E27"
	if tp, ok := e.app.(interface{ GetThemeManager() *theme.Manager }); ok {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			brandAccent = t.BrandAccent
			brandHighlight = t.BrandHighlight
			brandWarning = t.BrandWarning
			brandSuccess = t.BrandSuccess
			brandMuted = t.BrandMuted
			brandBg = t.Background
		}
	}

	header := styles.FromHexBold(types.Icons.Query+"  SQL Editor", brandAccent)
	breadcrumb := styles.FromHex(types.Icons.Pointer+" Home › ", brandMuted) + styles.FromHexBold("Editor", brandAccent)

	leftWidth := max(22, int(float64(width)*0.20))
	rightWidth := max(28, int(float64(width)*0.24))
	centerWidth := width - leftWidth - rightWidth - 6
	if centerWidth < 46 {
		centerWidth = 46
		rightWidth = max(24, width-leftWidth-centerWidth-6)
	}
	if rightWidth < 24 {
		rightWidth = 24
	}

	bodyHeight := max(10, height-9)
	stacked := width < 120

	leftPane := lipgloss.NewStyle().
		Width(leftWidth).
		Height(bodyHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(brandMuted)).
		Background(lipgloss.Color(brandBg)).
		Padding(0, 1).
		Render(e.renderContextPane(leftWidth-2, bodyHeight))

	centerPane := lipgloss.NewStyle().
		Width(centerWidth).
		Height(bodyHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(brandAccent)).
		Background(lipgloss.Color(brandBg)).
		Padding(0, 1).
		Render(e.renderEditorPane(centerWidth-2, bodyHeight))

	rightPane := lipgloss.NewStyle().
		Width(rightWidth).
		Height(bodyHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(brandHighlight)).
		Background(lipgloss.Color(brandBg)).
		Padding(0, 1).
		Render(e.renderPreviewPane(rightWidth-2, bodyHeight, brandMuted))

	content := lipgloss.JoinHorizontal(lipgloss.Top, leftPane, centerPane, rightPane)
	if stacked {
		mainWidth := max(40, width-4)
		mainHeight := max(8, (bodyHeight*2)/3)
		previewHeight := max(6, bodyHeight-mainHeight)

		stackedMain := lipgloss.NewStyle().
			Width(mainWidth).
			Height(mainHeight).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(brandAccent)).
			Background(lipgloss.Color(brandBg)).
			Padding(0, 1).
			Render(e.renderEditorPane(mainWidth-2, mainHeight))

		stackedPreview := lipgloss.NewStyle().
			Width(mainWidth).
			Height(previewHeight).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(brandHighlight)).
			Background(lipgloss.Color(brandBg)).
			Padding(0, 1).
			Render(e.renderPreviewPane(mainWidth-2, previewHeight, brandMuted))

		content = lipgloss.JoinVertical(lipgloss.Left, stackedMain, stackedPreview)
	}

	var statusIcon string
	var statusColor string
	if e.executing {
		statusIcon = types.Icons.Running + " "
		statusColor = brandWarning
	} else if strings.HasPrefix(e.status, "Error") {
		statusIcon = types.Icons.Error + " "
		statusColor = brandHighlight
	} else if strings.Contains(strings.ToLower(e.status), "success") {
		statusIcon = types.Icons.Success + " "
		statusColor = brandSuccess
	} else {
		statusIcon = types.Icons.Info + " "
		statusColor = brandAccent
	}

	statusBar := styles.FromHexBold(statusIcon+e.status, statusColor)
	helpText := styles.FromHexBold("F5", brandAccent) + styles.FromHex(" Run  ", brandMuted) +
		styles.FromHexBold("Ctrl+D", brandAccent) + styles.FromHex(" Duplicate  ", brandMuted) +
		styles.FromHexBold("Ctrl+Space", brandAccent) + styles.FromHex(" Complete  ", brandMuted) +
		styles.FromHexBold("Esc", brandAccent) + styles.FromHex(" Back", brandMuted)

	return strings.Join([]string{
		header,
		breadcrumb,
		"",
		content,
		"",
		statusBar,
		helpText,
	}, "\n")
}

func (e *EditorScreen) renderContextPane(width, height int) string {
	lines := []string{
		styles.FromHexBold(types.Icons.Database+" Context", "#FFB86C"),
		"",
		fmt.Sprintf("Lines: %d", len(strings.Split(e.textarea.Value(), "\n"))),
		fmt.Sprintf("History: %d", len(e.history)),
		fmt.Sprintf("Mode: %s", strings.ToUpper(strings.TrimSpace(e.status))),
		"",
		styles.FromHexBold(types.Icons.Key+" Shortcuts", "#FFB86C"),
		"F1 help",
		"F5 execute",
		"Ctrl+Enter execute",
		"Ctrl+K clear",
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	content := strings.Join(lines, "\n")
	return lipgloss.NewStyle().Width(width).MaxHeight(height).Render(content)
}

func (e *EditorScreen) renderEditorPane(width, height int) string {
	title := styles.FromHexBold(types.Icons.Edit+" SQL Buffer", "#00D9FF")
	contentHeight := height - 2
	if contentHeight < 3 {
		contentHeight = 3
	}
	view := lipgloss.NewStyle().Width(width).MaxHeight(contentHeight).Render(e.textarea.View())
	return lipgloss.JoinVertical(lipgloss.Left, title, view)
}

func (e *EditorScreen) renderPreviewPane(width, height int, muted string) string {
	title := styles.FromHexBold(types.Icons.Query+" Syntax Preview", "#FF006E")
	previewHeight := height - 2
	if previewHeight < 3 {
		previewHeight = 3
	}
	h := components.NewHighlightedTextView(e.textarea.Value(), e.highlighter)
	h.SetDimensions(width, previewHeight)
	h.SetLineNumbers(true)
	highlighted := h.Render()

	if e.autocomplete.IsVisible() {
		suggestions := e.autocomplete.RenderSuggestions()
		if suggestions != "" {
			highlighted += "\n\n" + styles.FromHex(suggestions, muted)
		}
	}

	view := lipgloss.NewStyle().Width(width).MaxHeight(previewHeight).Render(highlighted)
	return lipgloss.JoinVertical(lipgloss.Left, title, view)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// isWordChar checks if a character can be part of a word for autocomplete, how nice, the entire modern keyboard
func isWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}
