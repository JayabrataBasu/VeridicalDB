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
	showPreview  bool // syntax-highlighted preview pane (Ctrl+P)
	width        int
	height       int
}

// NewEditorScreen creates a new SQL editor screen
func NewEditorScreen(app types.StyleProvider) *EditorScreen {
	SyncScreenIcons()
	ta := textarea.New()
	ta.Placeholder = "Type a SQL statement, then press Ctrl+Enter (or Ctrl+R) to run."
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
		// Toggle help with F1 or ?
		case "f1", "?":
			e.showHelp = !e.showHelp
			return e, nil

		case "ctrl+p":
			e.showPreview = !e.showPreview
			if e.showPreview {
				e.status = "Syntax preview on"
			} else {
				e.status = "Syntax preview off"
			}
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

		// Execute query with Ctrl+R, Ctrl+Enter, or Ctrl+M (some terminals send Ctrl+M for Ctrl+Enter)
		case "ctrl+r", "ctrl+enter", "ctrl+m":
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
			return e, nil

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
		e.resizeTextarea()
	}

	// Update textarea
	e.textarea, cmd = e.textarea.Update(msg)
	cmds = append(cmds, cmd)

	return e, tea.Batch(cmds...)
}

// editorColors resolves the theme-aware palette for the editor.
func (e *EditorScreen) editorColors() (accent, warn, ok, bad, muted, border string) {
	accent, warn, ok, bad, muted, border = "#00D9FF", "#FFB86C", "#55FF55", "#FF5555", "#8A93A5", "#4A4E5A"
	if tp, is := e.app.(interface{ GetThemeManager() *theme.Manager }); is {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			accent, warn, ok, bad, muted, border = t.BrandAccent, t.BrandWarning, t.BrandSuccess, t.BrandDanger, t.Muted, t.Border
		}
	}
	return
}

func (e *EditorScreen) resizeTextarea() {
	w, h := e.width, e.height
	if w <= 0 {
		w = 120
	}
	if h <= 0 {
		h = 40
	}
	taW := w - 4 // frame padding
	taH := h - 4 // header + rule + footer
	if e.showPreview {
		taH = (taH * 3) / 5
	}
	if taW < 30 {
		taW = 30
	}
	if taH < 4 {
		taH = 4
	}
	e.textarea.SetWidth(taW)
	e.textarea.SetHeight(taH)
}

// View renders a full-width SQL editor with a single-line status footer.
func (e *EditorScreen) View() string {
	e.resizeTextarea()
	width := e.width
	if width <= 0 {
		width = 120
	}
	accent, warn, ok, bad, muted, border := e.editorColors()

	var b strings.Builder

	// Title row: name on the left, a clear run affordance on the right.
	run := styles.FromHexBold(" "+types.Icons.Execute+" Run ", ok) + styles.FromHex("Ctrl+Enter", muted)
	title := " " + styles.FromHexBold("SQL Editor", accent)
	gap := width - lipgloss.Width(title) - lipgloss.Width(run) - 1
	if gap < 1 {
		gap = 1
	}
	b.WriteString(title + strings.Repeat(" ", gap) + run + "\n")
	b.WriteString(styles.FromHex(strings.Repeat("─", width), border) + "\n")

	// Editor body.
	b.WriteString(indentLines(e.textarea.View(), 1))

	// Optional syntax preview.
	if e.showPreview {
		b.WriteString("\n" + styles.FromHex(strings.Repeat("╌", width), border) + "\n")
		b.WriteString(" " + styles.FromHexBold("Preview", accent) + "\n")
		prevH := e.height/3 - 1
		if prevH < 3 {
			prevH = 3
		}
		h := components.NewHighlightedTextView(e.textarea.Value(), e.highlighter)
		h.SetDimensions(width-2, prevH)
		h.SetLineNumbers(true)
		b.WriteString(indentLines(h.Render(), 1))
	}

	// Autocomplete popup.
	if e.autocomplete.IsVisible() {
		if s := e.autocomplete.RenderSuggestions(); s != "" {
			b.WriteString("\n" + indentLines(s, 1))
		}
	}

	// Footer: live status + key hints.
	b.WriteString("\n")
	statusColor := muted
	statusText := e.status
	switch {
	case e.executing:
		statusColor, statusText = warn, "running…"
	case strings.HasPrefix(e.status, "Error"):
		statusColor = bad
	case strings.Contains(strings.ToLower(e.status), "success"):
		statusColor = ok
	}
	lc := len(strings.Split(e.textarea.Value(), "\n"))
	meta := styles.FromHex(fmt.Sprintf("%d line", lc), muted)
	if lc != 1 {
		meta = styles.FromHex(fmt.Sprintf("%d lines", lc), muted)
	}
	if len(e.history) > 0 {
		meta += styles.FromHex(fmt.Sprintf("  ·  %d in history", len(e.history)), muted)
	}
	left := " " + styles.FromHex(statusText, statusColor) + "   " + meta
	hints := strings.Join([]string{
		styles.FromHexBold("^Enter", accent) + styles.FromHex(" run", muted),
		styles.FromHexBold("^P", accent) + styles.FromHex(" preview", muted),
		styles.FromHexBold("^K", accent) + styles.FromHex(" clear", muted),
		styles.FromHexBold("esc", accent) + styles.FromHex(" back", muted),
	}, styles.FromHex("  ", muted))
	fg := width - lipgloss.Width(left) - lipgloss.Width(hints) - 1
	if fg < 1 {
		fg = 1
	}
	b.WriteString(left + strings.Repeat(" ", fg) + hints)

	return b.String()
}

// indentLines prefixes each line of s with n spaces (ANSI-safe).
func indentLines(s string, n int) string {
	pad := strings.Repeat(" ", n)
	return pad + strings.ReplaceAll(s, "\n", "\n"+pad)
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
