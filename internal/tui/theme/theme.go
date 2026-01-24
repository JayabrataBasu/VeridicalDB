// Package theme provides an enhanced theming system for the TUI.
package theme

import (
	"github.com/charmbracelet/lipgloss"
)

// Theme defines comprehensive styling for the TUI application.
type Theme struct {
	Name        string
	Description string

	// Base colors
	Foreground string
	Background string

	// Semantic colors
	Primary   string
	Secondary string
	Accent    string
	Muted     string

	// Status colors
	Success string
	Warning string
	Error   string
	Info    string

	// UI Component colors
	Border         string
	BorderFocused  string
	Selection      string
	Highlight      string
	LineNumber     string
	Cursor         string
	CurrentLine    string
	Comment        string

	// Syntax highlighting colors
	Keyword   string
	String    string
	Number    string
	Function  string
	Operator  string
	Variable  string
	Type      string
	Constant  string

	// Table colors
	TableHeader    string
	TableBorder    string
	TableRowEven   string
	TableRowOdd    string
	TableSelected  string
	TableHighlight string
}

// Manager handles theme switching and management.
type Manager struct {
	current *Theme
	themes  map[string]*Theme
}

// NewManager creates a new theme manager with built-in themes.
func NewManager() *Manager {
	m := &Manager{
		themes: make(map[string]*Theme),
	}

	// Register built-in themes
	m.RegisterTheme(darkTheme())
	m.RegisterTheme(lightTheme())
	m.RegisterTheme(draculaTheme())
	m.RegisterTheme(monokaiTheme())
	m.RegisterTheme(solarizedDarkTheme())
	m.RegisterTheme(solarizedLightTheme())
	m.RegisterTheme(nordTheme())
	m.RegisterTheme(tokyoNightTheme())

	// Set default theme
	m.current = m.themes["dark"]

	return m
}

// RegisterTheme adds a custom theme to the manager.
func (m *Manager) RegisterTheme(theme *Theme) {
	m.themes[theme.Name] = theme
}

// SetTheme switches to the specified theme by name.
func (m *Manager) SetTheme(name string) bool {
	if theme, ok := m.themes[name]; ok {
		m.current = theme
		return true
	}
	return false
}

// Current returns the currently active theme.
func (m *Manager) Current() *Theme {
	return m.current
}

// ListThemes returns all available theme names.
func (m *Manager) ListThemes() []string {
	names := make([]string, 0, len(m.themes))
	for name := range m.themes {
		names = append(names, name)
	}
	return names
}

// GetTheme returns a specific theme by name.
func (m *Manager) GetTheme(name string) (*Theme, bool) {
	theme, ok := m.themes[name]
	return theme, ok
}

// Styles returns a collection of lipgloss styles based on the current theme.
func (t *Theme) Styles() *StyleSet {
	return &StyleSet{
		// Base styles
		Base: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Foreground)).
			Background(lipgloss.Color(t.Background)),

		// Semantic styles
		Primary: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Primary)).
			Bold(true),

		Secondary: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Secondary)),

		Accent: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Accent)).
			Bold(true),

		Muted: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Muted)),

		// Status styles
		Success: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Success)).
			Bold(true),

		Warning: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Warning)).
			Bold(true),

		Error: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Error)).
			Bold(true),

		Info: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Info)),

		// UI Component styles
		Border: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.Border)),

		BorderFocused: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BorderFocused)),

		Selection: lipgloss.NewStyle().
			Background(lipgloss.Color(t.Selection)).
			Foreground(lipgloss.Color(t.Foreground)),

		Highlight: lipgloss.NewStyle().
			Background(lipgloss.Color(t.Highlight)).
			Foreground(lipgloss.Color(t.Background)),

		// Table styles
		TableHeader: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.TableHeader)).
			Bold(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderBottom(true).
			BorderForeground(lipgloss.Color(t.TableBorder)),

		TableRowEven: lipgloss.NewStyle().
			Background(lipgloss.Color(t.TableRowEven)),

		TableRowOdd: lipgloss.NewStyle().
			Background(lipgloss.Color(t.TableRowOdd)),

		TableSelected: lipgloss.NewStyle().
			Background(lipgloss.Color(t.TableSelected)).
			Foreground(lipgloss.Color(t.Foreground)).
			Bold(true),

		// Status bar
		StatusBar: lipgloss.NewStyle().
			Background(lipgloss.Color(t.Border)).
			Foreground(lipgloss.Color(t.Primary)).
			Padding(0, 1),

		// Title styles
		Title: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Primary)).
			Bold(true).
			MarginBottom(1),

		Subtitle: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Secondary)).
			Italic(true),
	}
}

// StyleSet contains pre-configured lipgloss styles.
type StyleSet struct {
	Base          lipgloss.Style
	Primary       lipgloss.Style
	Secondary     lipgloss.Style
	Accent        lipgloss.Style
	Muted         lipgloss.Style
	Success       lipgloss.Style
	Warning       lipgloss.Style
	Error         lipgloss.Style
	Info          lipgloss.Style
	Border        lipgloss.Style
	BorderFocused lipgloss.Style
	Selection     lipgloss.Style
	Highlight     lipgloss.Style
	TableHeader   lipgloss.Style
	TableRowEven  lipgloss.Style
	TableRowOdd   lipgloss.Style
	TableSelected lipgloss.Style
	StatusBar     lipgloss.Style
	Title         lipgloss.Style
	Subtitle      lipgloss.Style
}
