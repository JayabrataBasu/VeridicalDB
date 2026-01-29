// Package theme provides an enhanced theming system for the TUI.
package theme

import (
	"fmt"
	"os"
	"strings"

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
	Border        string
	BorderFocused string
	Selection     string
	Highlight     string
	LineNumber    string
	Cursor        string
	CurrentLine   string
	Comment       string

	// Syntax highlighting colors
	Keyword  string
	String   string
	Number   string
	Function string
	Operator string
	Variable string
	Type     string
	Constant string

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
	m.RegisterTheme(cyberpunkTheme())

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

// DetectTrueColor checks if terminal supports 24-bit colors.
func DetectTrueColor() bool {
	colorterm := os.Getenv("COLORTERM")
	return strings.Contains(colorterm, "truecolor") || strings.Contains(colorterm, "24bit")
}

// GradientText creates a horizontal gradient text effect.
// For terminals without truecolor support, falls back to the start color.
func GradientText(text, startColor, endColor string) string {
	hasTrueColor := DetectTrueColor()

	// If not truecolor, use solid start color
	if !hasTrueColor {
		return lipgloss.NewStyle().Foreground(lipgloss.Color(startColor)).Render(text)
	}

	// For truecolor terminals, create character-by-character gradient
	if len(text) == 0 {
		return text
	}

	runes := []rune(text)
	length := len(runes)
	if length == 1 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color(startColor)).Render(text)
	}

	// Parse hex colors
	startR, startG, startB := hexToRGB(startColor)
	endR, endG, endB := hexToRGB(endColor)

	var result strings.Builder
	for i, r := range runes {
		// Calculate interpolation factor (0.0 to 1.0)
		t := float64(i) / float64(length-1)

		// Interpolate RGB values
		red := int(float64(startR) + t*float64(endR-startR))
		green := int(float64(startG) + t*float64(endG-startG))
		blue := int(float64(startB) + t*float64(endB-startB))

		// Create color string and render character
		color := fmt.Sprintf("#%02x%02x%02x", red, green, blue)
		result.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color(color)).Render(string(r)))
	}

	return result.String()
}

// hexToRGB converts hex color string to RGB values.
func hexToRGB(hex string) (int, int, int) {
	hex = strings.TrimPrefix(hex, "#")

	var r, g, b int
	if len(hex) == 6 {
		_, _ = fmt.Sscanf(hex, "%02x%02x%02x", &r, &g, &b)
	}

	return r, g, b
}
