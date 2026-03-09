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

	// Brand colors - Bold tech aesthetic with vibrant accents
	// These provide consistent styling across all themes while allowing
	// each theme to customize values for cohesion with its base palette.
	BrandAccent    string // Primary vibrant accent (neon cyan family)
	BrandHighlight string // Secondary vibrant accent (neon magenta family)
	BrandSelection string // Strong contrast selection background
	BrandFocus     string // Focus indicator border color
	BrandSuccess   string // Vibrant success indicator
	BrandWarning   string // Vibrant warning indicator
	BrandDanger    string // Vibrant danger/error indicator
	BrandMuted     string // Muted background for subtle elements
	BrandGlow      string // Glow effect color for special emphasis
	BrandGradientA string // Gradient start color
	BrandGradientB string // Gradient end color

	// Brand palette reference for accessing full palette utilities
	brandPalette *BrandPalette
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

		// Brand styles - Bold tech aesthetic
		BrandAccent: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandAccent)).
			Bold(true),

		BrandHighlight: lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandHighlight)).
			Foreground(lipgloss.Color("#FFFFFF")).
			Bold(true).
			Padding(0, 1),

		BrandFocus: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BrandFocus)).
			Padding(0, 1),

		BrandSelection: lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandSelection)).
			Foreground(lipgloss.Color(t.BrandAccent)).
			Bold(true),

		BrandSuccess: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandSuccess)).
			Bold(true),

		BrandWarning: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandWarning)).
			Bold(true),

		BrandDanger: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandDanger)).
			Bold(true),

		BrandMuted: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandMuted)),

		BrandGlow: lipgloss.NewStyle().
			BorderStyle(lipgloss.DoubleBorder()).
			BorderForeground(lipgloss.Color(t.BrandGlow)).
			Padding(1, 2),

		BrandCard: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BrandAccent)).
			Padding(1, 2).
			MarginBottom(1),

		BrandActiveRow: lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandSelection)).
			Foreground(lipgloss.Color(t.BrandAccent)).
			Bold(true).
			Padding(0, 1),

		BrandInactiveRow: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Foreground)).
			Padding(0, 1),

		BrandBadge: lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandAccent)).
			Foreground(lipgloss.Color(t.Background)).
			Bold(true).
			Padding(0, 1),

		BrandTag: lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandMuted)).
			Foreground(lipgloss.Color(t.Foreground)).
			Padding(0, 1),

		BrandPanel: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BrandAccent)).
			Background(lipgloss.Color(t.Background)).
			Padding(0, 1),

		BrandPanelAlt: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BrandMuted)).
			Background(lipgloss.Color(t.Background)).
			Padding(0, 1),

		BrandKeycap: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandAccent)).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color(t.BrandFocus)).
			Padding(0, 1),

		BrandSubtleBorder: lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(lipgloss.Color(t.BrandMuted)),

		// Gradient colors
		GradientStart: t.BrandGradientA,
		GradientEnd:   t.BrandGradientB,
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

	// Brand styles - Bold tech aesthetic
	BrandAccent       lipgloss.Style // Primary vibrant accent text
	BrandHighlight    lipgloss.Style // Secondary vibrant accent text
	BrandFocus        lipgloss.Style // Focus border style with glow effect
	BrandSelection    lipgloss.Style // Strong contrast selection
	BrandSuccess      lipgloss.Style // Vibrant success indicator
	BrandWarning      lipgloss.Style // Vibrant warning indicator
	BrandDanger       lipgloss.Style // Vibrant danger indicator
	BrandMuted        lipgloss.Style // Muted/disabled style
	BrandGlow         lipgloss.Style // Glow border effect for emphasis
	BrandCard         lipgloss.Style // Card container with brand accent border
	BrandActiveRow    lipgloss.Style // Active row in tables/lists
	BrandInactiveRow  lipgloss.Style // Inactive row style
	BrandBadge        lipgloss.Style // Badge/pill style
	BrandTag          lipgloss.Style // Tag style for labels
	BrandPanel        lipgloss.Style // Primary panel container
	BrandPanelAlt     lipgloss.Style // Secondary panel container
	BrandKeycap       lipgloss.Style // Keyboard shortcut keycap
	BrandSubtleBorder lipgloss.Style // Subtle border for separators/blocks

	// Gradient colors for text effects
	GradientStart string
	GradientEnd   string
}

// BrandPalette returns the brand palette for this theme.
func (t *Theme) BrandPalette() *BrandPalette {
	if t.brandPalette != nil {
		return t.brandPalette
	}
	// Return default palette if not set
	return DefaultBrandPalette()
}

// SetBrandPalette sets the brand palette for this theme.
func (t *Theme) SetBrandPalette(palette *BrandPalette) {
	t.brandPalette = palette
}

// BrandStyles returns the BrandStyles helper for this theme.
func (t *Theme) BrandStyles() *BrandStyles {
	return NewBrandStyles(t.BrandPalette())
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
