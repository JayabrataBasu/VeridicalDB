package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/charmbracelet/lipgloss"
)

// Theme defines the color and styling scheme for the TUI.
type Theme struct {
	// Name of the theme
	Name string

	// Base colors
	ForegroundColor string
	BackgroundColor string

	// Component colors
	PrimaryColor   string
	SecondaryColor string
	AccentColor    string
	ErrorColor     string
	WarningColor   string
	SuccessColor   string

	// Styles
	PrimaryStyle   lipgloss.Style
	SecondaryStyle lipgloss.Style
	ErrorStyle     lipgloss.Style
	SuccessStyle   lipgloss.Style
	HeaderStyle    lipgloss.Style
	BorderStyle    lipgloss.Style
	StatusBarStyle lipgloss.Style
	EditorStyle    lipgloss.Style
}

// NewTheme creates a new theme with the given name.
func NewTheme(name string) *Theme {
	theme := &Theme{
		Name: name,
	}

	switch name {
	case "light":
		theme.setupLightTheme()
	case "dark":
		theme.setupDarkTheme()
	case "high-contrast":
		theme.setupHighContrastTheme()
	default:
		theme.setupDarkTheme()
	}

	return theme
}

func (t *Theme) setupDarkTheme() {
	t.ForegroundColor = "#FFFFFF"
	t.BackgroundColor = "#000000"
	t.PrimaryColor = "#00D9FF"
	t.SecondaryColor = "#00AA88"
	t.AccentColor = "#FFB86C"
	t.ErrorColor = "#FF5555"
	t.WarningColor = "#FFAA00"
	t.SuccessColor = "#55FF55"

	t.PrimaryStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.SecondaryStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SecondaryColor))

	t.ErrorStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.ErrorColor))

	t.SuccessStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SuccessColor))

	t.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.BorderStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SecondaryColor))

	t.StatusBarStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("#1A1A1A")).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.EditorStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("#0A0A0A")).
		Foreground(lipgloss.Color(t.ForegroundColor))
}

func (t *Theme) setupLightTheme() {
	t.ForegroundColor = "#000000"
	t.BackgroundColor = "#FFFFFF"
	t.PrimaryColor = "#0066CC"
	t.SecondaryColor = "#00AA88"
	t.AccentColor = "#FF6600"
	t.ErrorColor = "#CC0000"
	t.WarningColor = "#FF9900"
	t.SuccessColor = "#00AA00"

	t.PrimaryStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.SecondaryStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SecondaryColor))

	t.ErrorStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.ErrorColor))

	t.SuccessStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SuccessColor))

	t.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.BorderStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SecondaryColor))

	t.StatusBarStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("#F0F0F0")).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.EditorStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("#FFFFFF")).
		Foreground(lipgloss.Color(t.ForegroundColor))
}

func (t *Theme) setupHighContrastTheme() {
	t.ForegroundColor = "#FFFFFF"
	t.BackgroundColor = "#000000"
	t.PrimaryColor = "#FFFF00"
	t.SecondaryColor = "#00FFFF"
	t.AccentColor = "#00FF00"
	t.ErrorColor = "#FF0000"
	t.WarningColor = "#FFFF00"
	t.SuccessColor = "#00FF00"

	t.PrimaryStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.SecondaryStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.SecondaryColor))

	t.ErrorStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.ErrorColor))

	t.SuccessStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.SuccessColor))

	t.HeaderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.BorderStyle = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.SecondaryColor))

	t.StatusBarStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("#000000")).
		Foreground(lipgloss.Color(t.PrimaryColor))

	t.EditorStyle = lipgloss.NewStyle().
		Background(lipgloss.Color("#000000")).
		Foreground(lipgloss.Color(t.ForegroundColor))
}

// Palette converts the theme into a screen-friendly style palette.
func (t *Theme) Palette() *types.StylePalette {
	return &types.StylePalette{
		Title:         t.HeaderStyle,
		Subtle:        lipgloss.NewStyle().Foreground(lipgloss.Color(t.ForegroundColor)).Faint(true),
		Highlight:     t.PrimaryStyle,
		Error:         t.ErrorStyle,
		Help:          lipgloss.NewStyle().Foreground(lipgloss.Color(t.SecondaryColor)),
		SidebarHeader: lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(t.AccentColor)),
		SidebarSub:    lipgloss.NewStyle().Foreground(lipgloss.Color(t.SecondaryColor)).Faint(true),
		Footer:        lipgloss.NewStyle().Background(lipgloss.Color("#101010")).Foreground(lipgloss.Color(t.PrimaryColor)).Padding(0, 1),
	}
}
