// Package screens provides TUI screens for VeridicalDB.
package screens

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/charmbracelet/lipgloss"
)

// ScreenStyles provides consistent styling utilities for all screens.
type ScreenStyles struct {
	theme *theme.Theme
	width int

	// Cached styles
	header        lipgloss.Style
	subHeader     lipgloss.Style
	container     lipgloss.Style
	focusedBorder lipgloss.Style
	statusSuccess lipgloss.Style
	statusError   lipgloss.Style
	statusWarning lipgloss.Style
	statusInfo    lipgloss.Style
	statusPending lipgloss.Style
	helpBar       lipgloss.Style
	breadcrumb    lipgloss.Style
	card          lipgloss.Style
	cardHeader    lipgloss.Style
	badge         lipgloss.Style
	tag           lipgloss.Style
}

// NewScreenStyles creates a new ScreenStyles with the given theme.
func NewScreenStyles(t *theme.Theme) *ScreenStyles {
	s := &ScreenStyles{theme: t, width: 80}
	s.updateStyles()
	return s
}

// SetTheme updates the theme and rebuilds styles.
func (s *ScreenStyles) SetTheme(t *theme.Theme) {
	s.theme = t
	s.updateStyles()
}

// SetWidth sets the screen width for responsive layout.
func (s *ScreenStyles) SetWidth(width int) {
	s.width = width
}

// updateStyles rebuilds all style caches.
func (s *ScreenStyles) updateStyles() {
	t := s.theme
	if t == nil {
		return
	}

	// Header with brand accent and bold tech aesthetic
	s.header = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.BrandAccent)).
		Background(lipgloss.Color(t.Background)).
		Padding(0, 2).
		MarginBottom(1)

	// Subheader for secondary titles
	s.subHeader = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandHighlight)).
		MarginBottom(1)

	// Container with rounded border and brand focus color
	s.container = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(t.BrandFocus)).
		Padding(1, 2).
		MarginBottom(1)

	// Focused container with accent border
	s.focusedBorder = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(t.BrandAccent)).
		Padding(1, 2).
		MarginBottom(1)

	// Status styles
	s.statusSuccess = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandSuccess)).
		Bold(true)

	s.statusError = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandDanger)).
		Bold(true)

	s.statusWarning = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandWarning)).
		Bold(true)

	s.statusInfo = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandHighlight))

	s.statusPending = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandWarning)).
		Bold(true)

	// Help bar with muted styling
	s.helpBar = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandMuted)).
		MarginTop(1).
		Padding(0, 1)

	// Breadcrumb styling
	s.breadcrumb = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandMuted))

	// Card with subtle border
	s.card = lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(t.Border)).
		Padding(1, 2)

	// Card header
	s.cardHeader = lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.BrandAccent)).
		MarginBottom(1)

	// Badge style for labels
	s.badge = lipgloss.NewStyle().
		Background(lipgloss.Color(t.BrandSelection)).
		Foreground(lipgloss.Color(t.BrandAccent)).
		Padding(0, 1)

	// Tag style
	s.tag = lipgloss.NewStyle().
		Background(lipgloss.Color(t.BrandMuted)).
		Foreground(lipgloss.Color(t.Foreground)).
		Padding(0, 1)
}

// Header returns the header style.
func (s *ScreenStyles) Header() lipgloss.Style { return s.header }

// SubHeader returns the subheader style.
func (s *ScreenStyles) SubHeader() lipgloss.Style { return s.subHeader }

// Container returns the container style.
func (s *ScreenStyles) Container() lipgloss.Style { return s.container }

// FocusedContainer returns the focused container style.
func (s *ScreenStyles) FocusedContainer() lipgloss.Style { return s.focusedBorder }

// StatusSuccess returns the success status style.
func (s *ScreenStyles) StatusSuccess() lipgloss.Style { return s.statusSuccess }

// StatusError returns the error status style.
func (s *ScreenStyles) StatusError() lipgloss.Style { return s.statusError }

// StatusWarning returns the warning status style.
func (s *ScreenStyles) StatusWarning() lipgloss.Style { return s.statusWarning }

// StatusInfo returns the info status style.
func (s *ScreenStyles) StatusInfo() lipgloss.Style { return s.statusInfo }

// StatusPending returns the pending status style.
func (s *ScreenStyles) StatusPending() lipgloss.Style { return s.statusPending }

// HelpBar returns the help bar style.
func (s *ScreenStyles) HelpBar() lipgloss.Style { return s.helpBar }

// Breadcrumb returns the breadcrumb style.
func (s *ScreenStyles) Breadcrumb() lipgloss.Style { return s.breadcrumb }

// Card returns the card style.
func (s *ScreenStyles) Card() lipgloss.Style { return s.card }

// CardHeader returns the card header style.
func (s *ScreenStyles) CardHeader() lipgloss.Style { return s.cardHeader }

// Badge returns the badge style.
func (s *ScreenStyles) Badge() lipgloss.Style { return s.badge }

// Tag returns the tag style.
func (s *ScreenStyles) Tag() lipgloss.Style { return s.tag }

// Theme returns the current theme.
func (s *ScreenStyles) Theme() *theme.Theme { return s.theme }

// ============================================================================
// Nerd Font Icons for Screens
// ============================================================================

// NerdIcons provides Nerd Font icons for screen elements.
var NerdIcons = struct {
	// Navigation
	Home    string
	Back    string
	Forward string
	Up      string
	Down    string

	// Status
	Success string
	Error   string
	Warning string
	Info    string
	Pending string
	Running string

	// Database
	Database string
	Table    string
	Index    string
	Column   string
	Key      string

	// Actions
	Query   string
	Execute string
	Save    string
	Delete  string
	Edit    string
	Copy    string
	Refresh string

	// UI
	Folder   string
	File     string
	Settings string
	User     string
	Lock     string
	Unlock   string
	Calendar string
	Clock    string

	// Metrics
	CPU     string
	Memory  string
	Disk    string
	Network string
}{
	// Navigation
	Home:    "",
	Back:    "",
	Forward: "",
	Up:      "",
	Down:    "",

	// Status
	Success: "",
	Error:   "",
	Warning: "",
	Info:    "",
	Pending: "",
	Running: "",

	// Database
	Database: "",
	Table:    "",
	Index:    "",
	Column:   "",
	Key:      "",

	// Actions
	Query:   "",
	Execute: "",
	Save:    "",
	Delete:  "",
	Edit:    "",
	Copy:    "",
	Refresh: "",

	// UI
	Folder:   "",
	File:     "",
	Settings: "",
	User:     "",
	Lock:     "",
	Unlock:   "",
	Calendar: "",
	Clock:    "",

	// Metrics
	CPU:     "",
	Memory:  "",
	Disk:    "",
	Network: "",
}

// ============================================================================
// Utility Functions
// ============================================================================

// RenderBreadcrumb renders a breadcrumb navigation path.
func RenderBreadcrumb(styles *ScreenStyles, items ...string) string {
	if len(items) == 0 {
		return ""
	}

	t := styles.Theme()
	if t == nil {
		return ""
	}

	separator := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandMuted)).
		Render(" › ")

	activeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandAccent)).
		Bold(true)

	inactiveStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandMuted))

	var result string
	for i, item := range items {
		if i == len(items)-1 {
			result += activeStyle.Render(item)
		} else {
			result += inactiveStyle.Render(item)
			result += separator
		}
	}

	return result
}

// RenderKeyHint renders a keyboard shortcut hint.
func RenderKeyHint(styles *ScreenStyles, key, description string) string {
	t := styles.Theme()
	if t == nil {
		return key + " " + description
	}

	keyStyle := lipgloss.NewStyle().
		Background(lipgloss.Color(t.BrandSelection)).
		Foreground(lipgloss.Color(t.BrandAccent)).
		Bold(true).
		Padding(0, 1)

	descStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.BrandMuted))

	return keyStyle.Render(key) + " " + descStyle.Render(description)
}

// RenderStatusLine renders a status line with icon.
func RenderStatusLine(styles *ScreenStyles, status string, statusType string) string {
	var icon string
	var style lipgloss.Style

	switch statusType {
	case "success":
		icon = NerdIcons.Success
		style = styles.StatusSuccess()
	case "error":
		icon = NerdIcons.Error
		style = styles.StatusError()
	case "warning":
		icon = NerdIcons.Warning
		style = styles.StatusWarning()
	case "pending":
		icon = NerdIcons.Pending
		style = styles.StatusPending()
	default:
		icon = NerdIcons.Info
		style = styles.StatusInfo()
	}

	return style.Render(icon + " " + status)
}
