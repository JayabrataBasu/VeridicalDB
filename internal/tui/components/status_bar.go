// Package components provides reusable TUI components with brand styling.
package components

import (
	"fmt"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/charmbracelet/lipgloss"
)

// StatusBarSection represents a section in the status bar.
type StatusBarSection struct {
	Text     string
	Icon     string // Nerd Font icon
	Style    StatusSectionStyle
	Priority int // Higher priority = shown first
}

// StatusSectionStyle defines the visual style for a section.
type StatusSectionStyle int

const (
	StatusStyleNormal StatusSectionStyle = iota
	StatusStyleAccent
	StatusStyleSuccess
	StatusStyleWarning
	StatusStyleDanger
	StatusStyleMuted
	StatusStyleInfo
)

// Nerd Font status icons.
const (
	IconDatabase   = "" // nf-fa-database
	IconConnection = "" // nf-cod-debug_disconnect / nf-md-lan_connect
	IconBranch     = "" // nf-oct-git_branch
	IconClock      = "" // nf-fa-clock_o
	IconMemory     = "" // nf-fa-microchip
	IconCPU        = "" // nf-oct-cpu
	IconCheck      = "" // nf-fa-check
	IconWarning    = "" // nf-fa-warning
	IconError      = "" // nf-fa-times_circle
	IconLoading    = "" // nf-fa-spinner
	IconLocked     = "" // nf-fa-lock
	IconUnlocked   = "" // nf-fa-unlock
	IconUser       = "" // nf-fa-user
	IconServer     = "" // nf-fa-server
	IconQuery      = "" // nf-fa-search
	IconEdit       = "" // nf-fa-pencil
	IconSave       = "" // nf-fa-save
	IconKey        = "" // nf-fa-key
)

// StatusBar provides a feature-rich status bar with brand styling.
type StatusBar struct {
	leftSections   []StatusBarSection
	centerSections []StatusBarSection
	rightSections  []StatusBarSection
	width          int
	theme          *theme.Theme

	// Dynamic content
	mode         string
	database     string
	connectionOK bool
	lastActivity time.Time
	queryCount   int
	errorCount   int

	// Style cache
	baseStyle      lipgloss.Style
	normalStyle    lipgloss.Style
	accentStyle    lipgloss.Style
	successStyle   lipgloss.Style
	warningStyle   lipgloss.Style
	dangerStyle    lipgloss.Style
	mutedStyle     lipgloss.Style
	infoStyle      lipgloss.Style
	separatorStyle lipgloss.Style
}

// NewStatusBar creates a new status bar.
func NewStatusBar(t *theme.Theme) *StatusBar {
	sb := &StatusBar{
		theme:          t,
		mode:           "NORMAL",
		database:       "default",
		connectionOK:   true,
		lastActivity:   time.Now(),
		leftSections:   make([]StatusBarSection, 0),
		centerSections: make([]StatusBarSection, 0),
		rightSections:  make([]StatusBarSection, 0),
	}
	sb.updateStyles()
	return sb
}

// updateStyles rebuilds the style cache from the theme.
func (sb *StatusBar) updateStyles() {
	t := sb.theme
	if t == nil {
		return
	}

	// Use BrandMuted as status bar background and Foreground for text
	sb.baseStyle = lipgloss.NewStyle().
		Background(lipgloss.Color(t.BrandMuted)).
		Foreground(lipgloss.Color(t.Foreground)).
		Padding(0, 1)

	sb.normalStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.Foreground))

	sb.accentStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.BrandAccent)).
		Bold(true)

	sb.successStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.BrandSuccess)).
		Bold(true)

	sb.warningStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.BrandWarning)).
		Bold(true)

	sb.dangerStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.BrandDanger)).
		Bold(true)

	sb.mutedStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.Muted))

	sb.infoStyle = sb.baseStyle.
		Foreground(lipgloss.Color(t.BrandHighlight))

	sb.separatorStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Muted)).
		Background(lipgloss.Color(t.BrandMuted))
}

// SetTheme updates the theme.
func (sb *StatusBar) SetTheme(t *theme.Theme) {
	sb.theme = t
	sb.updateStyles()
}

// SetWidth sets the status bar width.
func (sb *StatusBar) SetWidth(width int) {
	sb.width = width
}

// SetMode sets the current mode (e.g., NORMAL, INSERT, VISUAL).
func (sb *StatusBar) SetMode(mode string) {
	sb.mode = mode
}

// SetDatabase sets the current database name.
func (sb *StatusBar) SetDatabase(db string) {
	sb.database = db
}

// SetConnectionStatus sets the connection status.
func (sb *StatusBar) SetConnectionStatus(ok bool) {
	sb.connectionOK = ok
}

// SetQueryCount sets the query count.
func (sb *StatusBar) SetQueryCount(count int) {
	sb.queryCount = count
}

// SetErrorCount sets the error count.
func (sb *StatusBar) SetErrorCount(count int) {
	sb.errorCount = count
}

// UpdateActivity updates the last activity timestamp.
func (sb *StatusBar) UpdateActivity() {
	sb.lastActivity = time.Now()
}

// SetLeftSections sets custom left sections.
func (sb *StatusBar) SetLeftSections(sections []StatusBarSection) {
	sb.leftSections = sections
}

// SetCenterSections sets custom center sections.
func (sb *StatusBar) SetCenterSections(sections []StatusBarSection) {
	sb.centerSections = sections
}

// SetRightSections sets custom right sections.
func (sb *StatusBar) SetRightSections(sections []StatusBarSection) {
	sb.rightSections = sections
}

// getStyleForSection returns the appropriate style for a section.
func (sb *StatusBar) getStyleForSection(style StatusSectionStyle) lipgloss.Style {
	switch style {
	case StatusStyleAccent:
		return sb.accentStyle
	case StatusStyleSuccess:
		return sb.successStyle
	case StatusStyleWarning:
		return sb.warningStyle
	case StatusStyleDanger:
		return sb.dangerStyle
	case StatusStyleMuted:
		return sb.mutedStyle
	case StatusStyleInfo:
		return sb.infoStyle
	default:
		return sb.normalStyle
	}
}

// renderSection renders a single section.
func (sb *StatusBar) renderSection(section StatusBarSection) string {
	style := sb.getStyleForSection(section.Style)
	content := section.Text
	if section.Icon != "" {
		content = section.Icon + " " + content
	}
	return style.Render(content)
}

// renderSections renders multiple sections with separators.
func (sb *StatusBar) renderSections(sections []StatusBarSection) string {
	if len(sections) == 0 {
		return ""
	}

	var parts []string
	sep := sb.separatorStyle.Render(" │ ")

	for _, section := range sections {
		parts = append(parts, sb.renderSection(section))
	}

	return strings.Join(parts, sep)
}

// buildDefaultSections builds the default status bar content.
func (sb *StatusBar) buildDefaultSections() (left, center, right []StatusBarSection) {
	// Left: Mode + Database
	var modeStyle StatusSectionStyle
	switch sb.mode {
	case "INSERT":
		modeStyle = StatusStyleSuccess
	case "VISUAL":
		modeStyle = StatusStyleWarning
	default:
		modeStyle = StatusStyleAccent
	}

	left = []StatusBarSection{
		{Text: sb.mode, Style: modeStyle, Icon: "", Priority: 100},
		{Text: sb.database, Style: StatusStyleInfo, Icon: IconDatabase, Priority: 90},
	}

	// Center: Connection status
	connStyle := StatusStyleSuccess
	connText := "Connected"
	connIcon := IconCheck
	if !sb.connectionOK {
		connStyle = StatusStyleDanger
		connText = "Disconnected"
		connIcon = IconError
	}
	center = []StatusBarSection{
		{Text: connText, Style: connStyle, Icon: connIcon, Priority: 100},
	}

	// Right: Stats + Time
	right = []StatusBarSection{
		{Text: fmt.Sprintf("%d", sb.queryCount), Style: StatusStyleMuted, Icon: IconQuery, Priority: 80},
	}

	if sb.errorCount > 0 {
		right = append(right, StatusBarSection{
			Text:     fmt.Sprintf("%d", sb.errorCount),
			Style:    StatusStyleDanger,
			Icon:     IconWarning,
			Priority: 90,
		})
	}

	right = append(right, StatusBarSection{
		Text:     time.Now().Format("15:04"),
		Style:    StatusStyleMuted,
		Icon:     IconClock,
		Priority: 70,
	})

	return
}

// View renders the status bar.
func (sb *StatusBar) View() string {
	// Build sections
	left, center, right := sb.buildDefaultSections()

	// Override with custom sections if set
	if len(sb.leftSections) > 0 {
		left = sb.leftSections
	}
	if len(sb.centerSections) > 0 {
		center = sb.centerSections
	}
	if len(sb.rightSections) > 0 {
		right = sb.rightSections
	}

	// Render each part
	leftContent := sb.renderSections(left)
	centerContent := sb.renderSections(center)
	rightContent := sb.renderSections(right)

	// Calculate widths
	leftWidth := lipgloss.Width(leftContent)
	centerWidth := lipgloss.Width(centerContent)
	rightWidth := lipgloss.Width(rightContent)

	totalContent := leftWidth + centerWidth + rightWidth
	availableSpace := sb.width - totalContent

	if availableSpace < 0 {
		// Content too wide, truncate center and right
		availableSpace = 0
		centerContent = ""
		if leftWidth+rightWidth > sb.width {
			rightContent = ""
		}
	}

	// Build the bar with spacing
	leftPadding := 0
	rightPadding := 0

	if availableSpace > 0 {
		// Distribute space
		if centerContent != "" {
			leftPadding = (availableSpace - centerWidth) / 2
			rightPadding = availableSpace - leftPadding - centerWidth
		} else {
			rightPadding = availableSpace
		}
	}

	bgStyle := lipgloss.NewStyle()
	if sb.theme != nil {
		bgStyle = bgStyle.Background(lipgloss.Color(sb.theme.BrandMuted))
	}

	result := leftContent
	if leftPadding > 0 {
		result += bgStyle.Render(strings.Repeat(" ", leftPadding))
	}
	result += centerContent
	if rightPadding > 0 {
		result += bgStyle.Render(strings.Repeat(" ", rightPadding))
	}
	result += rightContent

	// Ensure full width
	currentWidth := lipgloss.Width(result)
	if currentWidth < sb.width {
		result += bgStyle.Render(strings.Repeat(" ", sb.width-currentWidth))
	}

	return result
}

// VimModeBar creates a vim-style mode indicator bar.
func VimModeBar(mode string, t *theme.Theme) string {
	if t == nil {
		return mode
	}

	var style lipgloss.Style
	switch strings.ToUpper(mode) {
	case "NORMAL":
		style = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandAccent)).
			Foreground(lipgloss.Color(t.Background)).
			Bold(true).
			Padding(0, 1)
	case "INSERT":
		style = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandSuccess)).
			Foreground(lipgloss.Color(t.Background)).
			Bold(true).
			Padding(0, 1)
	case "VISUAL":
		style = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandWarning)).
			Foreground(lipgloss.Color(t.Background)).
			Bold(true).
			Padding(0, 1)
	case "COMMAND":
		style = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandHighlight)).
			Foreground(lipgloss.Color(t.Background)).
			Bold(true).
			Padding(0, 1)
	default:
		style = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandMuted)).
			Foreground(lipgloss.Color(t.Background)).
			Padding(0, 1)
	}

	return style.Render(strings.ToUpper(mode))
}
