// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import "github.com/charmbracelet/lipgloss"

// PaneState represents the state of a pane (active or inactive).
type PaneState int

const (
	// PaneInactive means the pane is not focused.
	PaneInactive PaneState = iota
	// PaneActive means the pane is currently focused.
	PaneActive
)

// PaneStyles defines the styling for a pane in different states.
type PaneStyles struct {
	// ActiveBorder is the border style when pane is active/focused.
	ActiveBorder lipgloss.Style
	// InactiveBorder is the border style when pane is inactive.
	InactiveBorder lipgloss.Style
	// ActiveTitle is the title style when pane is active.
	ActiveTitle lipgloss.Style
	// InactiveTitle is the title style when pane is inactive.
	InactiveTitle lipgloss.Style
	// ActiveBackground is applied when pane is active (subtle tint).
	ActiveBackground lipgloss.Style
	// InactiveBackground is applied when pane is inactive.
	InactiveBackground lipgloss.Style
}

// Pane represents a styled container with a title and content.
type Pane struct {
	Title   string
	Content string
	Width   int
	Height  int
	State   PaneState
	styles  *PaneStyles
}

// NewPane creates a new pane with the given title and dimensions.
func NewPane(title string, width, height int, styles *PaneStyles) *Pane {
	return &Pane{
		Title:  title,
		Width:  width,
		Height: height,
		State:  PaneInactive,
		styles: styles,
	}
}

// SetActive sets the pane to active state.
func (p *Pane) SetActive() {
	p.State = PaneActive
}

// SetInactive sets the pane to inactive state.
func (p *Pane) SetInactive() {
	p.State = PaneInactive
}

// SetContent updates the pane content.
func (p *Pane) SetContent(content string) {
	p.Content = content
}

// SetDimensions updates the pane dimensions.
func (p *Pane) SetDimensions(width, height int) {
	p.Width = width
	p.Height = height
}

// View renders the pane with rounded borders and appropriate styling.
func (p *Pane) View() string {
	var borderStyle, titleStyle, bgStyle lipgloss.Style

	if p.State == PaneActive {
		borderStyle = p.styles.ActiveBorder
		titleStyle = p.styles.ActiveTitle
		bgStyle = p.styles.ActiveBackground
	} else {
		borderStyle = p.styles.InactiveBorder
		titleStyle = p.styles.InactiveTitle
		bgStyle = p.styles.InactiveBackground
	}

	// Create the bordered container with rounded corners
	container := borderStyle.
		Width(p.Width-2). // Account for border width
		Height(p.Height-2).
		Padding(0, 1)

	// Render title if present
	titleBar := ""
	if p.Title != "" {
		titleBar = titleStyle.Render(" " + p.Title + " ")
	}

	// Apply background style to content
	styledContent := bgStyle.Render(p.Content)

	// Combine title and content
	if titleBar != "" {
		return lipgloss.JoinVertical(lipgloss.Left,
			titleBar,
			container.Render(styledContent),
		)
	}

	return container.Render(styledContent)
}

// NewPaneStyles creates PaneStyles from a Theme.
func NewPaneStyles(theme *Theme) *PaneStyles {
	// Active state: bright border, subtle background tint
	activeBorderColor := theme.PrimaryColor
	activeBgColor := "#0D1117" // Subtle dark tint for active

	// Inactive state: dimmed border, no background tint
	inactiveBorderColor := "#3D4450" // Dimmed gray
	inactiveBgColor := "#010409"     // Very dark, almost no tint

	return &PaneStyles{
		ActiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(activeBorderColor)),

		InactiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(inactiveBorderColor)),

		ActiveTitle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(theme.PrimaryColor)).
			Background(lipgloss.Color(activeBgColor)).
			Padding(0, 1),

		InactiveTitle: lipgloss.NewStyle().
			Foreground(lipgloss.Color(inactiveBorderColor)).
			Faint(true).
			Padding(0, 1),

		ActiveBackground: lipgloss.NewStyle().
			Background(lipgloss.Color(activeBgColor)),

		InactiveBackground: lipgloss.NewStyle().
			Background(lipgloss.Color(inactiveBgColor)),
	}
}

// NewPaneStylesFromPalette creates PaneStyles for different themes.
func NewPaneStylesFromPalette(themeName string) *PaneStyles {
	switch themeName {
	case "light":
		return &PaneStyles{
			ActiveBorder: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#0066CC")),
			InactiveBorder: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#CCCCCC")),
			ActiveTitle: lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#0066CC")).
				Padding(0, 1),
			InactiveTitle: lipgloss.NewStyle().
				Foreground(lipgloss.Color("#999999")).
				Padding(0, 1),
			ActiveBackground: lipgloss.NewStyle().
				Background(lipgloss.Color("#F5F8FA")),
			InactiveBackground: lipgloss.NewStyle().
				Background(lipgloss.Color("#FFFFFF")),
		}

	case "high-contrast":
		return &PaneStyles{
			ActiveBorder: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#FFFF00")),
			InactiveBorder: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#808080")),
			ActiveTitle: lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#FFFF00")).
				Padding(0, 1),
			InactiveTitle: lipgloss.NewStyle().
				Foreground(lipgloss.Color("#808080")).
				Padding(0, 1),
			ActiveBackground: lipgloss.NewStyle().
				Background(lipgloss.Color("#000000")),
			InactiveBackground: lipgloss.NewStyle().
				Background(lipgloss.Color("#000000")),
		}

	default: // dark theme
		return &PaneStyles{
			ActiveBorder: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#00D9FF")),
			InactiveBorder: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("#3D4450")),
			ActiveTitle: lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#00D9FF")).
				Background(lipgloss.Color("#0D1117")).
				Padding(0, 1),
			InactiveTitle: lipgloss.NewStyle().
				Foreground(lipgloss.Color("#6E7681")).
				Faint(true).
				Padding(0, 1),
			ActiveBackground: lipgloss.NewStyle().
				Background(lipgloss.Color("#0D1117")),
			InactiveBackground: lipgloss.NewStyle().
				Background(lipgloss.Color("#010409")),
		}
	}
}
