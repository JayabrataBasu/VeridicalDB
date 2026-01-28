package tui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// PlaceholderScreen represents a screen that's not yet implemented.
type PlaceholderScreen struct {
	app   *Model
	title string
}

// NewPlaceholderScreen creates a new placeholder screen.
func NewPlaceholderScreen(app *Model, title string) *PlaceholderScreen {
	return &PlaceholderScreen{
		app:   app,
		title: title,
	}
}

// Init initializes the placeholder screen.
func (p *PlaceholderScreen) Init() tea.Cmd {
	return nil
}

// Update handles messages for the placeholder screen.
func (p *PlaceholderScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			// Return to home
			return p, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "home"}
			}
		}
	}
	return p, nil
}

// View renders the placeholder screen with premium styling.
func (p *PlaceholderScreen) View() string {
	var buf strings.Builder

	// Icon map for different screens
	icons := map[string]string{
		"User Management":  "👤",
		"Backup & Restore": "💾",
		"Settings":         "⚙️",
		"About":            "ℹ️",
	}

	icon := icons[p.title]
	if icon == "" {
		icon = "📋"
	}

	// Premium header style matching other screens
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 3).
		MarginBottom(2)

	contentStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Padding(2, 4).
		MarginTop(2)

	comingSoonStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFB86C")).
		Bold(true)

	hintStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		MarginTop(2)

	// Header
	header := headerStyle.Render(icon + "  " + p.title)
	buf.WriteString(header)
	buf.WriteString("\n\n")

	// Content box
	content := comingSoonStyle.Render("🚧 Coming Soon") + "\n\n" +
		lipgloss.NewStyle().Foreground(lipgloss.Color("#888888")).Render("This feature is under development.")

	buf.WriteString(contentStyle.Render(content))
	buf.WriteString("\n")

	// Help hint
	buf.WriteString(hintStyle.Render("Esc/q Return to menu"))

	return buf.String()
}
