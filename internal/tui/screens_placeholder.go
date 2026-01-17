package tui

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
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

// View renders the placeholder screen.
func (p *PlaceholderScreen) View() string {
	var buf strings.Builder

	// Header
	header := p.app.theme.HeaderStyle.Render("═══════════════════════════════════════")
	headerText := p.app.theme.HeaderStyle.Render("      " + p.title)
	headerBottom := p.app.theme.HeaderStyle.Render("═══════════════════════════════════════")

	buf.WriteString(header)
	buf.WriteString("\n")
	buf.WriteString(headerText)
	buf.WriteString("\n")
	buf.WriteString(headerBottom)
	buf.WriteString("\n\n")

	// Content
	buf.WriteString(p.app.theme.SecondaryStyle.Render("  This feature is coming soon!"))
	buf.WriteString("\n\n\n")
	buf.WriteString(p.app.theme.SecondaryStyle.Render("  Press ESC or 'q' to return to menu"))
	buf.WriteString("\n")

	return buf.String()
}
