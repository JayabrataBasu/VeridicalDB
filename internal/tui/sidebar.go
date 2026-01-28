package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Sidebar renders contextual info on the right side of the TUI.
type Sidebar struct {
	app *Model
}

// NewSidebar creates a new Sidebar.
func NewSidebar(app *Model) *Sidebar {
	return &Sidebar{app: app}
}

// View renders the sidebar content with compact, premium styling.
func (s *Sidebar) View(width int) string {
	w := width
	if w < 14 {
		w = 14
	}
	if w > 24 {
		w = 24
	}

	// Premium compact styles
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Width(w).
		Align(lipgloss.Center)

	sectionStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FFB86C")).
		PaddingLeft(1)

	itemStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#AAAAAA")).
		PaddingLeft(1)

	activeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#50FA7B")).
		PaddingLeft(1)

	inactiveStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#555555")).
		PaddingLeft(1)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#555555")).
		Italic(true).
		PaddingLeft(1)

	borderStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#3a3a5c"))

	// Container with border
	containerStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Width(w - 2).
		Padding(0, 1)

	var b strings.Builder

	// Compact header
	b.WriteString(headerStyle.Render("◆ VERIDICAL DB"))
	b.WriteString("\n")
	b.WriteString(lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		Width(w).
		Align(lipgloss.Center).
		Render("v1.0.0"))
	b.WriteString("\n")
	b.WriteString(borderStyle.Render(strings.Repeat("─", w-2)))
	b.WriteString("\n")

	// Compact sections - single line each
	b.WriteString(sectionStyle.Render("📋 Context"))
	b.WriteString("\n")
	b.WriteString(itemStyle.Render("Files: 0 │ Unsaved: 0"))
	b.WriteString("\n\n")

	b.WriteString(sectionStyle.Render("🔌 LSP"))
	b.WriteString("\n")
	b.WriteString(activeStyle.Render("● Go"))
	b.WriteString(itemStyle.Render(" │ "))
	b.WriteString(activeStyle.Render("● Nix"))
	b.WriteString("\n\n")

	b.WriteString(sectionStyle.Render("🤖 MCP"))
	b.WriteString("\n")
	b.WriteString(inactiveStyle.Render("○ None"))
	b.WriteString("\n\n")

	b.WriteString(sectionStyle.Render("🗄️ Database"))
	b.WriteString("\n")
	b.WriteString(activeStyle.Render("● Connected"))
	b.WriteString("\n")
	b.WriteString(itemStyle.Render("Tables: --"))
	b.WriteString("\n\n")

	b.WriteString(borderStyle.Render(strings.Repeat("─", w-2)))
	b.WriteString("\n")
	b.WriteString(helpStyle.Render("? Help"))

	return containerStyle.Render(b.String())
}
