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

// View renders the sidebar content with premium styling and icons.
func (s *Sidebar) View(width int) string {
	w := 32
	if width > 0 && width < w {
		w = width
	}

	// Define premium styles
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 1).
		Width(w).
		Align(lipgloss.Center)

	sectionStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FFB86C")).
		MarginTop(1)

	itemStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FFFFFF")).
		PaddingLeft(2)

	activeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#50FA7B")).
		PaddingLeft(2)

	inactiveStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		PaddingLeft(2)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		MarginTop(1)

	borderStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#3a3a5c"))

	var b strings.Builder

	// Logo / header with premium styling
	logo := "◆ VERIDICAL DB"
	b.WriteString(headerStyle.Render(logo))
	b.WriteString("\n")

	// Version info
	versionStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888")).
		Width(w).
		Align(lipgloss.Center)
	b.WriteString(versionStyle.Render("v1.0.0"))
	b.WriteString("\n\n")

	// Section: Context
	b.WriteString(sectionStyle.Render("📋 Context"))
	b.WriteString("\n")
	b.WriteString(borderStyle.Render(strings.Repeat("─", w-4)))
	b.WriteString("\n")
	b.WriteString(itemStyle.Render("📁 Modified Files: None"))
	b.WriteString("\n")
	b.WriteString(itemStyle.Render("💾 Unsaved: 0"))
	b.WriteString("\n\n")

	// Section: LSPs
	b.WriteString(sectionStyle.Render("🔌 Language Servers"))
	b.WriteString("\n")
	b.WriteString(borderStyle.Render(strings.Repeat("─", w-4)))
	b.WriteString("\n")
	b.WriteString(activeStyle.Render("● Go (gopls)"))
	b.WriteString("\n")
	b.WriteString(activeStyle.Render("● Nix (nil)"))
	b.WriteString("\n\n")

	// Section: MCPs
	b.WriteString(sectionStyle.Render("🤖 MCP Servers"))
	b.WriteString("\n")
	b.WriteString(borderStyle.Render(strings.Repeat("─", w-4)))
	b.WriteString("\n")
	b.WriteString(inactiveStyle.Render("○ None connected"))
	b.WriteString("\n\n")

	// Section: Database Status
	b.WriteString(sectionStyle.Render("🗄️ Database"))
	b.WriteString("\n")
	b.WriteString(borderStyle.Render(strings.Repeat("─", w-4)))
	b.WriteString("\n")
	b.WriteString(activeStyle.Render("● Connected"))
	b.WriteString("\n")
	b.WriteString(itemStyle.Render("📊 Tables: --"))
	b.WriteString("\n\n")

	// Footer separator
	b.WriteString(borderStyle.Render(strings.Repeat("─", w)))
	b.WriteString("\n")

	// Help with keyboard icon
	b.WriteString(helpStyle.Render("  ⌨ Press ? for help"))

	return lipgloss.NewStyle().Width(w).Render(b.String())
}
