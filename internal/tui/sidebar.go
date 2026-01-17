package tui

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
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

// View renders the sidebar content with a fixed width.
func (s *Sidebar) View(width int) string {
	palette := s.app.GetStyles()
	if palette == nil {
		palette = &types.StylePalette{}
	}

	w := 32
	if width > 0 && width < w {
		w = width
	}

	var b strings.Builder

	// Logo / header - Simple clean logo
	logoLines := []string{
		"  ╦  ╦╔═╗╦═╗╦╔╦╗╦╔═╗╔═╗╦  ",
		"  ╚╗╔╝║╣ ╠╦╝║ ║║║║  ╠═╣║  ",
		"   ╚╝ ╚═╝╩╚═╩═╩╝╩╚═╝╩ ╩╩═╝",
		"        ── Database ──",
	}
	for _, l := range logoLines {
		b.WriteString(palette.SidebarHeader.Render(l))
		b.WriteString("\n")
	}
	b.WriteString("\n")

	// Section: Run info
	b.WriteString(palette.SidebarSub.Render("━━ Context ━━"))
	b.WriteString("\n\n")

	// Placeholder items
	b.WriteString(palette.Subtle.Render(" Modified Files: "))
	b.WriteString("None\n\n")

	b.WriteString(palette.SidebarSub.Render("━━ LSPs ━━"))
	b.WriteString("\n")
	b.WriteString(" • Go (gopls)\n")
	b.WriteString(" • Nix (nil)\n\n")

	b.WriteString(palette.SidebarSub.Render("━━ MCPs ━━"))
	b.WriteString("\n")
	b.WriteString(" None\n\n")

	// Footer
	b.WriteString(palette.Subtle.Render(strings.Repeat("─", w)))
	b.WriteString("\n")
	b.WriteString(palette.Help.Render("? help"))

	return lipgloss.NewStyle().Width(w).Render(b.String())
}
