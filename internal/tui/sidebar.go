package tui

import (
	"fmt"
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

	// Logo / header
	logoText := `  ____  _   _
 / ___|| |_| |__   ___ 
 \___ \\ __| '_ \\ / _ \
  ___) | |_| | | |  __/
 |____/ \\__|_| |_|\\___|`
	logo := strings.Split(logoText, "\n")
	for _, l := range logo {
		b.WriteString(palette.SidebarHeader.Render(fmt.Sprintf("%s\n", l)))
	}
	b.WriteString("\n")

	// Section: Run info
	b.WriteString(palette.SidebarSub.Render("Sidebar LSP MCP Section\n"))
	b.WriteString("\n")

	// Placeholder items
	b.WriteString(palette.SidebarSub.Render("Modified Files\n"))
	b.WriteString("  None\n\n")

	b.WriteString(palette.SidebarSub.Render("LSPs\n"))
	b.WriteString("  • Go gopls\n  • Nix nil\n\n")

	b.WriteString(palette.SidebarSub.Render("MCPs\n"))
	b.WriteString("  None\n\n")

	// Footer
	b.WriteString(strings.Repeat("-", w) + "\n")
	b.WriteString(palette.Footer.Render(" Press ? for help "))

	return lipgloss.NewStyle().Width(w).Render(b.String())
}
