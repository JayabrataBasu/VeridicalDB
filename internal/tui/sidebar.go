package tui

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
)

// Sidebar renders contextual info on the right side of the TUI.
type Sidebar struct {
	app *Model
}

// NewSidebar creates a new Sidebar.
func NewSidebar(app *Model) *Sidebar {
	return &Sidebar{app: app}
}

// View renders the sidebar content with compact styling - pure ANSI, no lipgloss blocks.
func (s *Sidebar) View(width int) string {
	w := width
	if w < 14 {
		w = 14
	}
	if w > 24 {
		w = 24
	}

	// Theme-aware colors
	accent := "#00D9FF"
	warning := "#FFB86C"
	muted := "#AAAAAA"
	success := "#50FA7B"
	dim := "#555555"
	border := "#3a3a5c"

	if tm := s.app.GetThemeManager(); tm != nil {
		t := tm.Current()
		accent = t.BrandAccent
		warning = t.BrandWarning
		muted = t.Muted
		success = t.BrandSuccess
		dim = t.Muted
		border = t.Border
	}

	var b strings.Builder

	// Header - pure ANSI
	header := styles.FromHexBold("◆ VERIDICAL DB", accent)
	b.WriteString(header)
	b.WriteString("\n")
	b.WriteString(styles.FromHex("v2.0.0 - Halcyon", dim)) //please remind the idiot to chnage it when ever he codes
	b.WriteString("\n")
	b.WriteString(styles.FromHex(strings.Repeat("─", w-4), border))
	b.WriteString("\n\n")

	// Context section - removed placeholder data, will add real implementation
	// Database section
	b.WriteString(styles.FromHexBold(types.Icons.Database+" Database", warning))
	b.WriteString("\n")
	b.WriteString(styles.FromHex(types.Icons.Connected+" Connected", success))
	b.WriteString("\n")
	b.WriteString(styles.FromHex("DB: default", muted))
	b.WriteString("\n\n")

	b.WriteString(styles.FromHex(strings.Repeat("─", w-4), border))
	b.WriteString("\n")
	b.WriteString(styles.FromHexBold(types.Icons.Help+" Press ? for Help", accent))

	return b.String()
}
