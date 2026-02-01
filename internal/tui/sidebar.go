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
	b.WriteString(styles.FromHex("v1.0.0", dim))
	b.WriteString("\n")
	b.WriteString(styles.FromHex(strings.Repeat("─", w-4), border))
	b.WriteString("\n\n")

	// Context section
	b.WriteString(styles.FromHexBold(types.Icons.Table+" Context", warning))
	b.WriteString("\n")
	b.WriteString(styles.FromHex("Files: 0 │ Unsaved: 0", muted))
	b.WriteString("\n\n")

	// LSP section
	b.WriteString(styles.FromHexBold(types.Icons.Connected+" LSP", warning))
	b.WriteString("\n")
	b.WriteString(styles.FromHex("● Go", success) + styles.FromHex(" │ ", muted) + styles.FromHex("● Nix", success))
	b.WriteString("\n\n")

	// MCP section
	b.WriteString(styles.FromHexBold(types.Icons.Network+" MCP", warning))
	b.WriteString("\n")
	b.WriteString(styles.FromHex("○ None", dim))
	b.WriteString("\n\n")

	// Database section
	b.WriteString(styles.FromHexBold(types.Icons.Database+" Database", warning))
	b.WriteString("\n")
	b.WriteString(styles.FromHex("● Connected", success))
	b.WriteString("\n")
	b.WriteString(styles.FromHex("Tables: --", muted))
	b.WriteString("\n\n")

	b.WriteString(styles.FromHex(strings.Repeat("─", w-4), border))
	b.WriteString("\n")
	b.WriteString(styles.DimText("? Help"))

	return b.String()
}
