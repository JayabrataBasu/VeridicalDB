package tui

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
	"github.com/charmbracelet/lipgloss"
)

// screenTitles maps screen IDs to their display names for the top bar / breadcrumb.
var screenTitles = map[string]string{
	"home":       "Home",
	"editor":     "Editor",
	"results":    "Results",
	"history":    "Query History",
	"browser":    "Database Browser",
	"monitoring": "Monitoring",
	"users":      "Users",
	"backup":     "Backup & Restore",
	"settings":   "Settings",
	"about":      "About",
}

// screenTitle returns a friendly name for the current screen.
func (m *Model) screenTitle() string {
	if t, ok := screenTitles[m.screenID]; ok {
		return t
	}
	return ""
}

// currentDBName returns the session's active database, or "default".
func (m *Model) currentDBName() string {
	if m.session != nil {
		if db := m.session.CurrentDatabase(); db != "" {
			return db
		}
	}
	return "default"
}

// renderTopBar draws the full-width application header: brand on the left, the
// current screen next to it, and the database / connection / version on the
// right. Returns two lines (header + a hairline rule).
func (m *Model) renderTopBar(width int) string {
	t := m.themeManager.Current()
	accent := t.BrandAccent
	fg := t.Foreground
	muted := t.Muted
	rule := t.Border

	sep := styles.FromHex("  "+Icons.Separator+"  ", rule)

	left := " " + styles.FromHexBold("VERIDICAL", accent) + styles.FromHexBold("DB", fg)
	if title := m.screenTitle(); title != "" && m.screenID != "home" {
		left += sep + styles.FromHex(title, fg)
	}

	conn := styles.FromHex(Icons.Connected, t.BrandSuccess)
	if m.session == nil {
		conn = styles.FromHex(Icons.Disconnected, muted)
	}
	right := styles.FromHex(m.currentDBName()+" ", muted) + conn +
		sep + styles.FromHex("v2.0.0 · Halcyon", rule) + " "

	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		gap = 1
	}
	line := left + strings.Repeat(" ", gap) + right
	hairline := styles.FromHex(strings.Repeat("─", width), rule)
	return line + "\n" + hairline
}
