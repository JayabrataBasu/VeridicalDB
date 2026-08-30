package tui

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var replUnicodeBanner = []string{
	"██╗   ██╗███████╗██████╗ ██╗██████╗ ██╗ ██████╗ █████╗ ██╗     ██████╗ ██████╗ ",
	"██║   ██║██╔════╝██╔══██╗██║██╔══██╗██║██╔════╝██╔══██╗██║     ██╔══██╗██╔══██╗",
	"██║   ██║█████╗  ██████╔╝██║██║  ██║██║██║     ███████║██║     ██║  ██║██████╔╝",
	"╚██╗ ██╔╝██╔══╝  ██╔══██╗██║██║  ██║██║██║     ██╔══██║██║     ██║  ██║██╔══██╗",
	" ╚████╔╝ ███████╗██║  ██║██║██████╔╝██║╚██████╗██║  ██║███████╗██████╔╝██████╔╝",
	"  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚═╝╚═════╝ ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═════╝ ╚═════╝",
}

// indent prefixes every line of s with n spaces (safe for ANSI-coloured text).
func indent(s string, n int) string {
	pad := strings.Repeat(" ", n)
	return pad + strings.ReplaceAll(s, "\n", "\n"+pad)
}

func bannerWidth(lines []string) int {
	max := 0
	for _, line := range lines {
		w := lipgloss.Width(line)
		if w > max {
			max = w
		}
	}
	return max
}

func themedBanner(lines []string, leftColor, rightColor string) string {
	if len(lines) == 0 {
		return ""
	}

	var b strings.Builder
	for i, line := range lines {
		b.WriteString(theme.GradientText(line, leftColor, rightColor))
		if i < len(lines)-1 {
			b.WriteString("\n")
		}
	}

	return b.String()
}

// HomeScreen is the main menu screen of the TUI.
type HomeScreen struct {
	app     *Model
	menuIdx int
	items   []MenuItem
}

// MenuItem represents a menu option.
type MenuItem struct {
	Title       string
	Description string
	ScreenID    string
	Icon        string // Icon key from IconSet
}

// NewHomeScreen creates a new home screen.
func NewHomeScreen(app *Model) *HomeScreen {
	return &HomeScreen{
		app:     app,
		menuIdx: 0,
		items: []MenuItem{
			{
				Title:       "New Query",
				Description: "Execute a new SQL query",
				ScreenID:    "editor",
				Icon:        "Query",
			},
			{
				Title:       "Query History",
				Description: "View recent queries and results",
				ScreenID:    "history",
				Icon:        "History",
			},
			{
				Title:       "Database Browser",
				Description: "Browse tables and schemas",
				ScreenID:    "browser",
				Icon:        "Database",
			},
			{
				Title:       "Monitoring",
				Description: "View system metrics and activity",
				ScreenID:    "monitoring",
				Icon:        "Dashboard",
			},
			{
				Title:       "User Management",
				Description: "Manage database users and permissions",
				ScreenID:    "users",
				Icon:        "Users",
			},
			{
				Title:       "Backup & Restore",
				Description: "Manage backups and recovery",
				ScreenID:    "backup",
				Icon:        "Backup",
			},
			{
				Title:       "Settings",
				Description: "Configure TUI settings",
				ScreenID:    "settings",
				Icon:        "Settings",
			},
			{
				Title:       "About",
				Description: "About VeridicalDB",
				ScreenID:    "about",
				Icon:        "About",
			},
			{
				Title:       "Exit",
				Description: "Quit the application",
				ScreenID:    "",
				Icon:        "Exit",
			},
		},
	}
}

// Init initializes the home screen.
func (h *HomeScreen) Init() tea.Cmd {
	return nil
}

// Update handles messages for the home screen.
func (h *HomeScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			h.menuIdx = (h.menuIdx - 1 + len(h.items)) % len(h.items)
			return h, nil

		case "down", "j":
			h.menuIdx = (h.menuIdx + 1) % len(h.items)
			return h, nil

		case "enter":
			if h.items[h.menuIdx].ScreenID == "" {
				// Exit
				return h, tea.Quit
			}
			// Change screen
			return h, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: h.items[h.menuIdx].ScreenID}
			}
		}
	}
	return h, nil
}

// getIcon returns the icon for a menu item but somehow the backup section is not working!
func (h *HomeScreen) getIcon(iconKey string) string {
	switch iconKey {
	case "Query":
		return Icons.Query
	case "Database":
		return Icons.Database
	case "Dashboard":
		return Icons.Dashboard
	case "Users":
		return Icons.Users
	case "History":
		return Icons.Clock
	case "Backup":
		return Icons.Backup
	case "Settings":
		return Icons.Settings
	case "About":
		return Icons.About
	case "Exit":
		return Icons.Exit
	default:
		return Icons.Bullet
	}
}

// View renders the home screen with cyberpunk glow styling.
func (h *HomeScreen) View() string {
	var buf strings.Builder

	// Theme-aware palette
	accent := "#00f2ff"
	highlight := "#bd00ff"
	success := "#7dce13"
	danger := "#ff5370"
	muted := "#6e7681"
	if tm := h.app.GetThemeManager(); tm != nil {
		t := tm.Current()
		accent = t.BrandGradientA
		highlight = t.BrandGradientB
		success = t.BrandSuccess
		danger = t.BrandDanger
		muted = t.Muted
	}

	// Full banner when the terminal is wide enough; a compact wordmark otherwise.
	buf.WriteString("\n")
	if h.app.GetWidth() >= bannerWidth(replUnicodeBanner)+2 {
		buf.WriteString(indent(themedBanner(replUnicodeBanner, accent, highlight), 1))
	} else {
		buf.WriteString(" " + theme.GradientText("VERIDICALDB", accent, highlight))
	}
	buf.WriteString("\n")
	buf.WriteString("   " + styles.FromHex("An embeddable SQL database  ·  v2.0.0 Halcyon", muted))
	buf.WriteString("\n\n")

	// Menu.
	for i, item := range h.items {
		selected := i == h.menuIdx
		title := item.Title
		if selected {
			marker := styles.FromHexBold(" "+Icons.Pointer+" ", accent)
			buf.WriteString(marker + styles.FromHexBold(title, accent))
			buf.WriteString("   " + styles.FromHex(item.Description, muted) + "\n")
		} else {
			buf.WriteString("   " + styles.FromHex(title, muted) + "\n")
		}
	}

	// Footer.
	buf.WriteString("\n ")
	footer := strings.Join([]string{
		styles.FromHexBold("↑↓", success) + styles.FromHex(" move", muted),
		styles.FromHexBold("enter", accent) + styles.FromHex(" open", muted),
		styles.FromHexBold("?", highlight) + styles.FromHex(" help", muted),
		styles.FromHexBold("^T", highlight) + styles.FromHex(" theme", muted),
		styles.FromHexBold("q", danger) + styles.FromHex(" quit", muted),
	}, styles.FromHex("   ", muted))
	buf.WriteString(footer)

	return buf.String()
}
