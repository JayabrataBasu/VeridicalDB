package tui

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	tea "github.com/charmbracelet/bubbletea"
)

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

	// Title with gradient - using raw ANSI, no lipgloss blocks
	gradientTitle := theme.GradientText("▶ VERIDICALDB", accent, highlight)
	title := styles.FromHexBold(Icons.Database, success) + " " + gradientTitle
	buf.WriteString(title)
	buf.WriteString("\n\n\n")

	// Menu items - pure ANSI styling, no background blocks
	for i, item := range h.items {
		icon := h.getIcon(item.Icon)

		if i == h.menuIdx {
			// Selected item: accent color, bold, underlined
			marker := styles.FromHexBold("›", accent)
			content := styles.FromHexBold(fmt.Sprintf("%s %s", icon, item.Title), accent)
			buf.WriteString(marker + " " + content + "\n")
			// Description with gradient
			descGradient := theme.GradientText("  "+item.Description, muted, highlight)
			buf.WriteString(descGradient + "\n")
		} else {
			// Normal item: muted color
			marker := styles.FromHex("·", muted)
			content := styles.FromHex(fmt.Sprintf("%s %s", icon, item.Title), muted)
			buf.WriteString(marker + " " + content + "\n")
		}
	}

	// Footer with keybindings - pure ANSI
	buf.WriteString("\n")
	footer := fmt.Sprintf("%s Navigate  %s  %s Select  %s  %s Theme  %s  %s Quit",
		styles.FromHexBold("j/k", success),
		Icons.Separator,
		styles.FromHexBold("Enter", accent),
		Icons.Separator,
		styles.FromHexBold("Ctrl+T", highlight),
		Icons.Separator,
		styles.FromHexBold("q", danger),
	)
	buf.WriteString(styles.FromHex(footer, muted))

	return buf.String()
}
