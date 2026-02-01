package tui

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
	border := "#21262d"
	activeBg := "#1c2938"
	if tm := h.app.GetThemeManager(); tm != nil {
		t := tm.Current()
		accent = t.BrandGradientA
		highlight = t.BrandGradientB
		success = t.BrandSuccess
		danger = t.BrandDanger
		muted = t.Muted
		border = t.Border
		activeBg = t.BrandMuted
	}

	// Cyberpunk gradient header
	gradientTitle := theme.GradientText("▶ VERIDICALDB", accent, highlight)
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		MarginBottom(1).
		MarginTop(1)

	logoStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(success))

	// Title line with gradient
	title := logoStyle.Render(Icons.Database) + " " + headerStyle.Render(gradientTitle)
	buf.WriteString(title)
	buf.WriteString("\n")

	// Neon separator
	sepStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(border))
	buf.WriteString(sepStyle.Render(strings.Repeat("━", 40)))
	buf.WriteString("\n\n")

	// Menu items with cyberpunk glow effect
	// Active item: thick left border + gradient background
	// Create gradient background effect for active item (dark gray to black)
	activeGlowStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(accent)).
		Background(lipgloss.Color(activeBg)). // Subtle glow background
		Bold(true).
		Padding(0, 1)

	inactiveStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted))

	// Thick border pipe for active selection
	thickBorderStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(accent)). // Cyan glow
		Bold(true)

	normalBorderStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(border))

	for i, item := range h.items {
		icon := h.getIcon(item.Icon)

		if i == h.menuIdx {
			// Selected item with thick border and glow
			border := thickBorderStyle.Render("┃")
			content := activeGlowStyle.Render(fmt.Sprintf("%s %s", icon, item.Title))
			line := border + " " + content
			buf.WriteString(line)
			buf.WriteString("\n")
			// Show description for selected item with gradient
			descGradient := theme.GradientText("  "+item.Description, muted, highlight)
			buf.WriteString(descGradient)
			buf.WriteString("\n")
		} else {
			// Normal item with subtle border
			border := normalBorderStyle.Render("│")
			line := border + " " + inactiveStyle.Render(fmt.Sprintf("%s %s", icon, item.Title))
			buf.WriteString(line)
			buf.WriteString("\n")
		}
	}

	// Footer with neon keybindings
	buf.WriteString("\n")
	footerStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted))

	// Add gradient accents to key indicators
	keybindings := fmt.Sprintf("%s Navigate  %s  %s Select  %s  %s Theme  %s  %s Quit",
		lipgloss.NewStyle().Foreground(lipgloss.Color(success)).Bold(true).Render("j/k"),
		Icons.Separator,
		lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true).Render("Enter"),
		Icons.Separator,
		lipgloss.NewStyle().Foreground(lipgloss.Color(highlight)).Bold(true).Render("Ctrl+T"),
		Icons.Separator,
		lipgloss.NewStyle().Foreground(lipgloss.Color(danger)).Bold(true).Render("q"),
	)
	buf.WriteString(footerStyle.Render(keybindings))

	return buf.String()
}
