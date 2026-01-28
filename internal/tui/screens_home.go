package tui

import (
	"fmt"
	"strings"

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

// getIcon returns the icon for a menu item.
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

// View renders the home screen with CLI-native styling.
func (h *HomeScreen) View() string {
	t := h.app.themeManager.Current()
	var buf strings.Builder

	// Header - clean and minimal
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.Primary)).
		MarginBottom(1)

	logoStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.Success))

	// Title line
	title := logoStyle.Render(Icons.Database) + " " + headerStyle.Render("VeridicalDB")
	buf.WriteString(title)
	buf.WriteString("\n")

	// Separator
	sepStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(t.Border))
	buf.WriteString(sepStyle.Render(strings.Repeat("─", 40)))
	buf.WriteString("\n\n")

	// Menu items - list style with pipe indicator
	activeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Primary)).
		Bold(true)

	inactiveStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Foreground))

	descStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Muted)).
		MarginLeft(4)

	pipeStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Primary)).
		Bold(true)

	for i, item := range h.items {
		icon := h.getIcon(item.Icon)

		if i == h.menuIdx {
			// Selected item with pipe indicator
			line := pipeStyle.Render("│ ") + activeStyle.Render(fmt.Sprintf("%s %s", icon, item.Title))
			buf.WriteString(line)
			buf.WriteString("\n")
			// Show description for selected item
			buf.WriteString(descStyle.Render(item.Description))
			buf.WriteString("\n")
		} else {
			// Normal item (indented to align with selected)
			line := "  " + inactiveStyle.Render(fmt.Sprintf("%s %s", icon, item.Title))
			buf.WriteString(line)
			buf.WriteString("\n")
		}
	}

	// Footer with keybindings
	buf.WriteString("\n")
	footerStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Muted))

	keybindings := fmt.Sprintf("j/k Navigate  %s  Enter Select  %s  Ctrl+T Theme  %s  q Quit",
		Icons.Separator, Icons.Separator, Icons.Separator)
	buf.WriteString(footerStyle.Render(keybindings))

	return buf.String()
}
