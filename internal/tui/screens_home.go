package tui

import (
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
			},
			{
				Title:       "Database Browser",
				Description: "Browse tables and schemas",
				ScreenID:    "browser",
			},
			{
				Title:       "Monitoring",
				Description: "View system metrics and activity",
				ScreenID:    "monitoring",
			},
			{
				Title:       "User Management",
				Description: "Manage database users and permissions",
				ScreenID:    "users",
			},
			{
				Title:       "Backup & Restore",
				Description: "Manage backups and recovery",
				ScreenID:    "backup",
			},
			{
				Title:       "Settings",
				Description: "Configure TUI settings",
				ScreenID:    "settings",
			},
			{
				Title:       "About",
				Description: "About VeridicalDB",
				ScreenID:    "about",
			},
			{
				Title:       "Exit",
				Description: "Quit the application",
				ScreenID:    "",
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

// View renders the home screen with premium styling.
func (h *HomeScreen) View() string {
	var buf strings.Builder

	// Define premium styles with better spacing
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(1, 4).
		MarginBottom(2).
		Width(50).
		Align(lipgloss.Center)

	logoStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#50FA7B"))

	// Wider cards with more padding
	menuCardStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3a3a5c")).
		Padding(0, 3).
		Width(48).
		MarginLeft(1).
		MarginBottom(1)

	selectedCardStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#00D9FF")).
		Background(lipgloss.Color("#1a1a2e")).
		Padding(0, 3).
		Width(48).
		MarginLeft(1).
		MarginBottom(1)

	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FFFFFF"))

	selectedTitleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF"))

	descStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888")).
		Italic(true)

	footerStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		MarginTop(2).
		MarginLeft(1)

	// Icons for each menu item
	icons := map[string]string{
		"New Query":        "📝",
		"Database Browser": "🗄️",
		"Monitoring":       "📊",
		"User Management":  "👤",
		"Backup & Restore": "💾",
		"Settings":         "⚙️",
		"About":            "ℹ️",
		"Exit":             "🚪",
	}

	// Header with logo
	logo := logoStyle.Render("◆ VeridicalDB")
	header := headerStyle.Render(logo + " TUI v1.0.0")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	// Subtitle with spacing
	subtitle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#888888")).
		MarginLeft(1).
		MarginBottom(1).
		Render("Select an option to continue:")
	buf.WriteString(subtitle)
	buf.WriteString("\n\n")

	// Menu items as cards with better spacing
	for i, item := range h.items {
		icon := icons[item.Title]
		if icon == "" {
			icon = "•"
		}

		if i == h.menuIdx {
			// Selected item - highlighted card with description
			title := selectedTitleStyle.Render(icon + "  " + item.Title)
			desc := descStyle.Render("    " + item.Description)
			card := selectedCardStyle.Render(title + "\n" + desc)
			buf.WriteString(card)
		} else {
			// Normal item - compact
			title := titleStyle.Render(icon + "  " + item.Title)
			card := menuCardStyle.Render(title)
			buf.WriteString(card)
		}
		buf.WriteString("\n")
	}

	// Footer with keybindings and more breathing room
	buf.WriteString("\n")
	keybindings := []string{
		"↑↓/jk Navigate",
		"Enter Select",
		"q/Ctrl+C Quit",
	}
	footer := footerStyle.Render(strings.Join(keybindings, "  │  "))
	buf.WriteString(footer)
	buf.WriteString("\n")

	// Error message if any
	if h.app.lastError != "" {
		errorStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF5555")).
			Bold(true).
			MarginTop(1).
			MarginLeft(2)
		buf.WriteString(errorStyle.Render("✗ Error: " + h.app.lastError))
		buf.WriteString("\n")
	}

	// Status message if any
	if h.app.statusMessage != "" {
		statusStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#50FA7B")).
			MarginTop(1).
			MarginLeft(2)
		buf.WriteString(statusStyle.Render("✓ " + h.app.statusMessage))
		buf.WriteString("\n")
	}

	return buf.String()
}
