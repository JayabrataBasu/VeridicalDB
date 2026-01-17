package tui

import (
	"fmt"
	"strings"

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

// View renders the home screen.
func (h *HomeScreen) View() string {
	var buf strings.Builder

	// Header
	header := h.app.theme.HeaderStyle.Render("═══════════════════════════════════════")
	headerText := h.app.theme.HeaderStyle.Render("      VeridicalDB TUI v1.0.0")
	headerBottom := h.app.theme.HeaderStyle.Render("═══════════════════════════════════════")

	buf.WriteString(header)
	buf.WriteString("\n")
	buf.WriteString(headerText)
	buf.WriteString("\n")
	buf.WriteString(headerBottom)
	buf.WriteString("\n\n")

	// Menu
	buf.WriteString("Select an option:\n\n")

	for i, item := range h.items {
		if i == h.menuIdx {
			// Highlighted item
			prefix := h.app.theme.PrimaryStyle.Render("› ")
			title := h.app.theme.PrimaryStyle.Render(item.Title)
			buf.WriteString(prefix)
			buf.WriteString(title)
		} else {
			// Normal item
			buf.WriteString("  ")
			buf.WriteString(item.Title)
		}
		buf.WriteString("\n")
		if i == h.menuIdx {
			// Show description for selected item
			description := h.app.theme.SecondaryStyle.Render("  → " + item.Description)
			buf.WriteString(description)
			buf.WriteString("\n")
		}
		buf.WriteString("\n")
	}

	// Footer
	buf.WriteString("\n")
	buf.WriteString(h.app.theme.BorderStyle.Render(strings.Repeat("─", 43)))
	buf.WriteString("\n")
	buf.WriteString(h.app.theme.SecondaryStyle.Render("Use ↑↓ or j/k to navigate, Enter to select, Ctrl+C to quit"))
	buf.WriteString("\n")

	// Error message if any
	if h.app.lastError != "" {
		buf.WriteString("\n")
		buf.WriteString(h.app.theme.ErrorStyle.Render(fmt.Sprintf("Error: %s", h.app.lastError)))
		buf.WriteString("\n")
	}

	// Status
	if h.app.statusMessage != "" {
		buf.WriteString("\n")
		buf.WriteString(h.app.theme.SuccessStyle.Render(h.app.statusMessage))
		buf.WriteString("\n")
	}

	return buf.String()
}
