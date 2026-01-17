// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	tea "github.com/charmbracelet/bubbletea"
)

// Model represents the main TUI application state.
type Model struct {
	// Current screen
	screen Screen

	// Shared session for database operations
	session *sql.Session

	// Theme configuration
	theme *Theme

	// Screen registry
	screens map[string]Screen

	// Message channel for async operations
	msgChan chan tea.Msg

	// Application quit flag
	quitting bool

	// Last error message
	lastError string

	// Status message
	statusMessage string
}

// New creates a new TUI Model.
func New(session *sql.Session) *Model {
	theme := NewTheme("dark")

	m := &Model{
		session:  session,
		theme:    theme,
		screens:  make(map[string]Screen),
		msgChan:  make(chan tea.Msg, 100),
		quitting: false,
	}

	// Initialize screens (will be populated by screen constructors)
	homeScreen := NewHomeScreen(m)
	m.screens["home"] = homeScreen
	m.screen = homeScreen

	return m
}

// Init initializes the model and returns an initial command.
func (m *Model) Init() tea.Cmd {
	return nil
}

// Update handles messages and updates the model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "ctrl+q":
			m.quitting = true
			return m, tea.Quit

		case "ctrl+l":
			// Clear screen
			return m, tea.ClearScreen

		case "f1", "ctrl+h":
			// Show help (TODO)
			m.statusMessage = "Help not yet implemented"
			return m, nil
		}

	case ScreenChangeMsg:
		// Switch to new screen
		if screen, ok := m.screens[msg.ScreenID]; ok {
			m.screen = screen
			return m, nil
		}
		m.lastError = "Screen not found: " + msg.ScreenID
		return m, nil

	case ErrorMsg:
		m.lastError = msg.Error
		return m, nil

	case StatusMsg:
		m.statusMessage = msg.Message
		return m, nil
	}

	// Delegate to current screen
	if m.screen != nil {
		newScreen, cmd := m.screen.Update(msg)
		m.screen = newScreen
		return m, cmd
	}

	return m, nil
}

// View renders the TUI.
func (m *Model) View() string {
	if m.quitting {
		return ""
	}

	if m.screen == nil {
		return "No screen active"
	}

	return m.screen.View()
}

// RegisterScreen registers a screen in the TUI.
func (m *Model) RegisterScreen(id string, screen Screen) {
	m.screens[id] = screen
}

// GetSession returns the shared database session.
func (m *Model) GetSession() *sql.Session {
	return m.session
}

// GetTheme returns the current theme.
func (m *Model) GetTheme() *Theme {
	return m.theme
}

// SetTheme updates the theme.
func (m *Model) SetTheme(theme *Theme) {
	m.theme = theme
}

// ShowError displays an error message.
func (m *Model) ShowError(err string) {
	m.lastError = err
}

// GetLastError returns the last error message.
func (m *Model) GetLastError() string {
	return m.lastError
}

// ClearError clears the last error message.
func (m *Model) ClearError() {
	m.lastError = ""
}

// SetStatus updates the status message.
func (m *Model) SetStatus(msg string) {
	m.statusMessage = msg
}

// GetStatus returns the current status message.
func (m *Model) GetStatus() string {
	return m.statusMessage
}
