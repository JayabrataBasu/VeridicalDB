// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/screens"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
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

	// Initialize screens
	homeScreen := NewHomeScreen(m)
	editorScreen := screens.NewEditorScreen(m)
	resultsScreen := screens.NewResultsScreen(m)

	m.screens["home"] = homeScreen
	m.screens["editor"] = editorScreen
	m.screens["results"] = resultsScreen
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
			return m, screen.Init()
		}
		m.lastError = "Screen not found: " + msg.ScreenID
		return m, nil

	case screens.ExecuteQueryMsg:
		// Execute query asynchronously
		return m, m.executeQuery(msg.SQL)

	case screens.QueryCompletedMsg:
		// Update results screen with query result
		if resultsScreen, ok := m.screens["results"].(*screens.ResultsScreen); ok {
			if msg.Error != nil {
				m.lastError = msg.Error.Error()
				m.statusMessage = "Query failed"
			} else {
				resultsScreen.SetResult(msg.Result)
				m.statusMessage = "Query executed successfully"
			}
		}
		// Forward to current screen
		if m.screen != nil {
			newScreen, cmd := m.screen.Update(msg)
			m.screen = newScreen
			return m, cmd
		}
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

// GetStyles returns the shared style palette for screens.
func (m *Model) GetStyles() *types.StylePalette {
	if m.theme == nil {
		return nil
	}
	return m.theme.Palette()
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

// executeQuery executes a SQL query asynchronously.
func (m *Model) executeQuery(sqlQuery string) tea.Cmd {
	return func() tea.Msg {
		// Execute query using session
		result, err := m.session.ExecuteSQL(sqlQuery)

		if err != nil {
			return screens.QueryCompletedMsg{
				Result: nil,
				Error:  err,
			}
		}

		// Convert result to QueryResult format
		queryResult := &screens.QueryResult{
			Columns: result.Columns,
			Rows:    convertRows(result.Rows),
			Message: result.Message,
		}

		return screens.QueryCompletedMsg{
			Result: queryResult,
			Error:  nil,
		}
	}
}

// convertRows converts catalog.Value rows to interface{} rows for display.
func convertRows(catalogRows [][]catalog.Value) [][]interface{} {
	if catalogRows == nil {
		return nil
	}

	rows := make([][]interface{}, len(catalogRows))
	for i, catalogRow := range catalogRows {
		rows[i] = make([]interface{}, len(catalogRow))
		for j, val := range catalogRow {
			rows[i][j] = convertValue(val)
		}
	}
	return rows
}

// convertValue converts a catalog.Value to interface{} for display.
func convertValue(v catalog.Value) interface{} {
	if v.IsNull {
		return nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return v.Int32
	case catalog.TypeInt64:
		return v.Int64
	case catalog.TypeFloat64:
		return v.Float64
	case catalog.TypeText:
		return v.Text
	case catalog.TypeBool:
		return v.Bool
	case catalog.TypeTimestamp:
		return v.Timestamp
	case catalog.TypeJSON:
		return v.JSON
	default:
		return nil
	}
}
