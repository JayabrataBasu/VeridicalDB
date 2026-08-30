// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/screens"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model represents the main TUI application state.
type Model struct {
	// Current screen
	screen Screen

	// Shared session for database operations
	session Session

	// Theme configuration (legacy, for compatibility)
	theme *Theme

	// Theme manager for advanced theming
	themeManager *theme.Manager

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

	// Layout manager for 3-pane layout
	layout *Layout

	// Status bar
	statusBar *StatusBar

	// Global help overlay
	helpOverlay *screens.HelpOverlay

	// Window dimensions
	width  int
	height int

	// Sidebar collapsed state
	sidebarCollapsed bool

	// Available theme names for cycling
	themeNames []string
	themeIndex int
}

// New creates a new TUI Model.
func New(session Session) *Model {
	// Initialize icons based on environment
	InitIcons()

	// Initialize theme manager
	themeManager := theme.NewManager()
	themeNames := themeManager.ListThemes()

	// Legacy theme for backward compatibility
	legacyTheme := NewTheme("dark")

	// Configure compact sidebar layout
	layoutConfig := &LayoutConfig{
		SidebarWidth:       20, // Compact sidebar
		AuxiliaryWidth:     40,
		SidebarCollapsed:   false,
		AuxiliaryCollapsed: true,
		MinSidebarWidth:    14,
		MinAuxiliaryWidth:  30,
		MinMainWidth:       50,
	}

	// Create pane styles using theme colors - NO backgrounds
	t := themeManager.Current()
	paneStyles := &PaneStyles{
		ActiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BorderFocused)),
		InactiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.Border)),
		ActiveTitle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(t.Primary)).
			Padding(0, 1),
		InactiveTitle: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Muted)).
			Padding(0, 1),
		ActiveBackground:   lipgloss.NewStyle(),
		InactiveBackground: lipgloss.NewStyle(),
	}

	// Create status bar
	statusBar := NewStatusBar(themeManager)

	m := &Model{
		session:          session,
		theme:            legacyTheme,
		themeManager:     themeManager,
		screens:          make(map[string]Screen),
		msgChan:          make(chan tea.Msg, 100),
		quitting:         false,
		layout:           NewLayout(layoutConfig, paneStyles),
		statusBar:        statusBar,
		width:            120,
		height:           40,
		sidebarCollapsed: false,
		themeNames:       themeNames,
		themeIndex:       0,
		helpOverlay:      screens.NewHelpOverlay(),
	}

	// Prime overlay dimensions with initial viewport.
	if m.helpOverlay != nil {
		_ = m.helpOverlay.Update(tea.WindowSizeMsg{Width: m.width, Height: m.height})
	}

	// Initialize screens
	homeScreen := NewHomeScreen(m)
	editorScreen := screens.NewEditorScreen(m)
	resultsScreen := screens.NewResultsScreen(m)
	historyScreen := screens.NewQueryHistoryScreen(m)

	m.screens["home"] = homeScreen
	m.screens["editor"] = editorScreen
	m.screens["results"] = resultsScreen
	m.screens["history"] = historyScreen

	// Initialize real feature screens with adapters
	m.screens["browser"] = NewDatabaseBrowserAdapter(m, 120, 40)
	m.screens["monitoring"] = NewDashboardAdapter(m)

	// Feature screens
	m.screens["users"] = screens.NewUsersScreen(m)
	m.screens["backup"] = screens.NewBackupScreen(m)
	m.screens["settings"] = NewSettingsScreen(m)
	m.screens["about"] = NewAboutScreen(m)

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
	case tea.WindowSizeMsg:
		// Update dimensions for proper layout
		m.width = msg.Width
		m.height = msg.Height
		m.layout.SetDimensions(msg.Width, msg.Height)
		m.statusBar.SetWidth(msg.Width)
		if m.helpOverlay != nil {
			_ = m.helpOverlay.Update(msg)
		}
		// Forward adjusted size to the active screen (subtract sidebar + status bar)
		sideW := 22
		if m.sidebarCollapsed {
			sideW = 0
		}
		screenMsg := tea.WindowSizeMsg{
			Width:  msg.Width - sideW,
			Height: msg.Height - 2, // reserve for status bar
		}
		if m.screen != nil {
			newScreen, cmd := m.screen.Update(screenMsg)
			m.screen = newScreen
			return m, cmd
		}
		return m, nil

	case tea.KeyMsg:
		if m.helpOverlay != nil && m.helpOverlay.IsVisible() {
			cmd := m.helpOverlay.Update(msg)
			return m, cmd
		}

		switch msg.String() {
		case "ctrl+c", "ctrl+q":
			m.quitting = true
			return m, tea.Quit

		case "ctrl+b":
			// Toggle sidebar visibility
			m.sidebarCollapsed = !m.sidebarCollapsed
			m.layout.ToggleSidebar()
			return m, nil

		case "ctrl+t":
			// Cycle through themes
			themeName := m.CycleTheme()
			m.statusMessage = "Theme: " + themeName
			return m, nil

		case "ctrl+l":
			// Clear screen
			return m, tea.ClearScreen

		case "f1", "ctrl+h", "?":
			if m.helpOverlay != nil {
				m.helpOverlay.Show()
				m.statusMessage = ""
			}
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

	if m.helpOverlay != nil && m.helpOverlay.IsVisible() {
		return m.helpOverlay.View()
	}

	if m.screen == nil {
		return "No screen active"
	}

	// Screen already renders at the correct width (set via WindowSizeMsg).
	// Do NOT re-wrap with Width/Height/Background — that corrupts ANSI codes.
	mainContent := m.screen.View()
	_, isHomeScreen := m.screen.(*HomeScreen)
	if isHomeScreen {
		// Add breathing room on the landing screen so the banner and sidebar feel intentional.
		mainContent = lipgloss.NewStyle().PaddingTop(1).PaddingLeft(1).Render(mainContent)
	}

	// Render sidebar if not collapsed
	sidebarWidth := 22
	if m.sidebarCollapsed {
		sidebarWidth = 0
	}

	var layout string
	if !m.sidebarCollapsed {
		side := NewSidebar(m)
		sidebarContent := side.View(sidebarWidth)
		if isHomeScreen {
			// Add a gutter and lower the panel slightly to align with the visual weight of the banner.
			sidebarContent = lipgloss.NewStyle().PaddingTop(2).PaddingLeft(2).Render(sidebarContent)
		}
		layout = lipgloss.JoinHorizontal(lipgloss.Top, mainContent, sidebarContent)
	} else {
		layout = mainContent
	}

	// Update status bar state
	m.statusBar.SetDatabase("default", true)
	if m.lastError != "" {
		m.statusBar.SetInfo(Icons.Error + " " + m.lastError)
	} else if m.statusMessage != "" {
		m.statusBar.SetInfo(Icons.Success + " " + m.statusMessage)
	} else {
		m.statusBar.SetInfo("")
	}

	// Render status bar
	statusBarView := m.statusBar.View()

	// NO container background - let terminal handle background uniformly
	return lipgloss.JoinVertical(lipgloss.Left, layout, statusBarView)
}

// RegisterScreen registers a screen in the TUI.
func (m *Model) RegisterScreen(id string, screen Screen) {
	m.screens[id] = screen
}

// GetSession returns the shared database session.
func (m *Model) GetSession() Session {
	return m.session
}

// GetTheme returns the current theme (legacy compatibility).
func (m *Model) GetTheme() *Theme {
	return m.theme
}

// GetThemeManager returns the theme manager.
func (m *Model) GetThemeManager() *theme.Manager {
	return m.themeManager
}

// CycleTheme advances to the next available theme and returns its name.
func (m *Model) CycleTheme() string {
	if len(m.themeNames) == 0 {
		return ""
	}
	m.themeIndex = (m.themeIndex + 1) % len(m.themeNames)
	themeName := m.themeNames[m.themeIndex]
	m.themeManager.SetTheme(themeName)
	return themeName
}

// GetStyles returns the shared style palette for screens.
func (m *Model) GetStyles() *types.StylePalette {
	if m.theme == nil {
		return nil
	}
	return m.theme.Palette()
}

// SetTheme updates the theme (legacy compatibility).
func (m *Model) SetTheme(t *Theme) {
	m.theme = t
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

// GetWidth returns the current terminal width
func (m *Model) GetWidth() int {
	return m.width
}
