package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/screens"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/observability"
	tea "github.com/charmbracelet/bubbletea"
)

// DatabaseBrowserAdapter wraps DatabaseBrowser to implement the Screen interface
type DatabaseBrowserAdapter struct {
	browser *screens.DatabaseBrowser
	app     *Model
}

func buildSystemCatalog(app *Model) *observability.SystemCatalog {
	if app == nil || app.GetSession() == nil {
		return nil
	}

	session := app.GetSession()
	sc := observability.NewSystemCatalog(nil, nil, session.Catalog())
	if provider := session.ShardMetricsProvider(); provider != nil {
		sc.SetShardMetricsProvider(provider)
	}
	return sc
}

// NewDatabaseBrowserAdapter creates a new adapter for DatabaseBrowser
func NewDatabaseBrowserAdapter(app *Model, width, height int) *DatabaseBrowserAdapter {
	browser := screens.NewDatabaseBrowser(width, height, app.GetThemeManager())

	if sc := buildSystemCatalog(app); sc != nil {
		browser.SetSystemCatalog(sc)
	}

	return &DatabaseBrowserAdapter{
		browser: browser,
		app:     app,
	}
}

// Init implements Screen interface
func (d *DatabaseBrowserAdapter) Init() tea.Cmd {
	return d.browser.Init()
}

// Update implements Screen interface
func (d *DatabaseBrowserAdapter) Update(msg tea.Msg) (types.Screen, tea.Cmd) {
	// Handle screen-specific quit keys
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc", "q":
			// Return to home screen
			return d, func() tea.Msg {
				return types.ScreenChangeMsg{ScreenID: "home"}
			}
		}
	}

	// Delegate to browser
	updatedBrowser, cmd := d.browser.Update(msg)
	d.browser = updatedBrowser
	return d, cmd
}

// View implements Screen interface
func (d *DatabaseBrowserAdapter) View() string {
	return d.browser.View()
}

// DashboardAdapter wraps Dashboard to implement the Screen interface
type DashboardAdapter struct {
	dashboard *screens.Dashboard
	app       *Model
}

// NewDashboardAdapter creates a new adapter for Dashboard
func NewDashboardAdapter(app *Model) *DashboardAdapter {
	dashboard := screens.NewDashboard()

	if sc := buildSystemCatalog(app); sc != nil {
		dashboard.SetSystemCatalog(sc)

		// Pass DatabaseManager if available for instance-level metrics
		if dbMgr := app.GetSession().GetDatabaseManager(); dbMgr != nil {
			dashboard.SetDatabaseManager(dbMgr)
		}
	}

	return &DashboardAdapter{
		dashboard: dashboard,
		app:       app,
	}
}

// Init implements Screen interface
func (d *DashboardAdapter) Init() tea.Cmd {
	return d.dashboard.Init()
}

// Update implements Screen interface
func (d *DashboardAdapter) Update(msg tea.Msg) (types.Screen, tea.Cmd) {
	// Handle screen-specific quit keys
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "esc", "q":
			// Return to home screen
			return d, func() tea.Msg {
				return types.ScreenChangeMsg{ScreenID: "home"}
			}
		}
	}

	// Delegate to dashboard
	updatedModel, cmd := d.dashboard.Update(msg)
	if updatedDashboard, ok := updatedModel.(*screens.Dashboard); ok {
		d.dashboard = updatedDashboard
	}
	return d, cmd
}

// View implements Screen interface
func (d *DashboardAdapter) View() string {
	return d.dashboard.View()
}
