package screens

import (
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

func TestNewDashboard(t *testing.T) {
	dash := NewDashboard()
	if dash.metrics == nil {
		t.Error("Dashboard metrics should not be nil")
	}
	if dash.updateInterval != 2*time.Second {
		t.Errorf("Expected update interval 2s, got %v", dash.updateInterval)
	}
}

func TestDashboardInit(t *testing.T) {
	dash := NewDashboard()
	cmd := dash.Init()
	if cmd == nil {
		t.Error("Init should return a command")
	}
}

func TestDashboardUpdateWindowSize(t *testing.T) {
	dash := NewDashboard()
	msg := tea.WindowSizeMsg{Width: 120, Height: 40}

	model, _ := dash.Update(msg)
	updatedDash := model.(*Dashboard)

	if updatedDash.width != 120 {
		t.Errorf("Expected width 120, got %d", updatedDash.width)
	}
	if updatedDash.height != 40 {
		t.Errorf("Expected height 40, got %d", updatedDash.height)
	}
}

func TestDashboardUpdateMetrics(t *testing.T) {
	dash := NewDashboard()
	metrics := SystemMetrics{
		ActiveConnections: 10,
		TotalQueries:      1000,
		MemoryUsed:        1024 * 1024 * 512, // 512MB
	}

	msg := metricsMsg(metrics)
	model, _ := dash.Update(msg)
	updatedDash := model.(*Dashboard)

	if updatedDash.metrics.ActiveConnections != 10 {
		t.Errorf("Expected 10 active connections, got %d", updatedDash.metrics.ActiveConnections)
	}
	if updatedDash.metrics.TotalQueries != 1000 {
		t.Errorf("Expected 1000 total queries, got %d", updatedDash.metrics.TotalQueries)
	}
}

func TestDashboardView(t *testing.T) {
	dash := NewDashboard()
	dash.width = 120
	dash.height = 40

	view := dash.View()
	if view == "" {
		t.Error("View should not be empty")
	}
	if view == "Loading..." {
		t.Error("View should not show loading with dimensions set")
	}
}

func TestDashboardViewLoading(t *testing.T) {
	dash := NewDashboard()
	view := dash.View()
	if view != "Loading..." {
		t.Error("View should show loading without dimensions")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, test := range tests {
		result := formatBytes(test.input)
		if result != test.expected {
			t.Errorf("formatBytes(%d) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{500, "500"},
		{1500, "1.5K"},
		{1000000, "1.0M"},
		{1500000000, "1.5B"},
	}

	for _, test := range tests {
		result := formatNumber(test.input)
		if result != test.expected {
			t.Errorf("formatNumber(%d) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{30 * time.Minute, "30m"},
		{2 * time.Hour, "2h 0m"},
		{25 * time.Hour, "1d 1h 0m"},
		{50 * time.Hour, "2d 2h 0m"},
	}

	for _, test := range tests {
		result := formatDuration(test.input)
		if result != test.expected {
			t.Errorf("formatDuration(%v) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestSystemMetricsIntegration(t *testing.T) {
	dash := NewDashboard()
	dash.width = 120
	dash.height = 40

	// Set comprehensive metrics
	dash.metrics = &SystemMetrics{
		ActiveConnections: 20,
		TotalConnections:  500,
		MaxConnections:    100,

		TotalQueries:     10000,
		QueriesPerSecond: 50.5,
		AverageQueryTime: 15.2,
		SlowQueries:      5,

		MemoryUsed:      2147483648,
		MemoryAllocated: 3221225472,
		MemoryLimit:     4294967296,

		ActiveTransactions: 3,
		CommittedTxns:      5000,
		RolledBackTxns:     50,

		TotalDatabases: 5,
		TotalTables:    50,
		TotalRows:      100000,
		DataSize:       104857600,

		Uptime:        48 * time.Hour,
		CPUUsage:      45.5,
		DiskUsage:     70.0,
		CacheHitRatio: 0.92,
	}

	view := dash.View()
	if len(view) < 100 {
		t.Error("Dashboard view should be comprehensive")
	}

	// Check that view contains metrics sections
	if !contains(view, "Connections") {
		t.Error("View should contain Connections section")
	}
	if !contains(view, "Query Performance") {
		t.Error("View should contain Query Performance section")
	}
	if !contains(view, "Memory") {
		t.Error("View should contain Memory section")
	}
	if !contains(view, "Transactions") {
		t.Error("View should contain Transactions section")
	}
	if !contains(view, "Database") {
		t.Error("View should contain Database section")
	}
	if !contains(view, "System") {
		t.Error("View should contain System section")
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) >= len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
