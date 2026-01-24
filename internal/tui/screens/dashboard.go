// Package screens provides TUI screen implementations.
package screens

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/observability"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Dashboard displays system metrics and statistics.
type Dashboard struct {
	width  int
	height int

	// Metrics
	metrics *SystemMetrics

	// Update interval
	updateInterval time.Duration
	lastUpdate     time.Time

	// System catalog for real metrics
	sysCatalog *observability.SystemCatalog
}

// SystemMetrics holds database system metrics.
type SystemMetrics struct {
	// Connection stats
	ActiveConnections int
	TotalConnections  int
	MaxConnections    int

	// Query stats
	TotalQueries     int64
	QueriesPerSecond float64
	AverageQueryTime float64
	SlowQueries      int64

	// Memory stats
	MemoryUsed      int64
	MemoryAllocated int64
	MemoryLimit     int64

	// Transaction stats
	ActiveTransactions int
	CommittedTxns      int64
	RolledBackTxns     int64

	// Database stats
	TotalDatabases int
	TotalTables    int
	TotalRows      int64
	DataSize       int64

	// System stats
	Uptime        time.Duration
	CPUUsage      float64
	DiskUsage     float64
	CacheHitRatio float64
}

// NewDashboard creates a new dashboard screen.
func NewDashboard() *Dashboard {
	return &Dashboard{
		metrics:        &SystemMetrics{},
		updateInterval: 2 * time.Second,
		lastUpdate:     time.Now(),
		sysCatalog:     nil,
	}
}

// SetSystemCatalog sets the system catalog for real metrics.
func (d *Dashboard) SetSystemCatalog(sc *observability.SystemCatalog) {
	d.sysCatalog = sc
}

// Init initializes the dashboard.
func (d *Dashboard) Init() tea.Cmd {
	return tea.Batch(
		d.fetchMetrics(),
		d.tickCmd(),
	)
}

// Update handles dashboard updates.
func (d *Dashboard) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		d.width = msg.Width
		d.height = msg.Height
		return d, nil

	case tickMsg:
		if time.Since(d.lastUpdate) >= d.updateInterval {
			cmds = append(cmds, d.fetchMetrics())
		}
		cmds = append(cmds, d.tickCmd())
		return d, tea.Batch(cmds...)

	case metricsMsg:
		m := SystemMetrics(msg)
		d.metrics = &m
		d.lastUpdate = time.Now()
		return d, nil
	}

	return d, nil
}

// View renders the dashboard.
func (d *Dashboard) View() string {
	if d.width == 0 {
		return "Loading..."
	}

	var sections []string

	// Header
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#00D9FF")).
		MarginBottom(1)
	sections = append(sections, titleStyle.Render("📊 System Dashboard"))

	// Metrics grid (2 columns)
	col1 := d.renderConnectionMetrics()
	col2 := d.renderQueryMetrics()

	row1 := lipgloss.JoinHorizontal(
		lipgloss.Top,
		col1,
		strings.Repeat(" ", 4),
		col2,
	)
	sections = append(sections, row1)

	col3 := d.renderMemoryMetrics()
	col4 := d.renderTransactionMetrics()

	row2 := lipgloss.JoinHorizontal(
		lipgloss.Top,
		col3,
		strings.Repeat(" ", 4),
		col4,
	)
	sections = append(sections, row2)

	col5 := d.renderDatabaseMetrics()
	col6 := d.renderSystemMetrics()

	row3 := lipgloss.JoinHorizontal(
		lipgloss.Top,
		col5,
		strings.Repeat(" ", 4),
		col6,
	)
	sections = append(sections, row3)

	// Footer
	footer := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#6B7280")).
		Render(fmt.Sprintf("Last updated: %s | Press q to quit, r to refresh",
			d.lastUpdate.Format("15:04:05")))
	sections = append(sections, "\n"+footer)

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

func (d *Dashboard) renderConnectionMetrics() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3A3A3A")).
		Padding(1, 2).
		Width(40)

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FFB86C")).
		Render("🔌 Connections")

	usage := float64(0)
	if d.metrics.MaxConnections > 0 {
		usage = float64(d.metrics.ActiveConnections) / float64(d.metrics.MaxConnections)
	}
	bar := renderProgressBar(usage, 30)

	content := fmt.Sprintf(`%s

Active:  %d / %d
Total:   %d
Usage:   %.1f%%

%s`,
		title,
		d.metrics.ActiveConnections,
		d.metrics.MaxConnections,
		d.metrics.TotalConnections,
		usage*100,
		bar,
	)

	return style.Render(content)
}

func (d *Dashboard) renderQueryMetrics() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3A3A3A")).
		Padding(1, 2).
		Width(40)

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#50FA7B")).
		Render("⚡ Query Performance")

	content := fmt.Sprintf(`%s

Total Queries:   %s
Queries/sec:     %.2f
Avg Time:        %.2fms
Slow Queries:    %d`,
		title,
		formatNumber(d.metrics.TotalQueries),
		d.metrics.QueriesPerSecond,
		d.metrics.AverageQueryTime,
		d.metrics.SlowQueries,
	)

	return style.Render(content)
}

func (d *Dashboard) renderMemoryMetrics() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3A3A3A")).
		Padding(1, 2).
		Width(40)

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#BD93F9")).
		Render("💾 Memory")

	usage := float64(0)
	if d.metrics.MemoryLimit > 0 {
		usage = float64(d.metrics.MemoryUsed) / float64(d.metrics.MemoryLimit)
	}
	bar := renderProgressBar(usage, 30)

	content := fmt.Sprintf(`%s

Used:       %s / %s
Allocated:  %s
Usage:      %.1f%%

%s`,
		title,
		formatBytes(d.metrics.MemoryUsed),
		formatBytes(d.metrics.MemoryLimit),
		formatBytes(d.metrics.MemoryAllocated),
		usage*100,
		bar,
	)

	return style.Render(content)
}

func (d *Dashboard) renderTransactionMetrics() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3A3A3A")).
		Padding(1, 2).
		Width(40)

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FF79C6")).
		Render("🔄 Transactions")

	content := fmt.Sprintf(`%s

Active:      %d
Committed:   %s
Rolled Back: %s
Success Rate: %.1f%%`,
		title,
		d.metrics.ActiveTransactions,
		formatNumber(d.metrics.CommittedTxns),
		formatNumber(d.metrics.RolledBackTxns),
		float64(d.metrics.CommittedTxns)/(float64(d.metrics.CommittedTxns+d.metrics.RolledBackTxns)+0.001)*100,
	)

	return style.Render(content)
}

func (d *Dashboard) renderDatabaseMetrics() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3A3A3A")).
		Padding(1, 2).
		Width(40)

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#8BE9FD")).
		Render("🗄️  Database")

	content := fmt.Sprintf(`%s

Databases:   %d
Tables:      %d
Total Rows:  %s
Data Size:   %s`,
		title,
		d.metrics.TotalDatabases,
		d.metrics.TotalTables,
		formatNumber(d.metrics.TotalRows),
		formatBytes(d.metrics.DataSize),
	)

	return style.Render(content)
}

func (d *Dashboard) renderSystemMetrics() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#3A3A3A")).
		Padding(1, 2).
		Width(40)

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#F1FA8C")).
		Render("⚙️  System")

	content := fmt.Sprintf(`%s

Uptime:         %s
CPU Usage:      %.1f%%
Disk Usage:     %.1f%%
Cache Hit Rate: %.1f%%`,
		title,
		formatDuration(d.metrics.Uptime),
		d.metrics.CPUUsage,
		d.metrics.DiskUsage,
		d.metrics.CacheHitRatio*100,
	)

	return style.Render(content)
}

// Helper functions
func renderProgressBar(percentage float64, width int) string {
	if percentage < 0 {
		percentage = 0
	}
	if percentage > 1 {
		percentage = 1
	}

	if width <= 0 {
		width = 20
	}

	filled := int(float64(width) * percentage)
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}

	empty := width - filled
	if empty < 0 {
		empty = 0
	}

	fillStr := strings.Repeat("=", filled)
	emptyStr := strings.Repeat("-", empty)
	bar := "[" + fillStr + emptyStr + "]"

	// Color based on percentage
	style := lipgloss.NewStyle()
	if percentage > 0.8 {
		style = style.Foreground(lipgloss.Color("#FF5555")) // Red
	} else if percentage > 0.6 {
		style = style.Foreground(lipgloss.Color("#FFAA00")) // Yellow
	} else {
		style = style.Foreground(lipgloss.Color("#55FF55")) // Green
	}

	return style.Render(bar)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1000000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1000000000)
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}

// Messages
type tickMsg time.Time
type metricsMsg SystemMetrics

func (d *Dashboard) tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (d *Dashboard) fetchMetrics() tea.Cmd {
	return func() tea.Msg {
		if d.sysCatalog == nil {
			// No catalog available; return zeroed metrics
			return metricsMsg(SystemMetrics{})
		}

		stats := d.sysCatalog.Stats().Snapshot()
		uptime := time.Since(stats.StartTime)

		// Memory stats from runtime
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		avgQueryTimeMs := float64(0)
		if stats.QueriesExecuted > 0 {
			avgQueryTimeMs = float64(stats.TotalQueryTimeNs) / float64(stats.QueriesExecuted) / 1e6
		}

		queriesPerSecond := float64(0)
		if uptime.Seconds() > 0 {
			queriesPerSecond = float64(stats.QueriesExecuted) / uptime.Seconds()
		}

		cacheHitRatio := float64(0)
		if stats.IndexScans > 0 {
			cacheHitRatio = float64(stats.IndexHits) / float64(stats.IndexScans)
		}

		activeTxns := d.sysCatalog.GetActiveTransactions()
		tables := d.sysCatalog.GetTables()

		metrics := SystemMetrics{
			ActiveConnections: 0,
			TotalConnections:  0,
			MaxConnections:    100,

			TotalQueries:     stats.QueriesExecuted,
			QueriesPerSecond: queriesPerSecond,
			AverageQueryTime: avgQueryTimeMs,
			SlowQueries:      stats.QueriesFailed,

			MemoryUsed:      int64(m.HeapAlloc),
			MemoryAllocated: int64(m.HeapAlloc),
			MemoryLimit:     int64(m.HeapSys),

			ActiveTransactions: len(activeTxns),
			CommittedTxns:      stats.TransactionsCommitted,
			RolledBackTxns:     stats.TransactionsAborted,

			TotalDatabases: 1, // TODO: pull real count from catalog/manager
			TotalTables:    len(tables),
			TotalRows:      stats.RowsInserted, // proxy until row stats wired
			DataSize:       0,                  // TODO: disk usage tracking

			Uptime:        uptime,
			CPUUsage:      0, // TODO: CPU tracking
			DiskUsage:     0, // TODO: disk tracking
			CacheHitRatio: cacheHitRatio,
		}

		return metricsMsg(metrics)
	}
}
