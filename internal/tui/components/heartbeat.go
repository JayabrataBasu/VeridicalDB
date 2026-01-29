package components

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// HeartbeatModel represents a live system heartbeat widget with Braille charts.
type HeartbeatModel struct {
	spinner    spinner.Model
	cpuHistory []float64
	memHistory []float64
	maxPoints  int
	width      int
	height     int
	lastUpdate time.Time
	style      lipgloss.Style
	labelStyle lipgloss.Style
	valueStyle lipgloss.Style
}

// HeartbeatTickMsg is sent to update the heartbeat data.
type HeartbeatTickMsg time.Time

// NewHeartbeatModel creates a new heartbeat widget.
func NewHeartbeatModel(width, height int) HeartbeatModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("#7dce13"))

	maxPoints := width - 20 // Reserve space for labels
	if maxPoints < 10 {
		maxPoints = 10
	}

	return HeartbeatModel{
		spinner:    s,
		cpuHistory: make([]float64, 0, maxPoints),
		memHistory: make([]float64, 0, maxPoints),
		maxPoints:  maxPoints,
		width:      width,
		height:     height,
		lastUpdate: time.Now(),
		style: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#21262d")).
			Padding(1, 2),
		labelStyle: lipgloss.NewStyle().
			Foreground(lipgloss.Color("#6e7681")).
			Bold(true),
		valueStyle: lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7dce13")).
			Bold(true),
	}
}

// Init initializes the heartbeat model.
func (m HeartbeatModel) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		heartbeatTick(),
	)
}

// Update handles messages for the heartbeat widget.
func (m HeartbeatModel) Update(msg tea.Msg) (HeartbeatModel, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case HeartbeatTickMsg:
		// Simulate CPU and memory metrics (replace with real metrics if available)
		m.addDataPoint(rand.Float64()*30+40, rand.Float64()*20+50) // CPU: 40-70%, Mem: 50-70%
		m.lastUpdate = time.Time(msg)
		return m, heartbeatTick()

	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	return m, nil
}

// View renders the heartbeat widget.
func (m HeartbeatModel) View() string {
	var b strings.Builder

	// Title with spinner
	title := fmt.Sprintf("%s System Heartbeat", m.spinner.View())
	b.WriteString(m.valueStyle.Render(title))
	b.WriteString("\n\n")

	// Current values
	cpu := 0.0
	mem := 0.0
	if len(m.cpuHistory) > 0 {
		cpu = m.cpuHistory[len(m.cpuHistory)-1]
		mem = m.memHistory[len(m.memHistory)-1]
	}

	cpuColor := m.getColorForValue(cpu)
	memColor := m.getColorForValue(mem)

	b.WriteString(m.labelStyle.Render("CPU: "))
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color(cpuColor)).Bold(true).Render(fmt.Sprintf("%.1f%%", cpu)))
	b.WriteString("  ")
	b.WriteString(m.labelStyle.Render("MEM: "))
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color(memColor)).Bold(true).Render(fmt.Sprintf("%.1f%%", mem)))
	b.WriteString("\n\n")

	// Braille charts
	cpuChart := m.renderBrailleChart(m.cpuHistory, cpuColor)
	memChart := m.renderBrailleChart(m.memHistory, memColor)

	b.WriteString(m.labelStyle.Render("CPU ") + cpuChart + "\n")
	b.WriteString(m.labelStyle.Render("MEM ") + memChart + "\n")

	return m.style.Width(m.width).Render(b.String())
}

// addDataPoint adds a new data point to the history.
func (m *HeartbeatModel) addDataPoint(cpu, mem float64) {
	m.cpuHistory = append(m.cpuHistory, cpu)
	m.memHistory = append(m.memHistory, mem)

	// Keep only maxPoints
	if len(m.cpuHistory) > m.maxPoints {
		m.cpuHistory = m.cpuHistory[len(m.cpuHistory)-m.maxPoints:]
	}
	if len(m.memHistory) > m.maxPoints {
		m.memHistory = m.memHistory[len(m.memHistory)-m.maxPoints:]
	}
}

// renderBrailleChart renders a Braille-based line chart.
func (m HeartbeatModel) renderBrailleChart(data []float64, color string) string {
	if len(data) == 0 {
		return m.labelStyle.Render("No data")
	}

	// Braille characters for different heights (0-7 dots)
	// Using a simplified approach: map percentages to Braille heights
	brailleChars := []string{"⠀", "⠁", "⠃", "⠇", "⡇", "⡗", "⡷", "⣷", "⣿"}

	var chart strings.Builder
	chartStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(color))

	for _, value := range data {
		// Map 0-100% to 0-8 index
		idx := int(math.Round(value / 100.0 * 8))
		if idx > 8 {
			idx = 8
		}
		if idx < 0 {
			idx = 0
		}
		chart.WriteString(chartStyle.Render(brailleChars[idx]))
	}

	return chart.String()
}

// getColorForValue returns a color based on the value percentage.
func (m HeartbeatModel) getColorForValue(value float64) string {
	switch {
	case value < 50:
		return "#7dce13" // Green - low usage
	case value < 75:
		return "#f0b429" // Yellow - moderate usage
	default:
		return "#ff5370" // Red - high usage
	}
}

// heartbeatTick returns a command that sends a HeartbeatTickMsg after a delay.
func heartbeatTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return HeartbeatTickMsg(t)
	})
}

// SetSize updates the widget dimensions.
func (m *HeartbeatModel) SetSize(width, height int) {
	m.width = width
	m.height = height
	maxPoints := width - 20
	if maxPoints < 10 {
		maxPoints = 10
	}
	m.maxPoints = maxPoints
}
