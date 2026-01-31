// Package layout provides responsive lipgloss-based layout utilities for the TUI.
package layout

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/charmbracelet/lipgloss"
)

// Breakpoint defines responsive breakpoints.
type Breakpoint int

const (
	// BreakpointSmall is for terminals < 80 columns.
	BreakpointSmall Breakpoint = iota
	// BreakpointMedium is for terminals 80-120 columns.
	BreakpointMedium
	// BreakpointLarge is for terminals 120-160 columns.
	BreakpointLarge
	// BreakpointXLarge is for terminals > 160 columns.
	BreakpointXLarge
)

// GetBreakpoint returns the breakpoint for a given width.
func GetBreakpoint(width int) Breakpoint {
	switch {
	case width < 80:
		return BreakpointSmall
	case width < 120:
		return BreakpointMedium
	case width < 160:
		return BreakpointLarge
	default:
		return BreakpointXLarge
	}
}

// ResponsiveConfig holds responsive layout configuration.
type ResponsiveConfig struct {
	// Minimum dimensions
	MinWidth  int
	MinHeight int

	// Padding/margin settings
	OuterPadding int
	InnerPadding int
	GapSize      int

	// Border style
	UseBorders     bool
	UseRoundBorder bool

	// Theme reference
	Theme *theme.Theme
}

// DefaultResponsiveConfig returns sensible defaults.
func DefaultResponsiveConfig() *ResponsiveConfig {
	return &ResponsiveConfig{
		MinWidth:       80,
		MinHeight:      24,
		OuterPadding:   0,
		InnerPadding:   1,
		GapSize:        0,
		UseBorders:     true,
		UseRoundBorder: true,
	}
}

// Responsive provides responsive layout utilities.
type Responsive struct {
	config     *ResponsiveConfig
	width      int
	height     int
	breakpoint Breakpoint
}

// NewResponsive creates a new responsive layout helper.
func NewResponsive(config *ResponsiveConfig) *Responsive {
	if config == nil {
		config = DefaultResponsiveConfig()
	}
	return &Responsive{
		config:     config,
		breakpoint: BreakpointMedium,
	}
}

// SetDimensions updates the available dimensions.
func (r *Responsive) SetDimensions(width, height int) {
	r.width = width
	r.height = height
	r.breakpoint = GetBreakpoint(width)
}

// SetTheme updates the theme reference.
func (r *Responsive) SetTheme(t *theme.Theme) {
	r.config.Theme = t
}

// GetBreakpoint returns the current breakpoint.
func (r *Responsive) GetBreakpoint() Breakpoint {
	return r.breakpoint
}

// ContentWidth returns the width available for content after padding/borders.
func (r *Responsive) ContentWidth() int {
	w := r.width - (r.config.OuterPadding * 2)
	if r.config.UseBorders {
		w -= 2 // Border takes 1 char on each side
	}
	return max(w, r.config.MinWidth)
}

// ContentHeight returns the height available for content after padding/borders.
func (r *Responsive) ContentHeight() int {
	h := r.height - (r.config.OuterPadding * 2)
	if r.config.UseBorders {
		h -= 2 // Border takes 1 char on top and bottom
	}
	return max(h, 1)
}

// ============================================================================
// Pane Layout (Multi-pane splits)
// ============================================================================

// PaneRatio defines the ratio for pane splitting.
type PaneRatio struct {
	// Fixed width (if > 0, takes precedence over Ratio)
	Fixed int
	// Ratio of remaining space (0.0 to 1.0)
	Ratio float64
	// Minimum width
	Min int
	// Maximum width (0 = no max)
	Max int
	// Collapsed state
	Collapsed bool
}

// PaneLayout creates a horizontal split layout.
type PaneLayout struct {
	responsive *Responsive
	panes      []PaneRatio
	gap        int
}

// NewPaneLayout creates a new pane layout.
func NewPaneLayout(responsive *Responsive, gap int) *PaneLayout {
	return &PaneLayout{
		responsive: responsive,
		panes:      make([]PaneRatio, 0),
		gap:        gap,
	}
}

// AddPane adds a pane with the specified ratio configuration.
func (p *PaneLayout) AddPane(ratio PaneRatio) *PaneLayout {
	p.panes = append(p.panes, ratio)
	return p
}

// CalculateWidths calculates the width for each pane.
func (p *PaneLayout) CalculateWidths(totalWidth int) []int {
	if len(p.panes) == 0 {
		return nil
	}

	// Account for gaps
	visiblePanes := 0
	for _, pane := range p.panes {
		if !pane.Collapsed {
			visiblePanes++
		}
	}
	gapTotal := (visiblePanes - 1) * p.gap
	available := totalWidth - gapTotal

	widths := make([]int, len(p.panes))
	remaining := available

	// First pass: allocate fixed widths
	for i, pane := range p.panes {
		if pane.Collapsed {
			widths[i] = 0
			continue
		}
		if pane.Fixed > 0 {
			w := min(pane.Fixed, remaining)
			w = max(w, pane.Min)
			if pane.Max > 0 {
				w = min(w, pane.Max)
			}
			widths[i] = w
			remaining -= w
		}
	}

	// Second pass: allocate ratio-based widths
	totalRatio := 0.0
	for i, pane := range p.panes {
		if !pane.Collapsed && pane.Fixed == 0 {
			totalRatio += pane.Ratio
		}
		_ = i
	}

	if totalRatio > 0 {
		for i, pane := range p.panes {
			if pane.Collapsed || pane.Fixed > 0 {
				continue
			}
			w := int(float64(remaining) * (pane.Ratio / totalRatio))
			w = max(w, pane.Min)
			if pane.Max > 0 {
				w = min(w, pane.Max)
			}
			widths[i] = w
		}
	}

	return widths
}

// ============================================================================
// Card Layout (Bordered containers)
// ============================================================================

// CardStyle defines the visual style for a card.
type CardStyle struct {
	// Border settings
	ShowBorder   bool
	RoundBorder  bool
	DoubleBorder bool
	ThickBorder  bool

	// Colors (from theme if nil)
	BorderColor     string
	BackgroundColor string
	TitleColor      string

	// Spacing
	Padding int
	Margin  int

	// Title alignment
	TitleAlign lipgloss.Position

	// Focus state
	Focused bool
}

// DefaultCardStyle returns a sensible default card style.
func DefaultCardStyle() CardStyle {
	return CardStyle{
		ShowBorder:  true,
		RoundBorder: true,
		Padding:     1,
		Margin:      0,
		TitleAlign:  lipgloss.Left,
	}
}

// Card renders a bordered card container.
func Card(title, content string, width, height int, style CardStyle, t *theme.Theme) string {
	// Determine border style
	var borderStyle lipgloss.Border
	switch {
	case style.DoubleBorder:
		borderStyle = lipgloss.DoubleBorder()
	case style.ThickBorder:
		borderStyle = lipgloss.ThickBorder()
	case style.RoundBorder:
		borderStyle = lipgloss.RoundedBorder()
	default:
		borderStyle = lipgloss.NormalBorder()
	}

	// Get colors from theme or style
	borderColor := style.BorderColor
	bgColor := style.BackgroundColor
	titleColor := style.TitleColor

	if t != nil {
		if borderColor == "" {
			if style.Focused {
				borderColor = t.BrandFocus
			} else {
				borderColor = t.Border
			}
		}
		if titleColor == "" {
			if style.Focused {
				titleColor = t.BrandAccent
			} else {
				titleColor = t.Primary
			}
		}
	}

	// Build the container style
	containerStyle := lipgloss.NewStyle()
	if style.ShowBorder {
		containerStyle = containerStyle.
			Border(borderStyle).
			BorderForeground(lipgloss.Color(borderColor))
	}
	if bgColor != "" {
		containerStyle = containerStyle.Background(lipgloss.Color(bgColor))
	}

	// Calculate content dimensions
	contentWidth := width - (style.Padding * 2) - (style.Margin * 2)
	contentHeight := height - (style.Padding * 2) - (style.Margin * 2)
	if style.ShowBorder {
		contentWidth -= 2
		contentHeight -= 2
	}
	if title != "" {
		contentHeight -= 1
	}

	contentWidth = max(contentWidth, 1)
	contentHeight = max(contentHeight, 1)

	// Build title bar
	var titleBar string
	if title != "" {
		titleStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color(titleColor)).
			Bold(true).
			Width(contentWidth).
			Align(style.TitleAlign)
		titleBar = titleStyle.Render(title)
	}

	// Apply padding to content
	contentStyle := lipgloss.NewStyle().
		Width(contentWidth).
		Height(contentHeight)

	renderedContent := contentStyle.Render(content)

	// Combine title and content
	var inner string
	if title != "" {
		inner = lipgloss.JoinVertical(lipgloss.Left, titleBar, renderedContent)
	} else {
		inner = renderedContent
	}

	// Apply container style
	containerStyle = containerStyle.
		Padding(style.Padding - 1). // -1 because content already has some padding
		Margin(style.Margin)

	return containerStyle.Render(inner)
}

// ============================================================================
// Table Layout (Styled tables with alternating rows)
// ============================================================================

// TableStyle defines the visual style for a table.
type TableStyle struct {
	// Header styling
	HeaderBold       bool
	HeaderBackground string
	HeaderForeground string

	// Row styling
	RowEvenBackground  string
	RowOddBackground   string
	SelectedBackground string
	SelectedForeground string

	// Border styling
	ShowBorder  bool
	BorderColor string

	// Cell padding
	CellPadding int
}

// DefaultTableStyle returns a sensible default table style from theme.
func DefaultTableStyle(t *theme.Theme) TableStyle {
	if t == nil {
		return TableStyle{
			HeaderBold:  true,
			ShowBorder:  true,
			CellPadding: 1,
		}
	}
	return TableStyle{
		HeaderBold:         true,
		HeaderBackground:   "",
		HeaderForeground:   t.TableHeader,
		RowEvenBackground:  t.TableRowEven,
		RowOddBackground:   t.TableRowOdd,
		SelectedBackground: t.BrandSelection,
		SelectedForeground: t.BrandAccent,
		ShowBorder:         true,
		BorderColor:        t.TableBorder,
		CellPadding:        1,
	}
}

// TableColumn defines a table column.
type TableColumn struct {
	Header string
	Width  int
	Align  lipgloss.Position
}

// TableRow represents a table row.
type TableRow struct {
	Cells    []string
	Selected bool
}

// Table renders a styled table.
func Table(columns []TableColumn, rows []TableRow, style TableStyle, maxHeight int) string {
	if len(columns) == 0 {
		return ""
	}

	var result strings.Builder

	// Calculate column widths if not specified
	for i := range columns {
		if columns[i].Width == 0 {
			columns[i].Width = len(columns[i].Header) + (style.CellPadding * 2)
		}
	}

	// Render header
	headerStyle := lipgloss.NewStyle().Bold(style.HeaderBold)
	if style.HeaderForeground != "" {
		headerStyle = headerStyle.Foreground(lipgloss.Color(style.HeaderForeground))
	}
	if style.HeaderBackground != "" {
		headerStyle = headerStyle.Background(lipgloss.Color(style.HeaderBackground))
	}

	var headerCells []string
	for _, col := range columns {
		cellStyle := headerStyle.Width(col.Width).Align(col.Align).Padding(0, style.CellPadding)
		headerCells = append(headerCells, cellStyle.Render(col.Header))
	}
	result.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, headerCells...))
	result.WriteString("\n")

	// Render separator
	if style.ShowBorder {
		sepStyle := lipgloss.NewStyle()
		if style.BorderColor != "" {
			sepStyle = sepStyle.Foreground(lipgloss.Color(style.BorderColor))
		}
		totalWidth := 0
		for _, col := range columns {
			totalWidth += col.Width + (style.CellPadding * 2)
		}
		result.WriteString(sepStyle.Render(strings.Repeat("─", totalWidth)))
		result.WriteString("\n")
	}

	// Render rows
	displayRows := rows
	if maxHeight > 0 && len(rows) > maxHeight-2 { // -2 for header and separator
		displayRows = rows[:maxHeight-2]
	}

	for i, row := range displayRows {
		rowStyle := lipgloss.NewStyle()
		if row.Selected {
			if style.SelectedBackground != "" {
				rowStyle = rowStyle.Background(lipgloss.Color(style.SelectedBackground))
			}
			if style.SelectedForeground != "" {
				rowStyle = rowStyle.Foreground(lipgloss.Color(style.SelectedForeground))
			}
			rowStyle = rowStyle.Bold(true)
		} else if i%2 == 0 {
			if style.RowEvenBackground != "" {
				rowStyle = rowStyle.Background(lipgloss.Color(style.RowEvenBackground))
			}
		} else {
			if style.RowOddBackground != "" {
				rowStyle = rowStyle.Background(lipgloss.Color(style.RowOddBackground))
			}
		}

		var cells []string
		for j, col := range columns {
			cellContent := ""
			if j < len(row.Cells) {
				cellContent = row.Cells[j]
			}
			cellStyle := rowStyle.Width(col.Width).Align(col.Align).Padding(0, style.CellPadding)
			cells = append(cells, cellStyle.Render(truncate(cellContent, col.Width)))
		}
		result.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, cells...))
		result.WriteString("\n")
	}

	return result.String()
}

// ============================================================================
// Stack Layout (Vertical/Horizontal stacking)
// ============================================================================

// StackDirection defines the stack direction.
type StackDirection int

const (
	StackVertical StackDirection = iota
	StackHorizontal
)

// StackItem represents an item in a stack layout.
type StackItem struct {
	Content string
	Grow    bool // If true, takes remaining space
	Size    int  // Fixed size (if Grow is false)
}

// Stack renders items in a stack layout.
func Stack(items []StackItem, direction StackDirection, totalSize int, gap int) string {
	if len(items) == 0 {
		return ""
	}

	// Calculate sizes
	fixedTotal := 0
	growCount := 0
	for _, item := range items {
		if item.Grow {
			growCount++
		} else {
			fixedTotal += item.Size
		}
	}

	gapTotal := (len(items) - 1) * gap
	remaining := totalSize - fixedTotal - gapTotal

	growSize := 0
	if growCount > 0 {
		growSize = remaining / growCount
	}

	// Render items
	var rendered []string
	for _, item := range items {
		size := item.Size
		if item.Grow {
			size = growSize
		}

		var style lipgloss.Style
		if direction == StackVertical {
			style = lipgloss.NewStyle().Height(size)
		} else {
			style = lipgloss.NewStyle().Width(size)
		}

		rendered = append(rendered, style.Render(item.Content))
	}

	// Join with appropriate direction
	if direction == StackVertical {
		return lipgloss.JoinVertical(lipgloss.Left, rendered...)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, rendered...)
}

// ============================================================================
// Helper Functions
// ============================================================================

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
