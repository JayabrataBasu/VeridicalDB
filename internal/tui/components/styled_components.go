// Package components provides reusable TUI components with brand styling.
package components

import (
	"fmt"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// SpinnerFrame represents animation frames for spinners.
type SpinnerFrame []string

// Common spinner frame sets.
var (
	// SpinnerDots is a dot-based spinner (Nerd Font compatible). Choso moment.
	SpinnerDots = SpinnerFrame{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

	// SpinnerLine is a classic line spinner. Around the World.
	SpinnerLine = SpinnerFrame{"|", "/", "-", "\\"}

	// SpinnerPulse is a pulsing dot. Around l'world
	SpinnerPulse = SpinnerFrame{"◐", "◓", "◑", "◒"}

	// SpinnerBars is a bar-based spinner.
	SpinnerBars = SpinnerFrame{"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█", "▇", "▆", "▅", "▄", "▃", "▂"}

	// SpinnerArrow is an arrow spinner.
	SpinnerArrow = SpinnerFrame{"←", "↖", "↑", "↗", "→", "↘", "↓", "↙"}
)

// AsyncSpinner provides an animated spinner for async operations.
type AsyncSpinner struct {
	frames       SpinnerFrame
	frameIndex   int
	message      string
	running      bool
	interval     time.Duration
	style        lipgloss.Style
	messageStyle lipgloss.Style
}

// NewAsyncSpinner creates a new spinner with the given frames and theme.
func NewAsyncSpinner(frames SpinnerFrame, t *theme.Theme) *AsyncSpinner {
	spinnerStyle := lipgloss.NewStyle().Bold(true)
	msgStyle := lipgloss.NewStyle()

	if t != nil {
		spinnerStyle = spinnerStyle.Foreground(lipgloss.Color(t.BrandAccent))
		msgStyle = msgStyle.Foreground(lipgloss.Color(t.Foreground))
	}

	return &AsyncSpinner{
		frames:       frames,
		frameIndex:   0,
		interval:     100 * time.Millisecond,
		style:        spinnerStyle,
		messageStyle: msgStyle,
	}
}

// SpinnerTickMsg triggers the next spinner frame.
type SpinnerTickMsg struct {
	ID string
}

// Start begins the spinner animation.
func (s *AsyncSpinner) Start() tea.Cmd {
	s.running = true
	return s.tick()
}

// Stop stops the spinner animation.
func (s *AsyncSpinner) Stop() {
	s.running = false
}

// SetMessage updates the spinner message.
func (s *AsyncSpinner) SetMessage(msg string) {
	s.message = msg
}

// Update handles spinner tick messages.
func (s *AsyncSpinner) Update(msg tea.Msg) tea.Cmd {
	switch msg.(type) {
	case SpinnerTickMsg:
		if s.running {
			s.frameIndex = (s.frameIndex + 1) % len(s.frames)
			return s.tick()
		}
	}
	return nil
}

// View renders the spinner.
func (s *AsyncSpinner) View() string {
	if !s.running {
		return ""
	}

	frame := s.style.Render(s.frames[s.frameIndex])
	if s.message != "" {
		return frame + " " + s.messageStyle.Render(s.message)
	}
	return frame
}

func (s *AsyncSpinner) tick() tea.Cmd {
	return tea.Tick(s.interval, func(t time.Time) tea.Msg {
		return SpinnerTickMsg{}
	})
}

// ============================================================================
// Progress Card
// ============================================================================

// ProgressCard displays progress with a branded card container.
type ProgressCard struct {
	title       string
	current     int
	total       int
	message     string
	showPercent bool
	width       int
	theme       *theme.Theme
	style       ProgressCardStyle
}

// ProgressCardStyle defines the visual style.
type ProgressCardStyle struct {
	BarFilled    lipgloss.Style
	BarEmpty     lipgloss.Style
	TitleStyle   lipgloss.Style
	MessageStyle lipgloss.Style
	BorderStyle  lipgloss.Style
}

// NewProgressCard creates a new progress card.
func NewProgressCard(title string, total int, t *theme.Theme) *ProgressCard {
	style := ProgressCardStyle{}
	if t != nil {
		style.BarFilled = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandAccent)).
			Foreground(lipgloss.Color(t.Background))
		style.BarEmpty = lipgloss.NewStyle().
			Background(lipgloss.Color(t.BrandMuted))
		style.TitleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.BrandAccent)).
			Bold(true)
		style.MessageStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Muted))
		style.BorderStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BrandAccent)).
			Padding(1, 2)
	}

	return &ProgressCard{
		title:       title,
		total:       total,
		showPercent: true,
		width:       40,
		theme:       t,
		style:       style,
	}
}

// SetProgress updates the current progress.
func (p *ProgressCard) SetProgress(current int) {
	p.current = current
}

// SetMessage updates the progress message.
func (p *ProgressCard) SetMessage(msg string) {
	p.message = msg
}

// SetWidth sets the card width.
func (p *ProgressCard) SetWidth(width int) {
	p.width = width
}

// View renders the progress card.
func (p *ProgressCard) View() string {
	// Calculate percentage
	percent := 0.0
	if p.total > 0 {
		percent = float64(p.current) / float64(p.total)
	}

	// Build progress bar
	barWidth := p.width - 10 // Leave room for percentage
	if barWidth < 10 {
		barWidth = 10
	}

	filledWidth := int(float64(barWidth) * percent)
	emptyWidth := barWidth - filledWidth

	filled := p.style.BarFilled.Render(strings.Repeat("█", filledWidth))
	empty := p.style.BarEmpty.Render(strings.Repeat("░", emptyWidth))
	bar := filled + empty

	// Build percentage text
	percentText := ""
	if p.showPercent {
		percentText = lipgloss.NewStyle().
			Foreground(lipgloss.Color(p.theme.BrandAccent)).
			Bold(true).
			Render(strings.Repeat(" ", 2) + formatPercent(percent))
	}

	// Combine elements
	var content strings.Builder
	content.WriteString(p.style.TitleStyle.Render(p.title))
	content.WriteString("\n\n")
	content.WriteString(bar + percentText)

	if p.message != "" {
		content.WriteString("\n\n")
		content.WriteString(p.style.MessageStyle.Render(p.message))
	}

	return p.style.BorderStyle.Width(p.width).Render(content.String())
}

func formatPercent(p float64) string {
	return strings.TrimRight(strings.TrimRight(
		strings.Replace(
			lipgloss.NewStyle().Render(strings.Repeat(" ", 3-len(strings.Split(
				strings.TrimRight(strings.TrimRight(
					strings.Replace(lipgloss.NewStyle().Render(""), ".", ",", 1),
					"0"),
					","),
				",")[0]))+
				strings.Split(
					strings.TrimRight(strings.TrimRight(
						strings.Replace(lipgloss.NewStyle().Render(""), ".", ",", 1),
						"0"),
						","),
					",")[0]),
			".", ",", 1),
		"0"),
		",") + lipgloss.NewStyle().Render(formatPercentValue(p))
}

func formatPercentValue(p float64) string {
	pct := int(p * 100)
	pctStr := fmt.Sprintf("%3d%%", pct)
	return pctStr
}

// ============================================================================
// Modal Dialog
// ============================================================================

// ModalType defines the type of modal dialog.
type ModalType int

const (
	ModalInfo ModalType = iota
	ModalConfirm
	ModalInput
	ModalError
	ModalWarning
)

// ModalButton represents a modal button.
type ModalButton struct {
	Label    string
	Key      string // Keyboard shortcut
	Primary  bool
	OnSelect func() tea.Msg
}

// ModalDialog provides a modal dialog overlay.
type ModalDialog struct {
	title      string
	message    string
	modalType  ModalType
	buttons    []ModalButton
	activeBtn  int
	inputValue string
	inputLabel string
	width      int
	theme      *theme.Theme
	visible    bool
}

// NewModalDialog creates a new modal dialog.
func NewModalDialog(title, message string, modalType ModalType, t *theme.Theme) *ModalDialog {
	return &ModalDialog{
		title:     title,
		message:   message,
		modalType: modalType,
		buttons:   make([]ModalButton, 0),
		width:     50,
		theme:     t,
		visible:   false,
	}
}

// AddButton adds a button to the modal.
func (m *ModalDialog) AddButton(btn ModalButton) *ModalDialog {
	m.buttons = append(m.buttons, btn)
	return m
}

// SetInputLabel sets the input field label for input modals.
func (m *ModalDialog) SetInputLabel(label string) {
	m.inputLabel = label
}

// GetInputValue returns the current input value.
func (m *ModalDialog) GetInputValue() string {
	return m.inputValue
}

// Show displays the modal.
func (m *ModalDialog) Show() {
	m.visible = true
	m.activeBtn = 0
}

// Hide hides the modal.
func (m *ModalDialog) Hide() {
	m.visible = false
}

// IsVisible returns whether the modal is visible.
func (m *ModalDialog) IsVisible() bool {
	return m.visible
}

// Update handles modal input.
func (m *ModalDialog) Update(msg tea.Msg) tea.Cmd {
	if !m.visible {
		return nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "tab", "right", "l":
			m.activeBtn = (m.activeBtn + 1) % len(m.buttons)
		case "shift+tab", "left", "h":
			m.activeBtn = (m.activeBtn - 1 + len(m.buttons)) % len(m.buttons)
		case "enter":
			if len(m.buttons) > 0 && m.buttons[m.activeBtn].OnSelect != nil {
				return func() tea.Msg { return m.buttons[m.activeBtn].OnSelect() }
			}
		case "esc":
			m.Hide()
		default:
			// Handle keyboard shortcuts
			for i, btn := range m.buttons {
				if strings.EqualFold(msg.String(), btn.Key) {
					m.activeBtn = i
					if btn.OnSelect != nil {
						return func() tea.Msg { return btn.OnSelect() }
					}
				}
			}

			// Handle input for input modals
			if m.modalType == ModalInput {
				if msg.String() == "backspace" && len(m.inputValue) > 0 {
					m.inputValue = m.inputValue[:len(m.inputValue)-1]
				} else if len(msg.String()) == 1 {
					m.inputValue += msg.String()
				}
			}
		}
	}

	return nil
}

// View renders the modal.
func (m *ModalDialog) View() string {
	if !m.visible {
		return ""
	}

	t := m.theme

	// Determine colors based on modal type
	var borderColor, titleColor string
	if t != nil {
		switch m.modalType {
		case ModalError:
			borderColor = t.BrandDanger
			titleColor = t.BrandDanger
		case ModalWarning:
			borderColor = t.BrandWarning
			titleColor = t.BrandWarning
		case ModalConfirm:
			borderColor = t.BrandAccent
			titleColor = t.BrandAccent
		default:
			borderColor = t.BrandAccent
			titleColor = t.Primary
		}
	}

	// Build title
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(titleColor)).
		Width(m.width - 4).
		Align(lipgloss.Center)

	// Build message
	messageStyle := lipgloss.NewStyle().
		Width(m.width - 4).
		Align(lipgloss.Center)
	if t != nil {
		messageStyle = messageStyle.Foreground(lipgloss.Color(t.Foreground))
	}

	// Build input field for input modals
	var inputField string
	if m.modalType == ModalInput {
		inputStyle := lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			Width(m.width-8).
			Padding(0, 1)
		if t != nil {
			inputStyle = inputStyle.BorderForeground(lipgloss.Color(t.Border))
		}
		labelStyle := lipgloss.NewStyle()
		if t != nil {
			labelStyle = labelStyle.Foreground(lipgloss.Color(t.Muted))
		}
		inputField = labelStyle.Render(m.inputLabel) + "\n" + inputStyle.Render(m.inputValue+"█")
	}

	// Build buttons
	var buttonViews []string
	for i, btn := range m.buttons {
		btnStyle := lipgloss.NewStyle().Padding(0, 2)
		if i == m.activeBtn {
			if t != nil {
				btnStyle = btnStyle.
					Background(lipgloss.Color(t.BrandAccent)).
					Foreground(lipgloss.Color(t.Background)).
					Bold(true)
			}
		} else {
			if t != nil {
				btnStyle = btnStyle.
					Border(lipgloss.NormalBorder()).
					BorderForeground(lipgloss.Color(t.Border))
			}
		}
		label := btn.Label
		if btn.Key != "" {
			label = "[" + btn.Key + "] " + label
		}
		buttonViews = append(buttonViews, btnStyle.Render(label))
	}
	buttonsRow := lipgloss.JoinHorizontal(lipgloss.Center, buttonViews...)

	// Combine content
	var content strings.Builder
	content.WriteString(titleStyle.Render(m.title))
	content.WriteString("\n\n")
	content.WriteString(messageStyle.Render(m.message))
	if inputField != "" {
		content.WriteString("\n\n")
		content.WriteString(inputField)
	}
	content.WriteString("\n\n")
	content.WriteString(lipgloss.NewStyle().Width(m.width - 4).Align(lipgloss.Center).Render(buttonsRow))

	// Build container
	containerStyle := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(lipgloss.Color(borderColor)).
		Padding(1, 2).
		Width(m.width)

	return containerStyle.Render(content.String())
}

// ============================================================================
// Breadcrumb Navigation
// ============================================================================

// BreadcrumbItem represents a single breadcrumb.
type BreadcrumbItem struct {
	Label    string
	OnSelect func() tea.Msg
}

// Breadcrumb provides navigation breadcrumbs.
type Breadcrumb struct {
	items     []BreadcrumbItem
	separator string
	theme     *theme.Theme
}

// NewBreadcrumb creates a new breadcrumb navigation.
func NewBreadcrumb(t *theme.Theme) *Breadcrumb {
	return &Breadcrumb{
		items:     make([]BreadcrumbItem, 0),
		separator: " → ", // Nerd Font arrow
		theme:     t,
	}
}

// SetItems sets the breadcrumb items.
func (b *Breadcrumb) SetItems(items []BreadcrumbItem) {
	b.items = items
}

// Push adds an item to the breadcrumb path.
func (b *Breadcrumb) Push(item BreadcrumbItem) {
	b.items = append(b.items, item)
}

// Pop removes the last breadcrumb item.
func (b *Breadcrumb) Pop() {
	if len(b.items) > 0 {
		b.items = b.items[:len(b.items)-1]
	}
}

// View renders the breadcrumb.
func (b *Breadcrumb) View() string {
	if len(b.items) == 0 {
		return ""
	}

	t := b.theme
	var parts []string

	separatorStyle := lipgloss.NewStyle()
	if t != nil {
		separatorStyle = separatorStyle.Foreground(lipgloss.Color(t.Muted))
	}

	for i, item := range b.items {
		itemStyle := lipgloss.NewStyle()
		if t != nil {
			if i == len(b.items)-1 {
				// Current (last) item
				itemStyle = itemStyle.
					Foreground(lipgloss.Color(t.BrandAccent)).
					Bold(true)
			} else {
				// Previous items
				itemStyle = itemStyle.Foreground(lipgloss.Color(t.Muted))
			}
		}
		parts = append(parts, itemStyle.Render(item.Label))
	}

	return strings.Join(parts, separatorStyle.Render(b.separator))
}

// ============================================================================
// Helper
// ============================================================================

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
