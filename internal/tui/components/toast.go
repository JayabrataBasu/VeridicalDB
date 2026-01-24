package components

import (
	"time"

	"github.com/charmbracelet/lipgloss"
)

// ToastType represents the type of toast notification
type ToastType int

const (
	ToastSuccess ToastType = iota
	ToastWarning
	ToastError
	ToastInfo
)

// Toast represents a single toast notification
type Toast struct {
	ID        string
	Message   string
	Type      ToastType
	CreatedAt time.Time
	Duration  time.Duration // 0 means no auto-dismiss
	Dismisses bool          // True if can be manually dismissed
}

// ToastManager manages a queue of toast notifications
type ToastManager struct {
	toasts           []Toast
	maxVisible       int
	width            int
	autoCloseEnabled bool
	theme            ToastTheme
}

// ToastTheme provides colors for toast messages
type ToastTheme struct {
	SuccessBG   lipgloss.Color
	SuccessFG   lipgloss.Color
	WarningBG   lipgloss.Color
	WarningFG   lipgloss.Color
	ErrorBG     lipgloss.Color
	ErrorFG     lipgloss.Color
	InfoBG      lipgloss.Color
	InfoFG      lipgloss.Color
	Border      lipgloss.Color
	DimmedFG    lipgloss.Color
}

// NewToastManager creates a new toast manager
func NewToastManager(width int) *ToastManager {
	return &ToastManager{
		toasts:           make([]Toast, 0),
		maxVisible:       3,
		width:            width,
		autoCloseEnabled: true,
		theme:            DefaultToastTheme(),
	}
}

// DefaultToastTheme returns the default toast theme
func DefaultToastTheme() ToastTheme {
	return ToastTheme{
		SuccessBG:   lipgloss.Color("22"),   // Dark green
		SuccessFG:   lipgloss.Color("46"),   // Light green
		WarningBG:   lipgloss.Color("136"),  // Dark yellow
		WarningFG:   lipgloss.Color("226"),  // Bright yellow
		ErrorBG:     lipgloss.Color("52"),   // Dark red
		ErrorFG:     lipgloss.Color("196"),  // Bright red
		InfoBG:      lipgloss.Color("17"),   // Dark blue
		InfoFG:      lipgloss.Color("39"),   // Light blue
		Border:      lipgloss.Color("238"),  // Gray
		DimmedFG:    lipgloss.Color("245"),  // Light gray
	}
}

// ShowSuccess shows a success toast
func (tm *ToastManager) ShowSuccess(message string) string {
	return tm.Show(message, ToastSuccess, 3*time.Second)
}

// ShowWarning shows a warning toast
func (tm *ToastManager) ShowWarning(message string) string {
	return tm.Show(message, ToastWarning, 4*time.Second)
}

// ShowError shows an error toast
func (tm *ToastManager) ShowError(message string) string {
	return tm.Show(message, ToastError, 5*time.Second)
}

// ShowInfo shows an info toast
func (tm *ToastManager) ShowInfo(message string) string {
	return tm.Show(message, ToastInfo, 3*time.Second)
}

// Show adds a toast to the queue and returns its ID
func (tm *ToastManager) Show(message string, toastType ToastType, duration time.Duration) string {
	toast := Toast{
		ID:        generateToastID(),
		Message:   message,
		Type:      toastType,
		CreatedAt: time.Now(),
		Duration:  duration,
		Dismisses: true,
	}

	tm.toasts = append(tm.toasts, toast)

	// Trim if exceeds max visible
	if len(tm.toasts) > tm.maxVisible {
		tm.toasts = tm.toasts[len(tm.toasts)-tm.maxVisible:]
	}

	return toast.ID
}

// Dismiss removes a toast by ID
func (tm *ToastManager) Dismiss(id string) {
	for i, toast := range tm.toasts {
		if toast.ID == id {
			tm.toasts = append(tm.toasts[:i], tm.toasts[i+1:]...)
			return
		}
	}
}

// DismissAll removes all toasts
func (tm *ToastManager) DismissAll() {
	tm.toasts = make([]Toast, 0)
}

// RemoveExpired removes expired toasts (returns count removed)
func (tm *ToastManager) RemoveExpired() int {
	if !tm.autoCloseEnabled {
		return 0
	}

	beforeCount := len(tm.toasts)
	now := time.Now()
	filtered := make([]Toast, 0)

	for _, toast := range tm.toasts {
		// Only remove if duration is set and has expired
		if toast.Duration > 0 && now.Sub(toast.CreatedAt) > toast.Duration {
			continue
		}
		filtered = append(filtered, toast)
	}

	tm.toasts = filtered
	return beforeCount - len(tm.toasts)
}

// GetVisible returns visible toasts (up to maxVisible)
func (tm *ToastManager) GetVisible() []Toast {
	count := len(tm.toasts)
	if count > tm.maxVisible {
		return tm.toasts[count-tm.maxVisible:]
	}
	return tm.toasts
}

// Count returns total number of toasts in queue
func (tm *ToastManager) Count() int {
	return len(tm.toasts)
}

// SetTheme sets a custom theme
func (tm *ToastManager) SetTheme(theme ToastTheme) {
	tm.theme = theme
}

// SetWidth sets the width for rendering
func (tm *ToastManager) SetWidth(width int) {
	tm.width = width
}

// SetMaxVisible sets the maximum number of visible toasts
func (tm *ToastManager) SetMaxVisible(max int) {
	tm.maxVisible = max
	// Trim existing toasts if needed
	if len(tm.toasts) > max {
		tm.toasts = tm.toasts[len(tm.toasts)-max:]
	}
}

// SetAutoClose enables/disables automatic closing of toasts
func (tm *ToastManager) SetAutoClose(enabled bool) {
	tm.autoCloseEnabled = enabled
}

// RenderToasts renders all visible toasts
func (tm *ToastManager) RenderToasts() string {
	visible := tm.GetVisible()
	if len(visible) == 0 {
		return ""
	}

	lines := make([]string, 0, len(visible))
	for _, toast := range visible {
		lines = append(lines, tm.renderToast(toast))
	}

	return lipgloss.JoinVertical(lipgloss.Top, lines...)
}

// renderToast renders a single toast
func (tm *ToastManager) renderToast(toast Toast) string {
	// Get icon and colors based on type
	icon, bgColor, fgColor := tm.getToastStyle(toast.Type)

	// Build message with icon
	content := icon + " " + toast.Message

	// Truncate if too long
	maxLen := tm.width - 4 // Leave room for borders/padding
	if len(content) > maxLen {
		content = content[:maxLen-1] + "…"
	}

	// Pad to width
	padding := maxLen - len(content)
	if padding > 0 {
		content = content + repeatStr(" ", padding)
	}

	// Apply styling
	style := lipgloss.NewStyle().
		Background(bgColor).
		Foreground(fgColor).
		Bold(true).
		Padding(0, 1).
		Width(tm.width - 2)

	return style.Render(content)
}

// getToastStyle returns icon and colors for a toast type
func (tm *ToastManager) getToastStyle(toastType ToastType) (string, lipgloss.Color, lipgloss.Color) {
	switch toastType {
	case ToastSuccess:
		return "✓", tm.theme.SuccessBG, tm.theme.SuccessFG
	case ToastWarning:
		return "⚠", tm.theme.WarningBG, tm.theme.WarningFG
	case ToastError:
		return "✗", tm.theme.ErrorBG, tm.theme.ErrorFG
	case ToastInfo:
		return "ℹ", tm.theme.InfoBG, tm.theme.InfoFG
	default:
		return "•", tm.theme.InfoBG, tm.theme.InfoFG
	}
}

// Helper functions

var toastCounter int = 0

func generateToastID() string {
	toastCounter++
	return "toast_" + string(rune(toastCounter))
}

func repeatStr(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}

// NotificationCenter provides high-level notification API
type NotificationCenter struct {
	manager *ToastManager
}

// NewNotificationCenter creates a new notification center
func NewNotificationCenter(width int) *NotificationCenter {
	return &NotificationCenter{
		manager: NewToastManager(width),
	}
}

// Notify sends a generic notification
func (nc *NotificationCenter) Notify(message string, toastType ToastType) string {
	return nc.manager.Show(message, toastType, 3*time.Second)
}

// NotifySuccess sends a success notification
func (nc *NotificationCenter) NotifySuccess(message string) string {
	return nc.manager.ShowSuccess(message)
}

// NotifyError sends an error notification
func (nc *NotificationCenter) NotifyError(message string) string {
	return nc.manager.ShowError(message)
}

// NotifyWarning sends a warning notification
func (nc *NotificationCenter) NotifyWarning(message string) string {
	return nc.manager.ShowWarning(message)
}

// NotifyInfo sends an info notification
func (nc *NotificationCenter) NotifyInfo(message string) string {
	return nc.manager.ShowInfo(message)
}

// GetManager returns the underlying ToastManager
func (nc *NotificationCenter) GetManager() *ToastManager {
	return nc.manager
}
