package components

import (
	"strings"
	"testing"
	"time"
)

// TestNewToastManager tests toast manager creation
func TestNewToastManager(t *testing.T) {
	manager := NewToastManager(80)

	if manager.width != 80 {
		t.Errorf("expected width 80, got %d", manager.width)
	}
	if manager.maxVisible != 3 {
		t.Errorf("expected maxVisible 3, got %d", manager.maxVisible)
	}
	if len(manager.toasts) != 0 {
		t.Errorf("expected empty toasts, got %d", len(manager.toasts))
	}
	if !manager.autoCloseEnabled {
		t.Errorf("expected autoCloseEnabled true")
	}
}

// TestShowSuccess tests success toast
func TestShowSuccess(t *testing.T) {
	manager := NewToastManager(80)
	id := manager.ShowSuccess("Operation completed")

	if len(manager.toasts) != 1 {
		t.Fatalf("expected 1 toast, got %d", len(manager.toasts))
	}

	toast := manager.toasts[0]
	if toast.Type != ToastSuccess {
		t.Errorf("expected ToastSuccess, got %v", toast.Type)
	}
	if toast.Message != "Operation completed" {
		t.Errorf("expected message 'Operation completed', got %s", toast.Message)
	}
	if id == "" {
		t.Errorf("expected non-empty ID")
	}
	if toast.Duration != 3*time.Second {
		t.Errorf("expected 3 second duration, got %v", toast.Duration)
	}
}

// TestShowError tests error toast
func TestShowError(t *testing.T) {
	manager := NewToastManager(80)
	id := manager.ShowError("Something went wrong")

	if len(manager.toasts) != 1 {
		t.Fatalf("expected 1 toast, got %d", len(manager.toasts))
	}

	toast := manager.toasts[0]
	if toast.Type != ToastError {
		t.Errorf("expected ToastError, got %v", toast.Type)
	}
	if toast.Duration != 5*time.Second {
		t.Errorf("expected 5 second duration, got %v", toast.Duration)
	}
	if id == "" {
		t.Errorf("expected non-empty ID")
	}
}

// TestShowWarning tests warning toast
func TestShowWarning(t *testing.T) {
	manager := NewToastManager(80)
	manager.ShowWarning("Take caution")

	toast := manager.toasts[0]
	if toast.Type != ToastWarning {
		t.Errorf("expected ToastWarning, got %v", toast.Type)
	}
	if toast.Duration != 4*time.Second {
		t.Errorf("expected 4 second duration, got %v", toast.Duration)
	}
}

// TestShowInfo tests info toast
func TestShowInfo(t *testing.T) {
	manager := NewToastManager(80)
	manager.ShowInfo("Just FYI")

	toast := manager.toasts[0]
	if toast.Type != ToastInfo {
		t.Errorf("expected ToastInfo, got %v", toast.Type)
	}
}

// TestShow tests generic show method
func TestShow(t *testing.T) {
	manager := NewToastManager(80)
	duration := 2 * time.Second
	id := manager.Show("Custom message", ToastWarning, duration)

	if id == "" {
		t.Errorf("expected non-empty ID")
	}

	toast := manager.toasts[0]
	if toast.Type != ToastWarning {
		t.Errorf("expected ToastWarning")
	}
	if toast.Duration != duration {
		t.Errorf("expected %v duration, got %v", duration, toast.Duration)
	}
}

// TestDismiss tests manual dismissal
func TestDismiss(t *testing.T) {
	manager := NewToastManager(80)
	id1 := manager.ShowSuccess("First")
	id2 := manager.ShowError("Second")
	id3 := manager.ShowInfo("Third")

	if len(manager.toasts) != 3 {
		t.Fatalf("expected 3 toasts, got %d", len(manager.toasts))
	}

	manager.Dismiss(id2)

	if len(manager.toasts) != 2 {
		t.Errorf("expected 2 toasts after dismiss, got %d", len(manager.toasts))
	}

	// Verify correct toast was removed and others remain
	hasID1 := false
	hasID3 := false
	for _, toast := range manager.toasts {
		if toast.ID == id1 {
			hasID1 = true
		}
		if toast.ID == id3 {
			hasID3 = true
		}
		if toast.ID == id2 {
			t.Errorf("toast %s should have been removed", id2)
		}
	}

	if !hasID1 || !hasID3 {
		t.Errorf("expected both remaining toasts to be present")
	}
}

// TestDismissNonExistent tests dismissing non-existent toast
func TestDismissNonExistent(t *testing.T) {
	manager := NewToastManager(80)
	manager.ShowSuccess("Test")

	// Should not panic
	manager.Dismiss("nonexistent_id")

	if len(manager.toasts) != 1 {
		t.Errorf("expected 1 toast, got %d", len(manager.toasts))
	}
}

// TestDismissAll tests dismissing all toasts
func TestDismissAll(t *testing.T) {
	manager := NewToastManager(80)
	manager.ShowSuccess("First")
	manager.ShowError("Second")
	manager.ShowWarning("Third")

	if len(manager.toasts) != 3 {
		t.Fatalf("expected 3 toasts")
	}

	manager.DismissAll()

	if len(manager.toasts) != 0 {
		t.Errorf("expected 0 toasts, got %d", len(manager.toasts))
	}
}

// TestRemoveExpired tests expiration removal
func TestRemoveExpired(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetAutoClose(true)

	// Add toast with very short duration
	manager.Show("Short-lived", ToastSuccess, 1*time.Millisecond)

	// Add toast with no expiration
	manager.Show("Long-lived", ToastInfo, 0)

	if len(manager.toasts) != 2 {
		t.Fatalf("expected 2 toasts")
	}

	// Wait for first to expire
	time.Sleep(10 * time.Millisecond)
	removed := manager.RemoveExpired()

	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	if len(manager.toasts) != 1 {
		t.Errorf("expected 1 toast remaining, got %d", len(manager.toasts))
	}
	if manager.toasts[0].Message != "Long-lived" {
		t.Errorf("expected long-lived toast to remain")
	}
}

// TestRemoveExpiredDisabled tests that removal is skipped when auto-close is disabled
func TestRemoveExpiredDisabled(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetAutoClose(false)

	manager.Show("Old message", ToastSuccess, 1*time.Millisecond)

	time.Sleep(10 * time.Millisecond)
	removed := manager.RemoveExpired()

	if removed != 0 {
		t.Errorf("expected 0 removed when auto-close disabled, got %d", removed)
	}
	if len(manager.toasts) != 1 {
		t.Errorf("expected toast to remain when auto-close disabled")
	}
}

// TestGetVisible tests visible toast retrieval
func TestGetVisible(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetMaxVisible(2)

	manager.ShowSuccess("First")
	manager.ShowError("Second")
	manager.ShowWarning("Third")
	manager.ShowInfo("Fourth")

	visible := manager.GetVisible()

	if len(visible) != 2 {
		t.Errorf("expected 2 visible, got %d", len(visible))
	}

	// Should be the last two added
	if visible[0].Message != "Third" {
		t.Errorf("expected 'Third' as first visible")
	}
	if visible[1].Message != "Fourth" {
		t.Errorf("expected 'Fourth' as second visible")
	}
}

// TestCount tests toast count
func TestCount(t *testing.T) {
	manager := NewToastManager(80)

	if manager.Count() != 0 {
		t.Errorf("expected count 0, got %d", manager.Count())
	}

	manager.ShowSuccess("First")
	if manager.Count() != 1 {
		t.Errorf("expected count 1, got %d", manager.Count())
	}

	manager.ShowError("Second")
	if manager.Count() != 2 {
		t.Errorf("expected count 2, got %d", manager.Count())
	}
}

// TestSetWidth tests width update
func TestSetWidth(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetWidth(120)

	if manager.width != 120 {
		t.Errorf("expected width 120, got %d", manager.width)
	}
}

// TestSetMaxVisible tests max visible update
func TestSetMaxVisible(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetMaxVisible(5) // Set high limit first

	manager.ShowSuccess("1")
	manager.ShowSuccess("2")
	manager.ShowSuccess("3")
	manager.ShowSuccess("4")

	if len(manager.toasts) != 4 {
		t.Fatalf("expected 4 toasts, got %d", len(manager.toasts))
	}

	manager.SetMaxVisible(2)

	if len(manager.toasts) != 2 {
		t.Errorf("expected 2 toasts after SetMaxVisible, got %d", len(manager.toasts))
	}

	// Should keep the last 2
	if manager.toasts[0].Message != "3" {
		t.Errorf("expected '3' as first toast after trim, got %s", manager.toasts[0].Message)
	}
	if manager.toasts[1].Message != "4" {
		t.Errorf("expected '4' as second toast after trim, got %s", manager.toasts[1].Message)
	}
}

// TestSetAutoClose tests auto-close toggle
func TestSetAutoClose(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetAutoClose(true)
	if !manager.autoCloseEnabled {
		t.Errorf("expected autoCloseEnabled true")
	}

	manager.SetAutoClose(false)
	if manager.autoCloseEnabled {
		t.Errorf("expected autoCloseEnabled false")
	}
}

// TestMaxVisibleLimit tests that max visible is enforced
func TestMaxVisibleLimit(t *testing.T) {
	manager := NewToastManager(80)
	manager.SetMaxVisible(3)

	for i := 0; i < 5; i++ {
		manager.ShowSuccess("Message")
	}

	if len(manager.toasts) > 3 {
		t.Errorf("expected at most 3 toasts, got %d", len(manager.toasts))
	}
}

// TestRenderToasts tests toast rendering
func TestRenderToasts(t *testing.T) {
	manager := NewToastManager(60)
	manager.ShowSuccess("All good")

	rendered := manager.RenderToasts()

	if rendered == "" {
		t.Errorf("expected non-empty render")
	}
	if !strings.Contains(rendered, "All good") {
		t.Errorf("expected message in rendered output")
	}
	if !strings.Contains(rendered, "✓") {
		t.Errorf("expected success icon in rendered output")
	}
}

// TestRenderToastsEmpty tests rendering with no toasts
func TestRenderToastsEmpty(t *testing.T) {
	manager := NewToastManager(80)
	rendered := manager.RenderToasts()

	if rendered != "" {
		t.Errorf("expected empty render when no toasts, got %q", rendered)
	}
}

// TestToastIcons tests icons for different types
func TestToastIcons(t *testing.T) {
	manager := NewToastManager(60)

	tests := []struct {
		showFunc func(string) string
		icon     string
	}{
		{manager.ShowSuccess, "✓"},
		{manager.ShowError, "✗"},
		{manager.ShowWarning, "⚠"},
		{manager.ShowInfo, "ℹ"},
	}

	for _, test := range tests {
		manager.DismissAll()
		test.showFunc("Test message")
		rendered := manager.RenderToasts()
		if !strings.Contains(rendered, test.icon) {
			t.Errorf("expected %s icon in rendered output", test.icon)
		}
	}
}

// TestDefaultToastTheme tests default theme colors
func TestDefaultToastTheme(t *testing.T) {
	theme := DefaultToastTheme()

	if theme.SuccessBG == "" {
		t.Errorf("SuccessBG should not be empty")
	}
	if theme.ErrorFG == "" {
		t.Errorf("ErrorFG should not be empty")
	}
	if theme.WarningBG == "" {
		t.Errorf("WarningBG should not be empty")
	}
	if theme.InfoFG == "" {
		t.Errorf("InfoFG should not be empty")
	}
}

// TestSetTheme tests custom theme
func TestSetTheme(t *testing.T) {
	manager := NewToastManager(80)
	customTheme := ToastTheme{
		SuccessBG: "10",
		SuccessFG: "11",
		ErrorBG:   "12",
		ErrorFG:   "13",
	}
	manager.SetTheme(customTheme)

	if manager.theme.SuccessBG != "10" {
		t.Errorf("expected SuccessBG to be set")
	}
}

// TestNotificationCenter tests notification center
func TestNotificationCenter(t *testing.T) {
	center := NewNotificationCenter(80)

	if center.manager == nil {
		t.Fatalf("expected non-nil manager")
	}

	id := center.NotifySuccess("Test")
	if id == "" {
		t.Errorf("expected non-empty ID")
	}

	if center.manager.Count() != 1 {
		t.Errorf("expected 1 notification")
	}
}

// TestNotificationCenterMethods tests all notification methods
func TestNotificationCenterMethods(t *testing.T) {
	center := NewNotificationCenter(80)

	tests := []struct {
		notifyFunc func(string) string
		toastType  ToastType
	}{
		{center.NotifySuccess, ToastSuccess},
		{center.NotifyError, ToastError},
		{center.NotifyWarning, ToastWarning},
		{center.NotifyInfo, ToastInfo},
	}

	for _, test := range tests {
		center.GetManager().DismissAll()
		test.notifyFunc("Test")
		if center.GetManager().toasts[0].Type != test.toastType {
			t.Errorf("expected notification type %v", test.toastType)
		}
	}
}

// TestToastIDUniqueness tests that each toast gets a unique ID
func TestToastIDUniqueness(t *testing.T) {
	manager := NewToastManager(80)

	ids := make(map[string]bool)
	for i := 0; i < 10; i++ {
		id := manager.ShowSuccess("Message")
		if ids[id] {
			t.Errorf("duplicate ID: %s", id)
		}
		ids[id] = true
	}

	if len(ids) != 10 {
		t.Errorf("expected 10 unique IDs, got %d", len(ids))
	}
}

// TestToastTimestamp tests that toast has correct timestamp
func TestToastTimestamp(t *testing.T) {
	manager := NewToastManager(80)
	before := time.Now()
	manager.ShowSuccess("Test")
	after := time.Now()

	toast := manager.toasts[0]
	if toast.CreatedAt.Before(before) || toast.CreatedAt.After(after) {
		t.Errorf("toast timestamp not in expected range")
	}
}

// TestMultipleToastRendering tests rendering multiple toasts
func TestMultipleToastRendering(t *testing.T) {
	manager := NewToastManager(60)
	manager.SetMaxVisible(3)

	manager.ShowSuccess("Success message")
	manager.ShowError("Error message")
	manager.ShowWarning("Warning message")

	rendered := manager.RenderToasts()

	if !strings.Contains(rendered, "Success") {
		t.Errorf("expected success message in output")
	}
	if !strings.Contains(rendered, "Error") {
		t.Errorf("expected error message in output")
	}
	if !strings.Contains(rendered, "Warning") {
		t.Errorf("expected warning message in output")
	}
}

// TestNotifyGeneric tests generic Notify method
func TestNotifyGeneric(t *testing.T) {
	center := NewNotificationCenter(80)
	id := center.Notify("Generic message", ToastWarning)

	if id == "" {
		t.Errorf("expected non-empty ID")
	}

	if len(center.GetManager().toasts) != 1 {
		t.Errorf("expected 1 notification")
	}

	if center.GetManager().toasts[0].Type != ToastWarning {
		t.Errorf("expected ToastWarning type")
	}
}
