package screens

import (
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type mockStyles struct{ palette *types.StylePalette }

func (m mockStyles) GetStyles() *types.StylePalette { return m.palette }

func newPalette() *types.StylePalette {
	return &types.StylePalette{
		Title:     lipgloss.NewStyle().Foreground(lipgloss.Color("#00FFFF")).Bold(true),
		Subtle:    lipgloss.NewStyle().Foreground(lipgloss.Color("#666666")),
		Highlight: lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")),
		Error:     lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")),
		Help:      lipgloss.NewStyle().Foreground(lipgloss.Color("#AAAAAA")),
	}
}

func TestEditorViewRendersHeader(t *testing.T) {
	screen := NewEditorScreen(mockStyles{palette: newPalette()})
	output := screen.View()
	if !strings.Contains(output, "SQL Editor") {
		t.Fatalf("expected header in view output")
	}
}

func TestEditorHandlesQueryCompleted(t *testing.T) {
	screen := NewEditorScreen(mockStyles{palette: newPalette()})
	screen.executing = true

	_, _ = screen.Update(QueryCompletedMsg{Error: nil})

	if screen.executing {
		t.Fatalf("expected executing to be false after completion")
	}
	if !strings.Contains(screen.status, "Query executed") {
		t.Fatalf("unexpected status: %s", screen.status)
	}
}

func TestEditorHistoryNavigationSafeWhenEmpty(t *testing.T) {
	screen := NewEditorScreen(mockStyles{palette: newPalette()})
	// Should not panic
	_, _ = screen.Update(tea.KeyMsg{Type: tea.KeyCtrlUp})
	_, _ = screen.Update(tea.KeyMsg{Type: tea.KeyCtrlDown})
}
