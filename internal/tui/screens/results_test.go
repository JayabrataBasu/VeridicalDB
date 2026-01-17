package screens

import (
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/charmbracelet/lipgloss"
)

type mockStylesResults struct{ palette *types.StylePalette }

func (m mockStylesResults) GetStyles() *types.StylePalette { return m.palette }

func newResultsPalette() *types.StylePalette {
	return &types.StylePalette{
		Title:     lipgloss.NewStyle().Foreground(lipgloss.Color("#00FFFF")).Bold(true),
		Subtle:    lipgloss.NewStyle().Foreground(lipgloss.Color("#666666")),
		Highlight: lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")),
		Error:     lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")),
		Help:      lipgloss.NewStyle().Foreground(lipgloss.Color("#AAAAAA")),
	}
}

func TestResultsViewNoData(t *testing.T) {
	screen := NewResultsScreen(mockStylesResults{palette: newResultsPalette()})
	output := screen.View()
	if !strings.Contains(output, "No results") {
		t.Fatalf("expected 'No results' message, got: %s", output)
	}
}

func TestResultsSetResultUpdatesPagination(t *testing.T) {
	screen := NewResultsScreen(mockStylesResults{palette: newResultsPalette()})
	rows := make([][]interface{}, 60)
	for i := 0; i < len(rows); i++ {
		rows[i] = []interface{}{i}
	}
	result := &QueryResult{Columns: []string{"id"}, Rows: rows}
	screen.SetResult(result)

	if screen.totalPages != 2 {
		t.Fatalf("expected 2 pages, got %d", screen.totalPages)
	}
}

func TestResultsViewDisplaysMessage(t *testing.T) {
	screen := NewResultsScreen(mockStylesResults{palette: newResultsPalette()})
	screen.SetResult(&QueryResult{Message: "OK"})
	output := screen.View()
	if !strings.Contains(output, "OK") {
		t.Fatalf("expected message in view output")
	}
}
