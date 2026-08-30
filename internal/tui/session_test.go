package tui

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/exec"
)

// stubSession is a no-op Session for tests that need a Model but not a real
// SQL engine.
type stubSession struct {
	lastSQL string
	result  *exec.Result
}

func (s *stubSession) ExecuteSQL(input string) (*exec.Result, error) {
	s.lastSQL = input
	if s.result != nil {
		return s.result, nil
	}
	return &exec.Result{}, nil
}
func (s *stubSession) Catalog() *catalog.Catalog                    { return nil }
func (s *stubSession) GetDatabaseManager() *catalog.DatabaseManager { return nil }
func (s *stubSession) ShardMetricsProvider() exec.MetricsProvider   { return nil }

func TestModelAcceptsStubSession(t *testing.T) {
	var _ Session = (*stubSession)(nil)

	stub := &stubSession{result: &exec.Result{Columns: []string{"x"}, Rows: [][]catalog.Value{{catalog.NewInt32(1)}}}}
	m := New(stub)
	if m.GetSession() != stub {
		t.Fatal("Model did not retain the injected Session")
	}
}
