package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/exec"
)

// Session is the slice of the SQL session the TUI actually uses. Depending on
// this interface rather than *exec.Session keeps the SQL engine's surface out of
// the TUI model and lets tests supply a lightweight stub. The production
// implementation is *exec.Session, wired in by cmd/veridicaldb.
type Session interface {
	// ExecuteSQL runs a statement and returns its result.
	ExecuteSQL(input string) (*exec.Result, error)
	// CurrentDatabase reports the session's active database ("" if none selected).
	CurrentDatabase() string
	// Catalog exposes table metadata for the schema browser.
	Catalog() *catalog.Catalog
	// GetDatabaseManager exposes the multi-database manager, or nil.
	GetDatabaseManager() *catalog.DatabaseManager
	// ShardMetricsProvider exposes the optional distributed-shard metrics source.
	ShardMetricsProvider() exec.MetricsProvider
}
