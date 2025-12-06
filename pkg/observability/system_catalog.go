// Package observability provides system tables and monitoring for VeridicalDB.
package observability

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// SystemCatalog provides access to system tables.
type SystemCatalog struct {
	mu sync.RWMutex

	// References to core components
	txnMgr  *txn.Manager
	lockMgr *lock.Manager
	cat     *catalog.Catalog

	// Statistics
	stats *Statistics
}

// Statistics tracks database performance metrics.
type Statistics struct {
	mu sync.RWMutex

	// Query statistics
	QueriesExecuted  int64
	QueriesSucceeded int64
	QueriesFailed    int64
	TotalQueryTimeNs int64

	// Transaction statistics
	TransactionsStarted   int64
	TransactionsCommitted int64
	TransactionsAborted   int64

	// Storage statistics
	RowsInserted int64
	RowsUpdated  int64
	RowsDeleted  int64
	RowsScanned  int64

	// Index statistics
	IndexScans  int64
	IndexHits   int64
	IndexMisses int64

	// Start time
	StartTime time.Time
}

// NewSystemCatalog creates a new system catalog.
func NewSystemCatalog(txnMgr *txn.Manager, lockMgr *lock.Manager, cat *catalog.Catalog) *SystemCatalog {
	return &SystemCatalog{
		txnMgr:  txnMgr,
		lockMgr: lockMgr,
		cat:     cat,
		stats:   NewStatistics(),
	}
}

// NewStatistics creates a new statistics tracker.
func NewStatistics() *Statistics {
	return &Statistics{
		StartTime: time.Now(),
	}
}

// RecordQuery records a query execution.
func (s *Statistics) RecordQuery(success bool, durationNs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.QueriesExecuted++
	s.TotalQueryTimeNs += durationNs

	if success {
		s.QueriesSucceeded++
	} else {
		s.QueriesFailed++
	}
}

// RecordTransaction records transaction activity.
func (s *Statistics) RecordTransaction(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event {
	case "start":
		s.TransactionsStarted++
	case "commit":
		s.TransactionsCommitted++
	case "abort":
		s.TransactionsAborted++
	}
}

// RecordRowOp records a row operation.
func (s *Statistics) RecordRowOp(op string, count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch op {
	case "insert":
		s.RowsInserted += count
	case "update":
		s.RowsUpdated += count
	case "delete":
		s.RowsDeleted += count
	case "scan":
		s.RowsScanned += count
	}
}

// RecordIndexOp records an index operation.
func (s *Statistics) RecordIndexOp(hit bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.IndexScans++
	if hit {
		s.IndexHits++
	} else {
		s.IndexMisses++
	}
}

// Snapshot returns a copy of current statistics.
func (s *Statistics) Snapshot() Statistics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return Statistics{
		QueriesExecuted:       s.QueriesExecuted,
		QueriesSucceeded:      s.QueriesSucceeded,
		QueriesFailed:         s.QueriesFailed,
		TotalQueryTimeNs:      s.TotalQueryTimeNs,
		TransactionsStarted:   s.TransactionsStarted,
		TransactionsCommitted: s.TransactionsCommitted,
		TransactionsAborted:   s.TransactionsAborted,
		RowsInserted:          s.RowsInserted,
		RowsUpdated:           s.RowsUpdated,
		RowsDeleted:           s.RowsDeleted,
		RowsScanned:           s.RowsScanned,
		IndexScans:            s.IndexScans,
		IndexHits:             s.IndexHits,
		IndexMisses:           s.IndexMisses,
		StartTime:             s.StartTime,
	}
}

// SystemTableRow represents a row in a system table.
type SystemTableRow struct {
	Columns []string
	Values  []interface{}
}

// GetActiveTransactions returns information about active transactions.
func (sc *SystemCatalog) GetActiveTransactions() []SystemTableRow {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.txnMgr == nil {
		return nil
	}

	var rows []SystemTableRow
	for _, tx := range sc.txnMgr.GetActiveTransactions() {
		rows = append(rows, SystemTableRow{
			Columns: []string{"txn_id", "state", "snapshot_xmin"},
			Values:  []interface{}{tx.ID, tx.State.String(), tx.Snapshot.XMin},
		})
	}
	return rows
}

// GetLocks returns information about current locks.
func (sc *SystemCatalog) GetLocks() []SystemTableRow {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.lockMgr == nil {
		return nil
	}

	var rows []SystemTableRow
	for _, info := range sc.lockMgr.GetLockInfo() {
		rows = append(rows, SystemTableRow{
			Columns: []string{"resource", "mode", "holder_txns", "waiting_count"},
			Values:  []interface{}{info.Resource.String(), info.Mode.String(), info.HolderTxIDs, info.WaitingCount},
		})
	}
	return rows
}

// GetTables returns information about all tables.
func (sc *SystemCatalog) GetTables() []SystemTableRow {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.cat == nil {
		return nil
	}

	var rows []SystemTableRow
	for _, name := range sc.cat.ListTables() {
		meta, err := sc.cat.GetTable(name)
		if err != nil {
			continue
		}
		rows = append(rows, SystemTableRow{
			Columns: []string{"table_name", "column_count", "storage_type"},
			Values:  []interface{}{name, len(meta.Columns), meta.StorageType},
		})
	}
	return rows
}

// GetColumns returns column information for a table.
func (sc *SystemCatalog) GetColumns(tableName string) []SystemTableRow {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if sc.cat == nil {
		return nil
	}

	meta, err := sc.cat.GetTable(tableName)
	if err != nil {
		return nil
	}

	var rows []SystemTableRow
	for i, col := range meta.Columns {
		rows = append(rows, SystemTableRow{
			Columns: []string{"column_id", "column_name", "data_type", "not_null"},
			Values:  []interface{}{i, col.Name, col.Type.String(), col.NotNull},
		})
	}
	return rows
}

// GetIndexes returns information about all indexes.
// Note: This requires an IndexManager reference to be set.
func (sc *SystemCatalog) GetIndexes() []SystemTableRow {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	// Index information is managed by btree.IndexManager
	// This placeholder returns empty until IndexManager integration is added
	return nil
}

// GetStatistics returns database statistics.
func (sc *SystemCatalog) GetStatistics() []SystemTableRow {
	stats := sc.stats.Snapshot()
	uptime := time.Since(stats.StartTime)

	return []SystemTableRow{
		{Columns: []string{"metric", "value"}, Values: []interface{}{"uptime_seconds", int64(uptime.Seconds())}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"queries_executed", stats.QueriesExecuted}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"queries_succeeded", stats.QueriesSucceeded}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"queries_failed", stats.QueriesFailed}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"avg_query_time_ms", avgQueryTime(&stats)}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"transactions_started", stats.TransactionsStarted}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"transactions_committed", stats.TransactionsCommitted}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"transactions_aborted", stats.TransactionsAborted}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"rows_inserted", stats.RowsInserted}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"rows_updated", stats.RowsUpdated}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"rows_deleted", stats.RowsDeleted}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"rows_scanned", stats.RowsScanned}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"index_scans", stats.IndexScans}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"index_hit_rate", indexHitRate(&stats)}},
	}
}

func avgQueryTime(stats *Statistics) float64 {
	if stats.QueriesExecuted == 0 {
		return 0
	}
	return float64(stats.TotalQueryTimeNs) / float64(stats.QueriesExecuted) / 1e6
}

func indexHitRate(stats *Statistics) float64 {
	if stats.IndexScans == 0 {
		return 0
	}
	return float64(stats.IndexHits) / float64(stats.IndexScans) * 100
}

// GetMemoryStats returns memory statistics.
func (sc *SystemCatalog) GetMemoryStats() []SystemTableRow {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return []SystemTableRow{
		{Columns: []string{"metric", "value"}, Values: []interface{}{"heap_alloc_mb", m.HeapAlloc / 1024 / 1024}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"heap_sys_mb", m.HeapSys / 1024 / 1024}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"heap_objects", m.HeapObjects}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"goroutines", runtime.NumGoroutine()}},
		{Columns: []string{"metric", "value"}, Values: []interface{}{"gc_cycles", m.NumGC}},
	}
}

// Stats returns the statistics tracker.
func (sc *SystemCatalog) Stats() *Statistics {
	return sc.stats
}

// PrometheusMetrics returns metrics in Prometheus format.
func (sc *SystemCatalog) PrometheusMetrics() string {
	stats := sc.stats.Snapshot()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return fmt.Sprintf(`# HELP veridicaldb_queries_total Total number of queries executed
# TYPE veridicaldb_queries_total counter
veridicaldb_queries_total{status="success"} %d
veridicaldb_queries_total{status="failed"} %d

# HELP veridicaldb_transactions_total Total number of transactions
# TYPE veridicaldb_transactions_total counter
veridicaldb_transactions_total{status="committed"} %d
veridicaldb_transactions_total{status="aborted"} %d

# HELP veridicaldb_rows_total Total number of row operations
# TYPE veridicaldb_rows_total counter
veridicaldb_rows_total{op="insert"} %d
veridicaldb_rows_total{op="update"} %d
veridicaldb_rows_total{op="delete"} %d
veridicaldb_rows_total{op="scan"} %d

# HELP veridicaldb_heap_bytes Current heap memory usage in bytes
# TYPE veridicaldb_heap_bytes gauge
veridicaldb_heap_bytes %d

# HELP veridicaldb_goroutines Current number of goroutines
# TYPE veridicaldb_goroutines gauge
veridicaldb_goroutines %d

# HELP veridicaldb_uptime_seconds Server uptime in seconds
# TYPE veridicaldb_uptime_seconds gauge
veridicaldb_uptime_seconds %d
`,
		stats.QueriesSucceeded, stats.QueriesFailed,
		stats.TransactionsCommitted, stats.TransactionsAborted,
		stats.RowsInserted, stats.RowsUpdated, stats.RowsDeleted, stats.RowsScanned,
		m.HeapAlloc,
		runtime.NumGoroutine(),
		int64(time.Since(stats.StartTime).Seconds()),
	)
}
