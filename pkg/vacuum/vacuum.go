// Package vacuum provides MVCC garbage collection and table compaction.
// It identifies dead tuples that are no longer visible to any transaction
// and reclaims their space to prevent table bloat.
package vacuum

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Config holds vacuum configuration options.
type Config struct {
	// Enabled controls whether autovacuum runs.
	Enabled bool `yaml:"enabled"`

	// Interval is how often the vacuum daemon checks for work.
	Interval time.Duration `yaml:"interval"`

	// DeadTupleThreshold is the minimum number of dead tuples before vacuum triggers.
	DeadTupleThreshold int `yaml:"dead_tuple_threshold"`

	// DeadTupleRatio is the minimum ratio of dead tuples to total tuples (0.0-1.0).
	DeadTupleRatio float64 `yaml:"dead_tuple_ratio"`

	// MaxWorkersPerTable limits concurrent vacuum workers per table.
	MaxWorkersPerTable int `yaml:"max_workers_per_table"`

	// CostLimit is the max cost units before vacuum sleeps (throttling).
	CostLimit int `yaml:"cost_limit"`

	// CostDelay is how long to sleep when cost limit is hit.
	CostDelay time.Duration `yaml:"cost_delay"`

	// PageCostRead is the cost of reading a page.
	PageCostRead int `yaml:"page_cost_read"`

	// PageCostWrite is the cost of writing a page.
	PageCostWrite int `yaml:"page_cost_write"`
}

// DefaultConfig returns sensible defaults for vacuum configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:            true,
		Interval:           1 * time.Minute,
		DeadTupleThreshold: 50,
		DeadTupleRatio:     0.2, // 20% dead tuples triggers vacuum
		MaxWorkersPerTable: 1,
		CostLimit:          200,
		CostDelay:          10 * time.Millisecond,
		PageCostRead:       1,
		PageCostWrite:      10,
	}
}

// TableStats holds statistics about a table for vacuum decisions.
type TableStats struct {
	TableName     string
	TotalTuples   int64
	DeadTuples    int64
	TotalPages    int64
	FreeablePages int64
	LastVacuum    time.Time
}

// DeadRatio returns the ratio of dead tuples to total tuples.
func (s *TableStats) DeadRatio() float64 {
	if s.TotalTuples == 0 {
		return 0
	}
	return float64(s.DeadTuples) / float64(s.TotalTuples)
}

// NeedsVacuum checks if the table needs vacuuming based on config thresholds.
func (s *TableStats) NeedsVacuum(cfg Config) bool {
	if s.DeadTuples >= int64(cfg.DeadTupleThreshold) {
		return true
	}
	if s.DeadRatio() >= cfg.DeadTupleRatio {
		return true
	}
	return false
}

// VacuumResult holds the result of a vacuum operation.
type VacuumResult struct {
	TableName      string
	StartTime      time.Time
	EndTime        time.Time
	PagesScanned   int64
	PagesCompacted int64
	TuplesRemoved  int64
	BytesReclaimed int64
	Error          error
}

// Duration returns how long the vacuum took.
func (r *VacuumResult) Duration() time.Duration {
	return r.EndTime.Sub(r.StartTime)
}

// String returns a human-readable summary.
func (r *VacuumResult) String() string {
	if r.Error != nil {
		return fmt.Sprintf("VACUUM %s: ERROR %v", r.TableName, r.Error)
	}
	return fmt.Sprintf("VACUUM %s: %d pages scanned, %d compacted, %d tuples removed, %d bytes reclaimed in %v",
		r.TableName, r.PagesScanned, r.PagesCompacted, r.TuplesRemoved, r.BytesReclaimed, r.Duration())
}

// Manager coordinates vacuum operations across all tables.
type Manager struct {
	config  Config
	txnMgr  *txn.Manager
	storage StorageInterface
	catalog CatalogInterface

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Metrics
	metrics *Metrics
}

// StorageInterface defines storage operations needed by vacuum.
type StorageInterface interface {
	// ScanTable returns all RIDs in a table (for scanning).
	ScanTable(table string) ([]RID, error)
	// FetchPage reads a page into buffer.
	FetchPage(table string, pageID uint32) ([]byte, error)
	// WritePage writes a page buffer back.
	WritePage(table string, pageID uint32, data []byte) error
	// GetPageCount returns the number of pages in a table.
	GetPageCount(table string) (int, error)
	// FetchRaw returns raw tuple data for a RID.
	FetchRaw(rid RID) ([]byte, error)
	// DeleteSlot physically removes a tuple slot.
	DeleteSlot(rid RID) error
	// CompactPage compacts a page, removing dead tuples.
	CompactPage(table string, pageID uint32, deadSlots []int) (bytesReclaimed int64, err error)
}

// CatalogInterface defines catalog operations needed by vacuum.
type CatalogInterface interface {
	// ListTables returns all table names.
	ListTables() []string
	// GetTableStats returns statistics for a table.
	GetTableStats(table string) (*TableStats, error)
}

// RID is a row identifier (page + slot).
type RID struct {
	Table string
	Page  uint32
	Slot  uint16
}

// NewManager creates a new vacuum manager.
func NewManager(cfg Config, txnMgr *txn.Manager, storage StorageInterface, catalog CatalogInterface) *Manager {
	return &Manager{
		config:  cfg,
		txnMgr:  txnMgr,
		storage: storage,
		catalog: catalog,
		metrics: NewMetrics(),
	}
}

// Start begins the background vacuum daemon.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("vacuum manager already running")
	}

	if !m.config.Enabled {
		return nil // Autovacuum disabled
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.running = true

	m.wg.Add(1)
	go m.runDaemon(ctx)

	return nil
}

// Stop stops the background vacuum daemon.
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	m.cancel()
	m.wg.Wait()
	m.running = false

	return nil
}

// IsRunning returns whether the vacuum daemon is running.
func (m *Manager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// runDaemon is the main vacuum daemon loop.
func (m *Manager) runDaemon(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAndVacuum(ctx)
		}
	}
}

// checkAndVacuum checks all tables and vacuums those that need it.
func (m *Manager) checkAndVacuum(ctx context.Context) {
	tables := m.catalog.ListTables()

	for _, table := range tables {
		select {
		case <-ctx.Done():
			return
		default:
		}

		stats, err := m.catalog.GetTableStats(table)
		if err != nil {
			continue
		}

		if stats.NeedsVacuum(m.config) {
			result := m.VacuumTable(ctx, table, false)
			m.metrics.RecordVacuum(result)
		}
	}
}

// VacuumTable vacuums a specific table.
// If full is true, performs a full vacuum with aggressive compaction.
func (m *Manager) VacuumTable(ctx context.Context, table string, full bool) *VacuumResult {
	result := &VacuumResult{
		TableName: table,
		StartTime: time.Now(),
	}

	worker := &Worker{
		table:   table,
		config:  m.config,
		txnMgr:  m.txnMgr,
		storage: m.storage,
		full:    full,
		metrics: m.metrics,
	}

	err := worker.Run(ctx, result)
	result.Error = err
	result.EndTime = time.Now()

	return result
}

// VacuumAll vacuums all tables.
func (m *Manager) VacuumAll(ctx context.Context, full bool) []*VacuumResult {
	tables := m.catalog.ListTables()
	results := make([]*VacuumResult, 0, len(tables))

	for _, table := range tables {
		select {
		case <-ctx.Done():
			return results
		default:
		}

		result := m.VacuumTable(ctx, table, full)
		results = append(results, result)
		m.metrics.RecordVacuum(result)
	}

	return results
}

// GetMetrics returns the vacuum metrics.
func (m *Manager) GetMetrics() *Metrics {
	return m.metrics
}

// Metrics tracks vacuum statistics.
type Metrics struct {
	mu sync.Mutex

	// Counters
	VacuumRunsTotal     atomic.Int64
	TuplesRemovedTotal  atomic.Int64
	BytesReclaimedTotal atomic.Int64
	PagesScannedTotal   atomic.Int64
	PagesCompactedTotal atomic.Int64
	ErrorsTotal         atomic.Int64

	// Gauges (last vacuum)
	LastVacuumTimestamp atomic.Int64
	LastVacuumDuration  atomic.Int64 // nanoseconds

	// Per-table stats
	tableStats map[string]*TableVacuumStats
}

// TableVacuumStats tracks per-table vacuum statistics.
type TableVacuumStats struct {
	LastVacuum     time.Time
	TuplesRemoved  int64
	BytesReclaimed int64
	VacuumCount    int64
}

// NewMetrics creates a new Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		tableStats: make(map[string]*TableVacuumStats),
	}
}

// RecordVacuum records the result of a vacuum operation.
func (m *Metrics) RecordVacuum(result *VacuumResult) {
	m.VacuumRunsTotal.Add(1)
	m.TuplesRemovedTotal.Add(result.TuplesRemoved)
	m.BytesReclaimedTotal.Add(result.BytesReclaimed)
	m.PagesScannedTotal.Add(result.PagesScanned)
	m.PagesCompactedTotal.Add(result.PagesCompacted)
	m.LastVacuumTimestamp.Store(result.EndTime.Unix())
	m.LastVacuumDuration.Store(result.Duration().Nanoseconds())

	if result.Error != nil {
		m.ErrorsTotal.Add(1)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	stats, ok := m.tableStats[result.TableName]
	if !ok {
		stats = &TableVacuumStats{}
		m.tableStats[result.TableName] = stats
	}
	stats.LastVacuum = result.EndTime
	stats.TuplesRemoved += result.TuplesRemoved
	stats.BytesReclaimed += result.BytesReclaimed
	stats.VacuumCount++
}

// PrometheusMetrics returns metrics in Prometheus exposition format.
func (m *Metrics) PrometheusMetrics() string {
	var buf string

	buf += "# HELP veridicaldb_vacuum_runs_total Total number of vacuum runs\n"
	buf += "# TYPE veridicaldb_vacuum_runs_total counter\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_runs_total %d\n\n", m.VacuumRunsTotal.Load())

	buf += "# HELP veridicaldb_vacuum_tuples_removed_total Total tuples removed by vacuum\n"
	buf += "# TYPE veridicaldb_vacuum_tuples_removed_total counter\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_tuples_removed_total %d\n\n", m.TuplesRemovedTotal.Load())

	buf += "# HELP veridicaldb_vacuum_bytes_reclaimed_total Total bytes reclaimed by vacuum\n"
	buf += "# TYPE veridicaldb_vacuum_bytes_reclaimed_total counter\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_bytes_reclaimed_total %d\n\n", m.BytesReclaimedTotal.Load())

	buf += "# HELP veridicaldb_vacuum_pages_scanned_total Total pages scanned by vacuum\n"
	buf += "# TYPE veridicaldb_vacuum_pages_scanned_total counter\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_pages_scanned_total %d\n\n", m.PagesScannedTotal.Load())

	buf += "# HELP veridicaldb_vacuum_pages_compacted_total Total pages compacted by vacuum\n"
	buf += "# TYPE veridicaldb_vacuum_pages_compacted_total counter\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_pages_compacted_total %d\n\n", m.PagesCompactedTotal.Load())

	buf += "# HELP veridicaldb_vacuum_errors_total Total vacuum errors\n"
	buf += "# TYPE veridicaldb_vacuum_errors_total counter\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_errors_total %d\n\n", m.ErrorsTotal.Load())

	buf += "# HELP veridicaldb_vacuum_last_timestamp_seconds Unix timestamp of last vacuum\n"
	buf += "# TYPE veridicaldb_vacuum_last_timestamp_seconds gauge\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_last_timestamp_seconds %d\n\n", m.LastVacuumTimestamp.Load())

	buf += "# HELP veridicaldb_vacuum_last_duration_seconds Duration of last vacuum\n"
	buf += "# TYPE veridicaldb_vacuum_last_duration_seconds gauge\n"
	buf += fmt.Sprintf("veridicaldb_vacuum_last_duration_seconds %.6f\n", float64(m.LastVacuumDuration.Load())/1e9)

	return buf
}

// Reset resets all metrics to zero.
func (m *Metrics) Reset() {
	m.VacuumRunsTotal.Store(0)
	m.TuplesRemovedTotal.Store(0)
	m.BytesReclaimedTotal.Store(0)
	m.PagesScannedTotal.Store(0)
	m.PagesCompactedTotal.Store(0)
	m.ErrorsTotal.Store(0)
	m.LastVacuumTimestamp.Store(0)
	m.LastVacuumDuration.Store(0)

	m.mu.Lock()
	m.tableStats = make(map[string]*TableVacuumStats)
	m.mu.Unlock()
}

// GetTableStats returns vacuum stats for a specific table.
func (m *Metrics) GetTableStats(table string) *TableVacuumStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tableStats[table]
}
