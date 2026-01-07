package vacuum

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true by default")
	}
	if cfg.Interval != 1*time.Minute {
		t.Errorf("expected Interval to be 1m, got %v", cfg.Interval)
	}
	if cfg.DeadTupleThreshold != 50 {
		t.Errorf("expected DeadTupleThreshold to be 50, got %d", cfg.DeadTupleThreshold)
	}
	if cfg.DeadTupleRatio != 0.2 {
		t.Errorf("expected DeadTupleRatio to be 0.2, got %f", cfg.DeadTupleRatio)
	}
}

func TestTableStats_DeadRatio(t *testing.T) {
	tests := []struct {
		name     string
		total    int64
		dead     int64
		expected float64
	}{
		{"no tuples", 0, 0, 0},
		{"no dead", 100, 0, 0},
		{"half dead", 100, 50, 0.5},
		{"all dead", 100, 100, 1.0},
		{"quarter dead", 100, 25, 0.25},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := &TableStats{
				TotalTuples: tt.total,
				DeadTuples:  tt.dead,
			}
			if stats.DeadRatio() != tt.expected {
				t.Errorf("expected %f, got %f", tt.expected, stats.DeadRatio())
			}
		})
	}
}

func TestTableStats_NeedsVacuum(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeadTupleThreshold = 50
	cfg.DeadTupleRatio = 0.2

	tests := []struct {
		name     string
		total    int64
		dead     int64
		expected bool
	}{
		{"below threshold and ratio", 100, 10, false},
		{"above threshold", 100, 60, true},
		{"above ratio", 100, 25, true},
		{"at threshold", 100, 50, true},
		{"at ratio", 100, 20, true},
		{"empty table", 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := &TableStats{
				TotalTuples: tt.total,
				DeadTuples:  tt.dead,
			}
			if stats.NeedsVacuum(cfg) != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, stats.NeedsVacuum(cfg))
			}
		})
	}
}

func TestVacuumResult_Duration(t *testing.T) {
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	end := time.Now()

	result := &VacuumResult{
		StartTime: start,
		EndTime:   end,
	}

	if result.Duration() < 10*time.Millisecond {
		t.Errorf("expected duration >= 10ms, got %v", result.Duration())
	}
}

func TestVacuumResult_String(t *testing.T) {
	result := &VacuumResult{
		TableName:      "test_table",
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(100 * time.Millisecond),
		PagesScanned:   10,
		PagesCompacted: 3,
		TuplesRemoved:  25,
		BytesReclaimed: 1024,
	}

	str := result.String()
	if str == "" {
		t.Error("expected non-empty string")
	}
	if !contains(str, "test_table") {
		t.Error("expected table name in string")
	}
	if !contains(str, "25 tuples removed") {
		t.Error("expected tuples removed in string")
	}
}

func TestMetrics_RecordVacuum(t *testing.T) {
	metrics := NewMetrics()

	result := &VacuumResult{
		TableName:      "test",
		StartTime:      time.Now(),
		EndTime:        time.Now().Add(time.Second),
		PagesScanned:   100,
		PagesCompacted: 10,
		TuplesRemoved:  50,
		BytesReclaimed: 4096,
	}

	metrics.RecordVacuum(result)

	if metrics.VacuumRunsTotal.Load() != 1 {
		t.Errorf("expected 1 run, got %d", metrics.VacuumRunsTotal.Load())
	}
	if metrics.TuplesRemovedTotal.Load() != 50 {
		t.Errorf("expected 50 tuples, got %d", metrics.TuplesRemovedTotal.Load())
	}
	if metrics.BytesReclaimedTotal.Load() != 4096 {
		t.Errorf("expected 4096 bytes, got %d", metrics.BytesReclaimedTotal.Load())
	}
	if metrics.PagesScannedTotal.Load() != 100 {
		t.Errorf("expected 100 pages scanned, got %d", metrics.PagesScannedTotal.Load())
	}

	// Record another
	metrics.RecordVacuum(result)
	if metrics.VacuumRunsTotal.Load() != 2 {
		t.Errorf("expected 2 runs, got %d", metrics.VacuumRunsTotal.Load())
	}
}

func TestMetrics_PrometheusMetrics(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordVacuum(&VacuumResult{
		TableName:      "test",
		StartTime:      time.Now(),
		EndTime:        time.Now(),
		TuplesRemoved:  10,
		BytesReclaimed: 100,
	})

	output := metrics.PrometheusMetrics()

	expected := []string{
		"veridicaldb_vacuum_runs_total",
		"veridicaldb_vacuum_tuples_removed_total",
		"veridicaldb_vacuum_bytes_reclaimed_total",
		"veridicaldb_vacuum_errors_total",
	}

	for _, exp := range expected {
		if !contains(output, exp) {
			t.Errorf("expected %s in output", exp)
		}
	}
}

func TestMetrics_Reset(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordVacuum(&VacuumResult{
		TableName:      "test",
		StartTime:      time.Now(),
		EndTime:        time.Now(),
		TuplesRemoved:  10,
		BytesReclaimed: 100,
	})

	metrics.Reset()

	if metrics.VacuumRunsTotal.Load() != 0 {
		t.Errorf("expected 0 after reset, got %d", metrics.VacuumRunsTotal.Load())
	}
	if metrics.TuplesRemovedTotal.Load() != 0 {
		t.Errorf("expected 0 after reset, got %d", metrics.TuplesRemovedTotal.Load())
	}
}

// Mock storage for testing
type mockStorage struct {
	pages      map[string]map[uint32][]byte
	pageSize   int
	compactErr error
}

func newMockStorage(pageSize int) *mockStorage {
	return &mockStorage{
		pages:    make(map[string]map[uint32][]byte),
		pageSize: pageSize,
	}
}

func (m *mockStorage) ScanTable(table string) ([]RID, error) {
	pages, ok := m.pages[table]
	if !ok {
		return nil, nil
	}

	var rids []RID
	for pageID, data := range pages {
		slotCount := int(binary.LittleEndian.Uint16(data[10:12]))
		for slot := 0; slot < slotCount; slot++ {
			off := 24 + slot*4
			length := binary.LittleEndian.Uint16(data[off+2 : off+4])
			if length > 0 {
				rids = append(rids, RID{Table: table, Page: pageID, Slot: uint16(slot)})
			}
		}
	}
	return rids, nil
}

func (m *mockStorage) FetchPage(table string, pageID uint32) ([]byte, error) {
	pages, ok := m.pages[table]
	if !ok {
		return nil, nil
	}
	data, ok := pages[pageID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *mockStorage) WritePage(table string, pageID uint32, data []byte) error {
	if m.pages[table] == nil {
		m.pages[table] = make(map[uint32][]byte)
	}
	m.pages[table][pageID] = data
	return nil
}

func (m *mockStorage) GetPageCount(table string) (int, error) {
	pages, ok := m.pages[table]
	if !ok {
		return 0, nil
	}
	return len(pages), nil
}

func (m *mockStorage) FetchRaw(rid RID) ([]byte, error) {
	return nil, nil
}

func (m *mockStorage) DeleteSlot(rid RID) error {
	return nil
}

func (m *mockStorage) CompactPage(table string, pageID uint32, deadSlots []int) (int64, error) {
	if m.compactErr != nil {
		return 0, m.compactErr
	}

	data, err := m.FetchPage(table, pageID)
	if err != nil {
		return 0, err
	}

	var reclaimed int64
	for _, slot := range deadSlots {
		off := 24 + slot*4
		offset := binary.LittleEndian.Uint16(data[off : off+2])
		length := binary.LittleEndian.Uint16(data[off+2 : off+4])
		if length > 0 {
			reclaimed += int64(length)
			binary.LittleEndian.PutUint16(data[off:off+2], 0)
			binary.LittleEndian.PutUint16(data[off+2:off+4], 0)
		}
		_ = offset
	}

	return reclaimed, m.WritePage(table, pageID, data)
}

// addPage adds a test page with tuples to the mock storage
func (m *mockStorage) addPage(table string, pageID uint32, tuples [][]byte) {
	if m.pages[table] == nil {
		m.pages[table] = make(map[uint32][]byte)
	}

	data := make([]byte, m.pageSize)
	// Header
	binary.LittleEndian.PutUint16(data[8:10], 0xDB01) // magic
	binary.LittleEndian.PutUint16(data[10:12], uint16(len(tuples)))
	binary.LittleEndian.PutUint32(data[12:16], 24+uint32(len(tuples)*4))
	binary.LittleEndian.PutUint32(data[16:20], uint32(m.pageSize))

	// Add tuples from end
	freeEnd := m.pageSize
	for slot, tuple := range tuples {
		freeEnd -= len(tuple)
		copy(data[freeEnd:], tuple)

		off := 24 + slot*4
		binary.LittleEndian.PutUint16(data[off:off+2], uint16(freeEnd))
		binary.LittleEndian.PutUint16(data[off+2:off+4], uint16(len(tuple)))
	}
	binary.LittleEndian.PutUint32(data[16:20], uint32(freeEnd))

	m.pages[table][pageID] = data
}

// Mock catalog
type mockCatalog struct {
	tables []string
	stats  map[string]*TableStats
}

func newMockCatalog(tables []string) *mockCatalog {
	return &mockCatalog{
		tables: tables,
		stats:  make(map[string]*TableStats),
	}
}

func (m *mockCatalog) ListTables() []string {
	return m.tables
}

func (m *mockCatalog) GetTableStats(table string) (*TableStats, error) {
	if stats, ok := m.stats[table]; ok {
		return stats, nil
	}
	return &TableStats{TableName: table}, nil
}

func TestManager_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 100 * time.Millisecond

	txnMgr := txn.NewManager()
	storage := newMockStorage(4096)
	catalog := newMockCatalog([]string{})

	mgr := NewManager(cfg, txnMgr, storage, catalog)

	if mgr.IsRunning() {
		t.Error("manager should not be running initially")
	}

	if err := mgr.Start(); err != nil {
		t.Fatalf("failed to start: %v", err)
	}

	if !mgr.IsRunning() {
		t.Error("manager should be running after Start")
	}

	// Try starting again - should error
	if err := mgr.Start(); err == nil {
		t.Error("expected error when starting already running manager")
	}

	if err := mgr.Stop(); err != nil {
		t.Fatalf("failed to stop: %v", err)
	}

	if mgr.IsRunning() {
		t.Error("manager should not be running after Stop")
	}
}

func TestManager_VacuumTable(t *testing.T) {
	cfg := DefaultConfig()
	txnMgr := txn.NewManager()
	storage := newMockStorage(4096)
	catalog := newMockCatalog([]string{"test_table"})

	// Create a committed transaction for xmin
	tx := txnMgr.Begin()
	_ = txnMgr.Commit(tx.ID)

	// Create MVCC tuple (live - no xmax)
	liveTuple := make([]byte, 32)
	binary.LittleEndian.PutUint64(liveTuple[0:8], uint64(tx.ID)) // xmin
	binary.LittleEndian.PutUint64(liveTuple[8:16], 0)            // xmax = 0 (not deleted)

	// Create MVCC tuple (dead - has xmax from committed tx)
	tx2 := txnMgr.Begin()
	_ = txnMgr.Commit(tx2.ID)

	deadTuple := make([]byte, 32)
	binary.LittleEndian.PutUint64(deadTuple[0:8], uint64(tx.ID))   // xmin
	binary.LittleEndian.PutUint64(deadTuple[8:16], uint64(tx2.ID)) // xmax = committed tx

	storage.addPage("test_table", 0, [][]byte{liveTuple, deadTuple})

	mgr := NewManager(cfg, txnMgr, storage, catalog)

	ctx := context.Background()
	result := mgr.VacuumTable(ctx, "test_table", false)

	if result.Error != nil {
		t.Fatalf("vacuum error: %v", result.Error)
	}

	if result.PagesScanned != 1 {
		t.Errorf("expected 1 page scanned, got %d", result.PagesScanned)
	}

	// Dead tuple should have been removed
	if result.TuplesRemoved != 1 {
		t.Errorf("expected 1 tuple removed, got %d", result.TuplesRemoved)
	}
}

func TestWorker_IsTupleDead(t *testing.T) {
	txnMgr := txn.NewManager()

	// Create and commit a transaction
	tx1 := txnMgr.Begin()
	_ = txnMgr.Commit(tx1.ID)

	tx2 := txnMgr.Begin()
	_ = txnMgr.Commit(tx2.ID)

	worker := &Worker{
		txnMgr: txnMgr,
		config: DefaultConfig(),
	}

	tests := []struct {
		name         string
		xmin         txn.TxID
		xmax         txn.TxID
		oldestActive txn.TxID
		wantDead     bool
	}{
		{
			name:         "live tuple (no xmax)",
			xmin:         tx1.ID,
			xmax:         txn.InvalidTxID,
			oldestActive: txn.InvalidTxID,
			wantDead:     false,
		},
		{
			name:         "dead tuple (committed delete, no active txns)",
			xmin:         tx1.ID,
			xmax:         tx2.ID,
			oldestActive: txn.InvalidTxID,
			wantDead:     true,
		},
		{
			name:         "recently dead (delete newer than oldest active)",
			xmin:         tx1.ID,
			xmax:         tx2.ID,
			oldestActive: tx1.ID, // oldest active is before xmax
			wantDead:     false,  // might still be visible to tx1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &txn.MVCCHeader{
				XMin: tt.xmin,
				XMax: tt.xmax,
			}
			got := worker.isTupleDead(header, tt.oldestActive)
			if got != tt.wantDead {
				t.Errorf("isTupleDead() = %v, want %v", got, tt.wantDead)
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
