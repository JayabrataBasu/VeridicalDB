package observability

import (
	"os"
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestNewSystemCatalog(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()

	sc := NewSystemCatalog(txnMgr, lockMgr, cat)
	if sc == nil {
		t.Fatal("NewSystemCatalog returned nil")
	}

	if sc.txnMgr != txnMgr {
		t.Error("txnMgr not set correctly")
	}
	if sc.lockMgr != lockMgr {
		t.Error("lockMgr not set correctly")
	}
	if sc.cat != cat {
		t.Error("catalog not set correctly")
	}
	if sc.stats == nil {
		t.Error("stats not initialized")
	}
}

func TestStatistics(t *testing.T) {
	stats := NewStatistics()

	if stats.StartTime.IsZero() {
		t.Error("StartTime not set")
	}

	stats.RecordQuery(true, 1000000)
	stats.RecordQuery(true, 2000000)
	stats.RecordQuery(false, 500000)

	snapshot := stats.Snapshot()
	if snapshot.QueriesExecuted != 3 {
		t.Errorf("QueriesExecuted: got %d, want 3", snapshot.QueriesExecuted)
	}
	if snapshot.QueriesSucceeded != 2 {
		t.Errorf("QueriesSucceeded: got %d, want 2", snapshot.QueriesSucceeded)
	}
	if snapshot.QueriesFailed != 1 {
		t.Errorf("QueriesFailed: got %d, want 1", snapshot.QueriesFailed)
	}
	if snapshot.TotalQueryTimeNs != 3500000 {
		t.Errorf("TotalQueryTimeNs: got %d, want 3500000", snapshot.TotalQueryTimeNs)
	}
}

func TestStatisticsTransactions(t *testing.T) {
	stats := NewStatistics()

	stats.RecordTransaction("start")
	stats.RecordTransaction("start")
	stats.RecordTransaction("commit")
	stats.RecordTransaction("abort")

	snapshot := stats.Snapshot()
	if snapshot.TransactionsStarted != 2 {
		t.Errorf("TransactionsStarted: got %d, want 2", snapshot.TransactionsStarted)
	}
	if snapshot.TransactionsCommitted != 1 {
		t.Errorf("TransactionsCommitted: got %d, want 1", snapshot.TransactionsCommitted)
	}
	if snapshot.TransactionsAborted != 1 {
		t.Errorf("TransactionsAborted: got %d, want 1", snapshot.TransactionsAborted)
	}
}

func TestStatisticsRowOps(t *testing.T) {
	stats := NewStatistics()

	stats.RecordRowOp("insert", 10)
	stats.RecordRowOp("update", 5)
	stats.RecordRowOp("delete", 2)
	stats.RecordRowOp("scan", 100)

	snapshot := stats.Snapshot()
	if snapshot.RowsInserted != 10 {
		t.Errorf("RowsInserted: got %d, want 10", snapshot.RowsInserted)
	}
	if snapshot.RowsUpdated != 5 {
		t.Errorf("RowsUpdated: got %d, want 5", snapshot.RowsUpdated)
	}
	if snapshot.RowsDeleted != 2 {
		t.Errorf("RowsDeleted: got %d, want 2", snapshot.RowsDeleted)
	}
	if snapshot.RowsScanned != 100 {
		t.Errorf("RowsScanned: got %d, want 100", snapshot.RowsScanned)
	}
}

func TestStatisticsIndexOps(t *testing.T) {
	stats := NewStatistics()

	stats.RecordIndexOp(true)
	stats.RecordIndexOp(true)
	stats.RecordIndexOp(false)

	snapshot := stats.Snapshot()
	if snapshot.IndexScans != 3 {
		t.Errorf("IndexScans: got %d, want 3", snapshot.IndexScans)
	}
	if snapshot.IndexHits != 2 {
		t.Errorf("IndexHits: got %d, want 2", snapshot.IndexHits)
	}
	if snapshot.IndexMisses != 1 {
		t.Errorf("IndexMisses: got %d, want 1", snapshot.IndexMisses)
	}
}

func TestGetActiveTransactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()
	sc := NewSystemCatalog(txnMgr, lockMgr, cat)

	// Start some transactions
	tx1 := txnMgr.Begin()
	tx2 := txnMgr.Begin()

	rows := sc.GetActiveTransactions()
	if len(rows) != 2 {
		t.Errorf("GetActiveTransactions: got %d rows, want 2", len(rows))
	}

	// Commit one transaction
	_ = txnMgr.Commit(tx1.ID)

	rows = sc.GetActiveTransactions()
	if len(rows) != 1 {
		t.Errorf("After commit: got %d rows, want 1", len(rows))
	}

	// Check the remaining transaction
	if len(rows) > 0 && rows[0].Values[0] != tx2.ID {
		t.Errorf("Wrong transaction ID: got %v, want %v", rows[0].Values[0], tx2.ID)
	}
}

func TestGetLocks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()
	sc := NewSystemCatalog(txnMgr, lockMgr, cat)

	// Acquire some locks
	tx := txnMgr.Begin()
	resource := lock.ResourceID{Type: lock.ResourceRow, TableName: "test", RID: storage.RID{Table: "test", Page: 1, Slot: 100}}
	_ = lockMgr.Acquire(tx.ID, resource, lock.ModeExclusive)

	rows := sc.GetLocks()
	if len(rows) != 1 {
		t.Errorf("GetLocks: got %d rows, want 1", len(rows))
	}

	// Release lock
	_ = lockMgr.Release(tx.ID, resource)

	rows = sc.GetLocks()
	if len(rows) != 0 {
		t.Errorf("After release: got %d rows, want 0", len(rows))
	}
}

func TestGetTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	// Create a table
	cols := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt32, NotNull: true},
		{Name: "name", Type: catalog.TypeText},
	}
	_, _ = cat.CreateTable("users", cols, nil, "row")

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()
	sc := NewSystemCatalog(txnMgr, lockMgr, cat)

	rows := sc.GetTables()
	if len(rows) != 1 {
		t.Errorf("GetTables: got %d rows, want 1", len(rows))
	}

	row := rows[0]
	if row.Columns[0] != "table_name" || row.Columns[1] != "column_count" || row.Columns[2] != "storage_type" {
		t.Errorf("Wrong columns: %v", row.Columns)
	}
	if row.Values[0] != "users" {
		t.Errorf("Wrong table_name: got %v, want users", row.Values[0])
	}
	if row.Values[1] != 2 {
		t.Errorf("Wrong column_count: got %v, want 2", row.Values[1])
	}
	if row.Values[2] != "row" {
		t.Errorf("Wrong storage_type: got %v, want row", row.Values[2])
	}
}

func TestGetColumns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	cols := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt32, NotNull: true},
		{Name: "email", Type: catalog.TypeText, NotNull: false},
	}
	_, _ = cat.CreateTable("accounts", cols, nil, "row")

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()
	sc := NewSystemCatalog(txnMgr, lockMgr, cat)

	rows := sc.GetColumns("accounts")
	if len(rows) != 2 {
		t.Errorf("GetColumns: got %d rows, want 2", len(rows))
	}

	if rows[0].Values[1] != "id" {
		t.Errorf("First column name: got %v, want id", rows[0].Values[1])
	}
	if rows[0].Values[3] != true {
		t.Errorf("First column not_null: got %v, want true", rows[0].Values[3])
	}
}

func TestGetStatistics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()
	sc := NewSystemCatalog(txnMgr, lockMgr, cat)

	sc.Stats().RecordQuery(true, 1000000)
	sc.Stats().RecordTransaction("start")
	sc.Stats().RecordRowOp("insert", 5)

	rows := sc.GetStatistics()
	if len(rows) == 0 {
		t.Fatal("GetStatistics returned empty")
	}

	found := false
	for _, row := range rows {
		if row.Values[0] == "uptime_seconds" {
			found = true
			break
		}
	}
	if !found {
		t.Error("uptime_seconds metric not found")
	}
}

func TestGetMemoryStats(t *testing.T) {
	sc := NewSystemCatalog(nil, nil, nil)

	rows := sc.GetMemoryStats()
	if len(rows) != 5 {
		t.Errorf("GetMemoryStats: got %d rows, want 5", len(rows))
	}

	expectedMetrics := []string{"heap_alloc_mb", "heap_sys_mb", "heap_objects", "goroutines", "gc_cycles"}
	for i, expected := range expectedMetrics {
		if rows[i].Values[0] != expected {
			t.Errorf("Metric %d: got %v, want %v", i, rows[i].Values[0], expected)
		}
	}
}

func TestPrometheusMetrics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "observability_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := catalog.NewCatalog(tmpDir)
	if err != nil {
		t.Fatalf("create catalog: %v", err)
	}

	txnMgr := txn.NewManager()
	lockMgr := lock.NewManager()
	sc := NewSystemCatalog(txnMgr, lockMgr, cat)

	sc.Stats().RecordQuery(true, 1000000)
	sc.Stats().RecordQuery(false, 500000)
	sc.Stats().RecordTransaction("commit")
	sc.Stats().RecordRowOp("insert", 10)

	metrics := sc.PrometheusMetrics()

	expectedLines := []string{
		"veridicaldb_queries_total",
		"veridicaldb_transactions_total",
		"veridicaldb_rows_total",
		"veridicaldb_heap_bytes",
		"veridicaldb_goroutines",
		"veridicaldb_uptime_seconds",
	}

	for _, expected := range expectedLines {
		if !strings.Contains(metrics, expected) {
			t.Errorf("Missing metric: %s", expected)
		}
	}
}

func TestGetIndexesEmpty(t *testing.T) {
	sc := NewSystemCatalog(nil, nil, nil)

	rows := sc.GetIndexes()
	if rows != nil {
		t.Errorf("GetIndexes: expected nil, got %v", rows)
	}
}

func TestNilComponents(t *testing.T) {
	sc := NewSystemCatalog(nil, nil, nil)

	if rows := sc.GetActiveTransactions(); rows != nil {
		t.Error("GetActiveTransactions should return nil with nil txnMgr")
	}
	if rows := sc.GetLocks(); rows != nil {
		t.Error("GetLocks should return nil with nil lockMgr")
	}
	if rows := sc.GetTables(); rows != nil {
		t.Error("GetTables should return nil with nil catalog")
	}
	if rows := sc.GetColumns("test"); rows != nil {
		t.Error("GetColumns should return nil with nil catalog")
	}
}
