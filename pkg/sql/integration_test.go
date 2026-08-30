package sql

import (
	"fmt"
	"testing"
)

// TestIntegration_BufferPoolPerformance tests buffer pool performance improvement.
func TestIntegration_BufferPoolPerformance(t *testing.T) {
	session := newParitySession(t, setupTestTableManager(t))

	if _, err := session.ExecuteSQL("CREATE TABLE perf_test (id INT, data TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := session.ExecuteSQL(fmt.Sprintf("INSERT INTO perf_test VALUES (%d, 'd%d')", i, i)); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	// Repeated scans should be served from the buffer pool / query cache.
	for i := 0; i < 10; i++ {
		result, err := session.ExecuteSQL("SELECT * FROM perf_test")
		if err != nil {
			t.Fatalf("query %d failed: %v", i, err)
		}
		if len(result.Rows) != 50 {
			t.Errorf("expected 50 rows, got %d", len(result.Rows))
		}
	}
}

func TestIntegration_SessionQueryCacheRepeatedSQL(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	var err error

	_, err = session.ExecuteSQL("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO users VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	_, err = session.ExecuteSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("first select failed: %v", err)
	}

	_, err = session.ExecuteSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("second select failed: %v", err)
	}

	stats := session.QueryCacheStats()
	if stats.Hits < 1 {
		t.Fatalf("expected at least one session query cache hit, got %d", stats.Hits)
	}
	if stats.CurrentEntries == 0 {
		t.Fatal("expected session query cache to contain entries")
	}
	if stats.HitRate <= 0 {
		t.Fatalf("expected positive hit rate, got %.2f", stats.HitRate)
	}

	t.Logf("session cache stats: hits=%d misses=%d entries=%d hitrate=%.2f%%",
		stats.Hits, stats.Misses, stats.CurrentEntries, stats.HitRate)
}

func TestIntegration_SessionQueryCacheNormalization(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	var err error

	_, err = session.ExecuteSQL("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO users VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	_, err = session.ExecuteSQL("SELECT   *   FROM users WHERE id = 1;")
	if err != nil {
		t.Fatalf("first normalized select failed: %v", err)
	}

	_, err = session.ExecuteSQL("  SELECT * FROM users WHERE id = 1  ")
	if err != nil {
		t.Fatalf("second normalized select failed: %v", err)
	}

	stats := session.QueryCacheStats()
	if stats.Hits < 1 {
		t.Fatalf("expected normalized SQL variant to hit session cache, got hits=%d", stats.Hits)
	}
}

func TestIntegration_MVCCSelectLimitOffsetFastPath(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE nums (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = session.ExecuteSQL(fmt.Sprintf("INSERT INTO nums VALUES (%d, 'n%d')", i, i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	result, err := session.ExecuteSQL("SELECT id, name FROM nums LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("limited select failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int32 != 2 || result.Rows[1][0].Int32 != 3 {
		t.Fatalf("expected ids [2,3], got [%d,%d]",
			result.Rows[0][0].Int32, result.Rows[1][0].Int32)
	}

	zeroResult, err := session.ExecuteSQL("SELECT id FROM nums LIMIT 0")
	if err != nil {
		t.Fatalf("limit 0 select failed: %v", err)
	}
	if len(zeroResult.Rows) != 0 {
		t.Fatalf("expected 0 rows for LIMIT 0, got %d", len(zeroResult.Rows))
	}
}

func TestIntegration_MVCCUniqueEqualitySelect(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for i := 1; i <= 5; i++ {
		_, err = session.ExecuteSQL(fmt.Sprintf("INSERT INTO users VALUES (%d, 'u%d')", i, i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	result, err := session.ExecuteSQL("SELECT * FROM users WHERE id = 4")
	if err != nil {
		t.Fatalf("unique equality select failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected exactly 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int32 != 4 {
		t.Fatalf("expected id 4, got %d", result.Rows[0][0].Int32)
	}
}
