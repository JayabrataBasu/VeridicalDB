package sql

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// setupMVCCTest creates a temporary directory and returns a Session for testing.
func setupMVCCTest(t *testing.T) (*Session, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "mvcc_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	tm, err := catalog.NewTableManager(dir, 8192)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create table manager: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr)
	session := NewSession(mtm)

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return session, cleanup
}

// TestMVCCAutocommit tests that statements execute with autocommit by default.
func TestMVCCAutocommit(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	// Create table (doesn't need transaction)
	result, err := session.ExecuteSQL("CREATE TABLE users (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if result.Message == "" {
		t.Error("expected confirmation message")
	}

	// Insert row (autocommit)
	result, err = session.ExecuteSQL("INSERT INTO users VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify row is visible (autocommit means it's committed)
	result, err = session.ExecuteSQL("SELECT * FROM users;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestMVCCExplicitTransaction tests BEGIN/COMMIT.
func TestMVCCExplicitTransaction(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE products (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Start explicit transaction
	result, err := session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	if result.Message == "" || !session.InTransaction() {
		t.Error("expected to be in transaction after BEGIN")
	}

	// Insert within transaction
	_, err = session.ExecuteSQL("INSERT INTO products VALUES (1, 'Widget');")
	if err != nil {
		t.Fatalf("INSERT in transaction failed: %v", err)
	}

	// Row should be visible within same transaction
	result, err = session.ExecuteSQL("SELECT * FROM products;")
	if err != nil {
		t.Fatalf("SELECT in transaction failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row visible in transaction, got %d", len(result.Rows))
	}

	// Commit
	result, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
	if session.InTransaction() {
		t.Error("should not be in transaction after COMMIT")
	}

	// Row should still be visible after commit
	result, err = session.ExecuteSQL("SELECT * FROM products;")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after commit, got %d", len(result.Rows))
	}
}

// TestMVCCRollback tests that ROLLBACK discards changes.
func TestMVCCRollback(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	// Create table and insert initial row
	_, err := session.ExecuteSQL("CREATE TABLE orders (id INT, total INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO orders VALUES (1, 100);")
	if err != nil {
		t.Fatalf("Initial INSERT failed: %v", err)
	}

	// Start transaction
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert another row
	_, err = session.ExecuteSQL("INSERT INTO orders VALUES (2, 200);")
	if err != nil {
		t.Fatalf("INSERT in transaction failed: %v", err)
	}

	// Should see 2 rows in transaction
	result, err := session.ExecuteSQL("SELECT * FROM orders;")
	if err != nil {
		t.Fatalf("SELECT in transaction failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows in transaction, got %d", len(result.Rows))
	}

	// Rollback
	_, err = session.ExecuteSQL("ROLLBACK;")
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
	if session.InTransaction() {
		t.Error("should not be in transaction after ROLLBACK")
	}

	// Should only see 1 row (the committed one)
	result, err = session.ExecuteSQL("SELECT * FROM orders;")
	if err != nil {
		t.Fatalf("SELECT after rollback failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after rollback, got %d", len(result.Rows))
	}
}

// TestMVCCMultipleInserts tests multiple inserts in one transaction.
func TestMVCCMultipleInserts(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE items (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert multiple rows
	for i := 1; i <= 5; i++ {
		_, err = session.ExecuteSQL("INSERT INTO items VALUES (" + itoa(i) + ", 'item');")
		if err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	// All should be visible in transaction
	result, err := session.ExecuteSQL("SELECT * FROM items;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows in transaction, got %d", len(result.Rows))
	}

	// Commit
	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Still 5 rows after commit
	result, err = session.ExecuteSQL("SELECT * FROM items;")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows after commit, got %d", len(result.Rows))
	}
}

// TestMVCCUpdate tests UPDATE within a transaction.
func TestMVCCUpdate(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE accounts (id INT, balance INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO accounts VALUES (1, 1000);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Start transaction
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Update the balance
	result, err := session.ExecuteSQL("UPDATE accounts SET balance = 500 WHERE id = 1;")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 affected row, got %d", result.RowsAffected)
	}

	// Check updated value
	result, err = session.ExecuteSQL("SELECT balance FROM accounts WHERE id = 1;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int32 != 500 {
		t.Errorf("expected balance 500, got %d", result.Rows[0][0].Int32)
	}

	// Commit
	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify update persisted
	result, err = session.ExecuteSQL("SELECT balance FROM accounts WHERE id = 1;")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if result.Rows[0][0].Int32 != 500 {
		t.Errorf("expected balance 500 after commit, got %d", result.Rows[0][0].Int32)
	}
}

// TestMVCCDelete tests DELETE within a transaction.
func TestMVCCDelete(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE todos (id INT, task TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert initial data
	_, err = session.ExecuteSQL("INSERT INTO todos VALUES (1, 'Task A');")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO todos VALUES (2, 'Task B');")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	// Start transaction
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Delete one row
	result, err := session.ExecuteSQL("DELETE FROM todos WHERE id = 1;")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 affected row, got %d", result.RowsAffected)
	}

	// Only 1 row should be visible
	result, err = session.ExecuteSQL("SELECT * FROM todos;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after delete, got %d", len(result.Rows))
	}

	// Commit
	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify delete persisted
	result, err = session.ExecuteSQL("SELECT * FROM todos;")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after commit, got %d", len(result.Rows))
	}
}

// TestMVCCNestedBeginError tests that nested BEGIN fails.
func TestMVCCNestedBeginError(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("First BEGIN failed: %v", err)
	}

	_, err = session.ExecuteSQL("BEGIN;")
	if err == nil {
		t.Error("expected error on nested BEGIN, got nil")
	}

	// Clean up
	_, _ = session.ExecuteSQL("ROLLBACK;")
}

// TestMVCCCommitWithoutTransaction tests that COMMIT without transaction fails.
func TestMVCCCommitWithoutTransaction(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("COMMIT;")
	if err == nil {
		t.Error("expected error on COMMIT without transaction, got nil")
	}
}

// TestMVCCRollbackWithoutTransaction tests that ROLLBACK without transaction fails.
func TestMVCCRollbackWithoutTransaction(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("ROLLBACK;")
	if err == nil {
		t.Error("expected error on ROLLBACK without transaction, got nil")
	}
}

// Helper function to convert int to string (avoiding fmt import)
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var s string
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}
