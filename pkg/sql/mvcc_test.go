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

	tm, err := catalog.NewTableManager(dir, 8192, nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create table manager: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
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
	_, err = session.ExecuteSQL("INSERT INTO users VALUES (1, 'Alice');")
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
	_, err = session.ExecuteSQL("COMMIT;")
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

// TestMVCCSubqueries tests scalar, IN, and EXISTS subquery execution in MVCC executor.
func TestMVCCSubqueries(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	// Create base tables
	_, err := session.ExecuteSQL("CREATE TABLE a (id INT, v INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE a failed: %v", err)
	}
	_, err = session.ExecuteSQL("CREATE TABLE b (id INT, v INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE b failed: %v", err)
	}

	// Insert rows
	_, err = session.ExecuteSQL("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30);")
	if err != nil {
		t.Fatalf("INSERT a failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO b VALUES (100, 999), (2, 200);")
	if err != nil {
		t.Fatalf("INSERT b failed: %v", err)
	}

	// Scalar subquery: select rows from a where v = (SELECT MAX(v) FROM a)
	// Verify inner scalar subquery works on its own
	inner, innerErr := session.ExecuteSQL("SELECT MAX(v) FROM a;")
	if innerErr != nil {
		t.Fatalf("inner scalar subquery failed: %v", innerErr)
	}
	if len(inner.Rows) != 1 {
		t.Fatalf("unexpected inner subquery rows: %#v", inner.Rows)
	}

	res, err := session.ExecuteSQL("SELECT id FROM a WHERE v = (SELECT MAX(v) FROM a);")
	if err != nil {
		t.Fatalf("scalar subquery SELECT failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int32 != 3 {
		t.Fatalf("unexpected scalar subquery result: %#v", res.Rows)
	}

	// IN subquery: select ids from a where id IN (SELECT id FROM b)
	res, err = session.ExecuteSQL("SELECT id FROM a WHERE id IN (SELECT id FROM b) ORDER BY id;")
	if err != nil {
		t.Fatalf("IN subquery SELECT failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Int32 != 2 {
		t.Fatalf("unexpected IN subquery result: %#v", res.Rows)
	}

	// EXISTS uncorrelated: should return all rows in a if b has any rows
	res, err = session.ExecuteSQL("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b) ORDER BY id;")
	if err != nil {
		t.Fatalf("EXISTS subquery SELECT failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("unexpected EXISTS subquery result: %#v", res.Rows)
	}
}

// TestMVCCGroupBy tests GROUP BY and HAVING under MVCC executor.
func TestMVCCGroupBy(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE sales (dept TEXT, salary INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 5);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	res, err := session.ExecuteSQL("SELECT dept, SUM(salary) as total FROM sales GROUP BY dept ORDER BY dept;")
	if err != nil {
		t.Fatalf("GROUP BY SELECT failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("unexpected GROUP BY rows: %#v", res.Rows)
	}
	if res.Rows[0][0].Text != "A" || res.Rows[0][1].Int64 != 30 {
		t.Fatalf("unexpected GROUP BY first row: %#v", res.Rows[0])
	}

	// HAVING
	// (debug logged above)

	res, err = session.ExecuteSQL("SELECT dept, SUM(salary) as total FROM sales GROUP BY dept HAVING SUM(salary) > 15 ORDER BY dept;")
	if err != nil {
		t.Fatalf("HAVING SELECT failed: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0].Text != "A" || res.Rows[0][1].Int64 != 30 {
		t.Fatalf("unexpected HAVING result: %#v", res.Rows)
	}

	// Debug: inspect parsed columns (temporary)
	p := NewParser("SELECT dept, SUM(salary) as total FROM sales GROUP BY dept HAVING SUM(salary) > 15 ORDER BY dept;")
	parsed, perr := p.Parse()
	if perr != nil {
		t.Fatalf("parse failed: %v", perr)
	}
	sel, ok := parsed.(*SelectStmt)
	if !ok {
		t.Fatalf("unexpected parsed statement type: %T", parsed)
	}
	t.Logf("Parsed columns: %+v", sel.Columns)
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

// TestMVCCCreateView tests CREATE VIEW and SELECT from view within MVCC executor.
func TestMVCCCreateView(t *testing.T) {
	session, cleanup := setupMVCCTest(t)
	defer cleanup()

	_, err := session.ExecuteSQL("CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100), (2, 'Bob', 'Sales', 80), (3, 'Carol', 'Sales', 90);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Create view inside a transaction
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = session.ExecuteSQL("CREATE VIEW sales_people AS SELECT id, name, salary FROM employees WHERE department = 'Sales';")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Select from view inside transaction
	res, err := session.ExecuteSQL("SELECT id, name FROM sales_people ORDER BY id;")
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("unexpected rows from view: %#v", res.Rows)
	}

	// Commit and select again
	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	res, err = session.ExecuteSQL("SELECT id, name FROM sales_people ORDER BY id;")
	if err != nil {
		t.Fatalf("SELECT from view after commit failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("unexpected rows from view after commit: %#v", res.Rows)
	}

	// Drop view
	_, err = session.ExecuteSQL("DROP VIEW sales_people;")
	if err != nil {
		t.Fatalf("DROP VIEW failed: %v", err)
	}
	// Dropped view should error when selecting
	_, err = session.ExecuteSQL("SELECT * FROM sales_people;")
	if err == nil {
		t.Fatalf("expected error selecting from dropped view, got nil")
	}
}
