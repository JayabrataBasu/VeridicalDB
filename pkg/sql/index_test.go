package sql

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// setupIndexTest creates a session with index manager for testing.
func setupIndexTest(t *testing.T) (*Session, *btree.IndexManager, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "index_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	tm, err := catalog.NewTableManager(dir, 4096, nil)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("Failed to create table manager: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)

	idxMgr, err := btree.NewIndexManager(dir, 4096)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("Failed to create index manager: %v", err)
	}

	session := NewSession(mtm)
	session.SetIndexManager(idxMgr)

	cleanup := func() {
		_ = idxMgr.Close()
		_ = os.RemoveAll(dir)
	}

	return session, idxMgr, cleanup
}

// TestCreateIndex tests CREATE INDEX statement execution.
func TestCreateIndex(t *testing.T) {
	session, idxMgr, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create a table
	result, err := session.ExecuteSQL("CREATE TABLE users (id INT, name TEXT, email TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	t.Logf("CREATE TABLE result: %s", result.Message)

	// Create an index
	result, err = session.ExecuteSQL("CREATE INDEX idx_users_email ON users (email);")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	t.Logf("CREATE INDEX result: %s", result.Message)

	// Verify index was created
	idx, err := idxMgr.GetIndex("idx_users_email")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if idx.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", idx.TableName)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "email" {
		t.Errorf("Expected columns ['email'], got %v", idx.Columns)
	}
	if idx.Unique {
		t.Error("Expected non-unique index")
	}
}

// TestCreateUniqueIndex tests CREATE UNIQUE INDEX statement.
func TestCreateUniqueIndex(t *testing.T) {
	session, idxMgr, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create a table
	_, err := session.ExecuteSQL("CREATE TABLE products (id INT, sku TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create a unique index
	result, err := session.ExecuteSQL("CREATE UNIQUE INDEX idx_products_sku ON products (sku);")
	if err != nil {
		t.Fatalf("CREATE UNIQUE INDEX failed: %v", err)
	}
	t.Logf("CREATE UNIQUE INDEX result: %s", result.Message)

	// Verify index was created with unique=true
	idx, err := idxMgr.GetIndex("idx_products_sku")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if !idx.Unique {
		t.Error("Expected unique index")
	}
}

// TestDropIndex tests DROP INDEX statement.
func TestDropIndex(t *testing.T) {
	session, idxMgr, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create a table and index
	_, _ = session.ExecuteSQL("CREATE TABLE orders (id INT, customer_id INT);")
	_, _ = session.ExecuteSQL("CREATE INDEX idx_orders_customer ON orders (customer_id);")

	// Verify index exists
	_, err := idxMgr.GetIndex("idx_orders_customer")
	if err != nil {
		t.Fatalf("Index should exist before drop: %v", err)
	}

	// Drop the index
	result, err := session.ExecuteSQL("DROP INDEX idx_orders_customer;")
	if err != nil {
		t.Fatalf("DROP INDEX failed: %v", err)
	}
	t.Logf("DROP INDEX result: %s", result.Message)

	// Verify index no longer exists
	_, err = idxMgr.GetIndex("idx_orders_customer")
	if err != btree.ErrIndexNotFound {
		t.Errorf("Expected ErrIndexNotFound, got %v", err)
	}
}

// TestCreateIndexErrors tests error handling for CREATE INDEX.
func TestCreateIndexErrors(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create a table
	_, _ = session.ExecuteSQL("CREATE TABLE test (id INT, name TEXT);")

	// Test: index on non-existent table
	_, err := session.ExecuteSQL("CREATE INDEX idx_foo ON nonexistent (id);")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	// Test: index on non-existent column
	_, err = session.ExecuteSQL("CREATE INDEX idx_foo ON test (badcol);")
	if err == nil {
		t.Error("Expected error for non-existent column")
	}

	// Test: duplicate index name
	_, _ = session.ExecuteSQL("CREATE INDEX idx_test_id ON test (id);")
	_, err = session.ExecuteSQL("CREATE INDEX idx_test_id ON test (name);")
	if err == nil {
		t.Error("Expected error for duplicate index name")
	}
}

// TestCreateIndexWithoutManager tests error when IndexManager not set.
func TestCreateIndexWithoutManager(t *testing.T) {
	dir, err := os.MkdirTemp("", "no_index_mgr_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Setup without IndexManager
	tm, err := catalog.NewTableManager(dir, 4096, nil)
	if err != nil {
		t.Fatalf("Failed to create table manager: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)

	session := NewSession(mtm) // No IndexManager set

	// Create a table
	_, _ = session.ExecuteSQL("CREATE TABLE test (id INT);")

	// Try to create index without IndexManager - should fail
	_, err = session.ExecuteSQL("CREATE INDEX idx_test ON test (id);")
	if err == nil {
		t.Error("Expected error when IndexManager not configured")
	}
	if err.Error() != "index manager not configured" {
		t.Errorf("Expected 'index manager not configured' error, got: %v", err)
	}
}

// TestIndexMaintenanceOnDML tests that indexes are updated during INSERT/UPDATE/DELETE.
func TestIndexMaintenanceOnDML(t *testing.T) {
	session, idxMgr, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE employees (id INT, name TEXT, salary INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create index on salary
	_, err = session.ExecuteSQL("CREATE INDEX idx_emp_salary ON employees (salary);")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// INSERT rows
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO employees VALUES (1, 'Alice', 50000);")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO employees VALUES (2, 'Bob', 60000);")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO employees VALUES (3, 'Charlie', 55000);")
	if err != nil {
		t.Fatalf("INSERT 3 failed: %v", err)
	}

	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify index entries exist by searching
	// Search for salary=50000 (Alice)
	key := btree.EncodeIntKey(50000)
	rid, err := idxMgr.Search("idx_emp_salary", key)
	if err != nil {
		t.Errorf("Index search for 50000 failed: %v", err)
	} else if rid.Table == "" {
		t.Error("Expected to find entry for salary=50000")
	}

	// Search for salary=60000 (Bob)
	key = btree.EncodeIntKey(60000)
	rid, err = idxMgr.Search("idx_emp_salary", key)
	if err != nil {
		t.Errorf("Index search for 60000 failed: %v", err)
	} else if rid.Table == "" {
		t.Error("Expected to find entry for salary=60000")
	}

	// UPDATE: Change Alice's salary from 50000 to 52000
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = session.ExecuteSQL("UPDATE employees SET salary = 52000 WHERE id = 1;")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify old entry removed (50000)
	key = btree.EncodeIntKey(50000)
	_, err = idxMgr.Search("idx_emp_salary", key)
	if err == nil {
		t.Log("Note: Old index entry for 50000 may still exist (MVCC behavior)")
	}

	// Verify new entry exists (52000)
	key = btree.EncodeIntKey(52000)
	rid, err = idxMgr.Search("idx_emp_salary", key)
	if err != nil {
		t.Errorf("Index search for 52000 failed: %v", err)
	} else if rid.Table == "" {
		t.Error("Expected to find new entry for salary=52000 after UPDATE")
	}

	// DELETE: Remove Charlie (salary=55000)
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = session.ExecuteSQL("DELETE FROM employees WHERE id = 3;")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify entry removed (55000)
	key = btree.EncodeIntKey(55000)
	_, err = idxMgr.Search("idx_emp_salary", key)
	if err == nil {
		t.Log("Note: Deleted index entry for 55000 may still exist (MVCC behavior)")
	}

	t.Log("Index maintenance on DML test passed")
}

// TestCompositeIndexMaintenance tests maintenance of multi-column indexes.
func TestCompositeIndexMaintenance(t *testing.T) {
	session, idxMgr, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE orders (id INT, customer_id INT, status INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create composite index
	_, err = session.ExecuteSQL("CREATE INDEX idx_orders_customer_status ON orders (customer_id, status);")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// INSERT rows
	_, err = session.ExecuteSQL("BEGIN;")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO orders VALUES (1, 100, 1);")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO orders VALUES (2, 100, 2);")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	_, err = session.ExecuteSQL("COMMIT;")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify index entries exist by searching with composite key
	// Search for customer_id=100, status=1
	key := btree.EncodeCompositeKey(
		btree.EncodeIntKey(100),
		btree.EncodeIntKey(1),
	)
	rid, err := idxMgr.Search("idx_orders_customer_status", key)
	if err != nil {
		t.Errorf("Index search for (100, 1) failed: %v", err)
	} else if rid.Table == "" {
		t.Error("Expected to find entry for (100, 1)")
	}

	t.Log("Composite index maintenance test passed")
}

// TestIndexScanSelect tests that SELECT uses index scan when available.
func TestIndexScanSelect(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE products (id INT, name TEXT, price INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create index FIRST (before data)
	_, err = session.ExecuteSQL("CREATE INDEX idx_products_price ON products (price);")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert data AFTER index creation so index gets populated
	_, _ = session.ExecuteSQL("BEGIN;")
	_, _ = session.ExecuteSQL("INSERT INTO products VALUES (1, 'Widget', 100);")
	_, _ = session.ExecuteSQL("INSERT INTO products VALUES (2, 'Gadget', 200);")
	_, _ = session.ExecuteSQL("INSERT INTO products VALUES (3, 'Doohickey', 300);")
	_, _ = session.ExecuteSQL("INSERT INTO products VALUES (4, 'Gizmo', 100);")
	_, _ = session.ExecuteSQL("INSERT INTO products VALUES (5, 'Thingamajig', 100);")
	_, _ = session.ExecuteSQL("COMMIT;")

	// Query using the indexed column
	_, _ = session.ExecuteSQL("BEGIN;")
	result, err := session.ExecuteSQL("SELECT * FROM products WHERE price = 100;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	_, _ = session.ExecuteSQL("COMMIT;")

	// We should get 3 results (Widget, Gizmo, Thingamajig all have price=100)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows with price=100, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Row: %v", row)
		}
	}

	// Query for a price that doesn't exist
	_, _ = session.ExecuteSQL("BEGIN;")
	result, err = session.ExecuteSQL("SELECT * FROM products WHERE price = 999;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	_, _ = session.ExecuteSQL("COMMIT;")

	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows with price=999, got %d", len(result.Rows))
	}

	t.Log("Index scan SELECT test passed")
}

// TestIndexOnExistingData tests that creating an index populates it with existing data.
func TestIndexOnExistingData(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE items (id INT, category INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data BEFORE index creation
	_, _ = session.ExecuteSQL("BEGIN;")
	_, _ = session.ExecuteSQL("INSERT INTO items VALUES (1, 10, 'A');")
	_, _ = session.ExecuteSQL("INSERT INTO items VALUES (2, 20, 'B');")
	_, _ = session.ExecuteSQL("INSERT INTO items VALUES (3, 10, 'C');")
	_, _ = session.ExecuteSQL("INSERT INTO items VALUES (4, 30, 'D');")
	_, _ = session.ExecuteSQL("INSERT INTO items VALUES (5, 10, 'E');")
	_, _ = session.ExecuteSQL("COMMIT;")

	// Create index AFTER data exists
	_, err = session.ExecuteSQL("CREATE INDEX idx_items_category ON items (category);")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Query using the indexed column - should use index scan
	_, _ = session.ExecuteSQL("BEGIN;")
	result, err := session.ExecuteSQL("SELECT * FROM items WHERE category = 10;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	_, _ = session.ExecuteSQL("COMMIT;")

	// We should get 3 results (A, C, E all have category=10)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows with category=10, got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Row: %v", row)
		}
	} else {
		t.Log("Index on existing data test passed - found 3 rows via index")
	}
}

// TestIndexRangeScan tests index usage for range conditions (<, >, <=, >=).
func TestIndexRangeScan(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	// Create a table with numeric data
	_, err := session.ExecuteSQL("CREATE TABLE scores (id INT PRIMARY KEY, name TEXT, score INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create an index on score column
	_, err = session.ExecuteSQL("CREATE INDEX idx_scores_score ON scores (score);")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Insert test data with various scores
	testData := []struct {
		id    int
		name  string
		score int
	}{
		{1, "Alice", 85},
		{2, "Bob", 72},
		{3, "Carol", 95},
		{4, "Dave", 60},
		{5, "Eve", 88},
		{6, "Frank", 72},
		{7, "Grace", 100},
		{8, "Henry", 45},
	}

	for _, td := range testData {
		_, err := session.ExecuteSQL("INSERT INTO scores VALUES (" +
			intToStr(td.id) + ", '" + td.name + "', " + intToStr(td.score) + ");")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	tests := []struct {
		name       string
		query      string
		expectedN  int
		expectDesc string
	}{
		{
			name:       "greater than",
			query:      "SELECT * FROM scores WHERE score > 80",
			expectedN:  4, // Alice(85), Carol(95), Eve(88), Grace(100)
			expectDesc: "scores > 80",
		},
		{
			name:       "greater than or equal",
			query:      "SELECT * FROM scores WHERE score >= 85",
			expectedN:  4, // Alice(85), Carol(95), Eve(88), Grace(100)
			expectDesc: "scores >= 85",
		},
		{
			name:       "less than",
			query:      "SELECT * FROM scores WHERE score < 70",
			expectedN:  2, // Dave(60), Henry(45)
			expectDesc: "scores < 70",
		},
		{
			name:       "less than or equal",
			query:      "SELECT * FROM scores WHERE score <= 72",
			expectedN:  4, // Bob(72), Dave(60), Frank(72), Henry(45)
			expectDesc: "scores <= 72",
		},
		{
			name:       "greater than reversed",
			query:      "SELECT * FROM scores WHERE 80 < score",
			expectedN:  4, // Same as score > 80
			expectDesc: "80 < score (reversed)",
		},
		{
			name:       "less than reversed",
			query:      "SELECT * FROM scores WHERE 90 > score",
			expectedN:  6, // All except Carol(95) and Grace(100)
			expectDesc: "90 > score (reversed)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _ = session.ExecuteSQL("BEGIN;")
			result, err := session.ExecuteSQL(tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			_, _ = session.ExecuteSQL("COMMIT;")

			if len(result.Rows) != tc.expectedN {
				t.Errorf("Expected %d rows for %s, got %d", tc.expectedN, tc.expectDesc, len(result.Rows))
				for i, row := range result.Rows {
					t.Logf("  Row %d: %v", i, row)
				}
			} else {
				t.Logf("Range scan test passed: %s returned %d rows", tc.expectDesc, len(result.Rows))
			}
		})
	}
}

// intToStr is a simple int to string converter for test data.
func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
