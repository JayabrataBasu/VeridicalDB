package sql

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

func TestPreparedStatements(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "veridicaldb-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walLog, err := wal.Open(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	tm, err := catalog.NewTableManager(tmpDir, 4096, walLog)
	if err != nil {
		t.Fatal(err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr)
	session := NewSession(mtm)

	// Create table
	_, err = session.ExecuteSQL("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = session.ExecuteSQL("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test PREPARE
	_, err = session.ExecuteSQL("PREPARE get_user AS SELECT name FROM users WHERE id = $1")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	} // Test EXECUTE
	result, err := session.ExecuteSQL("EXECUTE get_user(1)")
	if err != nil {
		t.Fatalf("Failed to execute statement: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text != "Alice" {
		t.Errorf("Expected 'Alice', got %v", result.Rows[0][0])
	}

	// Test EXECUTE with different parameter
	_, err = session.ExecuteSQL("INSERT INTO users VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	result, err = session.ExecuteSQL("EXECUTE get_user(2)")
	if err != nil {
		t.Fatalf("Failed to execute statement: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text != "Bob" {
		t.Errorf("Expected 'Bob', got %v", result.Rows[0][0])
	}

	// Test DEALLOCATE
	_, err = session.ExecuteSQL("DEALLOCATE get_user")
	if err != nil {
		t.Fatalf("Failed to deallocate statement: %v", err)
	}

	// Test EXECUTE after DEALLOCATE (should fail)
	_, err = session.ExecuteSQL("EXECUTE get_user(1)")
	if err == nil {
		t.Fatal("Expected error executing deallocated statement, got nil")
	}
}

func TestPreparedInsert(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "veridicaldb-test-insert")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walLog, err := wal.Open(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	tm, err := catalog.NewTableManager(tmpDir, 4096, walLog)
	if err != nil {
		t.Fatal(err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr)
	session := NewSession(mtm)

	// Create table
	_, err = session.ExecuteSQL("CREATE TABLE products (id INT, price INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare INSERT
	_, err = session.ExecuteSQL("PREPARE insert_product AS INSERT INTO products VALUES ($1, $2)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	} // Execute INSERT
	_, err = session.ExecuteSQL("EXECUTE insert_product(100, 50)")
	if err != nil {
		t.Fatalf("Failed to execute statement: %v", err)
	}

	// Verify
	result, err := session.ExecuteSQL("SELECT * FROM products")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int32 != 100 || result.Rows[0][1].Int32 != 50 {
		t.Errorf("Unexpected row data: %v", result.Rows[0])
	}
}
