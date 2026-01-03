package catalog

import (
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

func TestCrashRecoveryIntegration(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	walDir := filepath.Join(dir, "wal")

	// Phase 1: Write data and "crash"
	func() {
		// Setup WAL
		w, err := wal.Open(walDir)
		if err != nil {
			t.Fatalf("Open WAL failed: %v", err)
		}
		// We will close manually to simulate crash

		// Setup Managers
		txnMgr := txn.NewManager()
		txnLogger := wal.NewTxnLogger(w, txnMgr)

		tm, err := NewTableManager(dataDir, 4096, w)
		if err != nil {
			t.Fatalf("NewTableManager failed: %v", err)
		}

		mtm := NewMVCCTableManager(tm, txnMgr, txnLogger)

		// Create table
		cols := []Column{
			{Name: "id", Type: TypeInt32, NotNull: true},
			{Name: "val", Type: TypeText, NotNull: false},
		}
		if err := tm.CreateTable("test_table", cols, nil); err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}

		// Tx1: Commit
		tx1, _ := txnLogger.Begin()
		vals1 := []Value{NewInt32(1), NewText("committed")}
		if _, err := mtm.Insert("test_table", vals1, tx1); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		if err := txnLogger.Commit(tx1.ID); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Tx2: No Commit (Crash)
		tx2, _ := txnLogger.Begin()
		vals2 := []Value{NewInt32(2), NewText("uncommitted")}
		if _, err := mtm.Insert("test_table", vals2, tx2); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Flush WAL to ensure records are on disk
		w.Flush()
		w.Close()
	}()

	// Phase 2: Recover and Verify
	func() {
		// Reopen WAL
		w, err := wal.Open(walDir)
		if err != nil {
			t.Fatalf("Reopen WAL failed: %v", err)
		}
		defer func() { _ = w.Close() }()

		// Reopen TableManager - this triggers StartRecovery
		tm, err := NewTableManager(dataDir, 4096, w)
		if err != nil {
			t.Fatalf("NewTableManager recovery failed: %v", err)
		}

		// Verify data
		// We need to scan the table.
		// Since we don't have MVCC manager here (or we can create one),
		// we can just check raw storage or use MVCC manager to scan.

		txnMgr := txn.NewManager()
		mtm := NewMVCCTableManager(tm, txnMgr, nil) // No logger needed for read

		// Start a new transaction to read
		tx := txnMgr.Begin()

		rows := 0
		foundCommitted := false
		foundUncommitted := false

		err = mtm.Scan("test_table", tx, func(row *MVCCRow) (bool, error) {
			rows++
			id := row.Values[0].Int32
			val := row.Values[1].Text

			if id == 1 && val == "committed" {
				foundCommitted = true
			}
			if id == 2 && val == "uncommitted" {
				foundUncommitted = true
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if !foundCommitted {
			t.Error("Expected to find committed row (id=1), but didn't")
		}
		if foundUncommitted {
			t.Error("Found uncommitted row (id=2), expected it to be rolled back")
		}
	}()
}
