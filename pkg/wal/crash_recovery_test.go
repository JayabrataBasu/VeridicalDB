package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// TestCrashRecoverySimulation simulates a crash and recovery scenario.
// This tests the complete WAL infrastructure working together.
func TestCrashRecoverySimulation(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Normal operation, then "crash"
	func() {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open WAL failed: %v", err)
		}
		// Don't defer close - simulating crash

		txnMgr := txn.NewManager()
		logger := NewTxnLogger(w, txnMgr)
		checkpointer := NewCheckpointer(w, logger)

		// tx1: fully committed - should survive
		tx1, _ := logger.Begin()
		logger.LogInsert(tx1.ID, "users", 1, 0, []byte("alice"))
		logger.LogInsert(tx1.ID, "users", 1, 1, []byte("bob"))
		logger.Commit(tx1.ID)

		// Checkpoint
		checkpointer.Checkpoint()

		// tx2: fully committed after checkpoint - should survive
		tx2, _ := logger.Begin()
		logger.LogInsert(tx2.ID, "users", 1, 2, []byte("charlie"))
		logger.Commit(tx2.ID)

		// tx3: in-progress at crash time - should be rolled back
		tx3, _ := logger.Begin()
		logger.LogInsert(tx3.ID, "users", 1, 3, []byte("dave"))
		logger.LogUpdate(tx3.ID, "users", 1, 0, []byte("alice"), []byte("alice-updated"))
		// No commit! Simulates crash

		// Flush to ensure data is on disk
		w.Flush()

		// "Crash" - close without proper cleanup
		w.Close()
	}()

	// Phase 2: Recovery
	func() {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Reopen WAL failed: %v", err)
		}
		defer w.Close()

		// Track redo and undo operations
		var redoOps []string
		var undoOps []string

		recovery := NewRecovery(w)
		recovery.SetRedoHandler(func(rec *Record) error {
			switch rec.Type {
			case RecordInsert:
				redoOps = append(redoOps, "redo-insert:"+rec.TableName+":"+string(rec.Data))
			case RecordUpdate:
				old, new, _ := ParseUpdateData(rec.Data)
				redoOps = append(redoOps, "redo-update:"+rec.TableName+":"+string(old)+"->"+string(new))
			}
			return nil
		})
		recovery.SetUndoHandler(func(rec *Record) error {
			switch rec.Type {
			case RecordInsert:
				undoOps = append(undoOps, "undo-insert:"+rec.TableName+":"+string(rec.Data))
			case RecordUpdate:
				old, new, _ := ParseUpdateData(rec.Data)
				undoOps = append(undoOps, "undo-update:"+rec.TableName+":"+string(new)+"->"+string(old))
			}
			return nil
		})

		stats, err := recovery.Recover()
		if err != nil {
			t.Fatalf("Recovery failed: %v", err)
		}

		// Verify stats
		// Note: Recovery starts from checkpoint, so tx1 (committed before checkpoint)
		// is not counted in winners. Only tx2 is counted.
		if stats.WinnersCount != 1 {
			t.Errorf("expected 1 committed transaction (after checkpoint), got %d", stats.WinnersCount)
		}
		if stats.LosersCount != 1 {
			t.Errorf("expected 1 loser transaction, got %d", stats.LosersCount)
		}

		// Should have redone data operations from checkpoint forward
		// tx2: 1 insert, tx3: 1 insert + 1 update = 3 total (tx1 is before checkpoint)
		if stats.RecordsRedone != 3 {
			t.Errorf("expected 3 redo operations (after checkpoint), got %d", stats.RecordsRedone)
		}

		// Should have undone tx3's operations (1 insert, 1 update)
		if stats.RecordsUndone != 2 {
			t.Errorf("expected 2 undo operations, got %d", stats.RecordsUndone)
		}

		// Undo should be in reverse order
		if len(undoOps) >= 1 && undoOps[0] != "undo-update:users:alice-updated->alice" {
			t.Errorf("first undo should be the update, got: %s", undoOps[0])
		}
		if len(undoOps) >= 2 && undoOps[1] != "undo-insert:users:dave" {
			t.Errorf("second undo should be dave insert, got: %s", undoOps[1])
		}

		t.Logf("Redo operations: %v", redoOps)
		t.Logf("Undo operations: %v", undoOps)
	}()
}

// TestCrashRecoveryWALPersistence tests that WAL data survives file close/reopen.
func TestCrashRecoveryWALPersistence(t *testing.T) {
	dir := t.TempDir()

	// Write some records and close
	func() {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open WAL failed: %v", err)
		}

		for i := 0; i < 100; i++ {
			w.Append(&Record{
				TxID:      uint64(i % 10),
				Type:      RecordInsert,
				TableName: "test",
				PageID:    uint32(i),
				Data:      []byte("data"),
			})
		}
		w.Flush()
		w.Close()
	}()

	// Reopen and verify all records are there
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen WAL failed: %v", err)
	}
	defer w.Close()

	var count int
	err = w.Iterate(0, func(rec *Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if count != 100 {
		t.Errorf("expected 100 records after reopen, got %d", count)
	}
}

// TestCrashRecoveryIncompleteWrite tests recovery when the last write was incomplete.
func TestCrashRecoveryIncompleteWrite(t *testing.T) {
	dir := t.TempDir()

	// Write some valid records
	func() {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open WAL failed: %v", err)
		}

		w.Append(&Record{TxID: 1, Type: RecordBegin})
		w.Append(&Record{TxID: 1, Type: RecordInsert, TableName: "t", PageID: 1, Data: []byte("data")})
		w.Append(&Record{TxID: 1, Type: RecordCommit})
		w.Flush()
		w.Close()
	}()

	// Corrupt the file by appending garbage (simulating incomplete write)
	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Open for corruption failed: %v", err)
	}
	f.Write([]byte{0x00, 0x01, 0x02, 0x03}) // Incomplete record header
	f.Close()

	// Recovery should still work with the valid records
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	var count int
	err = w.Iterate(0, func(rec *Record) error {
		count++
		return nil
	})

	// May get an error for the incomplete record, which is expected
	// but should have processed the valid records first
	if count < 3 {
		t.Errorf("expected at least 3 valid records, got %d (error: %v)", count, err)
	}
}

// TestCrashRecoveryMultipleCheckpoints tests recovery with multiple checkpoints.
func TestCrashRecoveryMultipleCheckpoints(t *testing.T) {
	dir := t.TempDir()

	// Create multiple checkpoints
	func() {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open WAL failed: %v", err)
		}

		txnMgr := txn.NewManager()
		logger := NewTxnLogger(w, txnMgr)
		checkpointer := NewCheckpointer(w, logger)

		// Checkpoint 1
		tx1, _ := logger.Begin()
		logger.LogInsert(tx1.ID, "t", 1, 0, []byte("c1-data"))
		logger.Commit(tx1.ID)
		checkpointer.Checkpoint()

		// Checkpoint 2
		tx2, _ := logger.Begin()
		logger.LogInsert(tx2.ID, "t", 1, 1, []byte("c2-data"))
		logger.Commit(tx2.ID)
		checkpointer.Checkpoint()

		// Checkpoint 3
		tx3, _ := logger.Begin()
		logger.LogInsert(tx3.ID, "t", 1, 2, []byte("c3-data"))
		logger.Commit(tx3.ID)
		checkpointer.Checkpoint()

		// Uncommitted after last checkpoint
		tx4, _ := logger.Begin()
		logger.LogInsert(tx4.ID, "t", 1, 3, []byte("uncommitted"))
		// No commit

		w.Flush()
		w.Close()
	}()

	// Recovery should use the last checkpoint
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen WAL failed: %v", err)
	}
	defer w.Close()

	// Find checkpoint
	cpLSN, _, err := FindLastCheckpoint(w)
	if err != nil {
		t.Fatalf("FindLastCheckpoint failed: %v", err)
	}

	if cpLSN == InvalidLSN {
		t.Error("expected valid checkpoint")
	}

	// Recover
	var undoCount int
	recovery := NewRecovery(w)
	recovery.SetRedoHandler(func(rec *Record) error { return nil })
	recovery.SetUndoHandler(func(rec *Record) error {
		undoCount++
		return nil
	})

	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Only transactions after the LAST checkpoint are counted
	// tx1, tx2 are committed before checkpoint 3
	// tx3 is committed and checkpoint 3 is taken, so nothing is after checkpoint 3 except tx4
	// Actually, checkpoints are taken AFTER commits, so let's check what's really happening
	if stats.LosersCount != 1 {
		t.Errorf("expected 1 loser, got %d", stats.LosersCount)
	}
	if undoCount != 1 {
		t.Errorf("expected 1 undo, got %d", undoCount)
	}
}

// TestCrashRecoveryAbortedTransaction tests that aborted transactions are not undone again.
func TestCrashRecoveryAbortedTransaction(t *testing.T) {
	dir := t.TempDir()

	func() {
		w, err := Open(dir)
		if err != nil {
			t.Fatalf("Open WAL failed: %v", err)
		}

		txnMgr := txn.NewManager()
		logger := NewTxnLogger(w, txnMgr)

		// Aborted transaction
		tx, _ := logger.Begin()
		logger.LogInsert(tx.ID, "t", 1, 0, []byte("aborted-data"))
		logger.Abort(tx.ID)

		w.Flush()
		w.Close()
	}()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen WAL failed: %v", err)
	}
	defer w.Close()

	var undoCount int
	recovery := NewRecovery(w)
	recovery.SetRedoHandler(func(rec *Record) error { return nil })
	recovery.SetUndoHandler(func(rec *Record) error {
		undoCount++
		return nil
	})

	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Aborted transaction should not be a loser (it's already handled)
	if stats.LosersCount != 0 {
		t.Errorf("expected 0 losers (already aborted), got %d", stats.LosersCount)
	}
	if undoCount != 0 {
		t.Errorf("expected 0 undos (already aborted), got %d", undoCount)
	}
}
