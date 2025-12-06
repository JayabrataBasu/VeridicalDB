package wal

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestRecoveryNoTransactions(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	recovery := NewRecovery(w)
	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.RecordsAnalyzed != 0 {
		t.Errorf("expected 0 records analyzed, got %d", stats.RecordsAnalyzed)
	}
}

func TestRecoveryAnalysisCommittedTransaction(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Committed transaction
	tx, _ := logger.Begin()
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row1"))
	logger.LogInsert(tx.ID, "t1", 1, 1, []byte("row2"))
	logger.Commit(tx.ID)
	w.Flush()

	// Analyze
	recovery := NewRecovery(w)
	result, err := recovery.AnalyzeOnly()
	if err != nil {
		t.Fatalf("AnalyzeOnly failed: %v", err)
	}

	if len(result.Winners) != 1 {
		t.Errorf("expected 1 winner, got %d", len(result.Winners))
	}
	if !result.Winners[tx.ID] {
		t.Error("expected tx to be a winner")
	}
	if len(result.Losers) != 0 {
		t.Errorf("expected 0 losers, got %d", len(result.Losers))
	}
}

func TestRecoveryAnalysisUncommittedTransaction(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Uncommitted transaction (simulates crash before commit)
	tx, _ := logger.Begin()
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row1"))
	logger.LogInsert(tx.ID, "t1", 1, 1, []byte("row2"))
	// No commit!
	w.Flush()

	// Analyze
	recovery := NewRecovery(w)
	result, err := recovery.AnalyzeOnly()
	if err != nil {
		t.Fatalf("AnalyzeOnly failed: %v", err)
	}

	if len(result.Winners) != 0 {
		t.Errorf("expected 0 winners, got %d", len(result.Winners))
	}
	if len(result.Losers) != 1 {
		t.Errorf("expected 1 loser, got %d", len(result.Losers))
	}
	if !result.Losers[tx.ID] {
		t.Error("expected tx to be a loser")
	}
}

func TestRecoveryAnalysisMixedTransactions(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// tx1: committed
	tx1, _ := logger.Begin()
	logger.LogInsert(tx1.ID, "t1", 1, 0, []byte("row1"))
	logger.Commit(tx1.ID)

	// tx2: uncommitted (loser)
	tx2, _ := logger.Begin()
	logger.LogInsert(tx2.ID, "t1", 1, 1, []byte("row2"))

	// tx3: committed
	tx3, _ := logger.Begin()
	logger.LogInsert(tx3.ID, "t1", 1, 2, []byte("row3"))
	logger.Commit(tx3.ID)

	// tx4: aborted
	tx4, _ := logger.Begin()
	logger.LogInsert(tx4.ID, "t1", 1, 3, []byte("row4"))
	logger.Abort(tx4.ID)

	w.Flush()

	// Analyze
	recovery := NewRecovery(w)
	result, err := recovery.AnalyzeOnly()
	if err != nil {
		t.Fatalf("AnalyzeOnly failed: %v", err)
	}

	if len(result.Winners) != 2 {
		t.Errorf("expected 2 winners, got %d", len(result.Winners))
	}
	if !result.Winners[tx1.ID] || !result.Winners[tx3.ID] {
		t.Error("tx1 and tx3 should be winners")
	}

	if len(result.Losers) != 1 {
		t.Errorf("expected 1 loser, got %d", len(result.Losers))
	}
	if !result.Losers[tx2.ID] {
		t.Error("tx2 should be a loser")
	}
}

func TestRecoveryRedo(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Committed transaction
	tx, _ := logger.Begin()
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row1"))
	logger.LogInsert(tx.ID, "t1", 1, 1, []byte("row2"))
	logger.Commit(tx.ID)
	w.Flush()

	// Track redo operations
	var redoRecords []*Record
	recovery := NewRecovery(w)
	recovery.SetRedoHandler(func(rec *Record) error {
		redoRecords = append(redoRecords, rec)
		return nil
	})

	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.RecordsRedone != 2 {
		t.Errorf("expected 2 records redone, got %d", stats.RecordsRedone)
	}

	if len(redoRecords) != 2 {
		t.Fatalf("expected 2 redo records, got %d", len(redoRecords))
	}

	// Both should be INSERTs
	for i, rec := range redoRecords {
		if rec.Type != RecordInsert {
			t.Errorf("record %d should be INSERT, got %v", i, rec.Type)
		}
	}
}

func TestRecoveryUndo(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Uncommitted transaction
	tx, _ := logger.Begin()
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row1"))
	logger.LogInsert(tx.ID, "t1", 1, 1, []byte("row2"))
	logger.LogInsert(tx.ID, "t1", 1, 2, []byte("row3"))
	// No commit!
	w.Flush()

	// Track undo operations
	var undoRecords []*Record
	recovery := NewRecovery(w)
	recovery.SetRedoHandler(func(rec *Record) error { return nil }) // No-op redo
	recovery.SetUndoHandler(func(rec *Record) error {
		undoRecords = append(undoRecords, rec)
		return nil
	})

	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.RecordsUndone != 3 {
		t.Errorf("expected 3 records undone, got %d", stats.RecordsUndone)
	}
	if stats.LosersCount != 1 {
		t.Errorf("expected 1 loser, got %d", stats.LosersCount)
	}

	if len(undoRecords) != 3 {
		t.Fatalf("expected 3 undo records, got %d", len(undoRecords))
	}

	// Undo should be in reverse order (LIFO)
	if string(undoRecords[0].Data) != "row3" {
		t.Errorf("first undo should be row3, got %s", string(undoRecords[0].Data))
	}
	if string(undoRecords[1].Data) != "row2" {
		t.Errorf("second undo should be row2, got %s", string(undoRecords[1].Data))
	}
	if string(undoRecords[2].Data) != "row1" {
		t.Errorf("third undo should be row1, got %s", string(undoRecords[2].Data))
	}
}

func TestRecoveryWithCheckpoint(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// tx1: committed before checkpoint
	tx1, _ := logger.Begin()
	logger.LogInsert(tx1.ID, "t1", 1, 0, []byte("row1"))
	logger.Commit(tx1.ID)

	// Checkpoint
	checkpointer.Checkpoint()

	// tx2: committed after checkpoint
	tx2, _ := logger.Begin()
	logger.LogInsert(tx2.ID, "t1", 1, 1, []byte("row2"))
	logger.Commit(tx2.ID)

	// tx3: uncommitted after checkpoint
	tx3, _ := logger.Begin()
	logger.LogInsert(tx3.ID, "t1", 1, 2, []byte("row3"))
	// No commit!

	w.Flush()

	// Recovery should:
	// - Start from checkpoint
	// - Redo tx2's insert (tx1 is before checkpoint, still redone for safety)
	// - Undo tx3's insert

	var redoRecords []*Record
	var undoRecords []*Record

	recovery := NewRecovery(w)
	recovery.SetRedoHandler(func(rec *Record) error {
		redoRecords = append(redoRecords, rec)
		return nil
	})
	recovery.SetUndoHandler(func(rec *Record) error {
		undoRecords = append(undoRecords, rec)
		return nil
	})

	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.CheckpointLSN == 0 {
		t.Error("expected valid checkpoint LSN")
	}
	if stats.LosersCount != 1 {
		t.Errorf("expected 1 loser, got %d", stats.LosersCount)
	}

	// tx3 should be undone
	if len(undoRecords) != 1 {
		t.Errorf("expected 1 undo, got %d", len(undoRecords))
	}
	if len(undoRecords) > 0 && string(undoRecords[0].Data) != "row3" {
		t.Errorf("should undo row3")
	}
}

func TestRecoveryFromCheckpointWithActiveTransaction(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// Start a transaction
	tx, _ := logger.Begin()
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("before-checkpoint"))

	// Checkpoint while transaction is active
	checkpointer.Checkpoint()

	// More work after checkpoint
	logger.LogInsert(tx.ID, "t1", 1, 1, []byte("after-checkpoint"))

	// Crash without commit!
	w.Flush()

	// Recovery should undo both inserts
	var undoRecords []*Record
	recovery := NewRecovery(w)
	recovery.SetRedoHandler(func(rec *Record) error { return nil })
	recovery.SetUndoHandler(func(rec *Record) error {
		undoRecords = append(undoRecords, rec)
		return nil
	})

	stats, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.LosersCount != 1 {
		t.Errorf("expected 1 loser, got %d", stats.LosersCount)
	}

	// Both inserts should be undone (from before and after checkpoint)
	if len(undoRecords) != 2 {
		t.Errorf("expected 2 undos, got %d", len(undoRecords))
	}
}

func TestRecoverWithHandlers(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Committed transaction
	tx, _ := logger.Begin()
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row1"))
	logger.Commit(tx.ID)
	w.Flush()

	// Use convenience function
	var redoCount int
	stats, err := RecoverWithHandlers(
		w,
		func(rec *Record) error {
			redoCount++
			return nil
		},
		func(rec *Record) error {
			return nil
		},
	)

	if err != nil {
		t.Fatalf("RecoverWithHandlers failed: %v", err)
	}

	if stats.RecordsRedone != 1 {
		t.Errorf("expected 1 record redone, got %d", stats.RecordsRedone)
	}
	if redoCount != 1 {
		t.Errorf("expected 1 redo call, got %d", redoCount)
	}
}
