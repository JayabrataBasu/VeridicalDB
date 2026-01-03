package wal

import (
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestCheckpointBasic(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// No checkpoint yet
	if checkpointer.LastCheckpointLSN() != 0 {
		t.Error("expected no checkpoint initially")
	}

	// Do some work first so the WAL has content
	tx, _ := logger.Begin()
	_, _ = logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row"))
	_ = logger.Commit(tx.ID)

	// Perform a checkpoint
	if err := checkpointer.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Verify checkpoint records in WAL
	var beginCount, endCount int
	var lastBeginLSN LSN
	err = w.Iterate(0, func(rec *Record) error {
		switch rec.Type {
		case RecordCheckpointBegin:
			beginCount++
			lastBeginLSN = rec.LSN
		case RecordCheckpointEnd:
			endCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if beginCount != 1 {
		t.Errorf("expected 1 CHECKPOINT_BEGIN, got %d", beginCount)
	}
	if endCount != 1 {
		t.Errorf("expected 1 CHECKPOINT_END, got %d", endCount)
	}

	// Checkpoint LSN should match
	cpLSN := checkpointer.LastCheckpointLSN()
	if cpLSN != lastBeginLSN {
		t.Errorf("checkpoint LSN mismatch: expected %d, got %d", lastBeginLSN, cpLSN)
	}
}

func TestCheckpointWithActiveTransactions(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// Start some transactions
	tx1, _ := logger.Begin()
	tx2, _ := logger.Begin()

	// Do some work
	_, _ = logger.LogInsert(tx1.ID, "t1", 1, 0, []byte("row1"))
	_, _ = logger.LogInsert(tx2.ID, "t1", 1, 1, []byte("row2"))

	// Checkpoint while transactions are active
	if err := checkpointer.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Find the checkpoint
	cpLSN, activeTxns, err := FindLastCheckpoint(w)
	if err != nil {
		t.Fatalf("FindLastCheckpoint failed: %v", err)
	}

	if cpLSN == InvalidLSN {
		t.Error("expected valid checkpoint LSN")
	}

	if len(activeTxns) != 2 {
		t.Errorf("expected 2 active transactions, got %d", len(activeTxns))
	}

	// Verify active transactions
	txIDs := make(map[txn.TxID]bool)
	for _, info := range activeTxns {
		txIDs[info.TxID] = true
	}

	if !txIDs[tx1.ID] {
		t.Error("tx1 should be in active list")
	}
	if !txIDs[tx2.ID] {
		t.Error("tx2 should be in active list")
	}

	// Now commit transactions
	_ = logger.Commit(tx1.ID)
	_ = logger.Commit(tx2.ID)

	// Another checkpoint
	if err := checkpointer.Checkpoint(); err != nil {
		t.Fatalf("Second checkpoint failed: %v", err)
	}

	// Now should have no active transactions
	_, activeTxns, err = FindLastCheckpoint(w)
	if err != nil {
		t.Fatalf("FindLastCheckpoint failed: %v", err)
	}

	if len(activeTxns) != 0 {
		t.Errorf("expected 0 active transactions after commits, got %d", len(activeTxns))
	}
}

func TestCheckpointWithPageFlusher(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// Track if page flusher was called
	flusherCalled := false
	checkpointer.SetPageFlusher(func() error {
		flusherCalled = true
		return nil
	})

	// Checkpoint
	if err := checkpointer.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if !flusherCalled {
		t.Error("page flusher should have been called")
	}
}

func TestFindLastCheckpointNoCheckpoint(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Write some records but no checkpoint
	tx, _ := logger.Begin()
	_, _ = logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row"))
	_ = logger.Commit(tx.ID)

	// Should find no checkpoint
	cpLSN, activeTxns, err := FindLastCheckpoint(w)
	if err != nil {
		t.Fatalf("FindLastCheckpoint failed: %v", err)
	}

	if cpLSN != InvalidLSN {
		t.Error("expected InvalidLSN when no checkpoint")
	}
	if activeTxns != nil {
		t.Error("expected nil active transactions when no checkpoint")
	}
}

func TestFindLastCheckpointMultiple(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// First checkpoint
	_ = checkpointer.Checkpoint()
	firstCPLSN := checkpointer.LastCheckpointLSN()

	// Some more work
	tx, _ := logger.Begin()
	_, _ = logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row"))
	_ = logger.Commit(tx.ID)

	// Second checkpoint
	_ = checkpointer.Checkpoint()
	secondCPLSN := checkpointer.LastCheckpointLSN()

	if secondCPLSN <= firstCPLSN {
		t.Error("second checkpoint LSN should be greater than first")
	}

	// FindLastCheckpoint should return the second (most recent)
	cpLSN, _, err := FindLastCheckpoint(w)
	if err != nil {
		t.Fatalf("FindLastCheckpoint failed: %v", err)
	}

	if cpLSN != secondCPLSN {
		t.Errorf("expected last checkpoint LSN %d, got %d", secondCPLSN, cpLSN)
	}
}

func TestCheckpointBackground(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer func() { _ = w.Close() }()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)
	checkpointer := NewCheckpointer(w, logger)

	// Set a short interval for testing
	checkpointer.SetCheckpointInterval(50 * time.Millisecond)

	// Start background checkpointer
	checkpointer.StartBackground()

	// Wait for at least one checkpoint
	time.Sleep(100 * time.Millisecond)

	// Stop
	checkpointer.StopBackground()

	// Should have at least one checkpoint
	cpLSN := checkpointer.LastCheckpointLSN()
	if cpLSN == 0 {
		t.Error("expected at least one background checkpoint")
	}
}

func TestParseCheckpointEndData(t *testing.T) {
	// Create checkpoint data with 2 active transactions
	data := make([]byte, 8+2*16)

	// Count = 2
	data[0] = 2
	data[1] = 0
	data[2] = 0
	data[3] = 0
	data[4] = 0
	data[5] = 0
	data[6] = 0
	data[7] = 0

	// TxID 1, LSN 100
	data[8] = 1
	data[16] = 100

	// TxID 2, LSN 200
	data[24] = 2
	data[32] = 200

	activeTxns, err := ParseCheckpointEndData(data)
	if err != nil {
		t.Fatalf("ParseCheckpointEndData failed: %v", err)
	}

	if len(activeTxns) != 2 {
		t.Fatalf("expected 2 active txns, got %d", len(activeTxns))
	}

	if activeTxns[0].TxID != 1 || activeTxns[0].LastLSN != 100 {
		t.Errorf("first txn mismatch: %+v", activeTxns[0])
	}
	if activeTxns[1].TxID != 2 || activeTxns[1].LastLSN != 200 {
		t.Errorf("second txn mismatch: %+v", activeTxns[1])
	}
}
