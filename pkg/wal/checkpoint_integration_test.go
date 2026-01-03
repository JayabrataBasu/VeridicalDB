package wal_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

func TestCheckpointIntegration(t *testing.T) {
	// 1. Setup
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	// Initialize WAL
	walLog, err := wal.Open(dataDir)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer func() { _ = walLog.Close() }()

	// Initialize TxnManager
	txnMgr := txn.NewManager()

	// Initialize TxnLogger
	txnLogger := wal.NewTxnLogger(walLog, txnMgr)

	// Initialize Checkpointer
	checkpointer := wal.NewCheckpointer(walLog, txnLogger)

	// Initialize TableManager
	tm, err := catalog.NewTableManager(dataDir, 4096, walLog)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	// Set page flusher
	checkpointer.SetPageFlusher(tm.Checkpoint)

	// 2. Perform Operations
	// Create a table
	cols := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt32, NotNull: true},
		{Name: "name", Type: catalog.TypeText, NotNull: false},
	}
	if err := tm.CreateTable("users", cols, nil); err != nil {
		t.Fatalf("CreateTable error: %v", err)
	}

	// Insert some rows
	_, err = tm.Insert("users", []catalog.Value{catalog.NewInt32(1), catalog.NewText("Alice")})
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}
	_, err = tm.Insert("users", []catalog.Value{catalog.NewInt32(2), catalog.NewText("Bob")})
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// 3. Trigger Checkpoint
	if err := checkpointer.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// 4. Verify Checkpoint
	lastCheckpointLSN := checkpointer.LastCheckpointLSN()
	if lastCheckpointLSN == 0 {
		t.Errorf("LastCheckpointLSN should be > 0")
	}

	// Scan WAL to verify records
	var foundBegin, foundEnd bool
	var beginLSN wal.LSN

	err = walLog.Iterate(0, func(rec *wal.Record) error {
		if rec.Type == wal.RecordCheckpointBegin {
			foundBegin = true
			beginLSN = rec.LSN
		}
		if rec.Type == wal.RecordCheckpointEnd {
			foundEnd = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WAL iteration failed: %v", err)
	}

	if !foundBegin {
		t.Errorf("CHECKPOINT_BEGIN record not found")
	}
	if !foundEnd {
		t.Errorf("CHECKPOINT_END record not found")
	}
	if beginLSN != lastCheckpointLSN {
		t.Errorf("LastCheckpointLSN mismatch: got %d, want %d", lastCheckpointLSN, beginLSN)
	}

	// 5. Verify Persistence (Simulate Crash/Restart)
	// Close everything
	_ = walLog.Close()

	// Reopen
	walLog2, err := wal.Open(dataDir)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer func() { _ = walLog2.Close() }()

	// Find last checkpoint
	ckptLSN, _, err := wal.FindLastCheckpoint(walLog2)
	if err != nil {
		t.Fatalf("FindLastCheckpoint failed: %v", err)
	}

	if ckptLSN != lastCheckpointLSN {
		t.Errorf("FindLastCheckpoint LSN mismatch: got %d, want %d", ckptLSN, lastCheckpointLSN)
	}
}

func TestAutomaticCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping automatic checkpoint test in short mode")
	}

	// 1. Setup
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	walLog, err := wal.Open(dataDir)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer func() { _ = walLog.Close() }()

	txnMgr := txn.NewManager()
	txnLogger := wal.NewTxnLogger(walLog, txnMgr)
	checkpointer := wal.NewCheckpointer(walLog, txnLogger)
	tm, err := catalog.NewTableManager(dataDir, 4096, walLog)
	if err != nil {
		t.Fatalf("failed to create table manager: %v", err)
	}

	checkpointer.SetPageFlusher(tm.Checkpoint)

	// Set short interval
	checkpointer.SetCheckpointInterval(100 * time.Millisecond)
	checkpointer.StartBackground()
	defer checkpointer.StopBackground()

	// Wait for a checkpoint
	time.Sleep(200 * time.Millisecond)

	// Verify
	if checkpointer.LastCheckpointLSN() == 0 {
		t.Errorf("Automatic checkpoint did not run")
	}
}
