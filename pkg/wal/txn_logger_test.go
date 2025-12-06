package wal

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestTxnLoggerBeginCommit(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Begin transaction
	tx, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	if tx.ID == 0 {
		t.Error("expected non-zero transaction ID")
	}

	// Commit transaction
	if err := logger.Commit(tx.ID); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify WAL contains BEGIN and COMMIT records
	var records []*Record
	err = w.Iterate(0, func(rec *Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0].Type != RecordBegin {
		t.Errorf("first record should be BEGIN, got %v", records[0].Type)
	}
	if records[1].Type != RecordCommit {
		t.Errorf("second record should be COMMIT, got %v", records[1].Type)
	}

	if records[0].TxID != uint64(tx.ID) {
		t.Errorf("BEGIN TxID mismatch")
	}
	if records[1].TxID != uint64(tx.ID) {
		t.Errorf("COMMIT TxID mismatch")
	}
}

func TestTxnLoggerBeginAbort(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Begin transaction
	tx, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Abort transaction
	if err := logger.Abort(tx.ID); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Verify WAL contains BEGIN and ABORT records
	var records []*Record
	err = w.Iterate(0, func(rec *Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[0].Type != RecordBegin {
		t.Errorf("first record should be BEGIN, got %v", records[0].Type)
	}
	if records[1].Type != RecordAbort {
		t.Errorf("second record should be ABORT, got %v", records[1].Type)
	}
}

func TestTxnLoggerInsert(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	tx, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Log an insert
	data := []byte("test row data")
	lsn, err := logger.LogInsert(tx.ID, "users", 1, 0, data)
	if err != nil {
		t.Fatalf("LogInsert failed: %v", err)
	}

	if lsn == InvalidLSN {
		t.Error("expected valid LSN")
	}

	if err := logger.Commit(tx.ID); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify WAL contains BEGIN, INSERT, COMMIT
	var records []*Record
	err = w.Iterate(0, func(rec *Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if records[1].Type != RecordInsert {
		t.Errorf("second record should be INSERT, got %v", records[1].Type)
	}
	if records[1].TableName != "users" {
		t.Errorf("expected table 'users', got '%s'", records[1].TableName)
	}
	if records[1].PageID != 1 {
		t.Errorf("expected pageID 1, got %d", records[1].PageID)
	}
	if string(records[1].Data) != string(data) {
		t.Errorf("data mismatch")
	}
}

func TestTxnLoggerUpdate(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	tx, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Log an update
	oldData := []byte("old value")
	newData := []byte("new value")
	lsn, err := logger.LogUpdate(tx.ID, "users", 2, 5, oldData, newData)
	if err != nil {
		t.Fatalf("LogUpdate failed: %v", err)
	}

	if lsn == InvalidLSN {
		t.Error("expected valid LSN")
	}

	if err := logger.Commit(tx.ID); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify WAL contains update with correct data
	var updateRec *Record
	err = w.Iterate(0, func(rec *Record) error {
		if rec.Type == RecordUpdate {
			updateRec = rec
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if updateRec == nil {
		t.Fatal("UPDATE record not found")
	}

	// Parse update data
	old, new, err := ParseUpdateData(updateRec.Data)
	if err != nil {
		t.Fatalf("ParseUpdateData failed: %v", err)
	}

	if string(old) != "old value" {
		t.Errorf("expected old 'old value', got '%s'", string(old))
	}
	if string(new) != "new value" {
		t.Errorf("expected new 'new value', got '%s'", string(new))
	}
}

func TestTxnLoggerDelete(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	tx, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Log a delete
	oldData := []byte("deleted row")
	lsn, err := logger.LogDelete(tx.ID, "users", 3, 10, oldData)
	if err != nil {
		t.Fatalf("LogDelete failed: %v", err)
	}

	if lsn == InvalidLSN {
		t.Error("expected valid LSN")
	}

	if err := logger.Commit(tx.ID); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify WAL contains DELETE with old data
	var deleteRec *Record
	err = w.Iterate(0, func(rec *Record) error {
		if rec.Type == RecordDelete {
			deleteRec = rec
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if deleteRec == nil {
		t.Fatal("DELETE record not found")
	}

	if string(deleteRec.Data) != string(oldData) {
		t.Errorf("data mismatch: expected '%s', got '%s'", string(oldData), string(deleteRec.Data))
	}
}

func TestTxnLoggerPrevLSNChain(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	tx, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Multiple operations to build undo chain
	logger.LogInsert(tx.ID, "t1", 1, 0, []byte("row1"))
	logger.LogInsert(tx.ID, "t1", 1, 1, []byte("row2"))
	logger.LogUpdate(tx.ID, "t1", 1, 0, []byte("row1"), []byte("row1-updated"))
	logger.Commit(tx.ID)

	// Verify prevLSN chain
	var records []*Record
	err = w.Iterate(0, func(rec *Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}

	// BEGIN has PrevLSN = 0
	if records[0].PrevLSN != 0 {
		t.Errorf("BEGIN should have PrevLSN 0, got %d", records[0].PrevLSN)
	}

	// First INSERT's PrevLSN should point to BEGIN
	if records[1].PrevLSN != records[0].LSN {
		t.Errorf("First INSERT PrevLSN should be %d, got %d", records[0].LSN, records[1].PrevLSN)
	}

	// Second INSERT's PrevLSN should point to first INSERT
	if records[2].PrevLSN != records[1].LSN {
		t.Errorf("Second INSERT PrevLSN should be %d, got %d", records[1].LSN, records[2].PrevLSN)
	}

	// UPDATE's PrevLSN should point to second INSERT
	if records[3].PrevLSN != records[2].LSN {
		t.Errorf("UPDATE PrevLSN should be %d, got %d", records[2].LSN, records[3].PrevLSN)
	}

	// COMMIT's PrevLSN should point to UPDATE
	if records[4].PrevLSN != records[3].LSN {
		t.Errorf("COMMIT PrevLSN should be %d, got %d", records[3].LSN, records[4].PrevLSN)
	}
}

func TestTxnLoggerMultipleTransactions(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// Start two transactions
	tx1, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin tx1 failed: %v", err)
	}

	tx2, err := logger.Begin()
	if err != nil {
		t.Fatalf("Begin tx2 failed: %v", err)
	}

	// Interleave operations
	logger.LogInsert(tx1.ID, "t1", 1, 0, []byte("tx1-row1"))
	logger.LogInsert(tx2.ID, "t1", 1, 1, []byte("tx2-row1"))
	logger.LogInsert(tx1.ID, "t1", 1, 2, []byte("tx1-row2"))

	// Commit tx1, abort tx2
	if err := logger.Commit(tx1.ID); err != nil {
		t.Fatalf("Commit tx1 failed: %v", err)
	}
	if err := logger.Abort(tx2.ID); err != nil {
		t.Fatalf("Abort tx2 failed: %v", err)
	}

	// Verify records
	tx1Records := make([]*Record, 0)
	tx2Records := make([]*Record, 0)

	err = w.Iterate(0, func(rec *Record) error {
		if txn.TxID(rec.TxID) == tx1.ID {
			tx1Records = append(tx1Records, rec)
		} else if txn.TxID(rec.TxID) == tx2.ID {
			tx2Records = append(tx2Records, rec)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// tx1: BEGIN, INSERT, INSERT, COMMIT
	if len(tx1Records) != 4 {
		t.Errorf("expected 4 records for tx1, got %d", len(tx1Records))
	}
	if tx1Records[len(tx1Records)-1].Type != RecordCommit {
		t.Error("tx1 should end with COMMIT")
	}

	// tx2: BEGIN, INSERT, ABORT
	if len(tx2Records) != 3 {
		t.Errorf("expected 3 records for tx2, got %d", len(tx2Records))
	}
	if tx2Records[len(tx2Records)-1].Type != RecordAbort {
		t.Error("tx2 should end with ABORT")
	}
}

func TestTxnLoggerGetActiveTransactions(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open WAL failed: %v", err)
	}
	defer w.Close()

	txnMgr := txn.NewManager()
	logger := NewTxnLogger(w, txnMgr)

	// No active transactions initially
	active := logger.GetActiveTransactions()
	if len(active) != 0 {
		t.Errorf("expected 0 active transactions, got %d", len(active))
	}

	// Start some transactions
	tx1, _ := logger.Begin()
	tx2, _ := logger.Begin()

	active = logger.GetActiveTransactions()
	if len(active) != 2 {
		t.Errorf("expected 2 active transactions, got %d", len(active))
	}

	// Commit one
	logger.Commit(tx1.ID)

	active = logger.GetActiveTransactions()
	if len(active) != 1 {
		t.Errorf("expected 1 active transaction, got %d", len(active))
	}

	// Abort the other
	logger.Abort(tx2.ID)

	active = logger.GetActiveTransactions()
	if len(active) != 0 {
		t.Errorf("expected 0 active transactions after all committed/aborted, got %d", len(active))
	}
}
