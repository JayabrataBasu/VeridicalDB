package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALOpenClose(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if w.CurrentLSN() != 8 {
		t.Errorf("expected initial LSN 8, got %d", w.CurrentLSN())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check that WAL file was created
	walPath := filepath.Join(dir, "wal.log")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file was not created")
	}
}

func TestWALAppendAndIterate(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Append several records
	records := []*Record{
		{TxID: 1, Type: RecordBegin, Data: nil},
		{TxID: 1, Type: RecordInsert, TableName: "users", PageID: 1, Data: []byte("row1")},
		{TxID: 1, Type: RecordInsert, TableName: "users", PageID: 1, Data: []byte("row2")},
		{TxID: 1, Type: RecordCommit, Data: nil},
	}

	var lsns []LSN
	for _, rec := range records {
		lsn, err := w.Append(rec)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	// LSNs should be monotonically increasing
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("LSN not increasing: %d <= %d", lsns[i], lsns[i-1])
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Iterate and verify
	var readRecords []*Record
	err = w.Iterate(0, func(rec *Record) error {
		readRecords = append(readRecords, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if len(readRecords) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(readRecords))
	}

	for i, rec := range readRecords {
		if rec.TxID != records[i].TxID {
			t.Errorf("record %d: expected TxID %d, got %d", i, records[i].TxID, rec.TxID)
		}
		if rec.Type != records[i].Type {
			t.Errorf("record %d: expected Type %v, got %v", i, records[i].Type, rec.Type)
		}
		if rec.LSN != lsns[i] {
			t.Errorf("record %d: expected LSN %d, got %d", i, lsns[i], rec.LSN)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALReopen(t *testing.T) {
	dir := t.TempDir()

	// Write some records
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	records := []*Record{
		{TxID: 1, Type: RecordBegin},
		{TxID: 1, Type: RecordInsert, TableName: "test", PageID: 1, Data: []byte("data1")},
		{TxID: 1, Type: RecordCommit},
	}

	for _, rec := range records {
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	lastLSN := w.CurrentLSN()
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify records are still there
	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}

	// CurrentLSN should be restored
	if w2.CurrentLSN() != lastLSN {
		t.Errorf("expected CurrentLSN %d after reopen, got %d", lastLSN, w2.CurrentLSN())
	}

	var count int
	err = w2.Iterate(0, func(rec *Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if count != len(records) {
		t.Errorf("expected %d records after reopen, got %d", len(records), count)
	}

	// Append more records
	_, err = w2.Append(&Record{TxID: 2, Type: RecordBegin})
	if err != nil {
		t.Fatalf("Append after reopen failed: %v", err)
	}

	if w2.CurrentLSN() <= lastLSN {
		t.Error("LSN should have increased after new append")
	}

	if err := w2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALIterateFromLSN(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Append records
	var lsns []LSN
	for i := 0; i < 5; i++ {
		lsn, err := w.Append(&Record{
			TxID: uint64(i + 1),
			Type: RecordBegin,
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Iterate from middle LSN
	startLSN := lsns[2]
	var readLSNs []LSN
	err = w.Iterate(startLSN, func(rec *Record) error {
		readLSNs = append(readLSNs, rec.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// Should get records from startLSN onwards
	if len(readLSNs) != 3 {
		t.Fatalf("expected 3 records from LSN %d, got %d", startLSN, len(readLSNs))
	}

	if readLSNs[0] != lsns[2] {
		t.Errorf("expected first read LSN %d, got %d", lsns[2], readLSNs[0])
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALFlushSync(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	lsn, err := w.Append(&Record{TxID: 1, Type: RecordBegin})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// The first record has LSN 8 (after header)
	if lsn != 8 {
		t.Errorf("expected first record LSN 8, got %d", lsn)
	}

	// Before flush, flushed LSN should be 8 (header flushed)
	if w.FlushedLSN() != 8 {
		t.Errorf("expected FlushedLSN 8 before flush, got %d", w.FlushedLSN())
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// After flush, flushed LSN should be updated to currentLSN (end of flushed data)
	// This is greater than the record's LSN because it points to the end
	if w.FlushedLSN() != w.CurrentLSN() {
		t.Errorf("expected FlushedLSN %d after flush, got %d", w.CurrentLSN(), w.FlushedLSN())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALMultipleTransactions(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Simulate interleaved transactions
	records := []*Record{
		{TxID: 1, Type: RecordBegin},
		{TxID: 2, Type: RecordBegin},
		{TxID: 1, Type: RecordInsert, TableName: "t1", PageID: 1, Data: []byte("tx1-row1")},
		{TxID: 2, Type: RecordInsert, TableName: "t1", PageID: 1, Data: []byte("tx2-row1")},
		{TxID: 1, Type: RecordCommit},
		{TxID: 2, Type: RecordAbort},
	}

	for _, rec := range records {
		if _, err := w.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Group records by transaction
	tx1Records := make([]*Record, 0)
	tx2Records := make([]*Record, 0)

	err = w.Iterate(0, func(rec *Record) error {
		switch rec.TxID {
		case 1:
			tx1Records = append(tx1Records, rec)
		case 2:
			tx2Records = append(tx2Records, rec)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	// Verify transaction 1: Begin, Insert, Commit
	if len(tx1Records) != 3 {
		t.Errorf("expected 3 records for tx1, got %d", len(tx1Records))
	}
	if tx1Records[0].Type != RecordBegin {
		t.Error("tx1 should start with Begin")
	}
	if tx1Records[2].Type != RecordCommit {
		t.Error("tx1 should end with Commit")
	}

	// Verify transaction 2: Begin, Insert, Abort
	if len(tx2Records) != 3 {
		t.Errorf("expected 3 records for tx2, got %d", len(tx2Records))
	}
	if tx2Records[0].Type != RecordBegin {
		t.Error("tx2 should start with Begin")
	}
	if tx2Records[2].Type != RecordAbort {
		t.Error("tx2 should end with Abort")
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALUpdateRecord(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	oldData := []byte("old value")
	newData := []byte("new value")

	// Encode update data: [oldLen:4][old][new] (as per record.go format)
	updateData := make([]byte, 4+len(oldData)+len(newData))

	// Little-endian uint32 encoding
	binary := func(v uint32) []byte {
		b := make([]byte, 4)
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
		b[3] = byte(v >> 24)
		return b
	}

	copy(updateData[0:4], binary(uint32(len(oldData))))
	copy(updateData[4:4+len(oldData)], oldData)
	copy(updateData[4+len(oldData):], newData)

	rec := &Record{
		TxID:      1,
		Type:      RecordUpdate,
		TableName: "users",
		PageID:    5,
		Data:      updateData,
	}

	_, err = w.Append(rec)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read back and parse
	err = w.Iterate(0, func(r *Record) error {
		if r.Type != RecordUpdate {
			t.Error("expected Update record")
			return nil
		}

		old, new, err := ParseUpdateData(r.Data)
		if err != nil {
			t.Errorf("ParseUpdateData failed: %v", err)
			return nil
		}

		if string(old) != "old value" {
			t.Errorf("expected old value 'old value', got '%s'", string(old))
		}
		if string(new) != "new value" {
			t.Errorf("expected new value 'new value', got '%s'", string(new))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALCheckpoint(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Simulate some transactions
	_, _ = w.Append(&Record{TxID: 1, Type: RecordBegin})
	_, _ = w.Append(&Record{TxID: 1, Type: RecordInsert, TableName: "t", PageID: 1, Data: []byte("d")})
	_, _ = w.Append(&Record{TxID: 1, Type: RecordCommit})

	// Write checkpoint
	checkpointLSN, err := w.Append(&Record{
		TxID: 0,
		Type: RecordCheckpointBegin,
	})
	if err != nil {
		t.Fatalf("Append checkpoint begin failed: %v", err)
	}

	// Encode active transactions (empty for this test)
	activeTxns := make([]byte, 8) // Just count = 0
	_, err = w.Append(&Record{
		TxID: 0,
		Type: RecordCheckpointEnd,
		Data: activeTxns,
	})
	if err != nil {
		t.Fatalf("Append checkpoint end failed: %v", err)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Find checkpoint
	var foundCheckpoint bool
	var checkpointRec *Record
	err = w.Iterate(0, func(rec *Record) error {
		if rec.Type == RecordCheckpointBegin {
			foundCheckpoint = true
			checkpointRec = rec
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if !foundCheckpoint {
		t.Error("checkpoint not found")
	}

	if checkpointRec.LSN != checkpointLSN {
		t.Errorf("checkpoint LSN mismatch: expected %d, got %d", checkpointLSN, checkpointRec.LSN)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALEmptyIterate(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	var count int
	err = w.Iterate(0, func(rec *Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate on empty WAL failed: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 records in empty WAL, got %d", count)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestWALLargeRecord(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Create a large but valid record (under MaxRecordSize)
	largeData := make([]byte, 32*1024) // 32KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	_, err = w.Append(&Record{
		TxID:      1,
		Type:      RecordInsert,
		TableName: "big_table",
		PageID:    1,
		Data:      largeData,
	})
	if err != nil {
		t.Fatalf("Append large record failed: %v", err)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Read back and verify
	var found bool
	err = w.Iterate(0, func(rec *Record) error {
		found = true
		if len(rec.Data) != len(largeData) {
			t.Errorf("data length mismatch: expected %d, got %d", len(largeData), len(rec.Data))
		}
		for i := range rec.Data {
			if rec.Data[i] != largeData[i] {
				t.Errorf("data mismatch at position %d", i)
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if !found {
		t.Error("large record not found")
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
