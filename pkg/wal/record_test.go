package wal

import (
	"bytes"
	"testing"
)

func TestRecordEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		record *Record
	}{
		{
			name:   "begin record",
			record: NewBeginRecord(1),
		},
		{
			name:   "commit record",
			record: NewCommitRecord(1, 100),
		},
		{
			name:   "abort record",
			record: NewAbortRecord(2, 200),
		},
		{
			name:   "insert record",
			record: NewInsertRecord(3, 300, "users", 5, 10, []byte("hello world")),
		},
		{
			name:   "delete record",
			record: NewDeleteRecord(4, 400, "products", 7, 3, []byte("old data")),
		},
		{
			name:   "update record",
			record: NewUpdateRecord(5, 500, "orders", 10, 5, []byte("old"), []byte("new data")),
		},
		{
			name:   "checkpoint begin",
			record: NewCheckpointBeginRecord(),
		},
		{
			name:   "checkpoint end",
			record: NewCheckpointEndRecord([]uint64{1, 2, 3, 100}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := tt.record.Encode()
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := DecodeRecord(encoded, tt.record.LSN)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Verify fields match
			if decoded.Type != tt.record.Type {
				t.Errorf("Type mismatch: got %v, want %v", decoded.Type, tt.record.Type)
			}
			if decoded.TxID != tt.record.TxID {
				t.Errorf("TxID mismatch: got %v, want %v", decoded.TxID, tt.record.TxID)
			}
			if decoded.PrevLSN != tt.record.PrevLSN {
				t.Errorf("PrevLSN mismatch: got %v, want %v", decoded.PrevLSN, tt.record.PrevLSN)
			}
			if decoded.TableName != tt.record.TableName {
				t.Errorf("TableName mismatch: got %q, want %q", decoded.TableName, tt.record.TableName)
			}
			if decoded.PageID != tt.record.PageID {
				t.Errorf("PageID mismatch: got %v, want %v", decoded.PageID, tt.record.PageID)
			}
			if decoded.SlotID != tt.record.SlotID {
				t.Errorf("SlotID mismatch: got %v, want %v", decoded.SlotID, tt.record.SlotID)
			}
			if !bytes.Equal(decoded.Data, tt.record.Data) {
				t.Errorf("Data mismatch: got %v, want %v", decoded.Data, tt.record.Data)
			}
		})
	}
}

func TestRecordTypeString(t *testing.T) {
	tests := []struct {
		rt   RecordType
		want string
	}{
		{RecordBegin, "BEGIN"},
		{RecordCommit, "COMMIT"},
		{RecordAbort, "ABORT"},
		{RecordInsert, "INSERT"},
		{RecordDelete, "DELETE"},
		{RecordUpdate, "UPDATE"},
		{RecordPageWrite, "PAGE_WRITE"},
		{RecordCheckpointBegin, "CHECKPOINT_BEGIN"},
		{RecordCheckpointEnd, "CHECKPOINT_END"},
		{RecordCLR, "CLR"},
		{RecordInvalid, "INVALID"},
	}

	for _, tt := range tests {
		if got := tt.rt.String(); got != tt.want {
			t.Errorf("RecordType(%d).String() = %q, want %q", tt.rt, got, tt.want)
		}
	}
}

func TestParseUpdateData(t *testing.T) {
	record := NewUpdateRecord(1, 0, "test", 0, 0, []byte("old data"), []byte("new data here"))

	oldData, newData, err := ParseUpdateData(record.Data)
	if err != nil {
		t.Fatalf("ParseUpdateData failed: %v", err)
	}

	if string(oldData) != "old data" {
		t.Errorf("oldData = %q, want %q", oldData, "old data")
	}
	if string(newData) != "new data here" {
		t.Errorf("newData = %q, want %q", newData, "new data here")
	}
}

func TestParseCheckpointEnd(t *testing.T) {
	txIDs := []uint64{10, 20, 30, 40, 50}
	record := NewCheckpointEndRecord(txIDs)

	parsed, err := ParseCheckpointEnd(record)
	if err != nil {
		t.Fatalf("ParseCheckpointEnd failed: %v", err)
	}

	if len(parsed) != len(txIDs) {
		t.Fatalf("parsed length = %d, want %d", len(parsed), len(txIDs))
	}

	for i, txID := range txIDs {
		if parsed[i] != txID {
			t.Errorf("parsed[%d] = %d, want %d", i, parsed[i], txID)
		}
	}
}

func TestCorruptedRecordCRC(t *testing.T) {
	record := NewInsertRecord(1, 0, "test", 1, 2, []byte("some data"))
	encoded, err := record.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Corrupt the data
	encoded[20] ^= 0xFF

	_, err = DecodeRecord(encoded, 0)
	if err != ErrCorruptedRecord {
		t.Errorf("Expected ErrCorruptedRecord, got %v", err)
	}
}

func TestRecordTooLarge(t *testing.T) {
	// Create a record with data larger than MaxRecordSize
	largeData := make([]byte, MaxRecordSize+1)
	record := NewInsertRecord(1, 0, "test", 1, 2, largeData)

	_, err := record.Encode()
	if err != ErrRecordTooLarge {
		t.Errorf("Expected ErrRecordTooLarge, got %v", err)
	}
}

func TestEmptyRecord(t *testing.T) {
	record := &Record{Type: RecordBegin, TxID: 42}
	encoded, err := record.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeRecord(encoded, 0)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type != RecordBegin {
		t.Errorf("Type = %v, want %v", decoded.Type, RecordBegin)
	}
	if decoded.TxID != 42 {
		t.Errorf("TxID = %d, want 42", decoded.TxID)
	}
	if decoded.TableName != "" {
		t.Errorf("TableName = %q, want empty", decoded.TableName)
	}
	if len(decoded.Data) != 0 {
		t.Errorf("Data length = %d, want 0", len(decoded.Data))
	}
}
