// Package wal provides Write-Ahead Logging for crash recovery.
// The WAL ensures durability by writing changes to a log file before
// they are applied to data pages. On crash recovery, the log is replayed
// to restore the database to a consistent state.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// LSN (Log Sequence Number) is a monotonically increasing identifier
// for log records. It represents the byte offset in the WAL file.
type LSN uint64

// InvalidLSN represents an invalid/unset LSN.
const InvalidLSN LSN = 0

// RecordType identifies the type of WAL record.
type RecordType uint8

const (
	// RecordInvalid is an invalid/uninitialized record type.
	RecordInvalid RecordType = iota

	// RecordBegin marks the start of a transaction.
	RecordBegin

	// RecordCommit marks the successful completion of a transaction.
	RecordCommit

	// RecordAbort marks the rollback of a transaction.
	RecordAbort

	// RecordInsert records a tuple insertion.
	RecordInsert

	// RecordDelete records a tuple deletion.
	RecordDelete

	// RecordUpdate records a tuple update (stores both old and new data).
	RecordUpdate

	// RecordPageWrite records a full page write (for page-level logging).
	RecordPageWrite

	// RecordCheckpointBegin marks the start of a checkpoint.
	RecordCheckpointBegin

	// RecordCheckpointEnd marks the end of a checkpoint.
	RecordCheckpointEnd

	// RecordCLR is a Compensation Log Record for undo operations.
	RecordCLR
)

// String returns the name of the record type.
func (rt RecordType) String() string {
	switch rt {
	case RecordBegin:
		return "BEGIN"
	case RecordCommit:
		return "COMMIT"
	case RecordAbort:
		return "ABORT"
	case RecordInsert:
		return "INSERT"
	case RecordDelete:
		return "DELETE"
	case RecordUpdate:
		return "UPDATE"
	case RecordPageWrite:
		return "PAGE_WRITE"
	case RecordCheckpointBegin:
		return "CHECKPOINT_BEGIN"
	case RecordCheckpointEnd:
		return "CHECKPOINT_END"
	case RecordCLR:
		return "CLR"
	default:
		return "INVALID"
	}
}

// Record represents a single WAL log record.
// Format on disk:
//
//	[Length:4][Type:1][TxID:8][PrevLSN:8][TableLen:2][Table:var]
//	[PageID:4][SlotID:2][DataLen:4][Data:var][CRC:4]
type Record struct {
	// LSN is the log sequence number (position in WAL file)
	LSN LSN

	// Type identifies what kind of operation this record represents
	Type RecordType

	// TxID is the transaction ID that wrote this record
	TxID uint64

	// PrevLSN points to the previous log record for this transaction
	// Used for transaction-level undo
	PrevLSN LSN

	// TableName is the affected table (for data operations)
	TableName string

	// PageID is the affected page (for data operations)
	PageID uint32

	// SlotID is the affected slot within the page
	SlotID uint16

	// Data contains the record payload (varies by record type)
	// For INSERT: the new tuple data
	// For DELETE: the old tuple data (for undo)
	// For UPDATE: [old_len:4][old_data][new_data]
	// For PAGE_WRITE: the full page data
	Data []byte

	// UndoNextLSN is used only for CLR records to skip already-undone records
	UndoNextLSN LSN
}

// HeaderSize is the fixed overhead of a WAL record (excluding variable parts).
// [Length:4][Type:1][TxID:8][PrevLSN:8][TableLen:2][PageID:4][SlotID:2][DataLen:4][CRC:4]
const HeaderSize = 4 + 1 + 8 + 8 + 2 + 4 + 2 + 4 + 4 // = 37 bytes

// MaxRecordSize is the maximum size of a single WAL record.
const MaxRecordSize = 64 * 1024 // 64KB

// Errors
var (
	ErrRecordTooLarge  = errors.New("wal: record too large")
	ErrInvalidRecord   = errors.New("wal: invalid record")
	ErrCorruptedRecord = errors.New("wal: corrupted record (CRC mismatch)")
	ErrWALClosed       = errors.New("wal: log is closed")
	ErrWALFull         = errors.New("wal: log file is full")
	ErrInvalidLSN      = errors.New("wal: invalid LSN")
)

// Encode serializes a WAL record to bytes.
func (r *Record) Encode() ([]byte, error) {
	tableBytes := []byte(r.TableName)
	if len(tableBytes) > 65535 {
		return nil, fmt.Errorf("table name too long")
	}

	// Calculate total size
	totalSize := HeaderSize + len(tableBytes) + len(r.Data)
	if totalSize > MaxRecordSize {
		return nil, ErrRecordTooLarge
	}

	buf := make([]byte, totalSize)
	offset := 0

	// Length (will be filled last, includes everything except the length field itself)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(totalSize-4))
	offset += 4

	// Type
	buf[offset] = byte(r.Type)
	offset++

	// TxID
	binary.LittleEndian.PutUint64(buf[offset:], r.TxID)
	offset += 8

	// PrevLSN
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.PrevLSN))
	offset += 8

	// Table name length
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(tableBytes)))
	offset += 2

	// PageID
	binary.LittleEndian.PutUint32(buf[offset:], r.PageID)
	offset += 4

	// SlotID
	binary.LittleEndian.PutUint16(buf[offset:], r.SlotID)
	offset += 2

	// Data length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(r.Data)))
	offset += 4

	// Table name
	copy(buf[offset:], tableBytes)
	offset += len(tableBytes)

	// Data
	copy(buf[offset:], r.Data)
	offset += len(r.Data)

	// CRC32 of everything except the CRC field itself
	crc := crc32.ChecksumIEEE(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], crc)

	return buf, nil
}

// DecodeRecord deserializes a WAL record from bytes.
func DecodeRecord(data []byte, lsn LSN) (*Record, error) {
	if len(data) < HeaderSize {
		return nil, ErrInvalidRecord
	}

	offset := 0

	// Length
	length := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if int(length)+4 > len(data) {
		return nil, ErrInvalidRecord
	}

	// Verify CRC
	expectedCRC := binary.LittleEndian.Uint32(data[int(length)+4-4:])
	actualCRC := crc32.ChecksumIEEE(data[:int(length)+4-4])
	if expectedCRC != actualCRC {
		return nil, ErrCorruptedRecord
	}

	r := &Record{LSN: lsn}

	// Type
	r.Type = RecordType(data[offset])
	offset++

	// TxID
	r.TxID = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// PrevLSN
	r.PrevLSN = LSN(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Table name length
	tableLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	// PageID
	r.PageID = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// SlotID
	r.SlotID = binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	// Data length
	dataLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Table name
	if offset+int(tableLen) > len(data) {
		return nil, ErrInvalidRecord
	}
	r.TableName = string(data[offset : offset+int(tableLen)])
	offset += int(tableLen)

	// Data
	if offset+int(dataLen) > len(data)-4 { // -4 for CRC
		return nil, ErrInvalidRecord
	}
	r.Data = make([]byte, dataLen)
	copy(r.Data, data[offset:offset+int(dataLen)])

	return r, nil
}

// NewBeginRecord creates a BEGIN transaction record.
func NewBeginRecord(txID uint64) *Record {
	return &Record{
		Type: RecordBegin,
		TxID: txID,
	}
}

// NewCommitRecord creates a COMMIT transaction record.
func NewCommitRecord(txID uint64, prevLSN LSN) *Record {
	return &Record{
		Type:    RecordCommit,
		TxID:    txID,
		PrevLSN: prevLSN,
	}
}

// NewAbortRecord creates an ABORT transaction record.
func NewAbortRecord(txID uint64, prevLSN LSN) *Record {
	return &Record{
		Type:    RecordAbort,
		TxID:    txID,
		PrevLSN: prevLSN,
	}
}

// NewInsertRecord creates an INSERT record.
func NewInsertRecord(txID uint64, prevLSN LSN, tableName string, pageID uint32, slotID uint16, tupleData []byte) *Record {
	return &Record{
		Type:      RecordInsert,
		TxID:      txID,
		PrevLSN:   prevLSN,
		TableName: tableName,
		PageID:    pageID,
		SlotID:    slotID,
		Data:      tupleData,
	}
}

// NewDeleteRecord creates a DELETE record.
func NewDeleteRecord(txID uint64, prevLSN LSN, tableName string, pageID uint32, slotID uint16, oldData []byte) *Record {
	return &Record{
		Type:      RecordDelete,
		TxID:      txID,
		PrevLSN:   prevLSN,
		TableName: tableName,
		PageID:    pageID,
		SlotID:    slotID,
		Data:      oldData,
	}
}

// NewUpdateRecord creates an UPDATE record.
func NewUpdateRecord(txID uint64, prevLSN LSN, tableName string, pageID uint32, slotID uint16, oldData, newData []byte) *Record {
	// Pack both old and new data
	data := make([]byte, 4+len(oldData)+len(newData))
	binary.LittleEndian.PutUint32(data, uint32(len(oldData)))
	copy(data[4:], oldData)
	copy(data[4+len(oldData):], newData)

	return &Record{
		Type:      RecordUpdate,
		TxID:      txID,
		PrevLSN:   prevLSN,
		TableName: tableName,
		PageID:    pageID,
		SlotID:    slotID,
		Data:      data,
	}
}

// NewCheckpointBeginRecord creates a checkpoint begin record.
func NewCheckpointBeginRecord() *Record {
	return &Record{
		Type: RecordCheckpointBegin,
	}
}

// NewCheckpointEndRecord creates a checkpoint end record.
// The data contains the active transaction table for recovery.
func NewCheckpointEndRecord(activeTransactions []uint64) *Record {
	data := make([]byte, 4+8*len(activeTransactions))
	binary.LittleEndian.PutUint32(data, uint32(len(activeTransactions)))
	for i, txID := range activeTransactions {
		binary.LittleEndian.PutUint64(data[4+8*i:], txID)
	}
	return &Record{
		Type: RecordCheckpointEnd,
		Data: data,
	}
}

// ParseCheckpointEnd extracts active transaction IDs from a checkpoint end record.
func ParseCheckpointEnd(r *Record) ([]uint64, error) {
	if r.Type != RecordCheckpointEnd {
		return nil, fmt.Errorf("not a checkpoint end record")
	}
	if len(r.Data) < 4 {
		return nil, ErrInvalidRecord
	}
	count := binary.LittleEndian.Uint32(r.Data)
	if len(r.Data) < 4+int(count)*8 {
		return nil, ErrInvalidRecord
	}
	txIDs := make([]uint64, count)
	for i := range txIDs {
		txIDs[i] = binary.LittleEndian.Uint64(r.Data[4+8*i:])
	}
	return txIDs, nil
}

// ParseUpdateData extracts old and new data from an UPDATE record.
func ParseUpdateData(data []byte) (oldData, newData []byte, err error) {
	if len(data) < 4 {
		return nil, nil, ErrInvalidRecord
	}
	oldLen := binary.LittleEndian.Uint32(data)
	if len(data) < 4+int(oldLen) {
		return nil, nil, ErrInvalidRecord
	}
	oldData = data[4 : 4+oldLen]
	newData = data[4+oldLen:]
	return oldData, newData, nil
}
