package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// WAL manages the Write-Ahead Log file operations.
type WAL struct {
	mu sync.Mutex

	// dataDir is the directory containing WAL files
	dataDir string

	// file is the currently active WAL segment
	file *os.File

	// currentLSN is the next LSN to be assigned
	currentLSN LSN

	// flushedLSN is the LSN up to which data has been fsynced
	flushedLSN LSN

	// lastCheckpointLSN is the LSN of the last checkpoint
	lastCheckpointLSN LSN

	// closed indicates if the WAL has been closed
	closed bool

	// buffer for batching writes
	buffer []byte

	// segmentSize is the maximum size of a WAL segment file
	segmentSize int64
}

// Config contains WAL configuration options.
type Config struct {
	// DataDir is the directory for WAL files
	DataDir string

	// SegmentSize is the maximum size of each WAL segment (default: 16MB)
	SegmentSize int64
}

// DefaultSegmentSize is 16MB
const DefaultSegmentSize = 16 * 1024 * 1024

// WAL file name pattern
const walFileName = "wal.log"

// Open opens or creates a WAL in the specified directory.
// Accepts either a string (directory path) or a Config struct.
func Open(arg interface{}) (*WAL, error) {
	var config Config

	switch v := arg.(type) {
	case string:
		config = Config{DataDir: v, SegmentSize: DefaultSegmentSize}
	case Config:
		config = v
	default:
		return nil, fmt.Errorf("Open requires string or Config, got %T", arg)
	}

	if config.SegmentSize == 0 {
		config.SegmentSize = DefaultSegmentSize
	}

	// Ensure directory exists
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}

	walPath := filepath.Join(config.DataDir, walFileName)

	// Open or create the WAL file
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	w := &WAL{
		dataDir:     config.DataDir,
		file:        file,
		segmentSize: config.SegmentSize,
		buffer:      make([]byte, 0, 64*1024), // 64KB buffer
	}

	// Determine current LSN from file size
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat wal file: %w", err)
	}
	w.currentLSN = LSN(info.Size())

	// If file is empty, write magic header to ensure LSN > 0
	if w.currentLSN == 0 {
		magic := []byte("VDBWAL01") // 8 bytes
		if _, err := file.Write(magic); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("write wal magic: %w", err)
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("sync wal magic: %w", err)
		}
		w.currentLSN = LSN(len(magic))
	}

	w.flushedLSN = w.currentLSN

	// Seek to end for appending
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek wal file: %w", err)
	}

	return w, nil
}

// Append writes a log record and returns its LSN.
// The record is buffered until Flush() is called.
func (w *WAL) Append(record *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return InvalidLSN, ErrWALClosed
	}

	// Encode the record
	data, err := record.Encode()
	if err != nil {
		return InvalidLSN, err
	}

	// Assign LSN
	lsn := w.currentLSN
	record.LSN = lsn

	// Check if we would exceed segment size
	if int64(w.currentLSN)+int64(len(data)) > w.segmentSize {
		// For now, just continue (no segment rotation)
		// A production system would rotate to a new segment
	}

	// Append to buffer
	w.buffer = append(w.buffer, data...)
	w.currentLSN += LSN(len(data))

	return lsn, nil
}

// AppendAndFlush writes a log record and immediately flushes to disk.
// This is used for commit records to ensure durability.
func (w *WAL) AppendAndFlush(record *Record) (LSN, error) {
	lsn, err := w.Append(record)
	if err != nil {
		return InvalidLSN, err
	}
	if err := w.Flush(); err != nil {
		return InvalidLSN, err
	}
	return lsn, nil
}

// Flush writes buffered records to disk and fsyncs.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	if len(w.buffer) == 0 {
		return nil
	}

	// Write buffer to file
	n, err := w.file.Write(w.buffer)
	if err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	if n != len(w.buffer) {
		return fmt.Errorf("short write: %d of %d bytes", n, len(w.buffer))
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}

	// Update flushed LSN
	w.flushedLSN = w.currentLSN

	// Clear buffer
	w.buffer = w.buffer[:0]

	return nil
}

// CurrentLSN returns the next LSN that will be assigned.
func (w *WAL) CurrentLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// FlushedLSN returns the LSN up to which data has been synced to disk.
func (w *WAL) FlushedLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushedLSN
}

// SetCheckpointLSN records the LSN of a checkpoint.
func (w *WAL) SetCheckpointLSN(lsn LSN) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastCheckpointLSN = lsn
}

// LastCheckpointLSN returns the LSN of the last checkpoint.
func (w *WAL) LastCheckpointLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastCheckpointLSN
}

// Close flushes any pending data and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	// Flush any remaining buffer
	if len(w.buffer) > 0 {
		if _, err := w.file.Write(w.buffer); err != nil {
			_ = w.file.Close()
			return fmt.Errorf("final write: %w", err)
		}
		if err := w.file.Sync(); err != nil {
			_ = w.file.Close()
			return fmt.Errorf("final sync: %w", err)
		}
	}

	return w.file.Close()
}

// Iterator reads records from the WAL sequentially.
type Iterator struct {
	file   *os.File
	offset int64
	size   int64
}

// NewIterator creates an iterator to read WAL records from the beginning.
func (w *WAL) NewIterator() (*Iterator, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Open a separate file handle for reading
	walPath := filepath.Join(w.dataDir, walFileName)
	file, err := os.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open wal for reading: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}

	offset := int64(0)
	// Check for magic header and skip it
	if info.Size() >= 8 {
		header := make([]byte, 8)
		if _, err := file.ReadAt(header, 0); err == nil {
			if bytes.Equal(header, []byte("VDBWAL01")) {
				offset = 8
			} else {
				fmt.Printf("Header mismatch: %v\n", header)
			}
		} else {
			fmt.Printf("ReadAt error: %v\n", err)
		}
	} else {
		fmt.Printf("File too small: %d\n", info.Size())
	}

	// Seek to offset
	if offset > 0 {
		if _, err := file.Seek(offset, 0); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("seek past header: %w", err)
		}
	}

	return &Iterator{
		file:   file,
		offset: offset,
		size:   info.Size(),
	}, nil
}

// NewIteratorFrom creates an iterator starting from a specific LSN.
func (w *WAL) NewIteratorFrom(startLSN LSN) (*Iterator, error) {
	it, err := w.NewIterator()
	if err != nil {
		return nil, err
	}

	// If startLSN is provided, seek to it.
	// Otherwise, keep the offset determined by NewIterator (which skips header).
	if startLSN > 0 {
		if _, err := it.file.Seek(int64(startLSN), io.SeekStart); err != nil {
			_ = it.Close()
			return nil, fmt.Errorf("seek to LSN: %w", err)
		}
		it.offset = int64(startLSN)
	}

	return it, nil
}

// Next reads the next record from the WAL.
// Returns nil, nil when there are no more records.
func (it *Iterator) Next() (*Record, error) {
	if it.offset >= it.size {
		return nil, nil // End of WAL
	}

	// Read record length (first 4 bytes)
	lenBuf := make([]byte, 4)
	n, err := it.file.Read(lenBuf)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read record length: %w", err)
	}
	if n < 4 {
		return nil, fmt.Errorf("short read on length")
	}

	recordLen := binary.LittleEndian.Uint32(lenBuf)
	totalLen := 4 + int(recordLen) // length field + record content

	// Read the full record
	recordBuf := make([]byte, totalLen)
	copy(recordBuf, lenBuf) // Include the length we already read

	remaining := totalLen - 4
	n, err = it.file.Read(recordBuf[4:])
	if err != nil {
		return nil, fmt.Errorf("read record data: %w", err)
	}
	if n < remaining {
		return nil, fmt.Errorf("short read on record data: got %d, want %d", n, remaining)
	}

	// Decode the record
	lsn := LSN(it.offset)
	record, err := DecodeRecord(recordBuf, lsn)
	if err != nil {
		return nil, fmt.Errorf("decode record at LSN %d: %w", lsn, err)
	}

	it.offset += int64(totalLen)
	return record, nil
}

// Close closes the iterator.
func (it *Iterator) Close() error {
	return it.file.Close()
}

// Truncate removes all WAL data before the specified LSN.
// This is typically called after a checkpoint.
func (w *WAL) Truncate(beforeLSN LSN) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWALClosed
	}

	// For simplicity, we don't actually truncate the file in this implementation.
	// A production system would create a new segment and remove old ones.
	// For now, just record that data before this LSN can be ignored.
	w.lastCheckpointLSN = beforeLSN

	return nil
}

// Size returns the current size of the WAL file.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int64(w.currentLSN)
}

// Iterate reads all records from startLSN and calls fn for each.
// This is a convenience method that wraps NewIteratorFrom.
func (w *WAL) Iterate(startLSN LSN, fn func(*Record) error) error {
	it, err := w.NewIteratorFrom(startLSN)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for {
		rec, err := it.Next()
		if err != nil {
			return err
		}
		if rec == nil {
			return nil // End of WAL
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}
