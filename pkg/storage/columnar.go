package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// ColumnarEngine implements StorageEngine with column-oriented storage.
// Data is organized into segments, each containing a fixed number of rows.
// Within a segment, data for each column is stored contiguously.
//
// Benefits:
// - Excellent for analytical queries that read few columns
// - Better compression (similar values stored together)
// - Vectorized execution friendly
//
// Trade-offs:
// - Updates are expensive (append + delete bitmap)
// - Point queries need to read from multiple files
type ColumnarEngine struct {
	mu sync.RWMutex

	dataDir   string
	tableName string
	schema    *Schema

	// segments holds all committed segments
	segments []*Segment

	// writeBuffer accumulates rows until a segment is full
	writeBuffer []*bufferedRow

	// deleteBitmap tracks deleted rows (segmentID, rowOffset)
	deleteBitmap map[uint64]bool

	// nextRowID is the next row ID to assign
	nextRowID uint64

	// config
	rowsPerSegment int
}

// bufferedRow is a row waiting to be written to a segment.
type bufferedRow struct {
	rowID uint64
	row   *Row
	xmin  txn.TxID
}

// Segment represents a columnar segment on disk.
type Segment struct {
	ID       uint64
	StartRow uint64 // First row ID in this segment
	RowCount int    // Number of rows in segment

	// Column files (one per column)
	// Stored as: dataDir/tableName/seg_<id>/col_<colID>.dat
	dataDir string

	// Metadata
	MinValues []Value // Min value per column (for pruning)
	MaxValues []Value // Max value per column (for pruning)

	// Loaded column data (lazy loaded)
	columnData [][]byte
}

const (
	// DefaultRowsPerSegment is the default segment size.
	DefaultRowsPerSegment = 65536 // 64K rows

	// ColumnarMagic identifies columnar segment files.
	ColumnarMagic = 0x434F4C53 // "COLS"
)

// NewColumnarEngine creates a new columnar storage engine.
func NewColumnarEngine(dataDir, tableName string, schema *Schema) (*ColumnarEngine, error) {
	engine := &ColumnarEngine{
		dataDir:        dataDir,
		tableName:      tableName,
		schema:         schema,
		segments:       make([]*Segment, 0),
		writeBuffer:    make([]*bufferedRow, 0),
		deleteBitmap:   make(map[uint64]bool),
		nextRowID:      1,
		rowsPerSegment: DefaultRowsPerSegment,
	}

	// Create table directory
	tableDir := filepath.Join(dataDir, tableName)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return nil, fmt.Errorf("create table dir: %w", err)
	}

	// Load existing segments
	if err := engine.loadSegments(); err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}

	return engine, nil
}

// loadSegments loads segment metadata from disk.
func (e *ColumnarEngine) loadSegments() error {
	tableDir := filepath.Join(e.dataDir, e.tableName)

	entries, err := os.ReadDir(tableDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		var segID uint64
		if _, err := fmt.Sscanf(entry.Name(), "seg_%d", &segID); err != nil {
			continue
		}

		seg, err := e.loadSegmentMetadata(segID)
		if err != nil {
			return fmt.Errorf("load segment %d: %w", segID, err)
		}

		e.segments = append(e.segments, seg)

		// Update nextRowID
		endRow := seg.StartRow + uint64(seg.RowCount)
		if endRow >= e.nextRowID {
			e.nextRowID = endRow
		}
	}

	return nil
}

// loadSegmentMetadata loads a segment's metadata without loading column data.
func (e *ColumnarEngine) loadSegmentMetadata(segID uint64) (*Segment, error) {
	segDir := filepath.Join(e.dataDir, e.tableName, fmt.Sprintf("seg_%d", segID))

	metaPath := filepath.Join(segDir, "meta.dat")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}

	if len(data) < 24 {
		return nil, fmt.Errorf("invalid segment metadata")
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != ColumnarMagic {
		return nil, fmt.Errorf("invalid segment magic")
	}

	seg := &Segment{
		ID:       binary.LittleEndian.Uint64(data[4:12]),
		StartRow: binary.LittleEndian.Uint64(data[12:20]),
		RowCount: int(binary.LittleEndian.Uint32(data[20:24])),
		dataDir:  segDir,
	}

	return seg, nil
}

// Insert adds a new row to the write buffer.
func (e *ColumnarEngine) Insert(row *Row, tx *txn.Transaction) (RID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(row.Values) != len(e.schema.Columns) {
		return RID{}, fmt.Errorf("column count mismatch: got %d, want %d",
			len(row.Values), len(e.schema.Columns))
	}

	rowID := e.nextRowID
	e.nextRowID++

	var xmin txn.TxID
	if tx != nil {
		xmin = tx.ID
	}

	e.writeBuffer = append(e.writeBuffer, &bufferedRow{
		rowID: rowID,
		row:   row,
		xmin:  xmin,
	})

	// Flush if buffer is full
	if len(e.writeBuffer) >= e.rowsPerSegment {
		if err := e.flushBuffer(); err != nil {
			return RID{}, fmt.Errorf("flush buffer: %w", err)
		}
	}

	// Return RID (using rowID as the identifier)
	return RID{Table: e.tableName, Page: uint32(rowID >> 16), Slot: uint16(rowID & 0xFFFF)}, nil
}

// flushBuffer writes the write buffer to a new segment.
func (e *ColumnarEngine) flushBuffer() error {
	if len(e.writeBuffer) == 0 {
		return nil
	}

	segID := uint64(len(e.segments))
	startRow := e.writeBuffer[0].rowID

	seg := &Segment{
		ID:         segID,
		StartRow:   startRow,
		RowCount:   len(e.writeBuffer),
		dataDir:    filepath.Join(e.dataDir, e.tableName, fmt.Sprintf("seg_%d", segID)),
		MinValues:  make([]Value, len(e.schema.Columns)),
		MaxValues:  make([]Value, len(e.schema.Columns)),
		columnData: make([][]byte, len(e.schema.Columns)),
	}

	// Create segment directory
	if err := os.MkdirAll(seg.dataDir, 0755); err != nil {
		return fmt.Errorf("create segment dir: %w", err)
	}

	// Write each column
	for colIdx := range e.schema.Columns {
		if err := e.writeColumn(seg, colIdx); err != nil {
			return fmt.Errorf("write column %d: %w", colIdx, err)
		}
	}

	// Write metadata
	if err := e.writeSegmentMetadata(seg); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	e.segments = append(e.segments, seg)
	e.writeBuffer = e.writeBuffer[:0]

	return nil
}

// writeColumn writes a single column's data for the buffered rows.
func (e *ColumnarEngine) writeColumn(seg *Segment, colIdx int) error {
	colInfo := &e.schema.Columns[colIdx]
	colPath := filepath.Join(seg.dataDir, fmt.Sprintf("col_%d.dat", colIdx))

	// Estimate size
	var size int
	if colInfo.Type.IsFixedWidth() {
		size = len(e.writeBuffer) * (colInfo.Type.Width() + 1) // +1 for null flag
	} else {
		// Variable width: estimate
		size = len(e.writeBuffer) * 64 // Average estimate
	}

	data := make([]byte, 0, size)

	// Write null bitmap (1 bit per row, padded to bytes)
	nullBitmapSize := (len(e.writeBuffer) + 7) / 8
	nullBitmap := make([]byte, nullBitmapSize)

	for i, br := range e.writeBuffer {
		if br.row.Values[colIdx].IsNull {
			nullBitmap[i/8] |= 1 << (i % 8)
		}
	}
	data = append(data, nullBitmap...)

	// Track min/max for pruning
	var minSet bool
	var minVal, maxVal Value

	// Write values
	for _, br := range e.writeBuffer {
		val := br.row.Values[colIdx]

		if !val.IsNull {
			if !minSet {
				minVal = val
				maxVal = val
				minSet = true
			} else {
				if compareValues(val, minVal) < 0 {
					minVal = val
				}
				if compareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
		}

		encoded := encodeValue(val, colInfo.Type)
		data = append(data, encoded...)
	}

	seg.MinValues[colIdx] = minVal
	seg.MaxValues[colIdx] = maxVal

	// Write to file
	if err := os.WriteFile(colPath, data, 0644); err != nil {
		return err
	}

	return nil
}

// encodeValue encodes a value to bytes.
func encodeValue(val Value, colType ColumnType) []byte {
	if val.IsNull {
		return nil // Null is handled by bitmap
	}

	switch colType {
	case TypeInt32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(val.Data.(int32)))
		return buf
	case TypeInt64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(val.Data.(int64)))
		return buf
	case TypeFloat64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(val.Data.(float64)))
		return buf
	case TypeBool:
		if val.Data.(bool) {
			return []byte{1}
		}
		return []byte{0}
	case TypeString:
		s := val.Data.(string)
		buf := make([]byte, 4+len(s))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(s)))
		copy(buf[4:], s)
		return buf
	case TypeBytes:
		b := val.Data.([]byte)
		buf := make([]byte, 4+len(b))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(b)))
		copy(buf[4:], b)
		return buf
	default:
		return nil
	}
}

// writeSegmentMetadata writes segment metadata to disk.
func (e *ColumnarEngine) writeSegmentMetadata(seg *Segment) error {
	metaPath := filepath.Join(seg.dataDir, "meta.dat")

	// Simple format: magic(4) + segID(8) + startRow(8) + rowCount(4)
	data := make([]byte, 24)
	binary.LittleEndian.PutUint32(data[0:4], ColumnarMagic)
	binary.LittleEndian.PutUint64(data[4:12], seg.ID)
	binary.LittleEndian.PutUint64(data[12:20], seg.StartRow)
	binary.LittleEndian.PutUint32(data[20:24], uint32(seg.RowCount))

	return os.WriteFile(metaPath, data, 0644)
}

// Delete marks a row as deleted.
func (e *ColumnarEngine) Delete(rid RID, tx *txn.Transaction) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rowID := (uint64(rid.Page) << 16) | uint64(rid.Slot)
	e.deleteBitmap[rowID] = true
	return nil
}

// Update is implemented as delete + insert for columnar storage.
func (e *ColumnarEngine) Update(rid RID, row *Row, tx *txn.Transaction) error {
	if err := e.Delete(rid, tx); err != nil {
		return err
	}
	_, err := e.Insert(row, tx)
	return err
}

// Fetch retrieves a single row by RID.
func (e *ColumnarEngine) Fetch(rid RID, tx *txn.Transaction) (*Row, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rowID := (uint64(rid.Page) << 16) | uint64(rid.Slot)

	// Check if deleted
	if e.deleteBitmap[rowID] {
		return nil, fmt.Errorf("row deleted")
	}

	// Check write buffer first
	for _, br := range e.writeBuffer {
		if br.rowID == rowID {
			return br.row, nil
		}
	}

	// Find in segments
	for _, seg := range e.segments {
		if rowID >= seg.StartRow && rowID < seg.StartRow+uint64(seg.RowCount) {
			return e.fetchFromSegment(seg, rowID)
		}
	}

	return nil, fmt.Errorf("row not found")
}

// fetchFromSegment reads a single row from a segment.
func (e *ColumnarEngine) fetchFromSegment(seg *Segment, rowID uint64) (*Row, error) {
	rowOffset := int(rowID - seg.StartRow)

	row := &Row{
		Values: make([]Value, len(e.schema.Columns)),
	}

	for colIdx, colInfo := range e.schema.Columns {
		val, err := e.readColumnValue(seg, colIdx, rowOffset, &colInfo)
		if err != nil {
			return nil, fmt.Errorf("read column %d: %w", colIdx, err)
		}
		row.Values[colIdx] = val
	}

	return row, nil
}

// readColumnValue reads a single value from a column file.
func (e *ColumnarEngine) readColumnValue(seg *Segment, colIdx, rowOffset int, colInfo *ColumnInfo) (Value, error) {
	colPath := filepath.Join(seg.dataDir, fmt.Sprintf("col_%d.dat", colIdx))

	data, err := os.ReadFile(colPath)
	if err != nil {
		return Value{}, err
	}

	nullBitmapSize := (seg.RowCount + 7) / 8
	if len(data) < nullBitmapSize {
		return Value{}, fmt.Errorf("invalid column file")
	}

	// Check null bitmap
	if data[rowOffset/8]&(1<<(rowOffset%8)) != 0 {
		return Value{IsNull: true}, nil
	}

	// Find value offset
	valueData := data[nullBitmapSize:]
	var offset int

	if colInfo.Type.IsFixedWidth() {
		width := colInfo.Type.Width()
		offset = rowOffset * width
	} else {
		// Variable width: need to scan from start
		for i := 0; i < rowOffset; i++ {
			if data[i/8]&(1<<(i%8)) != 0 {
				continue // Skip null
			}
			strLen := int(binary.LittleEndian.Uint32(valueData[offset:]))
			offset += 4 + strLen
		}
	}

	return decodeValue(valueData[offset:], colInfo.Type)
}

// decodeValue decodes a value from bytes.
func decodeValue(data []byte, colType ColumnType) (Value, error) {
	switch colType {
	case TypeInt32:
		if len(data) < 4 {
			return Value{}, fmt.Errorf("insufficient data for int32")
		}
		return Value{Data: int32(binary.LittleEndian.Uint32(data))}, nil
	case TypeInt64:
		if len(data) < 8 {
			return Value{}, fmt.Errorf("insufficient data for int64")
		}
		return Value{Data: int64(binary.LittleEndian.Uint64(data))}, nil
	case TypeFloat64:
		if len(data) < 8 {
			return Value{}, fmt.Errorf("insufficient data for float64")
		}
		return Value{Data: math.Float64frombits(binary.LittleEndian.Uint64(data))}, nil
	case TypeBool:
		if len(data) < 1 {
			return Value{}, fmt.Errorf("insufficient data for bool")
		}
		return Value{Data: data[0] != 0}, nil
	case TypeString:
		if len(data) < 4 {
			return Value{}, fmt.Errorf("insufficient data for string length")
		}
		strLen := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+strLen {
			return Value{}, fmt.Errorf("insufficient data for string")
		}
		return Value{Data: string(data[4 : 4+strLen])}, nil
	case TypeBytes:
		if len(data) < 4 {
			return Value{}, fmt.Errorf("insufficient data for bytes length")
		}
		bytesLen := int(binary.LittleEndian.Uint32(data))
		if len(data) < 4+bytesLen {
			return Value{}, fmt.Errorf("insufficient data for bytes")
		}
		result := make([]byte, bytesLen)
		copy(result, data[4:4+bytesLen])
		return Value{Data: result}, nil
	default:
		return Value{}, fmt.Errorf("unsupported type: %v", colType)
	}
}

// Scan returns an iterator over all visible rows.
func (e *ColumnarEngine) Scan(projectedCols []int, tx *txn.Transaction) (RowIterator, error) {
	return e.ScanWithPredicate(projectedCols, nil, tx)
}

// ScanWithPredicate returns an iterator with a filter predicate.
func (e *ColumnarEngine) ScanWithPredicate(projectedCols []int, pred Predicate, tx *txn.Transaction) (RowIterator, error) {
	e.mu.RLock()
	// Note: iterator will need to handle unlocking

	return &columnarIterator{
		engine:        e,
		projectedCols: projectedCols,
		predicate:     pred,
		tx:            tx,
		segmentIdx:    0,
		rowOffset:     -1,
		bufferIdx:     -1,
		inBuffer:      false,
	}, nil
}

// columnarIterator implements RowIterator for columnar storage.
type columnarIterator struct {
	engine        *ColumnarEngine
	projectedCols []int
	predicate     Predicate
	tx            *txn.Transaction

	// Segment iteration state
	segmentIdx int
	rowOffset  int

	// Buffer iteration state
	inBuffer  bool
	bufferIdx int

	// Current row
	currentRow *Row
	currentRID RID

	err    error
	closed bool
}

// Next advances to the next matching row.
func (it *columnarIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	for {
		// Try segments first
		if !it.inBuffer {
			for it.segmentIdx < len(it.engine.segments) {
				seg := it.engine.segments[it.segmentIdx]
				it.rowOffset++

				if it.rowOffset >= seg.RowCount {
					it.segmentIdx++
					it.rowOffset = -1
					continue
				}

				rowID := seg.StartRow + uint64(it.rowOffset)

				// Check delete bitmap
				if it.engine.deleteBitmap[rowID] {
					continue
				}

				// Fetch row
				row, err := it.engine.fetchFromSegment(seg, rowID)
				if err != nil {
					it.err = err
					return false
				}

				// Apply predicate
				if it.predicate != nil && !it.predicate.Evaluate(row) {
					continue
				}

				// Project columns if needed
				if it.projectedCols != nil {
					projected := &Row{Values: make([]Value, len(it.projectedCols))}
					for i, colIdx := range it.projectedCols {
						projected.Values[i] = row.Values[colIdx]
					}
					row = projected
				}

				it.currentRow = row
				it.currentRID = RID{Table: it.engine.tableName, Page: uint32(rowID >> 16), Slot: uint16(rowID & 0xFFFF)}
				return true
			}

			// Move to buffer
			it.inBuffer = true
			it.bufferIdx = -1
		}

		// Try write buffer
		it.bufferIdx++
		if it.bufferIdx >= len(it.engine.writeBuffer) {
			return false // No more rows
		}

		br := it.engine.writeBuffer[it.bufferIdx]

		// Check delete bitmap
		if it.engine.deleteBitmap[br.rowID] {
			continue
		}

		// Apply predicate
		if it.predicate != nil && !it.predicate.Evaluate(br.row) {
			continue
		}

		// Project columns if needed
		row := br.row
		if it.projectedCols != nil {
			projected := &Row{Values: make([]Value, len(it.projectedCols))}
			for i, colIdx := range it.projectedCols {
				projected.Values[i] = row.Values[colIdx]
			}
			row = projected
		}

		it.currentRow = row
		it.currentRID = RID{Table: it.engine.tableName, Page: uint32(br.rowID >> 16), Slot: uint16(br.rowID & 0xFFFF)}
		return true
	}
}

// Row returns the current row.
func (it *columnarIterator) Row() *Row {
	return it.currentRow
}

// RID returns the current row's RID.
func (it *columnarIterator) RID() RID {
	return it.currentRID
}

// Err returns any error encountered.
func (it *columnarIterator) Err() error {
	return it.err
}

// Close releases resources.
func (it *columnarIterator) Close() error {
	if !it.closed {
		it.closed = true
		it.engine.mu.RUnlock()
	}
	return nil
}

// Schema returns the table schema.
func (e *ColumnarEngine) Schema() *Schema {
	return e.schema
}

// StorageType returns StorageTypeColumn.
func (e *ColumnarEngine) StorageType() StorageType {
	return StorageTypeColumn
}

// Close releases resources.
func (e *ColumnarEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Flush any remaining buffer
	if len(e.writeBuffer) > 0 {
		if err := e.flushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

// Flush ensures all data is persisted.
func (e *ColumnarEngine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushBuffer()
}
