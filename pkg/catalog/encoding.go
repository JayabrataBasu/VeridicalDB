package catalog

import (
	"encoding/binary"
	"errors"
	"time"
)

// Row encoding format:
// [null bitmap (ceil(numCols/8) bytes)] [fixed-width values inline] [var-length: offset+length pairs, then data]
//
// For simplicity in this stage, we use a slightly simpler format:
// [null bitmap] [for each column in order: if fixed, value bytes; if var, 4-byte length + data]

// EncodeRow encodes values according to schema into bytes.
func EncodeRow(schema *Schema, values []Value) ([]byte, error) {
	if err := schema.Validate(values); err != nil {
		return nil, err
	}

	numCols := len(schema.Columns)
	nullBitmapSize := (numCols + 7) / 8
	nullBitmap := make([]byte, nullBitmapSize)

	// Mark nulls in bitmap
	for i, v := range values {
		if v.IsNull {
			nullBitmap[i/8] |= 1 << (i % 8)
		}
	}

	// Estimate size
	size := nullBitmapSize
	for i, col := range schema.Columns {
		if values[i].IsNull {
			continue
		}
		if col.Type.IsFixedWidth() {
			size += col.Type.FixedWidth()
		} else {
			// 4 bytes for length + data
			size += 4 + len(values[i].Text)
		}
	}

	buf := make([]byte, 0, size)
	buf = append(buf, nullBitmap...)

	// Encode each value
	for i, col := range schema.Columns {
		v := values[i]
		if v.IsNull {
			continue
		}
		switch col.Type {
		case TypeInt32:
			b := make([]byte, 4)
			binary.LittleEndian.PutUint32(b, uint32(v.Int32))
			buf = append(buf, b...)
		case TypeInt64:
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v.Int64))
			buf = append(buf, b...)
		case TypeBool:
			if v.Bool {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case TypeTimestamp:
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v.Timestamp.UnixNano()))
			buf = append(buf, b...)
		case TypeText:
			lenB := make([]byte, 4)
			binary.LittleEndian.PutUint32(lenB, uint32(len(v.Text)))
			buf = append(buf, lenB...)
			buf = append(buf, []byte(v.Text)...)
		}
	}

	return buf, nil
}

// DecodeRow decodes bytes into values according to schema.
func DecodeRow(schema *Schema, data []byte) ([]Value, error) {
	numCols := len(schema.Columns)
	nullBitmapSize := (numCols + 7) / 8

	if len(data) < nullBitmapSize {
		return nil, errors.New("data too short for null bitmap")
	}

	nullBitmap := data[:nullBitmapSize]
	pos := nullBitmapSize

	values := make([]Value, numCols)

	for i, col := range schema.Columns {
		isNull := (nullBitmap[i/8] & (1 << (i % 8))) != 0
		if isNull {
			values[i] = Null(col.Type)
			continue
		}

		switch col.Type {
		case TypeInt32:
			if pos+4 > len(data) {
				return nil, errors.New("unexpected end of data for INT32")
			}
			v := int32(binary.LittleEndian.Uint32(data[pos : pos+4]))
			values[i] = NewInt32(v)
			pos += 4
		case TypeInt64:
			if pos+8 > len(data) {
				return nil, errors.New("unexpected end of data for INT64")
			}
			v := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			values[i] = NewInt64(v)
			pos += 8
		case TypeBool:
			if pos+1 > len(data) {
				return nil, errors.New("unexpected end of data for BOOL")
			}
			values[i] = NewBool(data[pos] != 0)
			pos += 1
		case TypeTimestamp:
			if pos+8 > len(data) {
				return nil, errors.New("unexpected end of data for TIMESTAMP")
			}
			nanos := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			values[i] = NewTimestamp(time.Unix(0, nanos))
			pos += 8
		case TypeText:
			if pos+4 > len(data) {
				return nil, errors.New("unexpected end of data for TEXT length")
			}
			length := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+length > len(data) {
				return nil, errors.New("unexpected end of data for TEXT value")
			}
			values[i] = NewText(string(data[pos : pos+length]))
			pos += length
		default:
			return nil, errors.New("unknown type")
		}
	}

	return values, nil
}
