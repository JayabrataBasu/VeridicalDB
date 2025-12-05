package txn

import (
	"encoding/binary"
	"errors"
)

// MVCC Header Encoding:
// [8 bytes: XMin (uint64)] [8 bytes: XMax (uint64)]
// Total: 16 bytes prefix on every tuple

var (
	ErrInvalidMVCCHeader = errors.New("invalid MVCC header: data too short")
)

// EncodeMVCCHeader encodes an MVCC header to bytes.
func EncodeMVCCHeader(header *MVCCHeader) []byte {
	buf := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(header.XMin))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(header.XMax))
	return buf
}

// DecodeMVCCHeader decodes an MVCC header from bytes.
func DecodeMVCCHeader(data []byte) (*MVCCHeader, error) {
	if len(data) < HeaderSize {
		return nil, ErrInvalidMVCCHeader
	}
	return &MVCCHeader{
		XMin: TxID(binary.LittleEndian.Uint64(data[0:8])),
		XMax: TxID(binary.LittleEndian.Uint64(data[8:16])),
	}, nil
}

// WrapWithMVCC prepends an MVCC header to tuple data.
func WrapWithMVCC(header *MVCCHeader, tupleData []byte) []byte {
	result := make([]byte, HeaderSize+len(tupleData))
	binary.LittleEndian.PutUint64(result[0:8], uint64(header.XMin))
	binary.LittleEndian.PutUint64(result[8:16], uint64(header.XMax))
	copy(result[HeaderSize:], tupleData)
	return result
}

// UnwrapMVCC extracts the MVCC header and tuple data from wrapped bytes.
func UnwrapMVCC(data []byte) (*MVCCHeader, []byte, error) {
	if len(data) < HeaderSize {
		return nil, nil, ErrInvalidMVCCHeader
	}
	header := &MVCCHeader{
		XMin: TxID(binary.LittleEndian.Uint64(data[0:8])),
		XMax: TxID(binary.LittleEndian.Uint64(data[8:16])),
	}
	return header, data[HeaderSize:], nil
}

// SetXMax updates the XMax field in an already-encoded MVCC tuple.
// This modifies the data in place.
func SetXMax(data []byte, xmax TxID) error {
	if len(data) < HeaderSize {
		return ErrInvalidMVCCHeader
	}
	binary.LittleEndian.PutUint64(data[8:16], uint64(xmax))
	return nil
}

// GetXMax reads the XMax field from encoded MVCC tuple data.
func GetXMax(data []byte) (TxID, error) {
	if len(data) < HeaderSize {
		return InvalidTxID, ErrInvalidMVCCHeader
	}
	return TxID(binary.LittleEndian.Uint64(data[8:16])), nil
}

// GetXMin reads the XMin field from encoded MVCC tuple data.
func GetXMin(data []byte) (TxID, error) {
	if len(data) < HeaderSize {
		return InvalidTxID, ErrInvalidMVCCHeader
	}
	return TxID(binary.LittleEndian.Uint64(data[0:8])), nil
}
