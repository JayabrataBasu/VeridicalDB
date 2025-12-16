// Package pgwire implements the PostgreSQL wire protocol (version 3.0).
// This allows standard PostgreSQL clients (psql, JDBC, etc.) to connect.
package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Protocol version 3.0
const (
	ProtocolVersionNumber = 196608 // 3.0 = 3 << 16
	SSLRequestCode        = 80877103
	CancelRequestCode     = 80877102
)

// Frontend message types (client -> server)
const (
	MsgBind         byte = 'B'
	MsgClose        byte = 'C'
	MsgCopyData     byte = 'd'
	MsgCopyDone     byte = 'c'
	MsgCopyFail     byte = 'f'
	MsgDescribe     byte = 'D'
	MsgExecute      byte = 'E'
	MsgFlush        byte = 'H'
	MsgFunctionCall byte = 'F'
	MsgParse        byte = 'P'
	MsgPassword     byte = 'p'
	MsgQuery        byte = 'Q'
	MsgSync         byte = 'S'
	MsgTerminate    byte = 'X'
)

// Backend message types (server -> client)
const (
	MsgAuthentication     byte = 'R'
	MsgBackendKeyData     byte = 'K'
	MsgBindComplete       byte = '2'
	MsgCloseComplete      byte = '3'
	MsgCommandComplete    byte = 'C'
	MsgCopyInResponse     byte = 'G'
	MsgCopyOutResponse    byte = 'H'
	MsgCopyBothResponse   byte = 'W'
	MsgDataRow            byte = 'D'
	MsgEmptyQueryResponse byte = 'I'
	MsgErrorResponse      byte = 'E'
	MsgNoData             byte = 'n'
	MsgNoticeResponse     byte = 'N'
	MsgNotificationResp   byte = 'A'
	MsgParameterDesc      byte = 't'
	MsgParameterStatus    byte = 'S'
	MsgParseComplete      byte = '1'
	MsgPortalSuspended    byte = 's'
	MsgReadyForQuery      byte = 'Z'
	MsgRowDescription     byte = 'T'
)

// Authentication types
const (
	AuthOK                = 0
	AuthKerberosV5        = 2
	AuthCleartextPassword = 3
	AuthMD5Password       = 5
	AuthSCMCredential     = 6
	AuthGSS               = 7
	AuthGSSContinue       = 8
	AuthSSPI              = 9
	AuthSASL              = 10
	AuthSASLContinue      = 11
	AuthSASLFinal         = 12
)

// Transaction status indicators
const (
	TxnStatusIdle   byte = 'I' // Not in transaction
	TxnStatusInTxn  byte = 'T' // In transaction
	TxnStatusFailed byte = 'E' // In failed transaction
)

// Error field types for ErrorResponse
const (
	FieldSeverity         byte = 'S'
	FieldSeverityNonLocal byte = 'V'
	FieldSQLStateCode     byte = 'C'
	FieldMessage          byte = 'M'
	FieldDetail           byte = 'D'
	FieldHint             byte = 'H'
	FieldPosition         byte = 'P'
	FieldInternalPosition byte = 'p'
	FieldInternalQuery    byte = 'q'
	FieldWhere            byte = 'W'
	FieldSchemaName       byte = 's'
	FieldTableName        byte = 't'
	FieldColumnName       byte = 'c'
	FieldDataTypeName     byte = 'd'
	FieldConstraintName   byte = 'n'
	FieldFile             byte = 'F'
	FieldLine             byte = 'L'
	FieldRoutine          byte = 'R'
)

// PostgreSQL OIDs for common types
const (
	OIDUnknown   = 0
	OIDBool      = 16
	OIDInt2      = 21
	OIDInt4      = 23
	OIDInt8      = 20
	OIDFloat4    = 700
	OIDFloat8    = 701
	OIDText      = 25
	OIDVarchar   = 1043
	OIDTimestamp = 1114
	OIDDate      = 1082
	OIDNumeric   = 1700
)

// MessageReader reads PostgreSQL protocol messages from a connection.
type MessageReader struct {
	r io.Reader
}

// NewMessageReader creates a new message reader.
func NewMessageReader(r io.Reader) *MessageReader {
	return &MessageReader{r: r}
}

// ReadStartup reads the startup message (no type byte, just length + payload).
func (mr *MessageReader) ReadStartup() (int32, []byte, error) {
	// Read 4-byte length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(mr.r, lenBuf); err != nil {
		return 0, nil, err
	}
	length := int32(binary.BigEndian.Uint32(lenBuf))

	if length < 4 || length > 10000 {
		return 0, nil, fmt.Errorf("invalid startup message length: %d", length)
	}

	// Read payload (length includes the 4 bytes already read)
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(mr.r, payload); err != nil {
		return 0, nil, err
	}

	return length, payload, nil
}

// ReadMessage reads a frontend message (type byte + length + payload).
func (mr *MessageReader) ReadMessage() (byte, []byte, error) {
	// Read type byte
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(mr.r, typeBuf); err != nil {
		return 0, nil, err
	}
	msgType := typeBuf[0]

	// Read 4-byte length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(mr.r, lenBuf); err != nil {
		return 0, nil, err
	}
	length := int32(binary.BigEndian.Uint32(lenBuf))

	if length < 4 {
		return 0, nil, fmt.Errorf("invalid message length: %d", length)
	}

	// Read payload (length includes the 4 length bytes but not the type byte)
	payloadLen := length - 4
	if payloadLen > 1<<20 { // 1MB limit
		return 0, nil, fmt.Errorf("message too large: %d bytes", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(mr.r, payload); err != nil {
			return 0, nil, err
		}
	}

	return msgType, payload, nil
}

// MessageWriter writes PostgreSQL protocol messages to a connection.
type MessageWriter struct {
	w io.Writer
}

// NewMessageWriter creates a new message writer.
func NewMessageWriter(w io.Writer) *MessageWriter {
	return &MessageWriter{w: w}
}

// WriteMessage writes a backend message.
func (mw *MessageWriter) WriteMessage(msgType byte, payload []byte) error {
	// Type byte + length (4 bytes) + payload
	length := int32(4 + len(payload))

	buf := make([]byte, 1+4+len(payload))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(length))
	copy(buf[5:], payload)

	_, err := mw.w.Write(buf)
	return err
}

// WriteRaw writes raw bytes without framing.
func (mw *MessageWriter) WriteRaw(data []byte) error {
	_, err := mw.w.Write(data)
	return err
}

// Buffer helps build message payloads.
type Buffer struct {
	data []byte
}

// NewBuffer creates a new buffer.
func NewBuffer() *Buffer {
	return &Buffer{data: make([]byte, 0, 256)}
}

// WriteInt32 appends a 32-bit integer.
func (b *Buffer) WriteInt32(v int32) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(v))
	b.data = append(b.data, buf...)
}

// WriteInt16 appends a 16-bit integer.
func (b *Buffer) WriteInt16(v int16) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(v))
	b.data = append(b.data, buf...)
}

// WriteByte appends a single byte.
func (b *Buffer) WriteByte(v byte) error {
	b.data = append(b.data, v)
	return nil
}

// WriteString appends a null-terminated string.
func (b *Buffer) WriteString(s string) {
	b.data = append(b.data, []byte(s)...)
	b.data = append(b.data, 0)
}

// WriteBytes appends raw bytes.
func (b *Buffer) WriteBytes(data []byte) {
	b.data = append(b.data, data...)
}

// Bytes returns the buffer contents.
func (b *Buffer) Bytes() []byte {
	return b.data
}

// Reset clears the buffer.
func (b *Buffer) Reset() {
	b.data = b.data[:0]
}

// ReadInt32 reads a 32-bit integer from a byte slice.
func ReadInt32(data []byte) int32 {
	return int32(binary.BigEndian.Uint32(data))
}

// ReadInt16 reads a 16-bit integer from a byte slice.
func ReadInt16(data []byte) int16 {
	return int16(binary.BigEndian.Uint16(data))
}

// ReadCString reads a null-terminated string from a byte slice.
// Returns the string and the number of bytes consumed (including null).
func ReadCString(data []byte) (string, int) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), i + 1
		}
	}
	return string(data), len(data)
}
