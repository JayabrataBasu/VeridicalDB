package pgwire

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	stdbinary "encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Server implements a PostgreSQL wire protocol server.
type Server struct {
	listener net.Listener
	logger   *log.Logger
	mtm      *catalog.MVCCTableManager
	txnMgr   *txn.Manager

	// Connection management
	connID  atomic.Uint64
	conns   map[uint64]*Conn
	connsMu sync.Mutex

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool

	// Server identification
	serverVersion string

	// TLS configuration
	tlsConfig *tls.Config

	// newSession produces a fully-wired session per connection (see
	// ServerConfig.NewSession).
	newSession func() *sql.Session
}

// ServerConfig holds configuration for the pgwire server.
type ServerConfig struct {
	Port   int
	Logger *log.Logger

	// NewSession, when set, is the source of per-connection sessions. This is
	// how callers hand the server a fully-wired session (indexes, triggers,
	// procedures, FTS, multi-database, ...). When nil the server falls back to
	// a bare sql.NewSession(MTM), which has none of those capabilities.
	NewSession func() *sql.Session

	MTM           *catalog.MVCCTableManager
	TxnMgr        *txn.Manager
	ServerVersion string
	TLSConfig     *tls.Config // TLS configuration (nil = TLS disabled)
}

// NewServer creates a new PostgreSQL wire protocol server.
func NewServer(cfg ServerConfig) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	serverVersion := cfg.ServerVersion
	if serverVersion == "" {
		serverVersion = "VeridicalDB 0.1.0"
	}

	return &Server{
		logger:        cfg.Logger,
		mtm:           cfg.MTM,
		txnMgr:        cfg.TxnMgr,
		conns:         make(map[uint64]*Conn),
		ctx:           ctx,
		cancel:        cancel,
		serverVersion: serverVersion,
		tlsConfig:     cfg.TLSConfig,
		newSession:    cfg.NewSession,
	}
}

// Start starts the server listening on the specified port.
func (s *Server) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener
	s.running.Store(true)

	tlsStatus := "disabled"
	if s.tlsConfig != nil {
		tlsStatus = "enabled"
	}
	s.logger.Info("pgwire server started", "address", addr, "tls", tlsStatus)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// TLSEnabled returns whether TLS is enabled for this server.
func (s *Server) TLSEnabled() bool {
	return s.tlsConfig != nil
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	if !s.running.Load() {
		return nil
	}
	s.running.Store(false)
	s.cancel()

	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.connsMu.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.connsMu.Unlock()

	s.wg.Wait()
	s.logger.Info("pgwire server stopped")
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for s.running.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.running.Load() {
				s.logger.Error("accept error", "error", err)
			}
			continue
		}

		id := s.connID.Add(1)
		pgConn := newConn(id, conn, s)

		s.connsMu.Lock()
		s.conns[id] = pgConn
		s.connsMu.Unlock()

		s.wg.Add(1)
		go s.handleConnection(pgConn)
	}
}

func (s *Server) cancelConnection(pid uint64, secret int32) {
	s.connsMu.Lock()
	conn, ok := s.conns[pid]
	s.connsMu.Unlock()

	if ok && conn.cancelSecret == secret {
		s.logger.Info("cancelling connection", "id", pid)
		// In a real system, we would signal the session to cancel the current query.
		// For now, we'll just close the connection to stop any ongoing work.
		conn.Close()
	}
}

func (s *Server) handleConnection(c *Conn) {
	defer s.wg.Done()
	defer func() {
		s.connsMu.Lock()
		delete(s.conns, c.id)
		s.connsMu.Unlock()
		c.Close()
	}()

	s.logger.Debug("new connection", "id", c.id, "remote", c.conn.RemoteAddr())

	if err := c.handleStartup(); err != nil {
		s.logger.Error("startup failed", "id", c.id, "error", err)
		return
	}

	c.run()
}

// Conn represents a single client connection.
type Conn struct {
	id     uint64
	conn   net.Conn
	server *Server
	reader *MessageReader
	writer *MessageWriter
	bufW   *bufio.Writer

	// BackendKeyData fields for CancelRequest.
	cancelSecret int32

	// Session state
	session    *sql.Session
	parameters map[string]string
	txnStatus  byte

	// Prepared statements and portals (for extended query protocol)
	statements map[string]*PreparedStatement
	portals    map[string]*Portal

	// TLS state
	tlsActive bool

	closed atomic.Bool
}

// PreparedStatement holds a parsed statement.
type PreparedStatement struct {
	Name      string
	Query     string
	ParamOIDs []int32
}

// Portal holds a bound statement ready for execution.
type Portal struct {
	Name      string
	Statement *PreparedStatement
	Params    [][]byte
	Formats   []int16
	MaxRows   int32
	Result    *sql.Result
	RowOffset int
}

func newConn(id uint64, conn net.Conn, server *Server) *Conn {
	bufW := bufio.NewWriter(conn)
	return &Conn{
		id:           id,
		conn:         conn,
		server:       server,
		reader:       NewMessageReader(conn),
		writer:       NewMessageWriter(bufW),
		bufW:         bufW,
		cancelSecret: generateCancelSecret(id),
		parameters:   make(map[string]string),
		txnStatus:    TxnStatusIdle,
		statements:   make(map[string]*PreparedStatement),
		portals:      make(map[string]*Portal),
	}
}

func generateCancelSecret(connID uint64) int32 {
	var b [4]byte
	if _, err := cryptorand.Read(b[:]); err == nil {
		secret := int32(stdbinary.BigEndian.Uint32(b[:]))
		if secret != 0 {
			return secret
		}
	}

	// Deterministic non-zero fallback if entropy source is unavailable.
	fallback := int32((connID*1103515245 + 12345) & 0x7fffffff)
	if fallback == 0 {
		fallback = 1
	}
	return fallback
}

// Close closes the connection.
func (c *Conn) Close() {
	if c.closed.Swap(true) {
		return
	}
	_ = c.conn.Close()
}

// handleStartup processes the initial startup handshake.
func (c *Conn) handleStartup() error {
	// Read startup message
	length, payload, err := c.reader.ReadStartup()
	if err != nil {
		return fmt.Errorf("read startup: %w", err)
	}

	if length < 8 {
		return fmt.Errorf("startup message too short")
	}

	// First 4 bytes of payload are the protocol version or special code
	code := ReadInt32(payload[0:4])

	switch code {
	case SSLRequestCode:
		// Client wants SSL
		return c.handleSSLRequest()

	case CancelRequestCode:
		// Cancel request
		if len(payload) < 12 {
			return fmt.Errorf("cancel request too short")
		}
		pid := ReadInt32(payload[4:8])
		secret := ReadInt32(payload[8:12])

		c.server.cancelConnection(uint64(pid), secret)
		return io.EOF // Close this connection after processing cancel

	case ProtocolVersionNumber:
		// Normal startup
		return c.processStartup(payload[4:])

	default:
		return fmt.Errorf("unsupported protocol version: %d", code)
	}
}

// handleSSLRequest handles the PostgreSQL SSLRequest handshake.
// Per PostgreSQL protocol, server responds with 'S' if SSL is supported and will proceed
// with TLS handshake, or 'N' if SSL is not supported and client should continue without TLS.
func (c *Conn) handleSSLRequest() error {
	if c.server.tlsConfig == nil {
		// TLS not configured - send 'N' and continue without TLS
		if _, err := c.conn.Write([]byte{'N'}); err != nil {
			return fmt.Errorf("failed to send SSL rejection: %w", err)
		}
		c.server.logger.Debug("SSL not available, continuing plaintext", "id", c.id)
		// Client should send another startup message
		return c.handleStartup()
	}

	// TLS is configured - send 'S' to indicate we support SSL
	if _, err := c.conn.Write([]byte{'S'}); err != nil {
		return fmt.Errorf("failed to send SSL acceptance: %w", err)
	}

	// Perform TLS handshake
	tlsConn := tls.Server(c.conn, c.server.tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		c.server.logger.Error("TLS handshake failed", "id", c.id, "error", err)
		return fmt.Errorf("TLS handshake failed: %w", err)
	}

	// Log TLS connection details
	state := tlsConn.ConnectionState()
	c.server.logger.Debug("TLS handshake completed",
		"id", c.id,
		"version", tlsVersionString(state.Version),
		"cipher", tls.CipherSuiteName(state.CipherSuite),
		"client_cert", len(state.PeerCertificates) > 0,
	)

	// Replace the connection with the TLS connection
	c.conn = tlsConn
	c.reader = NewMessageReader(tlsConn)
	c.bufW = bufio.NewWriter(tlsConn)
	c.writer = NewMessageWriter(c.bufW)
	c.tlsActive = true

	// Client should send another startup message over the encrypted connection
	return c.handleStartup()
}

// IsTLSActive returns whether the connection is using TLS.
func (c *Conn) IsTLSActive() bool {
	return c.tlsActive
}

// tlsVersionString returns a human-readable TLS version string.
func tlsVersionString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("unknown (0x%04x)", version)
	}
}

func (c *Conn) processStartup(params []byte) error {
	// Parse startup parameters (key=value pairs, null-terminated)
	for len(params) > 0 {
		key, n := ReadCString(params)
		if key == "" || n >= len(params) {
			break
		}
		params = params[n:]

		value, n := ReadCString(params)
		params = params[n:]

		c.parameters[key] = value
	}

	// Create SQL session — fully wired if the caller supplied a factory.
	if c.server.newSession != nil {
		c.session = c.server.newSession()
	} else {
		c.session = sql.NewSession(c.server.mtm)
	}

	// Send AuthenticationOK
	buf := NewBuffer()
	buf.WriteInt32(AuthOK)
	if err := c.writer.WriteMessage(MsgAuthentication, buf.Bytes()); err != nil {
		return err
	}

	// Send ParameterStatus messages
	serverParams := map[string]string{
		"server_version":              c.server.serverVersion,
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}

	for k, v := range serverParams {
		buf.Reset()
		buf.WriteString(k)
		buf.WriteString(v)
		if err := c.writer.WriteMessage(MsgParameterStatus, buf.Bytes()); err != nil {
			return err
		}
	}

	// Send BackendKeyData (process ID and secret key for cancellation)
	buf.Reset()
	buf.WriteInt32(int32(c.id))    // process ID
	buf.WriteInt32(c.cancelSecret) // secret key
	if err := c.writer.WriteMessage(MsgBackendKeyData, buf.Bytes()); err != nil {
		return err
	}

	// Send ReadyForQuery
	if err := c.sendReadyForQuery(); err != nil {
		return err
	}

	return c.bufW.Flush()
}

func (c *Conn) sendReadyForQuery() error {
	buf := NewBuffer()
	if err := buf.WriteByte(c.txnStatus); err != nil {
		return err
	}
	return c.writer.WriteMessage(MsgReadyForQuery, buf.Bytes())
}

func (c *Conn) run() {
	for !c.closed.Load() {
		msgType, payload, err := c.reader.ReadMessage()
		if err != nil {
			if err != io.EOF && !c.closed.Load() {
				c.server.logger.Error("read message error", "id", c.id, "error", err)
			}
			return
		}

		if err := c.handleMessage(msgType, payload); err != nil {
			c.server.logger.Error("handle message error", "id", c.id, "type", string(msgType), "error", err)
			if err2 := c.sendError("ERROR", "XX000", err.Error()); err2 != nil {
				c.server.logger.Error("sendError failed", "id", c.id, "error", err2)
			}
			if err2 := c.sendReadyForQuery(); err2 != nil {
				c.server.logger.Error("sendReadyForQuery failed", "id", c.id, "error", err2)
			}
			if err2 := c.bufW.Flush(); err2 != nil {
				c.server.logger.Error("bufW.Flush failed", "id", c.id, "error", err2)
			}
		}
	}
}

func (c *Conn) handleMessage(msgType byte, payload []byte) error {
	switch msgType {
	case MsgQuery:
		return c.handleQuery(payload)
	case MsgParse:
		return c.handleParse(payload)
	case MsgBind:
		return c.handleBind(payload)
	case MsgDescribe:
		return c.handleDescribe(payload)
	case MsgExecute:
		return c.handleExecute(payload)
	case MsgSync:
		return c.handleSync()
	case MsgClose:
		return c.handleClose(payload)
	case MsgTerminate:
		c.Close()
		return nil
	case MsgFlush:
		return c.bufW.Flush()
	default:
		return fmt.Errorf("unknown message type: %c (0x%x)", msgType, msgType)
	}
}

// handleQuery implements the simple query protocol.
func (c *Conn) handleQuery(payload []byte) error {
	query, _ := ReadCString(payload)

	if query == "" {
		if err := c.writer.WriteMessage(MsgEmptyQueryResponse, nil); err != nil {
			return err
		}
		if err := c.sendReadyForQuery(); err != nil {
			return err
		}
		return c.bufW.Flush()
	}

	// Execute the query
	result, err := c.session.ExecuteSQL(query)
	if err != nil {
		if err2 := c.sendError("ERROR", "42000", err.Error()); err2 != nil {
			c.server.logger.Error("sendError failed", "id", c.id, "error", err2)
		}
		if err2 := c.sendReadyForQuery(); err2 != nil {
			c.server.logger.Error("sendReadyForQuery failed", "id", c.id, "error", err2)
		}
		return c.bufW.Flush()
	}

	// Send results
	if _, _, err := c.sendResult(result, query, 0, 0); err != nil {
		return err
	}

	if err := c.sendReadyForQuery(); err != nil {
		return err
	}

	return c.bufW.Flush()
}

func (c *Conn) sendResult(result *sql.Result, query string, maxRows int32, rowOffset int) (bool, int, error) {
	if result == nil {
		return false, 0, c.sendCommandComplete("", 0)
	}

	// If there are columns, send RowDescription and DataRows
	if len(result.Columns) > 0 {
		if err := c.sendRowDescription(result.Columns); err != nil {
			return false, 0, err
		}

		count := 0
		for i := rowOffset; i < len(result.Rows); i++ {
			row := result.Rows[i]
			if maxRows > 0 && int32(count) >= maxRows {
				// If we reached the limit, send PortalSuspended instead of CommandComplete
				return true, count, c.writer.WriteMessage(MsgPortalSuspended, nil)
			}
			if err := c.sendDataRow(row); err != nil {
				return false, count, err
			}
			count++
		}

		return false, count, c.sendCommandComplete("SELECT", len(result.Rows))
	}

	// For commands without results
	tag := result.Message
	if tag == "" {
		tag = "OK"
	}
	return false, 0, c.sendCommandComplete(tag, result.RowsAffected)
}

func (c *Conn) sendRowDescription(columns []string) error {
	buf := NewBuffer()
	buf.WriteInt16(int16(len(columns)))

	for _, col := range columns {
		buf.WriteString(col)    // column name
		buf.WriteInt32(0)       // table OID (0 = not from a table)
		buf.WriteInt16(0)       // column attribute number
		buf.WriteInt32(OIDText) // data type OID
		buf.WriteInt16(-1)      // data type size (-1 = variable)
		buf.WriteInt32(-1)      // type modifier
		buf.WriteInt16(0)       // format code (0 = text)
	}

	return c.writer.WriteMessage(MsgRowDescription, buf.Bytes())
}

func (c *Conn) sendDataRow(row []catalog.Value) error {
	buf := NewBuffer()
	buf.WriteInt16(int16(len(row)))

	for _, val := range row {
		if val.IsNull {
			buf.WriteInt32(-1) // NULL
		} else {
			text := val.String()
			buf.WriteInt32(int32(len(text)))
			buf.WriteBytes([]byte(text))
		}
	}

	return c.writer.WriteMessage(MsgDataRow, buf.Bytes())
}

func (c *Conn) sendCommandComplete(command string, rowCount int) error {
	var tag string
	switch command {
	case "SELECT":
		tag = fmt.Sprintf("SELECT %d", rowCount)
	case "INSERT":
		tag = fmt.Sprintf("INSERT 0 %d", rowCount)
	case "UPDATE":
		tag = fmt.Sprintf("UPDATE %d", rowCount)
	case "DELETE":
		tag = fmt.Sprintf("DELETE %d", rowCount)
	case "CREATE TABLE":
		tag = "CREATE TABLE"
	case "DROP TABLE":
		tag = "DROP TABLE"
	case "CREATE INDEX":
		tag = "CREATE INDEX"
	case "DROP INDEX":
		tag = "DROP INDEX"
	case "BEGIN":
		tag = "BEGIN"
	case "COMMIT":
		tag = "COMMIT"
	case "ROLLBACK":
		tag = "ROLLBACK"
	default:
		tag = command
	}

	buf := NewBuffer()
	buf.WriteString(tag)
	return c.writer.WriteMessage(MsgCommandComplete, buf.Bytes())
}

func (c *Conn) sendError(severity, code, message string) error {
	buf := NewBuffer()
	if err := buf.WriteByte(FieldSeverity); err != nil {
		return err
	}
	buf.WriteString(severity)
	if err := buf.WriteByte(FieldSQLStateCode); err != nil {
		return err
	}
	buf.WriteString(code)
	if err := buf.WriteByte(FieldMessage); err != nil {
		return err
	}
	buf.WriteString(message)
	if err := buf.WriteByte(0); err != nil { // terminator
		return err
	}

	return c.writer.WriteMessage(MsgErrorResponse, buf.Bytes())
}

// Extended Query Protocol handlers

func (c *Conn) handleParse(payload []byte) error {
	if len(payload) < 1 {
		return fmt.Errorf("parse message payload too short")
	}

	offset := 0

	// Statement name
	name, n := ReadCString(payload[offset:])
	offset += n

	// Query string
	query, n := ReadCString(payload[offset:])
	offset += n

	if offset+2 > len(payload) {
		return fmt.Errorf("invalid parse payload: missing parameter count")
	}

	// Number of parameter types
	numParams := ReadInt16(payload[offset:])
	offset += 2
	if numParams < 0 {
		return fmt.Errorf("invalid parse payload: negative parameter count")
	}
	if offset+int(numParams)*4 > len(payload) {
		return fmt.Errorf("invalid parse payload: truncated parameter OIDs")
	}

	paramOIDs := make([]int32, numParams)
	for i := int16(0); i < numParams; i++ {
		paramOIDs[i] = ReadInt32(payload[offset:])
		offset += 4
	}

	// Store prepared statement
	c.statements[name] = &PreparedStatement{
		Name:      name,
		Query:     query,
		ParamOIDs: paramOIDs,
	}

	return c.writer.WriteMessage(MsgParseComplete, nil)
}

func (c *Conn) handleBind(payload []byte) error {
	if len(payload) < 1 {
		return fmt.Errorf("bind message payload too short")
	}

	offset := 0

	// Portal name
	portalName, n := ReadCString(payload[offset:])
	offset += n

	// Statement name
	stmtName, n := ReadCString(payload[offset:])
	offset += n

	stmt, ok := c.statements[stmtName]
	if !ok {
		return fmt.Errorf("prepared statement %q not found", stmtName)
	}

	if offset+2 > len(payload) {
		return fmt.Errorf("invalid bind payload: missing parameter format count")
	}

	// Number of parameter format codes
	numFormats := ReadInt16(payload[offset:])
	offset += 2
	if numFormats < 0 {
		return fmt.Errorf("invalid bind payload: negative parameter format count")
	}
	if offset+int(numFormats)*2 > len(payload) {
		return fmt.Errorf("invalid bind payload: truncated parameter formats")
	}

	formats := make([]int16, numFormats)
	for i := int16(0); i < numFormats; i++ {
		formats[i] = ReadInt16(payload[offset:])
		offset += 2
	}

	if offset+2 > len(payload) {
		return fmt.Errorf("invalid bind payload: missing parameter value count")
	}

	// Number of parameter values
	numValues := ReadInt16(payload[offset:])
	offset += 2
	if numValues < 0 {
		return fmt.Errorf("invalid bind payload: negative parameter value count")
	}

	if len(stmt.ParamOIDs) > 0 && int(numValues) != len(stmt.ParamOIDs) {
		return fmt.Errorf("bind parameter count mismatch: statement expects %d, got %d", len(stmt.ParamOIDs), numValues)
	}

	params := make([][]byte, numValues)
	for i := int16(0); i < numValues; i++ {
		if offset+4 > len(payload) {
			return fmt.Errorf("invalid bind payload: truncated parameter length")
		}
		length := ReadInt32(payload[offset:])
		offset += 4
		if length == -1 {
			params[i] = nil // NULL
		} else {
			if length < 0 {
				return fmt.Errorf("invalid bind payload: negative parameter length")
			}
			if offset+int(length) > len(payload) {
				return fmt.Errorf("invalid bind payload: truncated parameter value")
			}
			params[i] = make([]byte, length)
			copy(params[i], payload[offset:offset+int(length)])
			offset += int(length)
		}
	}

	if offset+2 > len(payload) {
		return fmt.Errorf("invalid bind payload: missing result format count")
	}

	// Number of result format codes (currently ignored by server response path)
	numResultFormats := ReadInt16(payload[offset:])
	offset += 2
	if numResultFormats < 0 {
		return fmt.Errorf("invalid bind payload: negative result format count")
	}
	if offset+int(numResultFormats)*2 > len(payload) {
		return fmt.Errorf("invalid bind payload: truncated result formats")
	}
	// Consume result formats for completeness.
	offset += int(numResultFormats) * 2
	if offset != len(payload) {
		return fmt.Errorf("invalid bind payload: trailing bytes")
	}

	// Resolve actual parameter format per value:
	// - 0 formats => all text (0)
	// - 1 format => applies to all values
	// - N formats => one per value
	resolvedFormats := make([]int16, numValues)
	switch {
	case len(formats) == 0:
		for i := range resolvedFormats {
			resolvedFormats[i] = 0
		}
	case len(formats) == 1:
		for i := range resolvedFormats {
			resolvedFormats[i] = formats[0]
		}
	case len(formats) == int(numValues):
		copy(resolvedFormats, formats)
	default:
		return fmt.Errorf("bind parameter format count mismatch: got %d formats for %d values", len(formats), numValues)
	}

	for _, f := range resolvedFormats {
		if f != 0 && f != 1 {
			return fmt.Errorf("unsupported parameter format code: %d", f)
		}
	}

	c.portals[portalName] = &Portal{
		Name:      portalName,
		Statement: stmt,
		Params:    params,
		Formats:   resolvedFormats,
	}

	return c.writer.WriteMessage(MsgBindComplete, nil)
}

func (c *Conn) handleDescribe(payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("describe message payload too short")
	}

	descType := payload[0]
	name, _ := ReadCString(payload[1:])

	switch descType {
	case 'S': // Statement
		stmt, ok := c.statements[name]
		if !ok {
			return fmt.Errorf("statement %q not found", name)
		}

		// Send ParameterDescription
		buf := NewBuffer()
		buf.WriteInt16(int16(len(stmt.ParamOIDs)))
		for _, oid := range stmt.ParamOIDs {
			buf.WriteInt32(oid)
		}
		if err := c.writer.WriteMessage(MsgParameterDesc, buf.Bytes()); err != nil {
			return err
		}

		return c.sendDescribeResult(stmt.Query)

	case 'P': // Portal
		_, ok := c.portals[name]
		if !ok {
			return fmt.Errorf("portal %q not found", name)
		}
		return c.sendDescribeResult(c.portals[name].Statement.Query)

	default:
		return fmt.Errorf("unknown describe type: %c", descType)
	}
}

func (c *Conn) sendDescribeResult(query string) error {
	cols, hasRows := describeResultColumns(query)
	if !hasRows {
		return c.writer.WriteMessage(MsgNoData, nil)
	}
	if len(cols) == 0 {
		cols = []string{"?column?"}
	}
	return c.sendRowDescription(cols)
}

func describeResultColumns(query string) ([]string, bool) {
	parser := sql.NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, false
	}

	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, false
	}

	cols := make([]string, 0, len(sel.Columns))
	for _, col := range sel.Columns {
		switch {
		case col.Alias != "":
			cols = append(cols, col.Alias)
		case col.Star:
			cols = append(cols, "*")
		case col.Name != "":
			cols = append(cols, col.Name)
		case col.Aggregate != nil && col.Aggregate.Function != "":
			cols = append(cols, col.Aggregate.Function)
		default:
			cols = append(cols, "?column?")
		}
	}

	return cols, true
}

func (c *Conn) decodeParam(data []byte, oid int32, format int16) (catalog.Value, error) {
	if data == nil {
		return catalog.Null(catalog.TypeUnknown), nil
	}

	if format == 1 {
		return catalog.Value{}, fmt.Errorf("binary parameter format is not supported")
	}

	s := string(data)
	switch oid {
	case OIDInt4, OIDInt2:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("invalid int value %q: %w", s, err)
		}
		return catalog.NewInt32(int32(v)), nil
	case OIDInt8:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("invalid bigint value %q: %w", s, err)
		}
		return catalog.NewInt64(v), nil
	case OIDFloat4, OIDFloat8:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("invalid float value %q: %w", s, err)
		}
		return catalog.NewFloat64(v), nil
	case OIDBool:
		if s != "t" && s != "true" && s != "1" && s != "f" && s != "false" && s != "0" {
			return catalog.Value{}, fmt.Errorf("invalid bool value %q", s)
		}
		return catalog.NewBool(s == "t" || s == "true" || s == "1"), nil
	case OIDText, OIDVarchar:
		return catalog.NewText(s), nil
	default:
		// Fallback to text
		return catalog.NewText(s), nil
	}
}

func (c *Conn) handleExecute(payload []byte) error {
	if len(payload) < 1 {
		return fmt.Errorf("execute message payload too short")
	}

	portalName, n := ReadCString(payload)
	if n+4 > len(payload) {
		return fmt.Errorf("invalid execute payload: missing maxRows")
	}
	maxRows := ReadInt32(payload[n:])

	portal, ok := c.portals[portalName]
	if !ok {
		return fmt.Errorf("portal %q not found", portalName)
	}

	// Decode parameters
	params := make([]catalog.Value, len(portal.Params))
	for i, p := range portal.Params {
		oid := int32(OIDUnknown)
		if i < len(portal.Statement.ParamOIDs) {
			oid = portal.Statement.ParamOIDs[i]
		}
		format := int16(0)
		if i < len(portal.Formats) {
			format = portal.Formats[i]
		}
		val, err := c.decodeParam(p, oid, format)
		if err != nil {
			return c.sendError("ERROR", "22P02", err.Error())
		}
		params[i] = val
	}

	// Execute and cache portal result only once; subsequent Execute calls continue from RowOffset.
	if portal.Result == nil {
		// Parse the query
		parser := sql.NewParser(portal.Statement.Query)
		stmt, err := parser.Parse()
		if err != nil {
			return c.sendError("ERROR", "42601", err.Error())
		}

		// Substitute parameters into the AST
		newStmt, err := sql.SubstituteParams(stmt, params)
		if err != nil {
			return c.sendError("ERROR", "42000", err.Error())
		}

		// Execute the substituted statement
		result, err := c.session.Execute(newStmt)
		if err != nil {
			return c.sendError("ERROR", "42000", err.Error())
		}
		portal.Result = result
		portal.RowOffset = 0
	}

	suspended, emitted, err := c.sendResult(portal.Result, portal.Statement.Query, maxRows, portal.RowOffset)
	if err != nil {
		return err
	}
	portal.RowOffset += emitted

	if !suspended {
		// Completed full portal consumption; reset cached result so a subsequent Execute starts fresh.
		portal.Result = nil
		portal.RowOffset = 0
	}

	return nil
}

func (c *Conn) handleSync() error {
	if err := c.sendReadyForQuery(); err != nil {
		return err
	}
	return c.bufW.Flush()
}

func (c *Conn) handleClose(payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("close message payload too short")
	}

	closeType := payload[0]
	name, _ := ReadCString(payload[1:])

	switch closeType {
	case 'S': // Statement
		delete(c.statements, name)
	case 'P': // Portal
		delete(c.portals, name)
	default:
		return fmt.Errorf("unknown close type: %c", closeType)
	}

	return c.writer.WriteMessage(MsgCloseComplete, nil)
}
