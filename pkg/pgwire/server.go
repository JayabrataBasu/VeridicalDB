package pgwire

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
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
}

// ServerConfig holds configuration for the pgwire server.
type ServerConfig struct {
	Port          int
	Logger        *log.Logger
	MTM           *catalog.MVCCTableManager
	TxnMgr        *txn.Manager
	ServerVersion string
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

	s.logger.Info("pgwire server started", "address", addr)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
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

	if ok && int32(pid*7) == secret {
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

	// Session state
	session    *sql.Session
	parameters map[string]string
	txnStatus  byte

	// Prepared statements and portals (for extended query protocol)
	statements map[string]*PreparedStatement
	portals    map[string]*Portal

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
	MaxRows   int32
}

func newConn(id uint64, conn net.Conn, server *Server) *Conn {
	bufW := bufio.NewWriter(conn)
	return &Conn{
		id:         id,
		conn:       conn,
		server:     server,
		reader:     NewMessageReader(conn),
		writer:     NewMessageWriter(bufW),
		bufW:       bufW,
		parameters: make(map[string]string),
		txnStatus:  TxnStatusIdle,
		statements: make(map[string]*PreparedStatement),
		portals:    make(map[string]*Portal),
	}
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
		// Client wants SSL - we don't support it, send 'N'
		if _, err := c.conn.Write([]byte{'N'}); err != nil {
			return err
		}
		// Client should send another startup message
		return c.handleStartup()

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

	// Create SQL session
	c.session = sql.NewSession(c.server.mtm)

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
	buf.WriteInt32(int32(c.id))     // process ID
	buf.WriteInt32(int32(c.id * 7)) // secret key
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
	if _, err := c.sendResult(result, query, 0); err != nil {
		return err
	}

	if err := c.sendReadyForQuery(); err != nil {
		return err
	}

	return c.bufW.Flush()
}

func (c *Conn) sendResult(result *sql.Result, _query string, maxRows int32) (bool, error) {
	if result == nil {
		return false, c.sendCommandComplete("", 0)
	}

	// If there are columns, send RowDescription and DataRows
	if len(result.Columns) > 0 {
		if err := c.sendRowDescription(result.Columns); err != nil {
			return false, err
		}

		count := 0
		for _, row := range result.Rows {
			if maxRows > 0 && int32(count) >= maxRows {
				// If we reached the limit, send PortalSuspended instead of CommandComplete
				return true, c.writer.WriteMessage(MsgPortalSuspended, nil)
			}
			if err := c.sendDataRow(row); err != nil {
				return false, err
			}
			count++
		}

		return false, c.sendCommandComplete("SELECT", len(result.Rows))
	}

	// For commands without results
	tag := result.Message
	if tag == "" {
		tag = "OK"
	}
	return false, c.sendCommandComplete(tag, result.RowsAffected)
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
	switch {
	case command == "SELECT":
		tag = fmt.Sprintf("SELECT %d", rowCount)
	case command == "INSERT":
		tag = fmt.Sprintf("INSERT 0 %d", rowCount)
	case command == "UPDATE":
		tag = fmt.Sprintf("UPDATE %d", rowCount)
	case command == "DELETE":
		tag = fmt.Sprintf("DELETE %d", rowCount)
	case command == "CREATE TABLE":
		tag = "CREATE TABLE"
	case command == "DROP TABLE":
		tag = "DROP TABLE"
	case command == "CREATE INDEX":
		tag = "CREATE INDEX"
	case command == "DROP INDEX":
		tag = "DROP INDEX"
	case command == "BEGIN":
		tag = "BEGIN"
	case command == "COMMIT":
		tag = "COMMIT"
	case command == "ROLLBACK":
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
	offset := 0

	// Statement name
	name, n := ReadCString(payload[offset:])
	offset += n

	// Query string
	query, n := ReadCString(payload[offset:])
	offset += n

	// Number of parameter types
	numParams := ReadInt16(payload[offset:])
	offset += 2

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

	// Number of parameter format codes
	numFormats := ReadInt16(payload[offset:])
	offset += 2
	offset += int(numFormats) * 2 // Skip format codes (we assume text)

	// Number of parameter values
	numValues := ReadInt16(payload[offset:])
	offset += 2

	params := make([][]byte, numValues)
	for i := int16(0); i < numValues; i++ {
		length := ReadInt32(payload[offset:])
		offset += 4
		if length == -1 {
			params[i] = nil // NULL
		} else {
			params[i] = make([]byte, length)
			copy(params[i], payload[offset:offset+int(length)])
			offset += int(length)
		}
	}

	// Number of result format codes (skip)
	numResultFormats := ReadInt16(payload[offset:])
	offset += 2
	offset += int(numResultFormats) * 2

	c.portals[portalName] = &Portal{
		Name:      portalName,
		Statement: stmt,
		Params:    params,
	}

	return c.writer.WriteMessage(MsgBindComplete, nil)
}

func (c *Conn) handleDescribe(payload []byte) error {
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

		// Send NoData or RowDescription (we'll send NoData for simplicity)
		return c.writer.WriteMessage(MsgNoData, nil)

	case 'P': // Portal
		_, ok := c.portals[name]
		if !ok {
			return fmt.Errorf("portal %q not found", name)
		}
		// Send NoData (we don't know the result schema without executing)
		return c.writer.WriteMessage(MsgNoData, nil)

	default:
		return fmt.Errorf("unknown describe type: %c", descType)
	}
}

func (c *Conn) decodeParam(data []byte, oid int32) (catalog.Value, error) {
	if data == nil {
		return catalog.Null(catalog.TypeUnknown), nil
	}
	s := string(data)
	switch oid {
	case OIDInt4, OIDInt2:
		v, _ := strconv.ParseInt(s, 10, 32)
		return catalog.NewInt32(int32(v)), nil
	case OIDInt8:
		v, _ := strconv.ParseInt(s, 10, 64)
		return catalog.NewInt64(v), nil
	case OIDFloat4, OIDFloat8:
		v, _ := strconv.ParseFloat(s, 64)
		return catalog.NewFloat64(v), nil
	case OIDBool:
		return catalog.NewBool(s == "t" || s == "true" || s == "1"), nil
	case OIDText, OIDVarchar:
		return catalog.NewText(s), nil
	default:
		// Fallback to text
		return catalog.NewText(s), nil
	}
}

func (c *Conn) handleExecute(payload []byte) error {
	portalName, n := ReadCString(payload)
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
		val, err := c.decodeParam(p, oid)
		if err != nil {
			return err
		}
		params[i] = val
	}

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

	_, err = c.sendResult(result, portal.Statement.Query, maxRows)
	return err
}

func (c *Conn) handleSync() error {
	if err := c.sendReadyForQuery(); err != nil {
		return err
	}
	return c.bufW.Flush()
}

func (c *Conn) handleClose(payload []byte) error {
	closeType := payload[0]
	name, _ := ReadCString(payload[1:])

	switch closeType {
	case 'S': // Statement
		delete(c.statements, name)
	case 'P': // Portal
		delete(c.portals, name)
	}

	return c.writer.WriteMessage(MsgCloseComplete, nil)
}
