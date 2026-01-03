// Package net provides TCP server functionality for VeridicalDB.
// It allows multiple clients to connect and execute SQL concurrently.
package net

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Server is the TCP server for VeridicalDB.
type Server struct {
	listener net.Listener
	logger   *log.Logger
	mtm      *catalog.MVCCTableManager
	txnMgr   *txn.Manager
	lockMgr  *lock.Manager

	// Connection management
	connID  atomic.Uint64
	conns   map[uint64]*Connection
	connsMu sync.Mutex

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool
}

// ServerConfig holds configuration for the server.
type ServerConfig struct {
	Port    int
	Logger  *log.Logger
	MTM     *catalog.MVCCTableManager
	TxnMgr  *txn.Manager
	LockMgr *lock.Manager
}

// NewServer creates a new TCP server.
func NewServer(cfg ServerConfig) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		logger:  cfg.Logger,
		mtm:     cfg.MTM,
		txnMgr:  cfg.TxnMgr,
		lockMgr: cfg.LockMgr,
		conns:   make(map[uint64]*Connection),
		ctx:     ctx,
		cancel:  cancel,
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

	s.logger.Info("server started", "address", addr)

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

	// Close listener to stop accepting new connections
	if s.listener != nil {
		_ = s.listener.Close()
	}

	// Close all active connections
	s.connsMu.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.connsMu.Unlock()

	// Wait for all goroutines to finish
	s.wg.Wait()

	s.logger.Info("server stopped")
	return nil
}

// acceptLoop accepts new connections.
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

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a client connection.
func (s *Server) handleConnection(netConn net.Conn) {
	defer s.wg.Done()

	connID := s.connID.Add(1)
	conn := NewConnection(connID, netConn, s.mtm, s.txnMgr, s.lockMgr, s.logger)

	s.registerConn(conn)
	defer s.unregisterConn(connID)

	s.logger.Debug("client connected", "connID", connID, "remote", netConn.RemoteAddr())

	conn.Handle(s.ctx)

	s.logger.Debug("client disconnected", "connID", connID)
}

// registerConn adds a connection to the active set.
func (s *Server) registerConn(conn *Connection) {
	s.connsMu.Lock()
	s.conns[conn.id] = conn
	s.connsMu.Unlock()
}

// unregisterConn removes a connection from the active set.
func (s *Server) unregisterConn(connID uint64) {
	s.connsMu.Lock()
	delete(s.conns, connID)
	s.connsMu.Unlock()
}

// ActiveConnections returns the number of active connections.
func (s *Server) ActiveConnections() int {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	return len(s.conns)
}

// Connection represents a client connection.
type Connection struct {
	id      uint64
	conn    net.Conn
	session *sql.Session
	lockMgr *lock.Manager
	logger  *log.Logger
	closed  atomic.Bool
}

// NewConnection creates a new connection handler.
func NewConnection(id uint64, conn net.Conn, mtm *catalog.MVCCTableManager,
	txnMgr *txn.Manager, lockMgr *lock.Manager, logger *log.Logger) *Connection {

	session := sql.NewSession(mtm)
	if lockMgr != nil {
		session.SetLockManager(lockMgr)
	}

	return &Connection{
		id:      id,
		conn:    conn,
		session: session,
		lockMgr: lockMgr,
		logger:  logger,
	}
}

// Handle processes commands from the client.
func (c *Connection) Handle(ctx context.Context) {
	defer c.cleanup()

	// Send welcome message
	c.send("VeridicalDB v0.1.0 ready\n")

	reader := bufio.NewReader(c.conn)
	var buffer strings.Builder

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF && !c.closed.Load() {
				c.logger.Debug("read error", "connID", c.id, "error", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Accumulate input
		if buffer.Len() > 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(line)

		// Check if statement is complete
		input := buffer.String()
		if !strings.HasSuffix(input, ";") {
			continue
		}

		// Execute the statement
		response := c.execute(input)
		c.send(response)
		buffer.Reset()
	}
}

// execute processes a SQL statement and returns the response.
func (c *Connection) execute(input string) string {
	cmd := strings.TrimSuffix(strings.TrimSpace(input), ";")
	cmdUpper := strings.ToUpper(cmd)

	// Handle meta commands
	switch cmdUpper {
	case "QUIT", "EXIT", "\\Q":
		c.Close()
		return "Goodbye!\n"
	case "HELP", "\\H", "\\?":
		return c.helpText()
	case "STATUS", "\\S":
		return c.statusText()
	}

	// Execute SQL
	result, err := c.session.ExecuteSQL(input)
	if err != nil {
		return fmt.Sprintf("ERROR: %v\n", err)
	}

	return c.formatResult(result)
}

// formatResult formats a query result for the wire protocol.
func (c *Connection) formatResult(result *sql.Result) string {
	// Message-only result (DDL, DML)
	if result.Message != "" && result.Columns == nil {
		return result.Message + "\n"
	}

	// Query result
	var sb strings.Builder

	// Header
	if len(result.Columns) > 0 {
		sb.WriteString(strings.Join(result.Columns, "\t"))
		sb.WriteString("\n")
	}

	// Rows
	for _, row := range result.Rows {
		values := make([]string, len(row))
		for i, v := range row {
			if v.IsNull {
				values[i] = "NULL"
			} else {
				values[i] = v.String()
			}
		}
		sb.WriteString(strings.Join(values, "\t"))
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("(%d row(s))\n", len(result.Rows)))
	return sb.String()
}

// send writes a response to the client.
func (c *Connection) send(msg string) {
	if !c.closed.Load() {
		_, _ = c.conn.Write([]byte(msg))
	}
}

// Close closes the connection.
func (c *Connection) Close() {
	if c.closed.CompareAndSwap(false, true) {
		_ = c.conn.Close()
	}
}

// cleanup releases resources when connection ends.
func (c *Connection) cleanup() {
	// If in a transaction, abort it and release locks
	if c.session.InTransaction() {
		_, _ = c.session.ExecuteSQL("ROLLBACK;")
	}
	c.Close()
}

// helpText returns help information.
func (c *Connection) helpText() string {
	return `Commands:
  HELP;              Show this help
  STATUS;            Show connection status
  EXIT;              Disconnect

SQL:
  CREATE TABLE name (col type, ...);
  DROP TABLE name;
  INSERT INTO name VALUES (...);
  SELECT ... FROM name [WHERE ...];
  UPDATE name SET ... [WHERE ...];
  DELETE FROM name [WHERE ...];

Transactions:
  BEGIN;             Start transaction
  COMMIT;            Commit transaction
  ROLLBACK;          Rollback transaction
`
}

// statusText returns status information.
func (c *Connection) statusText() string {
	txStatus := "No"
	if c.session.InTransaction() {
		txStatus = "Yes"
	}
	return fmt.Sprintf("Connection ID: %d\nIn Transaction: %s\n", c.id, txStatus)
}
