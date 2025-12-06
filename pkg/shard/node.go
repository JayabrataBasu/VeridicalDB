package shard

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// ShardNode represents a single shard in the distributed database.
// Each shard runs a full VeridicalDB engine and handles queries
// for its portion of the data.
type ShardNode struct {
	info     *ShardInfo
	dataDir  string
	pageSize int

	tm     *catalog.TableManager
	mtm    *catalog.MVCCTableManager
	txnMgr *txn.Manager

	listener net.Listener

	sessions   map[string]*sql.Session
	sessionsMu sync.Mutex

	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	shutdown bool
	mu       sync.Mutex
}

// NewShardNode creates a new shard node.
func NewShardNode(info *ShardInfo, dataDir string, pageSize int) (*ShardNode, error) {
	tm, err := catalog.NewTableManager(dataDir, pageSize)
	if err != nil {
		return nil, fmt.Errorf("create table manager: %w", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr)

	ctx, cancel := context.WithCancel(context.Background())

	return &ShardNode{
		info:     info,
		dataDir:  dataDir,
		pageSize: pageSize,
		tm:       tm,
		mtm:      mtm,
		txnMgr:   txnMgr,
		sessions: make(map[string]*sql.Session),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Start starts the shard node server.
func (n *ShardNode) Start() error {
	listener, err := net.Listen("tcp", n.info.Address())
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	n.listener = listener

	n.wg.Add(1)
	go n.acceptLoop()

	return nil
}

// acceptLoop accepts incoming connections.
func (n *ShardNode) acceptLoop() {
	defer n.wg.Done()

	for {
		conn, err := n.listener.Accept()
		if err != nil {
			n.mu.Lock()
			shutdown := n.shutdown
			n.mu.Unlock()
			if shutdown {
				return
			}
			continue
		}

		n.wg.Add(1)
		go n.handleConnection(conn)
	}
}

// handleConnection handles a single client connection.
func (n *ShardNode) handleConnection(conn net.Conn) {
	defer n.wg.Done()
	defer conn.Close()

	sessionID := fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), n.info.ID)
	session := n.getOrCreateSession(sessionID)
	defer n.removeSession(sessionID)

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		// Read query (newline-terminated)
		query, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				// Connection closed
			}
			return
		}

		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}

		// Execute query
		result, err := session.ExecuteSQL(query)
		if err != nil {
			response := fmt.Sprintf("ERROR: %v\n", err)
			conn.Write([]byte(response))
			continue
		}

		// Format response
		response := formatResult(result)
		conn.Write([]byte(response))
	}
}

// getOrCreateSession gets or creates a session for the given ID.
func (n *ShardNode) getOrCreateSession(sessionID string) *sql.Session {
	n.sessionsMu.Lock()
	defer n.sessionsMu.Unlock()

	if session, ok := n.sessions[sessionID]; ok {
		return session
	}

	// Use MVCC table manager for session
	session := sql.NewSession(n.mtm)
	n.sessions[sessionID] = session
	return session
}

// removeSession removes a session.
func (n *ShardNode) removeSession(sessionID string) {
	n.sessionsMu.Lock()
	defer n.sessionsMu.Unlock()
	delete(n.sessions, sessionID)
}

// formatResult formats a SQL result as a string.
func formatResult(result *sql.Result) string {
	if result == nil {
		return "OK\n"
	}

	var sb strings.Builder

	if result.Message != "" {
		sb.WriteString(result.Message)
		sb.WriteString("\n")
		return sb.String()
	}

	// Format column headers
	if len(result.Columns) > 0 {
		sb.WriteString(strings.Join(result.Columns, "\t"))
		sb.WriteString("\n")
	}

	// Format rows
	for _, row := range result.Rows {
		for i, val := range row {
			if i > 0 {
				sb.WriteString("\t")
			}
			sb.WriteString(formatValue(val))
		}
		sb.WriteString("\n")
	}

	if result.RowsAffected > 0 {
		sb.WriteString(fmt.Sprintf("(%d rows affected)\n", result.RowsAffected))
	} else if len(result.Rows) > 0 {
		sb.WriteString(fmt.Sprintf("(%d rows)\n", len(result.Rows)))
	}

	return sb.String()
}

// formatValue formats a single value.
func formatValue(val catalog.Value) string {
	if val.IsNull {
		return "NULL"
	}
	switch val.Type {
	case catalog.TypeInt32:
		return fmt.Sprintf("%d", val.Int32)
	case catalog.TypeInt64:
		return fmt.Sprintf("%d", val.Int64)
	case catalog.TypeText:
		return val.Text
	case catalog.TypeBool:
		if val.Bool {
			return "true"
		}
		return "false"
	case catalog.TypeTimestamp:
		return val.Timestamp.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Stop stops the shard node server.
func (n *ShardNode) Stop() error {
	n.mu.Lock()
	n.shutdown = true
	n.mu.Unlock()

	n.cancel()

	if n.listener != nil {
		n.listener.Close()
	}

	n.wg.Wait()
	return nil
}

// Info returns the shard info.
func (n *ShardNode) Info() *ShardInfo {
	return n.info
}

// TableManager returns the table manager.
func (n *ShardNode) TableManager() *catalog.TableManager {
	return n.tm
}
