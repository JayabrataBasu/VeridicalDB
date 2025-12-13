package shard

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
)

// ShardClient represents a connection to a shard node.
type ShardClient struct {
	info   *ShardInfo
	conn   net.Conn
	mu     sync.Mutex
	closed bool
}

// NewShardClient creates a new client connection to a shard.
func NewShardClient(info *ShardInfo) (*ShardClient, error) {
	conn, err := net.Dial("tcp", info.Address())
	if err != nil {
		return nil, fmt.Errorf("connect to shard %d: %w", info.ID, err)
	}

	return &ShardClient{
		info: info,
		conn: conn,
	}, nil
}

// Execute sends a SQL command to the shard and returns the response.
func (c *ShardClient) Execute(query string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return "", fmt.Errorf("connection closed")
	}

	// Write query with length prefix
	queryBytes := []byte(query + "\n")
	if _, err := c.conn.Write(queryBytes); err != nil {
		return "", fmt.Errorf("write query: %w", err)
	}

	// Read response
	buf := make([]byte, 4096)
	n, err := c.conn.Read(buf)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("read response: %w", err)
	}

	return string(buf[:n]), nil
}

// Close closes the connection to the shard.
func (c *ShardClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// Coordinator routes queries to appropriate shards.
type Coordinator struct {
	config  *ShardConfig
	clients map[ShardID]*ShardClient
	mu      sync.RWMutex

	// parser for analyzing queries
	parser *sql.Parser

	// activeTxns tracks which shard each transaction is bound to
	activeTxns map[string]ShardID // sessionID -> shardID
	txnMu      sync.Mutex
}

// NewCoordinator creates a new coordinator.
func NewCoordinator(config *ShardConfig) *Coordinator {
	return &Coordinator{
		config:     config,
		clients:    make(map[ShardID]*ShardClient),
		activeTxns: make(map[string]ShardID),
	}
}

// Connect establishes connections to all shards.
func (c *Coordinator) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, shard := range c.config.GetAllActiveShards() {
		client, err := NewShardClient(shard)
		if err != nil {
			// Close any already-opened connections
			for _, cl := range c.clients {
				cl.Close()
			}
			c.clients = make(map[ShardID]*ShardClient)
			return fmt.Errorf("connect to shard %d: %w", shard.ID, err)
		}
		c.clients[shard.ID] = client
	}

	return nil
}

// Close closes all shard connections.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	for id, client := range c.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close shard %d: %w", id, err))
		}
	}
	c.clients = make(map[ShardID]*ShardClient)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing shards: %v", errs)
	}
	return nil
}

// RouteResult represents the result of routing a query.
type RouteResult struct {
	// TargetShard is set for single-shard queries
	TargetShard *ShardID

	// ScatterGather is true if query needs to go to all shards
	ScatterGather bool

	// Error is set if routing failed
	Error error
}

// Route determines which shard(s) a query should go to.
func (c *Coordinator) Route(query string, sessionID string) RouteResult {
	// Check if session has an active transaction bound to a shard
	c.txnMu.Lock()
	if boundShard, ok := c.activeTxns[sessionID]; ok {
		c.txnMu.Unlock()
		return RouteResult{TargetShard: &boundShard}
	}
	c.txnMu.Unlock()

	// Parse the query to determine routing
	parser := sql.NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return RouteResult{Error: fmt.Errorf("parse query: %w", err)}
	}

	return c.routeStatement(stmt, sessionID)
}

// routeStatement determines routing based on statement type.
func (c *Coordinator) routeStatement(stmt sql.Statement, sessionID string) RouteResult {
	switch s := stmt.(type) {
	case *sql.CreateTableStmt, *sql.DropTableStmt:
		// DDL goes to all shards
		return RouteResult{ScatterGather: true}

	case *sql.InsertStmt:
		return c.routeInsert(s)

	case *sql.SelectStmt:
		return c.routeSelect(s)

	case *sql.UpdateStmt:
		return c.routeUpdate(s)

	case *sql.DeleteStmt:
		return c.routeDelete(s)

	case *sql.BeginStmt:
		// BEGIN doesn't route yet - wait for first data statement
		return RouteResult{ScatterGather: false, TargetShard: nil}

	case *sql.CommitStmt, *sql.RollbackStmt:
		// Route to the shard bound to this session's transaction
		c.txnMu.Lock()
		if boundShard, ok := c.activeTxns[sessionID]; ok {
			delete(c.activeTxns, sessionID)
			c.txnMu.Unlock()
			return RouteResult{TargetShard: &boundShard}
		}
		c.txnMu.Unlock()
		return RouteResult{Error: fmt.Errorf("no active transaction")}

	default:
		return RouteResult{Error: fmt.Errorf("unsupported statement type: %T", stmt)}
	}
}

// routeInsert routes an INSERT statement.
func (c *Coordinator) routeInsert(stmt *sql.InsertStmt) RouteResult {
	// For multi-row INSERT, we use the first row for routing
	// In a full implementation, each row could go to different shards
	if len(stmt.ValuesList) == 0 {
		return RouteResult{Error: fmt.Errorf("INSERT has no values")}
	}
	firstRow := stmt.ValuesList[0]

	// Find shard key in values
	keyCol := c.config.ShardKeyColumn
	keyIdx := -1

	// If columns are specified, find the shard key column
	if len(stmt.Columns) > 0 {
		for i, col := range stmt.Columns {
			if col == keyCol {
				keyIdx = i
				break
			}
		}
	} else {
		// Assume first column is shard key (common pattern)
		keyIdx = 0
	}

	if keyIdx < 0 || keyIdx >= len(firstRow) {
		return RouteResult{Error: fmt.Errorf("shard key column '%s' not found in INSERT", keyCol)}
	}

	// Extract the key value from the first row
	keyExpr := firstRow[keyIdx]
	keyVal := extractLiteralValue(keyExpr)
	if keyVal == nil {
		return RouteResult{Error: fmt.Errorf("shard key must be a literal value")}
	}

	// Compute shard
	shardID, err := c.config.ComputeShardID(keyVal)
	if err != nil {
		return RouteResult{Error: err}
	}

	return RouteResult{TargetShard: &shardID}
}

// routeSelect routes a SELECT statement.
func (c *Coordinator) routeSelect(stmt *sql.SelectStmt) RouteResult {
	// Check if WHERE clause contains shard key equality
	if stmt.Where == nil {
		// No WHERE - scatter to all shards
		return RouteResult{ScatterGather: true}
	}

	keyCol := c.config.ShardKeyColumn
	keyVal := extractShardKeyFromWhere(stmt.Where, keyCol)
	if keyVal == nil {
		// Shard key not in WHERE - scatter to all shards
		return RouteResult{ScatterGather: true}
	}

	// Compute shard
	shardID, err := c.config.ComputeShardID(keyVal)
	if err != nil {
		return RouteResult{Error: err}
	}

	return RouteResult{TargetShard: &shardID}
}

// routeUpdate routes an UPDATE statement.
func (c *Coordinator) routeUpdate(stmt *sql.UpdateStmt) RouteResult {
	if stmt.Where == nil {
		// No WHERE - scatter to all shards
		return RouteResult{ScatterGather: true}
	}

	keyCol := c.config.ShardKeyColumn
	keyVal := extractShardKeyFromWhere(stmt.Where, keyCol)
	if keyVal == nil {
		return RouteResult{ScatterGather: true}
	}

	shardID, err := c.config.ComputeShardID(keyVal)
	if err != nil {
		return RouteResult{Error: err}
	}

	return RouteResult{TargetShard: &shardID}
}

// routeDelete routes a DELETE statement.
func (c *Coordinator) routeDelete(stmt *sql.DeleteStmt) RouteResult {
	if stmt.Where == nil {
		return RouteResult{ScatterGather: true}
	}

	keyCol := c.config.ShardKeyColumn
	keyVal := extractShardKeyFromWhere(stmt.Where, keyCol)
	if keyVal == nil {
		return RouteResult{ScatterGather: true}
	}

	shardID, err := c.config.ComputeShardID(keyVal)
	if err != nil {
		return RouteResult{Error: err}
	}

	return RouteResult{TargetShard: &shardID}
}

// extractLiteralValue extracts a literal value from an expression.
func extractLiteralValue(expr sql.Expression) interface{} {
	switch e := expr.(type) {
	case *sql.LiteralExpr:
		return getLiteralData(e.Value)
	default:
		return nil
	}
}

// getLiteralData extracts the underlying data from a catalog.Value.
func getLiteralData(v interface{}) interface{} {
	// The Value is a catalog.Value, extract the actual data
	return v
}

// extractShardKeyFromWhere extracts the shard key value from a WHERE clause.
func extractShardKeyFromWhere(where sql.Expression, keyCol string) interface{} {
	switch e := where.(type) {
	case *sql.BinaryExpr:
		// Check if this is keyCol = value
		if e.Op == sql.TOKEN_EQ {
			if col, ok := e.Left.(*sql.ColumnRef); ok {
				if col.Name == keyCol {
					return extractLiteralValue(e.Right)
				}
			}
			if col, ok := e.Right.(*sql.ColumnRef); ok {
				if col.Name == keyCol {
					return extractLiteralValue(e.Left)
				}
			}
		}
		// Check AND clauses recursively
		if e.Op == sql.TOKEN_AND {
			if val := extractShardKeyFromWhere(e.Left, keyCol); val != nil {
				return val
			}
			return extractShardKeyFromWhere(e.Right, keyCol)
		}
	}
	return nil
}

// BindTransaction binds a session's transaction to a shard.
func (c *Coordinator) BindTransaction(sessionID string, shardID ShardID) {
	c.txnMu.Lock()
	defer c.txnMu.Unlock()
	c.activeTxns[sessionID] = shardID
}

// UnbindTransaction removes a session's transaction binding.
func (c *Coordinator) UnbindTransaction(sessionID string) {
	c.txnMu.Lock()
	defer c.txnMu.Unlock()
	delete(c.activeTxns, sessionID)
}

// Execute executes a query, routing to appropriate shard(s).
func (c *Coordinator) Execute(ctx context.Context, query string, sessionID string) ([]string, error) {
	route := c.Route(query, sessionID)

	if route.Error != nil {
		return nil, route.Error
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if route.TargetShard != nil {
		// Single shard query
		client, ok := c.clients[*route.TargetShard]
		if !ok {
			return nil, fmt.Errorf("no connection to shard %d", *route.TargetShard)
		}

		result, err := client.Execute(query)
		if err != nil {
			return nil, err
		}
		return []string{result}, nil
	}

	if route.ScatterGather {
		// Scatter to all shards and gather results
		return c.scatterGather(ctx, query)
	}

	return nil, fmt.Errorf("unable to route query")
}

// scatterGather sends query to all shards and gathers results.
func (c *Coordinator) scatterGather(ctx context.Context, query string) ([]string, error) {
	var wg sync.WaitGroup
	results := make([]string, len(c.clients))
	errors := make([]error, len(c.clients))

	i := 0
	for id, client := range c.clients {
		wg.Add(1)
		go func(idx int, shardID ShardID, cl *ShardClient) {
			defer wg.Done()
			result, err := cl.Execute(query)
			results[idx] = result
			errors[idx] = err
		}(i, id, client)
		i++
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
	}

	return results, nil
}
