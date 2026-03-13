package shard

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type testShardServer struct {
	listener           net.Listener
	shardID            int
	delay              time.Duration
	closeAfterResponse bool

	mu      sync.Mutex
	queries []string
	accepts int
	wg      sync.WaitGroup
}

func startTestShardServer(t *testing.T, shardID int) *testShardServer {
	t.Helper()
	return startTestShardServerWithOptions(t, shardID, 0, false)
}

func startTestShardServerAtAddress(t *testing.T, shardID int, addr string) *testShardServer {
	t.Helper()
	return startTestShardServerAtAddressWithOptions(t, shardID, addr, 0, false)
}

func startTestShardServerWithDelay(t *testing.T, shardID int, delay time.Duration) *testShardServer {
	t.Helper()
	return startTestShardServerWithOptions(t, shardID, delay, false)
}

func startTestShardServerClosingAfterResponse(t *testing.T, shardID int) *testShardServer {
	t.Helper()
	return startTestShardServerWithOptions(t, shardID, 0, true)
}

func startTestShardServerWithOptions(t *testing.T, shardID int, delay time.Duration, closeAfterResponse bool) *testShardServer {
	t.Helper()
	return startTestShardServerAtAddressWithOptions(t, shardID, "127.0.0.1:0", delay, closeAfterResponse)
}

func startTestShardServerAtAddressWithOptions(t *testing.T, shardID int, addr string, delay time.Duration, closeAfterResponse bool) *testShardServer {
	t.Helper()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	srv := &testShardServer{listener: listener, shardID: shardID, delay: delay, closeAfterResponse: closeAfterResponse}
	srv.wg.Add(1)
	go func() {
		defer srv.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			srv.mu.Lock()
			srv.accepts++
			srv.mu.Unlock()
			srv.wg.Add(1)
			go func(c net.Conn) {
				defer srv.wg.Done()
				defer func() {
					if err := c.Close(); err != nil {
						t.Errorf("close test shard connection: %v", err)
					}
				}()

				reader := bufio.NewReader(c)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					query := strings.TrimSpace(line)
					srv.mu.Lock()
					srv.queries = append(srv.queries, query)
					srv.mu.Unlock()

					if srv.delay > 0 {
						time.Sleep(srv.delay)
					}

					_, _ = fmt.Fprintf(c, "shard=%d query=%s\n", srv.shardID, query)
					if srv.closeAfterResponse {
						return
					}
				}
			}(conn)
		}
	}()

	return srv
}

func p95Duration(samples []time.Duration) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	ordered := make([]time.Duration, len(samples))
	copy(ordered, samples)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	idx := (95*len(ordered)+99)/100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(ordered) {
		idx = len(ordered) - 1
	}
	return ordered[idx]
}

func (s *testShardServer) hostPort() (string, int) {
	addr := s.listener.Addr().String()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", 0
	}
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

func (s *testShardServer) close() {
	_ = s.listener.Close()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

func (s *testShardServer) recordedQueries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.queries))
	copy(out, s.queries)
	return out
}

func (s *testShardServer) acceptedConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepts
}

func TestCoordinatorExecuteCrossShardScatterGatherSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	results, err := coord.Execute(context.Background(), "SELECT * FROM users;", "session-smoke")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 shard results, got %d", len(results))
	}

	joined := strings.Join(results, " ")
	if !strings.Contains(joined, "shard=0 query=SELECT * FROM users;") {
		t.Fatalf("missing shard 0 result: %v", results)
	}
	if !strings.Contains(joined, "shard=1 query=SELECT * FROM users;") {
		t.Fatalf("missing shard 1 result: %v", results)
	}
}

func TestCoordinatorExecuteCrossShardReconnectsStaleReadConnectionsSmoke(t *testing.T) {
	shard0 := startTestShardServerClosingAfterResponse(t, 0)
	shard1 := startTestShardServerClosingAfterResponse(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	if _, err := coord.Execute(context.Background(), "SELECT * FROM users;", "session-read-retry-1"); err != nil {
		t.Fatalf("first Execute failed: %v", err)
	}

	results, err := coord.Execute(context.Background(), "SELECT * FROM users;", "session-read-retry-2")
	if err != nil {
		t.Fatalf("expected second SELECT to reconnect and succeed, got %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results after reconnect, got %d", len(results))
	}
	if got := len(shard0.recordedQueries()); got != 2 {
		t.Fatalf("expected shard 0 to receive query twice, got %d", got)
	}
	if got := len(shard1.recordedQueries()); got != 2 {
		t.Fatalf("expected shard 1 to receive query twice, got %d", got)
	}
	if shard0.acceptedConnections() < 2 || shard1.acceptedConnections() < 2 {
		t.Fatalf("expected both shards to accept a reconnect; got shard0=%d shard1=%d", shard0.acceptedConnections(), shard1.acceptedConnections())
	}
}

func TestCoordinatorExecuteBoundTransactionCommitSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	coord.BindTransaction("session-txn", ShardID(1))

	results, err := coord.Execute(context.Background(), "COMMIT;", "session-txn")
	if err != nil {
		t.Fatalf("Execute COMMIT failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 shard result for bound COMMIT, got %d", len(results))
	}
	if !strings.Contains(results[0], "shard=1 query=COMMIT;") {
		t.Fatalf("expected COMMIT to route to shard 1, got %v", results)
	}

	if got := shard0.recordedQueries(); len(got) != 0 {
		t.Fatalf("expected shard 0 to receive no queries, got %v", got)
	}
	if got := shard1.recordedQueries(); len(got) != 1 || got[0] != "COMMIT;" {
		t.Fatalf("expected shard 1 to receive COMMIT only, got %v", got)
	}

	coord.txnMu.Lock()
	_, stillBound := coord.activeTxns["session-txn"]
	coord.txnMu.Unlock()
	if stillBound {
		t.Fatal("expected transaction binding to be cleared after COMMIT")
	}
}

func TestCoordinatorExecuteBoundTransactionReconnectsClosedClientSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	coord.mu.RLock()
	failingClient := coord.clients[ShardID(1)]
	coord.mu.RUnlock()
	if failingClient == nil {
		t.Fatal("expected shard 1 client to exist")
	}
	if err := failingClient.Close(); err != nil {
		t.Fatalf("failed to close shard client: %v", err)
	}

	coord.BindTransaction("session-reconnect", ShardID(1))

	results, err := coord.Execute(context.Background(), "COMMIT;", "session-reconnect")
	if err != nil {
		t.Fatalf("expected reconnecting COMMIT to succeed, got %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 shard result after reconnect, got %d", len(results))
	}
	if !strings.Contains(results[0], "shard=1 query=COMMIT;") {
		t.Fatalf("expected COMMIT to reach shard 1 after reconnect, got %v", results)
	}
}

func TestCoordinatorExecuteCrossShardReconnectFailureSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	coord.mu.RLock()
	failingClient := coord.clients[ShardID(1)]
	coord.mu.RUnlock()
	if failingClient == nil {
		t.Fatal("expected shard 1 client to exist")
	}
	if err := failingClient.Close(); err != nil {
		t.Fatalf("failed to close shard client: %v", err)
	}
	shard1.close()

	_, err := coord.Execute(context.Background(), "SELECT * FROM users;", "session-failure")
	if err == nil {
		t.Fatal("expected scatter/gather error when reconnecting a closed shard client fails")
	}
	if !strings.Contains(err.Error(), "shard 1") {
		t.Fatalf("expected error to identify shard 1, got %v", err)
	}
}

func TestCoordinatorExecuteSingleShardWriteDoesNotRetryStaleConnectionSmoke(t *testing.T) {
	shard0 := startTestShardServerClosingAfterResponse(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	query := "INSERT INTO users VALUES (1, 'alice');"
	if _, err := coord.Execute(context.Background(), query, "session-write-1"); err != nil {
		t.Fatalf("first INSERT failed: %v", err)
	}

	targetShard, err := config.ComputeShardID(1)
	if err != nil {
		t.Fatalf("ComputeShardID failed: %v", err)
	}
	targetServer := shard0
	if targetShard == ShardID(1) {
		targetServer = shard1
	}

	_, err = coord.Execute(context.Background(), query, "session-write-2")
	if err == nil {
		t.Fatal("expected stale write connection to fail without retry")
	}
	if got := len(targetServer.recordedQueries()); got != 1 {
		t.Fatalf("expected write query to be recorded once without retry, got %d", got)
	}
}

func TestCoordinatorMetricsTrackReconnectsAndTimeouts(t *testing.T) {
	shard0 := startTestShardServerClosingAfterResponse(t, 0)
	shard1 := startTestShardServerWithDelay(t, 1, 200*time.Millisecond)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	if _, err := coord.Execute(context.Background(), "SELECT * FROM users;", "metrics-first"); err != nil {
		t.Fatalf("first read failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = coord.Execute(ctx, "SELECT * FROM users;", "metrics-timeout")

	snapshot := coord.MetricsSnapshot()
	if snapshot.QueriesTotal != 2 {
		t.Fatalf("expected 2 total queries, got %d", snapshot.QueriesTotal)
	}
	if snapshot.ScatterGatherQueriesTotal != 2 {
		t.Fatalf("expected 2 scatter-gather queries, got %d", snapshot.ScatterGatherQueriesTotal)
	}
	if snapshot.ReadRetriesTotal == 0 {
		t.Fatal("expected read retry metrics to increment")
	}
	if snapshot.ReconnectAttemptsTotal == 0 || snapshot.ReconnectSuccessesTotal == 0 {
		t.Fatalf("expected reconnect metrics to increment, got attempts=%d successes=%d", snapshot.ReconnectAttemptsTotal, snapshot.ReconnectSuccessesTotal)
	}
	if snapshot.TimeoutErrorsTotal == 0 {
		t.Fatal("expected timeout error metrics to increment")
	}
	if !strings.Contains(coord.PrometheusMetrics(), "veridicaldb_shard_read_retries_total") {
		t.Fatal("expected shard Prometheus metrics output")
	}
}

func TestCoordinatorExecuteCrossShardTimeoutSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServerWithDelay(t, 1, 200*time.Millisecond)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := coord.Execute(ctx, "SELECT * FROM users;", "session-timeout")
	if err == nil {
		t.Fatal("expected timeout error for delayed shard response")
	}
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

// TestCoordinatorBoundTransactionInsertThenCommitSmoke verifies the full
// bound-transaction lifecycle: INSERT is routed only to the bound shard,
// followed by COMMIT on the same shard, and the binding is cleared afterwards.
func TestCoordinatorBoundTransactionInsertThenCommitSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	const session = "session-insert-commit"
	coord.BindTransaction(session, ShardID(1))

	// INSERT should route to the bound shard (1), not shard 0, regardless of key routing.
	insertQuery := "INSERT INTO orders VALUES (99, 'widget');"
	results, err := coord.Execute(context.Background(), insertQuery, session)
	if err != nil {
		t.Fatalf("INSERT on bound shard failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for single-shard INSERT, got %d", len(results))
	}
	if !strings.Contains(results[0], "shard=1") {
		t.Fatalf("expected INSERT to reach shard 1, got %v", results)
	}
	if q0 := shard0.recordedQueries(); len(q0) != 0 {
		t.Fatalf("expected shard 0 to receive no queries during bound txn, got %v", q0)
	}

	// COMMIT must route to the same shard and clear the binding.
	results, err = coord.Execute(context.Background(), "COMMIT;", session)
	if err != nil {
		t.Fatalf("COMMIT on bound shard failed: %v", err)
	}
	if !strings.Contains(results[0], "shard=1 query=COMMIT;") {
		t.Fatalf("expected COMMIT on shard 1, got %v", results)
	}
	if q1 := shard1.recordedQueries(); len(q1) != 2 || q1[0] != insertQuery || q1[1] != "COMMIT;" {
		t.Fatalf("expected shard 1 to receive INSERT then COMMIT, got %v", q1)
	}

	coord.txnMu.Lock()
	_, stillBound := coord.activeTxns[session]
	coord.txnMu.Unlock()
	if stillBound {
		t.Fatal("expected binding to be cleared after COMMIT")
	}
}

// TestCoordinatorBoundTransactionRollbackClearsBindingSmoke verifies that
// ROLLBACK on a bound transaction routes to the bound shard and clears the binding.
func TestCoordinatorBoundTransactionRollbackClearsBindingSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	const session = "session-rollback"
	coord.BindTransaction(session, ShardID(0))

	results, err := coord.Execute(context.Background(), "ROLLBACK;", session)
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
	if len(results) != 1 || !strings.Contains(results[0], "shard=0 query=ROLLBACK;") {
		t.Fatalf("expected ROLLBACK to reach shard 0, got %v", results)
	}
	if q1 := shard1.recordedQueries(); len(q1) != 0 {
		t.Fatalf("expected shard 1 to receive nothing, got %v", q1)
	}

	coord.txnMu.Lock()
	_, stillBound := coord.activeTxns[session]
	coord.txnMu.Unlock()
	if stillBound {
		t.Fatal("expected binding to be cleared after ROLLBACK")
	}
}

// TestCoordinatorDDLScatterGatherFanoutSmoke verifies that DDL statements
// (CREATE TABLE) fan out to all shards via scatter-gather.
func TestCoordinatorDDLScatterGatherFanoutSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	ddl := "CREATE TABLE inventory (id INT, qty INT);"
	results, err := coord.Execute(context.Background(), ddl, "session-ddl")
	if err != nil {
		t.Fatalf("DDL Execute failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 shard results for DDL fan-out, got %d", len(results))
	}

	joined := strings.Join(results, " ")
	if !strings.Contains(joined, "shard=0") || !strings.Contains(joined, "shard=1") {
		t.Fatalf("expected both shards to receive DDL, got %v", results)
	}
	if q0 := shard0.recordedQueries(); len(q0) != 1 || q0[0] != ddl {
		t.Fatalf("expected shard 0 to receive exactly the DDL query, got %v", q0)
	}
	if q1 := shard1.recordedQueries(); len(q1) != 1 || q1[0] != ddl {
		t.Fatalf("expected shard 1 to receive exactly the DDL query, got %v", q1)
	}
}

// TestCoordinatorThreeShardScatterGatherAggregationSmoke verifies that a
// scatter-gather query across three shards aggregates all three results.
func TestCoordinatorThreeShardScatterGatherAggregationSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	shard2 := startTestShardServer(t, 2)
	defer shard0.close()
	defer shard1.close()
	defer shard2.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()
	host2, port2 := shard2.hostPort()

	config := NewShardConfig(3, "id")
	if err := config.CreateUniformShards(
		[]string{host0, host1, host2},
		[]int{port0, port1, port2},
	); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	results, err := coord.Execute(context.Background(), "SELECT * FROM products;", "session-3shard")
	if err != nil {
		t.Fatalf("three-shard scatter failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 shard results, got %d: %v", len(results), results)
	}

	joined := strings.Join(results, " ")
	for _, id := range []int{0, 1, 2} {
		if !strings.Contains(joined, fmt.Sprintf("shard=%d", id)) {
			t.Fatalf("missing result from shard %d: %v", id, results)
		}
	}
}

// TestCoordinatorWriteScatterFailsOnDeadShardSmoke verifies that a write
// scatter-gather (UPDATE without shard key) fails when one shard is unavailable,
// and that writes are not retried on the failing shard.
func TestCoordinatorWriteScatterFailsOnDeadShardSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	// Kill shard 0 — close the client and stop the listener so reconnects fail too.
	coord.mu.RLock()
	deadClient := coord.clients[ShardID(0)]
	coord.mu.RUnlock()
	if deadClient == nil {
		t.Fatal("expected shard 0 client to exist")
	}
	_ = deadClient.Close()
	shard0.close()

	// UPDATE without a WHERE clause scatters to all shards.
	_, err := coord.Execute(context.Background(), "UPDATE inventory SET qty=0;", "session-write-scatter-fail")
	if err == nil {
		t.Fatal("expected scatter write to fail when shard 0 is unavailable")
	}
	if !strings.Contains(err.Error(), "shard 0") {
		t.Fatalf("expected error to identify dead shard 0, got: %v", err)
	}

	// Writes must not be silently retried — shard 0 should have received nothing.
	if q0 := shard0.recordedQueries(); len(q0) != 0 {
		t.Fatalf("expected no queries on dead shard 0 (no write retry), got %v", q0)
	}
}

// TestCoordinatorBoundTransactionDMLExcludesOtherShardsSmoke verifies that
// DML that would normally scatter (UPDATE without shard-key WHERE) is instead
// routed exclusively to the bound shard while a transaction is active.
func TestCoordinatorBoundTransactionDMLExcludesOtherShardsSmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	const session = "session-bound-dml-exclusive"
	coord.BindTransaction(session, ShardID(1))

	// Without a bound transaction, UPDATE with no WHERE would scatter to all shards.
	// With a bound transaction it must go only to shard 1.
	updateQuery := "UPDATE inventory SET qty=5;"
	results, err := coord.Execute(context.Background(), updateQuery, session)
	if err != nil {
		t.Fatalf("UPDATE on bound shard failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result for bound DML, got %d (scatter should be suppressed)", len(results))
	}
	if !strings.Contains(results[0], "shard=1") {
		t.Fatalf("expected UPDATE to reach shard 1 only, got %v", results)
	}
	if q0 := shard0.recordedQueries(); len(q0) != 0 {
		t.Fatalf("expected shard 0 to receive no DML during bound transaction, got %v", q0)
	}

	// COMMIT and verify cleanup.
	if _, err := coord.Execute(context.Background(), "COMMIT;", session); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
	coord.txnMu.Lock()
	_, stillBound := coord.activeTxns[session]
	coord.txnMu.Unlock()
	if stillBound {
		t.Fatal("expected binding to be cleared after COMMIT")
	}
}

func TestCoordinatorNetworkPartitionRecoverySmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()
	partitionAddr := fmt.Sprintf("%s:%d", host1, port1)

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	if _, err := coord.Execute(context.Background(), "SELECT * FROM users;", "partition-warmup"); err != nil {
		t.Fatalf("warmup select failed: %v", err)
	}

	coord.mu.RLock()
	stale := coord.clients[ShardID(1)]
	coord.mu.RUnlock()
	if stale == nil {
		t.Fatal("expected shard 1 client")
	}
	_ = stale.Close()
	shard1.close()

	if _, err := coord.Execute(context.Background(), "SELECT * FROM users;", "partition-down"); err == nil {
		t.Fatal("expected error while shard 1 is partitioned")
	}

	recovered := startTestShardServerAtAddress(t, 1, partitionAddr)
	defer recovered.close()
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("expected reconnect after partition heal, got %v", err)
	}

	results, err := coord.Execute(context.Background(), "SELECT * FROM users;", "partition-healed")
	if err != nil {
		t.Fatalf("expected recovery after partition heal, got %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results after heal, got %d", len(results))
	}
	if recovered.acceptedConnections() == 0 {
		t.Fatal("expected recovered shard to accept at least one reconnect")
	}
}

func TestCoordinatorDelayedParticipantRecoverySmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	delayed := startTestShardServerWithDelay(t, 1, 350*time.Millisecond)
	defer shard0.close()
	defer delayed.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := delayed.hostPort()
	delayedAddr := fmt.Sprintf("%s:%d", host1, port1)

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if _, err := coord.Execute(ctx, "SELECT * FROM users;", "delay-timeout"); err == nil {
		t.Fatal("expected timeout with delayed participant")
	}

	delayed.close()
	fast := startTestShardServerAtAddress(t, 1, delayedAddr)
	defer fast.close()
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("expected reconnect after delayed shard replacement, got %v", err)
	}

	start := time.Now()
	results, err := coord.Execute(context.Background(), "SELECT * FROM users;", "delay-recovered")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected recovery once delay is removed, got %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results after delay recovery, got %d", len(results))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("expected recovered execution to complete promptly, took %s", elapsed)
	}
}

func TestCoordinatorRestartRecoverySmoke(t *testing.T) {
	shard0 := startTestShardServer(t, 0)
	shard1 := startTestShardServer(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord1 := NewCoordinator(config)
	if err := coord1.Connect(context.Background()); err != nil {
		t.Fatalf("coord1 connect failed: %v", err)
	}

	if _, err := coord1.Execute(context.Background(), "SELECT * FROM users;", "restart-before"); err != nil {
		t.Fatalf("coord1 execute failed: %v", err)
	}
	if err := coord1.Close(); err != nil {
		t.Fatalf("coord1 close failed: %v", err)
	}

	coord2 := NewCoordinator(config)
	if err := coord2.Connect(context.Background()); err != nil {
		t.Fatalf("coord2 connect failed after restart: %v", err)
	}
	defer func() { _ = coord2.Close() }()

	results, err := coord2.Execute(context.Background(), "SELECT * FROM users;", "restart-after")
	if err != nil {
		t.Fatalf("coord2 execute failed after restart: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 shard results after coordinator restart, got %d", len(results))
	}
}

func TestCoordinatorRecoveryLatencyP95AfterReconnectSmoke(t *testing.T) {
	shard0 := startTestShardServerClosingAfterResponse(t, 0)
	shard1 := startTestShardServerClosingAfterResponse(t, 1)
	defer shard0.close()
	defer shard1.close()

	host0, port0 := shard0.hostPort()
	host1, port1 := shard1.hostPort()

	config := NewShardConfig(2, "id")
	if err := config.CreateUniformShards([]string{host0, host1}, []int{port0, port1}); err != nil {
		t.Fatalf("CreateUniformShards failed: %v", err)
	}

	coord := NewCoordinator(config)
	if err := coord.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer func() { _ = coord.Close() }()

	samples := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		start := time.Now()
		if _, err := coord.Execute(context.Background(), "SELECT * FROM users;", fmt.Sprintf("recovery-%d", i)); err != nil {
			t.Fatalf("recovery iteration %d failed: %v", i, err)
		}
		samples = append(samples, time.Since(start))
	}

	p95 := p95Duration(samples)
	if p95 > 2*time.Second {
		t.Fatalf("expected recovery p95 <= 2s, got %s", p95)
	}
}
