package shard

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type testShardServer struct {
	listener net.Listener
	shardID  int
	delay    time.Duration
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

	listener, err := net.Listen("tcp", "127.0.0.1:0")
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
				defer c.Close()

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
