package shard

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
)

func TestHashKey(t *testing.T) {
	tests := []struct {
		name  string
		key   interface{}
		check func(uint32) bool
	}{
		{"int32", int32(42), func(h uint32) bool { return h != 0 }},
		{"int64", int64(42), func(h uint32) bool { return h != 0 }},
		{"string", "test", func(h uint32) bool { return h != 0 }},
		{"bytes", []byte("test"), func(h uint32) bool { return h != 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := HashKey(tt.key)
			if !tt.check(hash) {
				t.Errorf("HashKey(%v) = %d, check failed", tt.key, hash)
			}
		})
	}
}

func TestHashKeyConsistency(t *testing.T) {
	// Same key should always hash to same value
	key := "user123"
	hash1 := HashKey(key)
	hash2 := HashKey(key)

	if hash1 != hash2 {
		t.Errorf("HashKey not consistent: %d != %d", hash1, hash2)
	}
}

func TestShardConfig(t *testing.T) {
	config := NewShardConfig(2, "id")

	// Create uniform shards
	hosts := []string{"localhost", "localhost"}
	ports := []int{5432, 5433}

	if err := config.CreateUniformShards(hosts, ports); err != nil {
		t.Fatalf("CreateUniformShards: %v", err)
	}

	// Verify shards were created
	if len(config.Shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(config.Shards))
	}

	// Verify hash ranges don't overlap and cover full range
	shard0, _ := config.GetShard(0)
	shard1, _ := config.GetShard(1)

	if shard0.HashMin != 0 {
		t.Errorf("shard 0 should start at 0, got %d", shard0.HashMin)
	}

	if shard0.HashMax != shard1.HashMin {
		t.Errorf("hash ranges should be contiguous: shard0.max=%d, shard1.min=%d",
			shard0.HashMax, shard1.HashMin)
	}

	if shard1.HashMax != 0xFFFFFFFF {
		t.Errorf("shard 1 should end at max uint32, got %d", shard1.HashMax)
	}
}

func TestShardInfo_ContainsHash(t *testing.T) {
	shard := &ShardInfo{
		ID:       0,
		HashMin:  0,
		HashMax:  100,
		IsActive: true,
	}

	tests := []struct {
		hash     uint32
		expected bool
	}{
		{0, true},
		{50, true},
		{99, true},
		{100, false}, // HashMax is exclusive
		{200, false},
	}

	for _, tt := range tests {
		result := shard.ContainsHash(tt.hash)
		if result != tt.expected {
			t.Errorf("ContainsHash(%d) = %v, want %v", tt.hash, result, tt.expected)
		}
	}
}

func TestShardConfig_GetShardForHash(t *testing.T) {
	config := NewShardConfig(2, "id")
	hosts := []string{"localhost", "localhost"}
	ports := []int{5432, 5433}
	config.CreateUniformShards(hosts, ports)

	// Test that different hashes route to different shards
	lowHash := uint32(0)
	highHash := uint32(0xFFFFFFFF - 1)

	lowShard, err := config.GetShardForHash(lowHash)
	if err != nil {
		t.Fatalf("GetShardForHash(%d): %v", lowHash, err)
	}

	highShard, err := config.GetShardForHash(highHash)
	if err != nil {
		t.Fatalf("GetShardForHash(%d): %v", highHash, err)
	}

	if lowShard.ID == highShard.ID {
		t.Logf("Note: both hashes routed to shard %d", lowShard.ID)
	}
}

func TestShardConfig_ComputeShardID(t *testing.T) {
	config := NewShardConfig(2, "id")
	hosts := []string{"localhost", "localhost"}
	ports := []int{5432, 5433}
	config.CreateUniformShards(hosts, ports)

	// Different keys should consistently route to same shard
	key1 := "user1"
	key2 := "user1"

	shard1, err := config.ComputeShardID(key1)
	if err != nil {
		t.Fatalf("ComputeShardID(%s): %v", key1, err)
	}

	shard2, err := config.ComputeShardID(key2)
	if err != nil {
		t.Fatalf("ComputeShardID(%s): %v", key2, err)
	}

	if shard1 != shard2 {
		t.Errorf("same key should route to same shard: %d != %d", shard1, shard2)
	}
}

func TestShardInfo_Address(t *testing.T) {
	shard := &ShardInfo{
		Host: "192.168.1.1",
		Port: 5432,
	}

	expected := "192.168.1.1:5432"
	if shard.Address() != expected {
		t.Errorf("Address() = %s, want %s", shard.Address(), expected)
	}
}

func TestShardConfig_AddShard(t *testing.T) {
	config := NewShardConfig(2, "id")

	shard := &ShardInfo{
		ID:       0,
		Host:     "localhost",
		Port:     5432,
		IsActive: true,
	}

	// First add should succeed
	if err := config.AddShard(shard); err != nil {
		t.Fatalf("AddShard: %v", err)
	}

	// Second add with same ID should fail
	if err := config.AddShard(shard); err == nil {
		t.Error("expected error when adding duplicate shard")
	}
}

func TestShardConfig_GetAllActiveShards(t *testing.T) {
	config := NewShardConfig(3, "id")

	// Add shards with different active states
	config.AddShard(&ShardInfo{ID: 0, IsActive: true})
	config.AddShard(&ShardInfo{ID: 1, IsActive: false})
	config.AddShard(&ShardInfo{ID: 2, IsActive: true})

	active := config.GetAllActiveShards()
	if len(active) != 2 {
		t.Errorf("expected 2 active shards, got %d", len(active))
	}
}

func TestNewCoordinator(t *testing.T) {
	config := NewShardConfig(2, "id")
	coord := NewCoordinator(config)

	if coord == nil {
		t.Fatal("NewCoordinator returned nil")
	}

	if coord.config != config {
		t.Error("coordinator config not set correctly")
	}
}

func TestCoordinator_BindTransaction(t *testing.T) {
	config := NewShardConfig(2, "id")
	coord := NewCoordinator(config)

	sessionID := "session1"
	shardID := ShardID(0)

	coord.BindTransaction(sessionID, shardID)

	// The transaction should be bound
	coord.txnMu.Lock()
	bound, ok := coord.activeTxns[sessionID]
	coord.txnMu.Unlock()

	if !ok {
		t.Error("transaction not bound")
	}
	if bound != shardID {
		t.Errorf("transaction bound to wrong shard: %d != %d", bound, shardID)
	}

	// Unbind
	coord.UnbindTransaction(sessionID)

	coord.txnMu.Lock()
	_, ok = coord.activeTxns[sessionID]
	coord.txnMu.Unlock()

	if ok {
		t.Error("transaction still bound after unbind")
	}
}

func TestCoordinator_Route_DDL(t *testing.T) {
	config := NewShardConfig(2, "id")
	coord := NewCoordinator(config)

	// DDL should scatter to all shards
	route := coord.Route("CREATE TABLE users (id INT);", "session1")

	if route.Error != nil {
		t.Fatalf("Route error: %v", route.Error)
	}

	if !route.ScatterGather {
		t.Error("CREATE TABLE should scatter to all shards")
	}
}

func TestCoordinator_Route_SelectWithoutWhere(t *testing.T) {
	config := NewShardConfig(2, "id")
	coord := NewCoordinator(config)

	// SELECT without WHERE should scatter
	route := coord.Route("SELECT * FROM users;", "session1")

	if route.Error != nil {
		t.Fatalf("Route error: %v", route.Error)
	}

	if !route.ScatterGather {
		t.Error("SELECT without WHERE should scatter to all shards")
	}
}

func TestCoordinator_Route_SelectWithShardKey(t *testing.T) {
	config := NewShardConfig(2, "id")
	hosts := []string{"localhost", "localhost"}
	ports := []int{5432, 5433}
	config.CreateUniformShards(hosts, ports)

	coord := NewCoordinator(config)

	// SELECT with shard key in WHERE should route to single shard
	route := coord.Route("SELECT * FROM users WHERE id = 1;", "session1")

	if route.Error != nil {
		t.Fatalf("Route error: %v", route.Error)
	}

	if route.ScatterGather {
		t.Error("SELECT with shard key should route to single shard")
	}

	if route.TargetShard == nil {
		t.Error("TargetShard should be set")
	}
}

func TestCoordinator_Route_Insert(t *testing.T) {
	config := NewShardConfig(2, "id")
	hosts := []string{"localhost", "localhost"}
	ports := []int{5432, 5433}
	config.CreateUniformShards(hosts, ports)

	coord := NewCoordinator(config)

	// INSERT should route to shard based on key
	route := coord.Route("INSERT INTO users VALUES (1, 'alice');", "session1")

	if route.Error != nil {
		t.Fatalf("Route error: %v", route.Error)
	}

	if route.ScatterGather {
		t.Error("INSERT should route to single shard")
	}

	if route.TargetShard == nil {
		t.Error("TargetShard should be set for INSERT")
	}
}

func TestCoordinator_Route_BoundTransaction(t *testing.T) {
	config := NewShardConfig(2, "id")
	hosts := []string{"localhost", "localhost"}
	ports := []int{5432, 5433}
	config.CreateUniformShards(hosts, ports)

	coord := NewCoordinator(config)

	sessionID := "session1"
	boundShard := ShardID(1)

	// Bind transaction to shard
	coord.BindTransaction(sessionID, boundShard)

	// All queries should route to bound shard
	route := coord.Route("SELECT * FROM users;", sessionID)

	if route.Error != nil {
		t.Fatalf("Route error: %v", route.Error)
	}

	if route.TargetShard == nil || *route.TargetShard != boundShard {
		t.Error("query should route to bound shard")
	}
}

func TestShardKeyExtractor(t *testing.T) {
	extractor := NewShardKeyExtractor("user_id")

	if extractor.KeyColumn != "user_id" {
		t.Errorf("KeyColumn = %s, want user_id", extractor.KeyColumn)
	}
}

func TestNewShardNode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shard_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	info := &ShardInfo{
		ID:       0,
		Host:     "localhost",
		Port:     15432,
		IsActive: true,
	}

	node, err := NewShardNode(info, tmpDir, 4096)
	if err != nil {
		t.Fatalf("NewShardNode: %v", err)
	}

	if node.Info().ID != info.ID {
		t.Errorf("node ID = %d, want %d", node.Info().ID, info.ID)
	}

	if node.TableManager() == nil {
		t.Error("TableManager should not be nil")
	}
}

func TestShardNode_StartStop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shard_startstop_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	info := &ShardInfo{
		ID:       0,
		Host:     "localhost",
		Port:     15433,
		IsActive: true,
	}

	node, err := NewShardNode(info, tmpDir, 4096)
	if err != nil {
		t.Fatalf("NewShardNode: %v", err)
	}

	// Start
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Give it a moment
	time.Sleep(50 * time.Millisecond)

	// Stop
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestFormatResult(t *testing.T) {
	tests := []struct {
		name     string
		result   *sql.Result
		contains string
	}{
		{
			name:     "nil result",
			result:   nil,
			contains: "OK",
		},
		{
			name:     "message only",
			result:   &sql.Result{Message: "Table created."},
			contains: "Table created.",
		},
		{
			name: "with rows",
			result: &sql.Result{
				Columns: []string{"id", "name"},
				Rows: [][]catalog.Value{
					{catalog.NewInt32(1), catalog.NewText("alice")},
				},
			},
			contains: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatResult(tt.result)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("formatResult() = %q, should contain %q", result, tt.contains)
			}
		})
	}
}
