// Package shard provides sharding support for distributed VeridicalDB.
// It implements hash-based sharding with a coordinator that routes queries.
package shard

import (
	"fmt"
	"hash/fnv"
	"sync"
)

// ShardID uniquely identifies a shard.
type ShardID uint32

// ShardInfo contains metadata about a shard node.
type ShardInfo struct {
	ID       ShardID `json:"id"`
	Host     string  `json:"host"`
	Port     int     `json:"port"`
	HashMin  uint32  `json:"hash_min"` // Inclusive
	HashMax  uint32  `json:"hash_max"` // Exclusive
	IsActive bool    `json:"is_active"`
}

// Address returns the network address of the shard.
func (s *ShardInfo) Address() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

// ContainsHash returns true if this shard contains the given hash value.
func (s *ShardInfo) ContainsHash(hash uint32) bool {
	return hash >= s.HashMin && hash < s.HashMax
}

// ShardConfig holds the configuration for the sharding layer.
type ShardConfig struct {
	mu sync.RWMutex

	// Shards maps shard ID to shard info
	Shards map[ShardID]*ShardInfo `json:"shards"`

	// ShardKeyColumn is the default column used for sharding
	ShardKeyColumn string `json:"shard_key_column"`

	// NumShards is the total number of shards
	NumShards int `json:"num_shards"`
}

// NewShardConfig creates a new shard configuration.
func NewShardConfig(numShards int, shardKeyColumn string) *ShardConfig {
	return &ShardConfig{
		Shards:         make(map[ShardID]*ShardInfo),
		ShardKeyColumn: shardKeyColumn,
		NumShards:      numShards,
	}
}

// AddShard adds a shard to the configuration.
func (sc *ShardConfig) AddShard(info *ShardInfo) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, exists := sc.Shards[info.ID]; exists {
		return fmt.Errorf("shard %d already exists", info.ID)
	}

	sc.Shards[info.ID] = info
	return nil
}

// GetShard returns shard info by ID.
func (sc *ShardConfig) GetShard(id ShardID) (*ShardInfo, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	info, ok := sc.Shards[id]
	if !ok {
		return nil, fmt.Errorf("shard %d not found", id)
	}
	return info, nil
}

// GetShardForHash returns the shard that owns the given hash value.
func (sc *ShardConfig) GetShardForHash(hash uint32) (*ShardInfo, error) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	for _, shard := range sc.Shards {
		if shard.IsActive && shard.ContainsHash(hash) {
			return shard, nil
		}
	}
	return nil, fmt.Errorf("no shard found for hash %d", hash)
}

// GetAllActiveShards returns all active shards.
func (sc *ShardConfig) GetAllActiveShards() []*ShardInfo {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	var shards []*ShardInfo
	for _, shard := range sc.Shards {
		if shard.IsActive {
			shards = append(shards, shard)
		}
	}
	return shards
}

// HashKey computes the hash of a shard key value.
func HashKey(key interface{}) uint32 {
	h := fnv.New32a()
	switch v := key.(type) {
	case int32:
		h.Write([]byte(fmt.Sprintf("%d", v)))
	case int64:
		h.Write([]byte(fmt.Sprintf("%d", v)))
	case string:
		h.Write([]byte(v))
	case []byte:
		h.Write(v)
	default:
		h.Write([]byte(fmt.Sprintf("%v", v)))
	}
	return h.Sum32()
}

// ComputeShardID computes which shard owns a key.
func (sc *ShardConfig) ComputeShardID(key interface{}) (ShardID, error) {
	hash := HashKey(key)
	shard, err := sc.GetShardForHash(hash)
	if err != nil {
		return 0, err
	}
	return shard.ID, nil
}

// CreateUniformShards creates N shards with uniformly distributed hash ranges.
func (sc *ShardConfig) CreateUniformShards(hosts []string, ports []int) error {
	if len(hosts) != sc.NumShards || len(ports) != sc.NumShards {
		return fmt.Errorf("must provide %d hosts and ports", sc.NumShards)
	}

	rangeSize := uint32(0xFFFFFFFF) / uint32(sc.NumShards)

	for i := 0; i < sc.NumShards; i++ {
		hashMin := uint32(i) * rangeSize
		hashMax := hashMin + rangeSize
		if i == sc.NumShards-1 {
			hashMax = 0xFFFFFFFF // Last shard gets the remainder
		}

		info := &ShardInfo{
			ID:       ShardID(i),
			Host:     hosts[i],
			Port:     ports[i],
			HashMin:  hashMin,
			HashMax:  hashMax,
			IsActive: true,
		}

		if err := sc.AddShard(info); err != nil {
			return err
		}
	}

	return nil
}

// ShardKeyExtractor extracts shard key values from SQL statements.
type ShardKeyExtractor struct {
	KeyColumn string
}

// NewShardKeyExtractor creates a new shard key extractor.
func NewShardKeyExtractor(keyColumn string) *ShardKeyExtractor {
	return &ShardKeyExtractor{KeyColumn: keyColumn}
}
