package cli

import (
	"context"
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/config"
	"github.com/JayabrataBasu/VeridicalDB/pkg/shard"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
)

// SetupShardCoordinator creates and connects a shard coordinator when sharding is enabled.
func SetupShardCoordinator(cfg *config.Config, session *sql.Session) (*shard.Coordinator, error) {
	if cfg == nil || session == nil || !cfg.Sharding.Enabled {
		return nil, nil
	}

	nodes := cfg.Sharding.Nodes
	hosts := make([]string, 0, len(nodes))
	ports := make([]int, 0, len(nodes))
	for _, node := range nodes {
		hosts = append(hosts, node.Host)
		ports = append(ports, node.Port)
	}

	shardCfg := shard.NewShardConfig(len(nodes), cfg.Sharding.ShardKeyColumn)
	if err := shardCfg.CreateUniformShards(hosts, ports); err != nil {
		return nil, fmt.Errorf("create shard config: %w", err)
	}

	coordinator := shard.NewCoordinator(shardCfg)
	if err := coordinator.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("connect shard coordinator: %w", err)
	}

	session.SetShardMetricsProvider(coordinator)
	return coordinator, nil
}
