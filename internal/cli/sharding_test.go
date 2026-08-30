package cli

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/config"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/exec"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestSetupShardCoordinatorAttachesProvider(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cli_sharding_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	listener0, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen shard 0: %v", err)
	}
	defer func() { _ = listener0.Close() }()
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen shard 1: %v", err)
	}
	defer func() { _ = listener1.Close() }()
	done := make(chan struct{})
	defer close(done)

	acceptOnce := func(listener net.Listener) {
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer func() {
				if closeErr := conn.Close(); closeErr != nil {
					t.Errorf("close shard test connection: %v", closeErr)
				}
			}()
			<-done
		}()
	}
	acceptOnce(listener0)
	acceptOnce(listener1)

	tm, err := catalog.NewTableManager(tmpDir, 4096, nil)
	if err != nil {
		t.Fatalf("create table manager: %v", err)
	}
	session := exec.NewSession(catalog.NewMVCCTableManager(tm, txn.NewManager(), nil))

	host0, port0 := splitHostPort(t, listener0.Addr().String())
	host1, port1 := splitHostPort(t, listener1.Addr().String())
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Enabled:        true,
			ShardKeyColumn: "tenant_id",
			Nodes: []config.ShardNodeConfig{
				{Host: host0, Port: port0},
				{Host: host1, Port: port1},
			},
		},
	}

	coord, err := SetupShardCoordinator(cfg, session)
	if err != nil {
		t.Fatalf("SetupShardCoordinator failed: %v", err)
	}
	if coord == nil {
		t.Fatal("expected coordinator")
	}
	defer func() { _ = coord.Close() }()
	if session.ShardMetricsProvider() == nil {
		t.Fatal("expected shard metrics provider to be attached to session")
	}
}

func splitHostPort(t *testing.T, address string) (string, int) {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return host, port
}
