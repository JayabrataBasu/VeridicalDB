package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitAndValidateDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")

	if err := InitDataDir(dir); err != nil {
		t.Fatalf("InitDataDir: %v", err)
	}
	for _, sub := range []string{"wal", "tables", "indexes", "temp"} {
		if _, err := os.Stat(filepath.Join(dir, sub)); err != nil {
			t.Errorf("expected %s/ to exist: %v", sub, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, markerFile)); err != nil {
		t.Errorf("expected marker file: %v", err)
	}
	if err := ValidateDataDir(dir); err != nil {
		t.Errorf("ValidateDataDir on a fresh dir: %v", err)
	}
}

func TestValidateDataDirRejects(t *testing.T) {
	if err := ValidateDataDir(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("expected error for a nonexistent directory")
	}
	if err := ValidateDataDir(t.TempDir()); err == nil {
		t.Error("expected error for an uninitialized directory")
	}
}

func TestCreateDefaultConfigIsLoadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "veridicaldb.yaml")
	if err := CreateDefaultConfig(path, "/var/lib/veridicaldb"); err != nil {
		t.Fatalf("CreateDefaultConfig: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(generated default): %v", err)
	}
	if cfg.Storage.DataDir != "/var/lib/veridicaldb" {
		t.Errorf("data_dir = %q, want /var/lib/veridicaldb", cfg.Storage.DataDir)
	}
	if cfg.Storage.WalDir != "/var/lib/veridicaldb/wal" {
		t.Errorf("derived wal_dir = %q", cfg.Storage.WalDir)
	}
}

func TestShardingValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Sharding.Enabled = true
	cfg.Sharding.ShardKeyColumn = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error: sharding enabled with no shard key")
	}

	cfg = DefaultConfig()
	cfg.Sharding.Enabled = true
	cfg.Sharding.Nodes = []ShardNodeConfig{{Host: "", Port: 5432}}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error: shard node with empty host")
	}

	cfg = DefaultConfig()
	cfg.Sharding.Enabled = true
	cfg.Sharding.Nodes = []ShardNodeConfig{{Host: "127.0.0.1", Port: 15432}}
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid sharding config rejected: %v", err)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("VERIDICAL_SERVER_PORT", "6543")
	t.Setenv("VERIDICAL_LOG_LEVEL", "debug")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Port != 6543 {
		t.Errorf("env override of port: got %d", cfg.Server.Port)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("env override of log level: got %q", cfg.Logging.Level)
	}
}
