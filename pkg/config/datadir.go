package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// markerFile marks a directory as an initialized VeridicalDB data directory.
const markerFile = ".veridicaldb"

// ValidateDataDir checks that dir exists, is a directory, and has been
// initialized by InitDataDir.
func ValidateDataDir(dir string) error {
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("data directory does not exist: %s", dir)
	}
	if err != nil {
		return fmt.Errorf("cannot access data directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("data path is not a directory: %s", dir)
	}
	if _, err := os.Stat(filepath.Join(dir, markerFile)); os.IsNotExist(err) {
		return fmt.Errorf("directory is not a VeridicalDB data directory: %s", dir)
	}
	return nil
}

// InitDataDir creates and initializes a new data directory.
func InitDataDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	for _, sub := range []string{"wal", "tables", "indexes", "temp"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", sub, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, markerFile), []byte("VeridicalDB Data Directory v1\n"), 0o644); err != nil {
		return fmt.Errorf("failed to create marker file: %w", err)
	}
	return nil
}

// CreateDefaultConfig writes a commented default configuration file for a data
// directory at dataDir.
func CreateDefaultConfig(path, dataDir string) error {
	content := fmt.Sprintf(`# VeridicalDB configuration file.

server:
  host: 127.0.0.1
  port: 5432
  observability_port: 8081
  max_connections: 100
  read_timeout_sec: 30
  write_timeout_sec: 30

storage:
  data_dir: %s
  page_size: 8192        # 8 KiB pages (power of two, 4096-65536)
  buffer_pool_mb: 128
  wal_buffer_kb: 64
  checkpoint_sec: 300

logging:
  level: info            # debug, info, warn, error
  format: text           # text or json
  output: stderr         # stdout, stderr, or a file path

pgwire:
  tls:
    enabled: false
    # cert_file: server.crt
    # key_file: server.key
    # ca_file: ca.crt
    # client_auth: none  # none, request, require, verify, require_and_verify
    # min_version: "1.2"

backup:
  # backup_dir defaults to <data_dir>/backups
  # archive_dir defaults to <data_dir>/wal_archive
  compress: true
  retention_days: 30
  # archive_command: "aws s3 cp %%p s3://bucket/wal/%%f"
  # restore_command: "aws s3 cp s3://bucket/wal/%%f %%p"

sharding:
  enabled: false
  shard_key_column: id
  nodes:
    # - host: 127.0.0.1
    #   port: 15432
`, dataDir)
	return os.WriteFile(path, []byte(content), 0o644)
}
