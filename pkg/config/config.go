// Package config handles configuration loading and validation for VeridicalDB.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration settings for VeridicalDB. It is the single
// config schema for every entry point (server, CLI, TUI); the former
// internal/config package was merged into this one (plan phase P3).
type Config struct {
	Server   ServerConfig   `json:"server" yaml:"server"`
	Storage  StorageConfig  `json:"storage" yaml:"storage"`
	Logging  LoggingConfig  `json:"logging" yaml:"logging"`
	PgWire   PgWireConfig   `json:"pgwire" yaml:"pgwire"`
	Backup   BackupConfig   `json:"backup" yaml:"backup"`
	Sharding ShardingConfig `json:"sharding" yaml:"sharding"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	// Port for client connections (PostgreSQL wire protocol).
	Port int `json:"port" yaml:"port"`

	// Host address to bind to.
	Host string `json:"host" yaml:"host"`

	// ObservabilityPort for the metrics and health endpoints.
	ObservabilityPort int `json:"observability_port" yaml:"observability_port"`

	// MaxConnections caps concurrent client connections (0 = unlimited).
	MaxConnections int `json:"max_connections" yaml:"max_connections"`

	// ReadTimeoutSec / WriteTimeoutSec are per-connection socket timeouts.
	ReadTimeoutSec  int `json:"read_timeout_sec" yaml:"read_timeout_sec"`
	WriteTimeoutSec int `json:"write_timeout_sec" yaml:"write_timeout_sec"`
}

// PgWireConfig holds PostgreSQL wire protocol configuration.
type PgWireConfig struct {
	// TLS configuration for secure connections
	TLS TLSConfig `json:"tls" yaml:"tls"`
}

// TLSConfig holds TLS/SSL configuration for pgwire connections.
type TLSConfig struct {
	// Enabled indicates whether TLS is enabled
	Enabled bool `json:"enabled" yaml:"enabled"`

	// CertFile is the path to the server certificate file (PEM format)
	CertFile string `json:"cert_file" yaml:"cert_file"`

	// KeyFile is the path to the server private key file (PEM format)
	KeyFile string `json:"key_file" yaml:"key_file"`

	// CAFile is the path to the CA certificate file for client cert validation (optional)
	CAFile string `json:"ca_file" yaml:"ca_file"`

	// ClientAuth specifies the client authentication policy
	// Valid values: "none", "request", "require", "verify", "require_and_verify"
	// - none: Don't request client cert
	// - request: Request client cert but don't require it
	// - require: Require client cert but don't verify it
	// - verify: Request and verify client cert if provided
	// - require_and_verify: Require and verify client cert (mTLS)
	ClientAuth string `json:"client_auth" yaml:"client_auth"`

	// MinVersion is the minimum TLS version (optional, default: TLS 1.2)
	// Valid values: "1.0", "1.1", "1.2", "1.3"
	MinVersion string `json:"min_version" yaml:"min_version"`

	// MaxVersion is the maximum TLS version (optional)
	// Valid values: "1.0", "1.1", "1.2", "1.3"
	MaxVersion string `json:"max_version" yaml:"max_version"`
}

// StorageConfig holds storage-related configuration.
type StorageConfig struct {
	// DataDir is the directory where database files are stored.
	DataDir string `json:"data_dir" yaml:"data_dir"`

	// PageSize is the size of each page in bytes (default: 8192).
	PageSize int `json:"page_size" yaml:"page_size"`

	// BufferPoolMB is the buffer-pool size in megabytes.
	BufferPoolMB int `json:"buffer_pool_mb" yaml:"buffer_pool_mb"`

	// WalDir is the write-ahead log directory (empty = DataDir/wal).
	WalDir string `json:"wal_dir" yaml:"wal_dir"`

	// WalBufferKB is the in-memory WAL buffer size in kilobytes.
	WalBufferKB int `json:"wal_buffer_kb" yaml:"wal_buffer_kb"`

	// CheckpointSec is the background checkpoint interval in seconds.
	CheckpointSec int `json:"checkpoint_sec" yaml:"checkpoint_sec"`
}

// LoggingConfig holds logging-related configuration.
type LoggingConfig struct {
	// Level is the minimum log level (debug, info, warn, error).
	Level string `json:"level" yaml:"level"`

	// Format is the log format (text, json).
	Format string `json:"format" yaml:"format"`

	// Output is where logs are written (stdout, stderr, or a file path).
	Output string `json:"output" yaml:"output"`
}

// BackupConfig holds backup and point-in-time-recovery configuration.
type BackupConfig struct {
	// BackupDir is where base backups are stored (empty = DataDir/backups).
	BackupDir string `json:"backup_dir" yaml:"backup_dir"`

	// ArchiveDir is where archived WAL segments are stored
	// (empty = DataDir/wal_archive).
	ArchiveDir string `json:"archive_dir" yaml:"archive_dir"`

	// Compress enables gzip compression for base backups.
	Compress bool `json:"compress" yaml:"compress"`

	// RetentionDays is how long to keep backups (0 = forever).
	RetentionDays int `json:"retention_days" yaml:"retention_days"`

	// ArchiveCommand runs when a WAL segment is archived (%f = filename,
	// %p = full path).
	ArchiveCommand string `json:"archive_command" yaml:"archive_command"`

	// RestoreCommand runs to fetch an archived WAL segment (%f = filename,
	// %p = destination path).
	RestoreCommand string `json:"restore_command" yaml:"restore_command"`
}

// ShardingConfig holds the distributed shard-coordinator configuration.
type ShardingConfig struct {
	Enabled        bool              `json:"enabled" yaml:"enabled"`
	ShardKeyColumn string            `json:"shard_key_column" yaml:"shard_key_column"`
	Nodes          []ShardNodeConfig `json:"nodes" yaml:"nodes"`
}

// ShardNodeConfig describes a single shard endpoint.
type ShardNodeConfig struct {
	Host string `json:"host" yaml:"host"`
	Port int    `json:"port" yaml:"port"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:              5432,
			Host:              "127.0.0.1",
			ObservabilityPort: 8081,
			MaxConnections:    100,
			ReadTimeoutSec:    30,
			WriteTimeoutSec:   30,
		},
		Storage: StorageConfig{
			DataDir:       "./data",
			PageSize:      8192,
			BufferPoolMB:  128,
			WalBufferKB:   64,
			CheckpointSec: 300,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
			Output: "stderr",
		},
		PgWire: PgWireConfig{
			TLS: TLSConfig{
				Enabled:    false,
				ClientAuth: "none",
				MinVersion: "1.2",
			},
		},
		Backup: BackupConfig{
			Compress:      true,
			RetentionDays: 30,
		},
		Sharding: ShardingConfig{
			Enabled:        false,
			ShardKeyColumn: "id",
		},
	}
}

// Load reads configuration, layering: built-in defaults, then the file at path
// (YAML or JSON; skipped if path is empty or missing), then VERIDICAL_*
// environment overrides. Derived paths and validation run last.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if perr := parseInto(path, data, cfg); perr != nil {
				return nil, perr
			}
		case os.IsNotExist(err):
			// fall through to defaults + env
		default:
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	cfg.applyEnvOverrides()
	cfg.applyDerivedDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

func parseInto(path string, data []byte, cfg *Config) error {
	switch filepath.Ext(path) {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, cfg); err != nil {
			if err := json.Unmarshal(data, cfg); err != nil {
				return fmt.Errorf("failed to parse config file (tried YAML and JSON): %w", err)
			}
		}
	}
	return nil
}

// applyDerivedDefaults fills paths that default to a subdirectory of DataDir.
func (c *Config) applyDerivedDefaults() {
	if c.Storage.WalDir == "" {
		c.Storage.WalDir = filepath.Join(c.Storage.DataDir, "wal")
	}
	if c.Backup.BackupDir == "" {
		c.Backup.BackupDir = filepath.Join(c.Storage.DataDir, "backups")
	}
	if c.Backup.ArchiveDir == "" {
		c.Backup.ArchiveDir = filepath.Join(c.Storage.DataDir, "wal_archive")
	}
}

// applyEnvOverrides applies VERIDICAL_* environment variables over the loaded
// config. Only the commonly-overridden scalars are supported.
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("VERIDICAL_SERVER_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Server.Port = n
		}
	}
	if v := os.Getenv("VERIDICAL_SERVER_HOST"); v != "" {
		c.Server.Host = v
	}
	if v := os.Getenv("VERIDICAL_STORAGE_DATA_DIR"); v != "" {
		c.Storage.DataDir = v
	}
	if v := os.Getenv("VERIDICAL_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("VERIDICAL_LOG_FORMAT"); v != "" {
		c.Logging.Format = v
	}
	if v := os.Getenv("VERIDICAL_LOG_OUTPUT"); v != "" {
		c.Logging.Output = v
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid port: %d (must be 1-65535)", c.Server.Port)
	}

	if c.Storage.PageSize < 1024 || c.Storage.PageSize > 65536 {
		return fmt.Errorf("invalid page_size: %d (must be 1024-65536)", c.Storage.PageSize)
	}

	// Page size should be a power of 2
	if c.Storage.PageSize&(c.Storage.PageSize-1) != 0 {
		return fmt.Errorf("page_size must be a power of 2: %d", c.Storage.PageSize)
	}

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[c.Logging.Level] {
		return fmt.Errorf("invalid log level: %s (must be debug, info, warn, or error)", c.Logging.Level)
	}

	validFormats := map[string]bool{"text": true, "json": true}
	if !validFormats[c.Logging.Format] {
		return fmt.Errorf("invalid log format: %s (must be text or json)", c.Logging.Format)
	}

	// Validate TLS configuration
	if err := c.PgWire.TLS.Validate(); err != nil {
		return fmt.Errorf("invalid pgwire TLS config: %w", err)
	}

	if c.Storage.BufferPoolMB != 0 && c.Storage.BufferPoolMB < 8 {
		return fmt.Errorf("buffer_pool_mb must be at least 8")
	}

	if c.Sharding.Enabled {
		if c.Sharding.ShardKeyColumn == "" {
			return fmt.Errorf("sharding.shard_key_column is required when sharding is enabled")
		}
		if len(c.Sharding.Nodes) == 0 {
			return fmt.Errorf("sharding.nodes must contain at least one shard when sharding is enabled")
		}
		for i, node := range c.Sharding.Nodes {
			if node.Host == "" {
				return fmt.Errorf("sharding.nodes[%d].host is required", i)
			}
			if node.Port < 1 || node.Port > 65535 {
				return fmt.Errorf("sharding.nodes[%d].port must be between 1 and 65535", i)
			}
		}
	}

	return nil
}

// Validate checks if the TLS configuration is valid.
func (t *TLSConfig) Validate() error {
	if !t.Enabled {
		return nil
	}

	// Cert and key files are required when TLS is enabled
	if t.CertFile == "" {
		return fmt.Errorf("cert_file is required when TLS is enabled")
	}
	if t.KeyFile == "" {
		return fmt.Errorf("key_file is required when TLS is enabled")
	}

	// Validate client auth policy
	validClientAuth := map[string]bool{
		"none":               true,
		"request":            true,
		"require":            true,
		"verify":             true,
		"require_and_verify": true,
	}
	if t.ClientAuth != "" && !validClientAuth[t.ClientAuth] {
		return fmt.Errorf("invalid client_auth: %s (must be none, request, require, verify, or require_and_verify)", t.ClientAuth)
	}

	// CA file is required for client cert verification
	if (t.ClientAuth == "verify" || t.ClientAuth == "require_and_verify") && t.CAFile == "" {
		return fmt.Errorf("ca_file is required when client_auth is set to %s", t.ClientAuth)
	}

	// Validate TLS versions
	validVersions := map[string]bool{"": true, "1.0": true, "1.1": true, "1.2": true, "1.3": true}
	if !validVersions[t.MinVersion] {
		return fmt.Errorf("invalid min_version: %s (must be 1.0, 1.1, 1.2, or 1.3)", t.MinVersion)
	}
	if !validVersions[t.MaxVersion] {
		return fmt.Errorf("invalid max_version: %s (must be 1.0, 1.1, 1.2, or 1.3)", t.MaxVersion)
	}

	return nil
}

// BuildTLSConfig creates a tls.Config from the TLSConfig settings.
// Returns nil if TLS is not enabled.
func (t *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}

	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// Set minimum TLS version
	tlsConfig.MinVersion = tlsVersionFromString(t.MinVersion)
	if tlsConfig.MinVersion == 0 {
		tlsConfig.MinVersion = tls.VersionTLS12 // Default to TLS 1.2
	}

	// Set maximum TLS version if specified
	if t.MaxVersion != "" {
		tlsConfig.MaxVersion = tlsVersionFromString(t.MaxVersion)
	}

	// Configure client authentication
	switch t.ClientAuth {
	case "none", "":
		tlsConfig.ClientAuth = tls.NoClientCert
	case "request":
		tlsConfig.ClientAuth = tls.RequestClientCert
	case "require":
		tlsConfig.ClientAuth = tls.RequireAnyClientCert
	case "verify":
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	case "require_and_verify":
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	// Load CA certificate for client verification
	if t.CAFile != "" {
		caCert, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.ClientCAs = caPool
	}

	return tlsConfig, nil
}

// tlsVersionFromString converts a version string to a TLS version constant.
func tlsVersionFromString(version string) uint16 {
	switch version {
	case "1.0":
		return tls.VersionTLS10
	case "1.1":
		return tls.VersionTLS11
	case "1.2":
		return tls.VersionTLS12
	case "1.3":
		return tls.VersionTLS13
	default:
		return 0
	}
}
