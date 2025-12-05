// Package config handles configuration loading and validation for VeridicalDB
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for VeridicalDB
type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Storage StorageConfig `mapstructure:"storage"`
	Log     LogConfig     `mapstructure:"log"`
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port            int    `mapstructure:"port"`
	Host            string `mapstructure:"host"`
	MaxConnections  int    `mapstructure:"max_connections"`
	ReadTimeoutSec  int    `mapstructure:"read_timeout_sec"`
	WriteTimeoutSec int    `mapstructure:"write_timeout_sec"`
}

// StorageConfig holds storage engine configuration
type StorageConfig struct {
	DataDir       string `mapstructure:"data_dir"`
	PageSize      int    `mapstructure:"page_size"`
	BufferPoolMB  int    `mapstructure:"buffer_pool_mb"`
	WalDir        string `mapstructure:"wal_dir"`
	WalBufferKB   int    `mapstructure:"wal_buffer_kb"`
	CheckpointSec int    `mapstructure:"checkpoint_sec"`
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	Output string `mapstructure:"output"`
}

// Default configuration values
func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:            5433,
			Host:            "localhost",
			MaxConnections:  100,
			ReadTimeoutSec:  30,
			WriteTimeoutSec: 30,
		},
		Storage: StorageConfig{
			DataDir:       "./data",
			PageSize:      8192, // 8KB pages
			BufferPoolMB:  128,
			WalDir:        "", // defaults to DataDir/wal
			WalBufferKB:   64,
			CheckpointSec: 300,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
			Output: "stderr",
		},
	}
}

// Load reads configuration from file and environment
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	cfg := defaultConfig()
	v.SetDefault("server.port", cfg.Server.Port)
	v.SetDefault("server.host", cfg.Server.Host)
	v.SetDefault("server.max_connections", cfg.Server.MaxConnections)
	v.SetDefault("server.read_timeout_sec", cfg.Server.ReadTimeoutSec)
	v.SetDefault("server.write_timeout_sec", cfg.Server.WriteTimeoutSec)
	v.SetDefault("storage.data_dir", cfg.Storage.DataDir)
	v.SetDefault("storage.page_size", cfg.Storage.PageSize)
	v.SetDefault("storage.buffer_pool_mb", cfg.Storage.BufferPoolMB)
	v.SetDefault("storage.wal_buffer_kb", cfg.Storage.WalBufferKB)
	v.SetDefault("storage.checkpoint_sec", cfg.Storage.CheckpointSec)
	v.SetDefault("log.level", cfg.Log.Level)
	v.SetDefault("log.format", cfg.Log.Format)
	v.SetDefault("log.output", cfg.Log.Output)

	// Environment variable support
	v.SetEnvPrefix("VERIDICAL")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Load config file if specified
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	} else {
		// Search for config in common locations
		v.SetConfigName("veridicaldb")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.veridicaldb")
		v.AddConfigPath("/etc/veridicaldb")

		// It's okay if no config file is found - we use defaults
		_ = v.ReadInConfig()
	}

	// Unmarshal into struct
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set derived defaults
	if cfg.Storage.WalDir == "" {
		cfg.Storage.WalDir = filepath.Join(cfg.Storage.DataDir, "wal")
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks that configuration values are sensible
func (c *Config) Validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Server.Port)
	}

	if c.Storage.PageSize < 4096 || c.Storage.PageSize > 65536 {
		return fmt.Errorf("page_size must be between 4KB and 64KB")
	}

	// Page size should be power of 2
	if c.Storage.PageSize&(c.Storage.PageSize-1) != 0 {
		return fmt.Errorf("page_size must be a power of 2")
	}

	if c.Storage.BufferPoolMB < 8 {
		return fmt.Errorf("buffer_pool_mb must be at least 8MB")
	}

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[strings.ToLower(c.Log.Level)] {
		return fmt.Errorf("invalid log level: %s", c.Log.Level)
	}

	return nil
}

// ValidateDataDir checks if the data directory exists and is valid
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

	// Check for marker file that indicates initialized DB
	markerPath := filepath.Join(dir, ".veridicaldb")
	if _, err := os.Stat(markerPath); os.IsNotExist(err) {
		return fmt.Errorf("directory is not a VeridicalDB data directory: %s", dir)
	}

	return nil
}

// InitDataDir creates and initializes a new data directory
func InitDataDir(dir string) error {
	// Create main directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create subdirectories
	subdirs := []string{"wal", "tables", "indexes", "temp"}
	for _, sub := range subdirs {
		subPath := filepath.Join(dir, sub)
		if err := os.MkdirAll(subPath, 0755); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", sub, err)
		}
	}

	// Create marker file
	markerPath := filepath.Join(dir, ".veridicaldb")
	markerContent := []byte("VeridicalDB Data Directory v1\n")
	if err := os.WriteFile(markerPath, markerContent, 0644); err != nil {
		return fmt.Errorf("failed to create marker file: %w", err)
	}

	return nil
}

// CreateDefaultConfig writes a default configuration file
func CreateDefaultConfig(path string, dataDir string) error {
	content := fmt.Sprintf(`# VeridicalDB Configuration File

server:
  host: localhost
  port: 5433
  max_connections: 100
  read_timeout_sec: 30
  write_timeout_sec: 30

storage:
  data_dir: %s
  page_size: 8192        # 8KB pages
  buffer_pool_mb: 128    # Buffer pool size
  wal_buffer_kb: 64      # WAL buffer size
  checkpoint_sec: 300    # Checkpoint interval

log:
  level: info            # debug, info, warn, error
  format: text           # text or json
  output: stderr         # stderr, stdout, or file path
`, dataDir)

	return os.WriteFile(path, []byte(content), 0644)
}
