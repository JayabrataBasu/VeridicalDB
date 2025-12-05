// Package config handles configuration loading and validation for VeridicalDB.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration settings for VeridicalDB.
type Config struct {
	// Server settings
	Server ServerConfig `json:"server" yaml:"server"`

	// Storage settings
	Storage StorageConfig `json:"storage" yaml:"storage"`

	// Logging settings
	Logging LoggingConfig `json:"logging" yaml:"logging"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	// Port for TCP connections (used in later stages)
	Port int `json:"port" yaml:"port"`

	// Host address to bind to
	Host string `json:"host" yaml:"host"`
}

// StorageConfig holds storage-related configuration.
type StorageConfig struct {
	// DataDir is the directory where database files are stored
	DataDir string `json:"data_dir" yaml:"data_dir"`

	// PageSize is the size of each page in bytes (default: 8192)
	PageSize int `json:"page_size" yaml:"page_size"`
}

// LoggingConfig holds logging-related configuration.
type LoggingConfig struct {
	// Level is the minimum log level (debug, info, warn, error)
	Level string `json:"level" yaml:"level"`

	// Format is the log format (text, json)
	Format string `json:"format" yaml:"format"`

	// Output is where logs are written (stdout, stderr, or file path)
	Output string `json:"output" yaml:"output"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port: 5432,
			Host: "127.0.0.1",
		},
		Storage: StorageConfig{
			DataDir:  "./data",
			PageSize: 8192,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
			Output: "stdout",
		},
	}
}

// Load reads configuration from a file. Supports YAML and JSON.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // Use defaults if no config file
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	ext := filepath.Ext(path)
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		// Try YAML first, then JSON
		if err := yaml.Unmarshal(data, cfg); err != nil {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse config file (tried YAML and JSON): %w", err)
			}
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
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

	return nil
}
