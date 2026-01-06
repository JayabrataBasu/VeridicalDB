// Package config handles configuration loading and validation for VeridicalDB.
package config

import (
	"crypto/tls"
	"crypto/x509"
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

	// PgWire settings
	PgWire PgWireConfig `json:"pgwire" yaml:"pgwire"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	// Port for TCP connections (used in later stages)
	Port int `json:"port" yaml:"port"`

	// Host address to bind to
	Host string `json:"host" yaml:"host"`
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
		PgWire: PgWireConfig{
			TLS: TLSConfig{
				Enabled:    false,
				ClientAuth: "none",
				MinVersion: "1.2",
			},
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

	// Validate TLS configuration
	if err := c.PgWire.TLS.Validate(); err != nil {
		return fmt.Errorf("invalid pgwire TLS config: %w", err)
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
