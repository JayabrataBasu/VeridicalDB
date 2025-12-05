package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", cfg.Server.Port)
	}

	if cfg.Storage.PageSize != 8192 {
		t.Errorf("expected default page size 8192, got %d", cfg.Storage.PageSize)
	}

	if cfg.Logging.Level != "info" {
		t.Errorf("expected default log level 'info', got %s", cfg.Logging.Level)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:    "valid default config",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name:    "invalid port - too low",
			modify:  func(c *Config) { c.Server.Port = 0 },
			wantErr: true,
		},
		{
			name:    "invalid port - too high",
			modify:  func(c *Config) { c.Server.Port = 70000 },
			wantErr: true,
		},
		{
			name:    "invalid page size - too small",
			modify:  func(c *Config) { c.Storage.PageSize = 512 },
			wantErr: true,
		},
		{
			name:    "invalid page size - not power of 2",
			modify:  func(c *Config) { c.Storage.PageSize = 5000 },
			wantErr: true,
		},
		{
			name:    "invalid log level",
			modify:  func(c *Config) { c.Logging.Level = "verbose" },
			wantErr: true,
		},
		{
			name:    "invalid log format",
			modify:  func(c *Config) { c.Logging.Format = "xml" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadYAMLConfig(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
server:
  port: 9999
  host: "0.0.0.0"
storage:
  data_dir: "/tmp/veridical"
  page_size: 4096
logging:
  level: "debug"
  format: "json"
  output: "stderr"
`

	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Port != 9999 {
		t.Errorf("expected port 9999, got %d", cfg.Server.Port)
	}

	if cfg.Storage.PageSize != 4096 {
		t.Errorf("expected page size 4096, got %d", cfg.Storage.PageSize)
	}

	if cfg.Logging.Level != "debug" {
		t.Errorf("expected log level 'debug', got %s", cfg.Logging.Level)
	}
}

func TestLoadJSONConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	jsonContent := `{
  "server": {
    "port": 8888,
    "host": "localhost"
  },
  "storage": {
    "data_dir": "/var/lib/veridical",
    "page_size": 16384
  },
  "logging": {
    "level": "warn",
    "format": "text",
    "output": "stdout"
  }
}`

	if err := os.WriteFile(configPath, []byte(jsonContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Port != 8888 {
		t.Errorf("expected port 8888, got %d", cfg.Server.Port)
	}

	if cfg.Storage.PageSize != 16384 {
		t.Errorf("expected page size 16384, got %d", cfg.Storage.PageSize)
	}
}

func TestLoadMissingConfig(t *testing.T) {
	// Loading a non-existent config should return defaults
	cfg, err := Load("/nonexistent/config.yaml")
	if err != nil {
		t.Fatalf("Load() should not error for missing file, got %v", err)
	}

	// Should have default values
	if cfg.Server.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", cfg.Server.Port)
	}
}

func TestLoadEmptyPath(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", cfg.Server.Port)
	}
}
