package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Failed to load default config: %v", err)
	}

	// Check defaults
	if cfg.Server.Port != 5433 {
		t.Errorf("Expected default port 5433, got %d", cfg.Server.Port)
	}

	if cfg.Storage.PageSize != 8192 {
		t.Errorf("Expected default page size 8192, got %d", cfg.Storage.PageSize)
	}

	if cfg.Log.Level != "info" {
		t.Errorf("Expected default log level 'info', got %s", cfg.Log.Level)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(*Config)
		shouldError bool
	}{
		{
			name:        "valid config",
			modify:      func(c *Config) {},
			shouldError: false,
		},
		{
			name: "invalid port",
			modify: func(c *Config) {
				c.Server.Port = 0
			},
			shouldError: true,
		},
		{
			name: "invalid page size (too small)",
			modify: func(c *Config) {
				c.Storage.PageSize = 1024
			},
			shouldError: true,
		},
		{
			name: "invalid page size (not power of 2)",
			modify: func(c *Config) {
				c.Storage.PageSize = 5000
			},
			shouldError: true,
		},
		{
			name: "invalid buffer pool",
			modify: func(c *Config) {
				c.Storage.BufferPoolMB = 4
			},
			shouldError: true,
		},
		{
			name: "invalid log level",
			modify: func(c *Config) {
				c.Log.Level = "invalid"
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _ := Load("")
			tt.modify(cfg)
			err := cfg.Validate()

			if tt.shouldError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestInitDataDir(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "testdb")

	// Initialize
	err := InitDataDir(dataDir)
	if err != nil {
		t.Fatalf("InitDataDir failed: %v", err)
	}

	// Verify structure
	expectedDirs := []string{"wal", "tables", "indexes", "temp"}
	for _, dir := range expectedDirs {
		path := filepath.Join(dataDir, dir)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Expected directory %s to exist", dir)
		}
	}

	// Verify marker file
	markerPath := filepath.Join(dataDir, ".veridicaldb")
	if _, err := os.Stat(markerPath); os.IsNotExist(err) {
		t.Error("Expected marker file .veridicaldb to exist")
	}

	// Validate should succeed
	err = ValidateDataDir(dataDir)
	if err != nil {
		t.Errorf("ValidateDataDir failed: %v", err)
	}
}

func TestValidateDataDir_NotExists(t *testing.T) {
	err := ValidateDataDir("/nonexistent/path")
	if err == nil {
		t.Error("Expected error for nonexistent directory")
	}
}

func TestValidateDataDir_NotInitialized(t *testing.T) {
	tmpDir := t.TempDir()
	err := ValidateDataDir(tmpDir)
	if err == nil {
		t.Error("Expected error for uninitialized directory")
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test.yaml")

	content := `
server:
  port: 9999
  host: 0.0.0.0
storage:
  data_dir: /custom/path
  page_size: 16384
log:
  level: debug
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.Server.Port != 9999 {
		t.Errorf("Expected port 9999, got %d", cfg.Server.Port)
	}

	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Expected host 0.0.0.0, got %s", cfg.Server.Host)
	}

	if cfg.Storage.PageSize != 16384 {
		t.Errorf("Expected page size 16384, got %d", cfg.Storage.PageSize)
	}

	if cfg.Log.Level != "debug" {
		t.Errorf("Expected log level debug, got %s", cfg.Log.Level)
	}
}
