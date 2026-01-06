package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestTLSConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		tls     TLSConfig
		wantErr bool
	}{
		{
			name: "disabled TLS - no validation",
			tls: TLSConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "enabled TLS - missing cert_file",
			tls: TLSConfig{
				Enabled: true,
				KeyFile: "/path/to/key.pem",
			},
			wantErr: true,
		},
		{
			name: "enabled TLS - missing key_file",
			tls: TLSConfig{
				Enabled:  true,
				CertFile: "/path/to/cert.pem",
			},
			wantErr: true,
		},
		{
			name: "enabled TLS - valid basic config",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				ClientAuth: "none",
			},
			wantErr: false,
		},
		{
			name: "invalid client_auth value",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				ClientAuth: "invalid",
			},
			wantErr: true,
		},
		{
			name: "require_and_verify without CA file",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				ClientAuth: "require_and_verify",
			},
			wantErr: true,
		},
		{
			name: "require_and_verify with CA file",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				CAFile:     "/path/to/ca.pem",
				ClientAuth: "require_and_verify",
			},
			wantErr: false,
		},
		{
			name: "verify without CA file",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				ClientAuth: "verify",
			},
			wantErr: true,
		},
		{
			name: "invalid min_version",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				MinVersion: "1.4",
			},
			wantErr: true,
		},
		{
			name: "valid min_version 1.3",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				MinVersion: "1.3",
			},
			wantErr: false,
		},
		{
			name: "invalid max_version",
			tls: TLSConfig{
				Enabled:    true,
				CertFile:   "/path/to/cert.pem",
				KeyFile:    "/path/to/key.pem",
				MaxVersion: "2.0",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.tls.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("TLSConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildTLSConfig(t *testing.T) {
	// Create temporary certificate files for testing
	tmpDir := t.TempDir()

	// Generate a self-signed certificate for testing
	certPEM, keyPEM := generateTestCertificate(t)

	certFile := filepath.Join(tmpDir, "server.crt")
	keyFile := filepath.Join(tmpDir, "server.key")
	caFile := filepath.Join(tmpDir, "ca.crt")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}
	if err := os.WriteFile(caFile, certPEM, 0600); err != nil { // Use same cert as CA for testing
		t.Fatalf("failed to write CA file: %v", err)
	}

	t.Run("disabled TLS returns nil", func(t *testing.T) {
		cfg := TLSConfig{Enabled: false}
		tlsCfg, err := cfg.BuildTLSConfig()
		if err != nil {
			t.Errorf("BuildTLSConfig() error = %v", err)
		}
		if tlsCfg != nil {
			t.Error("BuildTLSConfig() should return nil when disabled")
		}
	})

	t.Run("basic TLS config", func(t *testing.T) {
		cfg := TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		}
		tlsCfg, err := cfg.BuildTLSConfig()
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if tlsCfg == nil {
			t.Fatal("BuildTLSConfig() returned nil")
		}
		if len(tlsCfg.Certificates) != 1 {
			t.Errorf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
		}
		if tlsCfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("expected MinVersion TLS 1.2, got %d", tlsCfg.MinVersion)
		}
	})

	t.Run("TLS with client auth", func(t *testing.T) {
		cfg := TLSConfig{
			Enabled:    true,
			CertFile:   certFile,
			KeyFile:    keyFile,
			CAFile:     caFile,
			ClientAuth: "require_and_verify",
		}
		tlsCfg, err := cfg.BuildTLSConfig()
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
			t.Errorf("expected RequireAndVerifyClientCert, got %v", tlsCfg.ClientAuth)
		}
		if tlsCfg.ClientCAs == nil {
			t.Error("ClientCAs should not be nil")
		}
	})

	t.Run("TLS version configuration", func(t *testing.T) {
		cfg := TLSConfig{
			Enabled:    true,
			CertFile:   certFile,
			KeyFile:    keyFile,
			MinVersion: "1.3",
			MaxVersion: "1.3",
		}
		tlsCfg, err := cfg.BuildTLSConfig()
		if err != nil {
			t.Fatalf("BuildTLSConfig() error = %v", err)
		}
		if tlsCfg.MinVersion != tls.VersionTLS13 {
			t.Errorf("expected MinVersion TLS 1.3, got %d", tlsCfg.MinVersion)
		}
		if tlsCfg.MaxVersion != tls.VersionTLS13 {
			t.Errorf("expected MaxVersion TLS 1.3, got %d", tlsCfg.MaxVersion)
		}
	})

	t.Run("invalid cert file", func(t *testing.T) {
		cfg := TLSConfig{
			Enabled:  true,
			CertFile: "/nonexistent/cert.pem",
			KeyFile:  keyFile,
		}
		_, err := cfg.BuildTLSConfig()
		if err == nil {
			t.Error("BuildTLSConfig() should fail with nonexistent cert file")
		}
	})

	t.Run("invalid CA file", func(t *testing.T) {
		cfg := TLSConfig{
			Enabled:    true,
			CertFile:   certFile,
			KeyFile:    keyFile,
			CAFile:     "/nonexistent/ca.pem",
			ClientAuth: "verify",
		}
		_, err := cfg.BuildTLSConfig()
		if err == nil {
			t.Error("BuildTLSConfig() should fail with nonexistent CA file")
		}
	})
}

func TestLoadYAMLConfigWithTLS(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	yamlContent := `
server:
  port: 5432
  host: "127.0.0.1"
storage:
  data_dir: "./data"
  page_size: 8192
logging:
  level: "info"
  format: "text"
  output: "stdout"
pgwire:
  tls:
    enabled: true
    cert_file: "/path/to/cert.pem"
    key_file: "/path/to/key.pem"
    ca_file: "/path/to/ca.pem"
    client_auth: "require_and_verify"
    min_version: "1.2"
    max_version: "1.3"
`

	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !cfg.PgWire.TLS.Enabled {
		t.Error("expected TLS to be enabled")
	}
	if cfg.PgWire.TLS.CertFile != "/path/to/cert.pem" {
		t.Errorf("expected cert_file '/path/to/cert.pem', got %s", cfg.PgWire.TLS.CertFile)
	}
	if cfg.PgWire.TLS.KeyFile != "/path/to/key.pem" {
		t.Errorf("expected key_file '/path/to/key.pem', got %s", cfg.PgWire.TLS.KeyFile)
	}
	if cfg.PgWire.TLS.CAFile != "/path/to/ca.pem" {
		t.Errorf("expected ca_file '/path/to/ca.pem', got %s", cfg.PgWire.TLS.CAFile)
	}
	if cfg.PgWire.TLS.ClientAuth != "require_and_verify" {
		t.Errorf("expected client_auth 'require_and_verify', got %s", cfg.PgWire.TLS.ClientAuth)
	}
	if cfg.PgWire.TLS.MinVersion != "1.2" {
		t.Errorf("expected min_version '1.2', got %s", cfg.PgWire.TLS.MinVersion)
	}
	if cfg.PgWire.TLS.MaxVersion != "1.3" {
		t.Errorf("expected max_version '1.3', got %s", cfg.PgWire.TLS.MaxVersion)
	}
}

// generateTestCertificate creates a self-signed certificate for testing.
func generateTestCertificate(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()

	// Generate a private key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	// Create a self-signed certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Organization"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	// Encode certificate to PEM
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	// Encode private key to PEM
	keyBytes, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPEM, keyPEM
}
