package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestS3Config_Defaults(t *testing.T) {
	cfg := DefaultS3Config()

	if cfg.Prefix != "wal/" {
		t.Errorf("Expected default prefix 'wal/', got %q", cfg.Prefix)
	}
	if cfg.Timeout != 5*time.Minute {
		t.Errorf("Expected 5 minute timeout, got %v", cfg.Timeout)
	}
}

func TestNewS3Archiver_RequiresBucket(t *testing.T) {
	_, err := NewS3Archiver(&S3Config{})
	if err == nil {
		t.Error("Expected error when bucket is empty")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("Expected bucket error, got: %v", err)
	}
}

func TestNewS3Archiver_ValidConfig(t *testing.T) {
	archiver, err := NewS3Archiver(&S3Config{
		Bucket: "my-bucket",
		Prefix: "wal/",
		Region: "us-west-2",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if archiver == nil {
		t.Fatal("Expected archiver to be non-nil")
	}
}

func TestS3Archiver_BuildS3URI(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		prefix   string
		key      string
		expected string
	}{
		{
			name:     "simple key",
			bucket:   "mybucket",
			prefix:   "wal/",
			key:      "segment.log",
			expected: "s3://mybucket/wal/segment.log",
		},
		{
			name:     "empty prefix",
			bucket:   "mybucket",
			prefix:   "",
			key:      "segment.log",
			expected: "s3://mybucket/segment.log",
		},
		{
			name:     "prefix without trailing slash",
			bucket:   "mybucket",
			prefix:   "db/wal",
			key:      "segment.log",
			expected: "s3://mybucket/db/wal/segment.log",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			archiver, _ := NewS3Archiver(&S3Config{
				Bucket: tc.bucket,
				Prefix: tc.prefix,
			})
			uri := archiver.buildS3URI(tc.key)
			if uri != tc.expected {
				t.Errorf("Expected %q, got %q", tc.expected, uri)
			}
		})
	}
}

func TestS3Archiver_BuildAWSArgs(t *testing.T) {
	archiver, _ := NewS3Archiver(&S3Config{
		Bucket:   "mybucket",
		Region:   "eu-west-1",
		Endpoint: "http://localhost:4566",
	})

	args := archiver.buildAWSArgs("s3", "cp", "src", "dst")

	// Should contain base args
	if args[0] != "s3" || args[1] != "cp" {
		t.Errorf("Missing base args: %v", args)
	}

	// Should contain region
	hasRegion := false
	for i, arg := range args {
		if arg == "--region" && i+1 < len(args) && args[i+1] == "eu-west-1" {
			hasRegion = true
			break
		}
	}
	if !hasRegion {
		t.Errorf("Missing --region flag: %v", args)
	}

	// Should contain endpoint
	hasEndpoint := false
	for i, arg := range args {
		if arg == "--endpoint-url" && i+1 < len(args) && args[i+1] == "http://localhost:4566" {
			hasEndpoint = true
			break
		}
	}
	if !hasEndpoint {
		t.Errorf("Missing --endpoint-url flag: %v", args)
	}
}

func TestS3Archiver_GenerateArchiveCommand(t *testing.T) {
	archiver, _ := NewS3Archiver(&S3Config{
		Bucket: "mybucket",
		Prefix: "wal/",
		Region: "us-east-1",
	})

	cmd := archiver.GenerateArchiveCommand()

	if !strings.Contains(cmd, "aws s3 cp %p") {
		t.Errorf("Expected 'aws s3 cp %%p' in command: %s", cmd)
	}
	if !strings.Contains(cmd, "s3://mybucket/wal/%f") {
		t.Errorf("Expected S3 URI with %%f placeholder: %s", cmd)
	}
	if !strings.Contains(cmd, "--region us-east-1") {
		t.Errorf("Expected --region flag: %s", cmd)
	}
}

func TestS3Archiver_GenerateRestoreCommand(t *testing.T) {
	archiver, _ := NewS3Archiver(&S3Config{
		Bucket:   "mybucket",
		Prefix:   "db/wal/",
		Endpoint: "http://minio:9000",
	})

	cmd := archiver.GenerateRestoreCommand()

	if !strings.Contains(cmd, "s3://mybucket/db/wal/%f") {
		t.Errorf("Expected S3 URI with %%f placeholder: %s", cmd)
	}
	if !strings.Contains(cmd, "%p") {
		t.Errorf("Expected %%p destination placeholder: %s", cmd)
	}
	if !strings.Contains(cmd, "--endpoint-url http://minio:9000") {
		t.Errorf("Expected --endpoint-url flag: %s", cmd)
	}
}

func TestS3Archiver_Close(t *testing.T) {
	archiver, _ := NewS3Archiver(&S3Config{Bucket: "test"})
	if err := archiver.Close(); err != nil {
		t.Errorf("Close should return nil: %v", err)
	}
}

// Integration test - skipped unless AWS_TEST_BUCKET is set
func TestS3Archiver_Integration(t *testing.T) {
	bucket := os.Getenv("AWS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping S3 integration test: AWS_TEST_BUCKET not set")
	}

	endpoint := os.Getenv("AWS_TEST_ENDPOINT") // For LocalStack/MinIO

	archiver, err := NewS3Archiver(&S3Config{
		Bucket:   bucket,
		Prefix:   "test-wal/",
		Region:   os.Getenv("AWS_REGION"),
		Endpoint: endpoint,
		Timeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}
	defer func() { _ = archiver.Close() }()

	ctx := context.Background()

	// Create a temp file to upload
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test_segment.log")
	testContent := []byte("test wal segment content")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test upload
	t.Run("Upload", func(t *testing.T) {
		err := archiver.ArchiveWALSegment(ctx, testFile)
		if err != nil {
			t.Fatalf("Upload failed: %v", err)
		}
	})

	// Test exists
	t.Run("Exists", func(t *testing.T) {
		exists, err := archiver.Exists(ctx, "test_segment.log")
		if err != nil {
			t.Fatalf("Exists check failed: %v", err)
		}
		if !exists {
			t.Error("Expected file to exist after upload")
		}
	})

	// Test list
	t.Run("List", func(t *testing.T) {
		keys, err := archiver.ListWALSegments(ctx)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		found := false
		for _, k := range keys {
			if k == "test_segment.log" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected test_segment.log in list, got: %v", keys)
		}
	})

	// Test download
	t.Run("Download", func(t *testing.T) {
		destPath := filepath.Join(tmpDir, "downloaded_segment.log")
		err := archiver.RetrieveWALSegment(ctx, "test_segment.log", destPath)
		if err != nil {
			t.Fatalf("Download failed: %v", err)
		}

		downloaded, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("Failed to read downloaded file: %v", err)
		}
		if string(downloaded) != string(testContent) {
			t.Errorf("Content mismatch: got %q, want %q", string(downloaded), string(testContent))
		}
	})

	// Test delete
	t.Run("Delete", func(t *testing.T) {
		err := archiver.Delete(ctx, "test_segment.log")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		exists, _ := archiver.Exists(ctx, "test_segment.log")
		if exists {
			t.Error("File should not exist after delete")
		}
	})
}
