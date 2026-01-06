// Package backup provides S3 archiver for remote WAL archiving and retrieval.
package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// S3Config holds S3-specific configuration for archiving.
type S3Config struct {
	// Bucket is the S3 bucket name
	Bucket string `json:"bucket" yaml:"bucket"`

	// Prefix is the key prefix for WAL segments (e.g., "wal/" or "db/wal/")
	Prefix string `json:"prefix" yaml:"prefix"`

	// Region is the AWS region (optional, uses SDK defaults if empty)
	Region string `json:"region" yaml:"region"`

	// Endpoint is a custom S3-compatible endpoint (e.g., MinIO, LocalStack)
	Endpoint string `json:"endpoint" yaml:"endpoint"`

	// UsePathStyle enables path-style addressing (required for some S3-compatible stores)
	UsePathStyle bool `json:"use_path_style" yaml:"use_path_style"`

	// Timeout for S3 operations
	Timeout time.Duration `json:"timeout" yaml:"timeout"`
}

// DefaultS3Config returns default S3 configuration.
func DefaultS3Config() *S3Config {
	return &S3Config{
		Prefix:  "wal/",
		Timeout: 5 * time.Minute,
	}
}

// S3Archiver provides S3-based WAL archiving using the AWS CLI.
// This implementation uses the AWS CLI for simplicity and broad compatibility
// without requiring the AWS SDK as a dependency.
type S3Archiver struct {
	config *S3Config
}

// NewS3Archiver creates a new S3 archiver.
func NewS3Archiver(config *S3Config) (*S3Archiver, error) {
	if config == nil {
		config = DefaultS3Config()
	}
	if config.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}
	return &S3Archiver{config: config}, nil
}

// Upload uploads a local file to S3.
func (s *S3Archiver) Upload(ctx context.Context, localPath, remoteKey string) error {
	s3URI := s.buildS3URI(remoteKey)

	args := s.buildAWSArgs("s3", "cp", localPath, s3URI)

	return s.runAWSCommand(ctx, args...)
}

// Download downloads a file from S3 to a local path.
func (s *S3Archiver) Download(ctx context.Context, remoteKey, localPath string) error {
	s3URI := s.buildS3URI(remoteKey)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}

	args := s.buildAWSArgs("s3", "cp", s3URI, localPath)

	return s.runAWSCommand(ctx, args...)
}

// List lists objects in S3 with the given prefix.
func (s *S3Archiver) List(ctx context.Context, prefix string) ([]string, error) {
	s3URI := s.buildS3URI(prefix)

	args := s.buildAWSArgs("s3", "ls", s3URI)

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "aws", args...)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("aws s3 ls failed: %s", string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("aws s3 ls failed: %w", err)
	}

	var keys []string
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		// AWS CLI ls output format: "2026-01-07 12:00:00    1234 filename"
		parts := strings.Fields(line)
		if len(parts) >= 4 {
			keys = append(keys, parts[3])
		}
	}

	return keys, nil
}

// Delete deletes an object from S3.
func (s *S3Archiver) Delete(ctx context.Context, remoteKey string) error {
	s3URI := s.buildS3URI(remoteKey)

	args := s.buildAWSArgs("s3", "rm", s3URI)

	return s.runAWSCommand(ctx, args...)
}

// Exists checks if an object exists in S3.
func (s *S3Archiver) Exists(ctx context.Context, remoteKey string) (bool, error) {
	s3URI := s.buildS3URI(remoteKey)

	args := s.buildAWSArgs("s3", "ls", s3URI)

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "aws", args...)
	err := cmd.Run()
	if err != nil {
		// Exit code 1 typically means not found
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// buildS3URI constructs an S3 URI from a key.
func (s *S3Archiver) buildS3URI(key string) string {
	prefix := strings.TrimSuffix(s.config.Prefix, "/")
	if prefix != "" {
		return fmt.Sprintf("s3://%s/%s/%s", s.config.Bucket, prefix, key)
	}
	return fmt.Sprintf("s3://%s/%s", s.config.Bucket, key)
}

// buildAWSArgs constructs AWS CLI arguments with common options.
func (s *S3Archiver) buildAWSArgs(args ...string) []string {
	result := make([]string, 0, len(args)+4)
	result = append(result, args...)

	if s.config.Region != "" {
		result = append(result, "--region", s.config.Region)
	}
	if s.config.Endpoint != "" {
		result = append(result, "--endpoint-url", s.config.Endpoint)
	}

	return result
}

// runAWSCommand executes an AWS CLI command with timeout.
func (s *S3Archiver) runAWSCommand(ctx context.Context, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "aws", args...)
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("aws command timed out after %v", s.config.Timeout)
	}
	if err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed != "" {
			return fmt.Errorf("aws command failed: %w: %s", err, trimmed)
		}
		return fmt.Errorf("aws command failed: %w", err)
	}

	return nil
}

// GenerateArchiveCommand returns an archive_command string for use with WAL archiving.
func (s *S3Archiver) GenerateArchiveCommand() string {
	prefix := strings.TrimSuffix(s.config.Prefix, "/")
	uri := fmt.Sprintf("s3://%s/%s/%%f", s.config.Bucket, prefix)

	cmd := fmt.Sprintf("aws s3 cp %%p %s", uri)
	if s.config.Region != "" {
		cmd += fmt.Sprintf(" --region %s", s.config.Region)
	}
	if s.config.Endpoint != "" {
		cmd += fmt.Sprintf(" --endpoint-url %s", s.config.Endpoint)
	}

	return cmd
}

// GenerateRestoreCommand returns a restore_command string for use with WAL restoration.
func (s *S3Archiver) GenerateRestoreCommand() string {
	prefix := strings.TrimSuffix(s.config.Prefix, "/")
	uri := fmt.Sprintf("s3://%s/%s/%%f", s.config.Bucket, prefix)

	cmd := fmt.Sprintf("aws s3 cp %s %%p", uri)
	if s.config.Region != "" {
		cmd += fmt.Sprintf(" --region %s", s.config.Region)
	}
	if s.config.Endpoint != "" {
		cmd += fmt.Sprintf(" --endpoint-url %s", s.config.Endpoint)
	}

	return cmd
}

// ArchiveWALSegment archives a WAL segment to S3.
func (s *S3Archiver) ArchiveWALSegment(ctx context.Context, localPath string) error {
	filename := filepath.Base(localPath)
	return s.Upload(ctx, localPath, filename)
}

// RetrieveWALSegment retrieves a WAL segment from S3.
func (s *S3Archiver) RetrieveWALSegment(ctx context.Context, segmentName, destPath string) error {
	return s.Download(ctx, segmentName, destPath)
}

// ListWALSegments lists all WAL segments in S3.
func (s *S3Archiver) ListWALSegments(ctx context.Context) ([]string, error) {
	return s.List(ctx, "")
}

// Ensure S3Archiver implements io.Closer for cleanup
var _ io.Closer = (*S3Archiver)(nil)

// Close is a no-op for CLI-based S3Archiver but satisfies io.Closer.
func (s *S3Archiver) Close() error {
	return nil
}
