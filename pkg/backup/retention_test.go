package backup

import (
	"fmt"
	"testing"
	"time"
)

func TestDefaultRetentionPolicy(t *testing.T) {
	policy := DefaultRetentionPolicy()

	if policy.KeepBackups != 7 {
		t.Errorf("Expected KeepBackups 7, got %d", policy.KeepBackups)
	}
	if policy.KeepDays != 30 {
		t.Errorf("Expected KeepDays 30, got %d", policy.KeepDays)
	}
	if !policy.KeepWALForBackups {
		t.Error("Expected KeepWALForBackups true")
	}
	if policy.MinWALSegments != 10 {
		t.Errorf("Expected MinWALSegments 10, got %d", policy.MinWALSegments)
	}
}

func TestNewRetentionManager(t *testing.T) {
	mgr := NewRetentionManager(nil, nil, nil)

	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
	if mgr.policy == nil {
		t.Error("Expected default policy")
	}
	if mgr.interval != 24*time.Hour {
		t.Errorf("Expected 24h interval, got %v", mgr.interval)
	}
}

func TestRetentionManager_SetInterval(t *testing.T) {
	mgr := NewRetentionManager(nil, nil, nil)
	mgr.SetInterval(1 * time.Hour)

	if mgr.interval != 1*time.Hour {
		t.Errorf("Expected 1h interval, got %v", mgr.interval)
	}
}

func TestRetentionManager_StartStop(t *testing.T) {
	mgr := NewRetentionManager(nil, nil, nil)
	mgr.SetInterval(100 * time.Millisecond)

	mgr.Start()
	if !mgr.running {
		t.Error("Expected running after Start")
	}

	// Starting again should be no-op
	mgr.Start()

	time.Sleep(50 * time.Millisecond)

	mgr.Stop()
	if mgr.running {
		t.Error("Expected not running after Stop")
	}

	// Stopping again should be no-op
	mgr.Stop()
}

func TestRetentionManager_RunOnce(t *testing.T) {
	mgr := NewRetentionManager(nil, nil, nil)

	result := mgr.RunOnce()

	if result == nil {
		t.Fatal("Expected result")
	}
	if result.StartTime.IsZero() {
		t.Error("Expected StartTime set")
	}
	if result.EndTime.IsZero() {
		t.Error("Expected EndTime set")
	}
	if result.Duration() < 0 {
		t.Error("Expected non-negative duration")
	}
	if !result.Success() {
		t.Error("Expected success with nil managers")
	}
}

func TestRetentionResult(t *testing.T) {
	start := time.Now()
	end := start.Add(1 * time.Second)
	result := &RetentionResult{
		StartTime:         start,
		EndTime:           end,
		BackupsPruned:     2,
		WALSegmentsPruned: 5,
	}

	if result.Duration() != 1*time.Second {
		t.Errorf("Expected 1s duration, got %v", result.Duration())
	}
	if !result.Success() {
		t.Error("Expected success with no errors")
	}

	result.Errors = append(result.Errors, fmt.Errorf("test error"))
	if result.Success() {
		t.Error("Expected failure with errors")
	}
}

func TestPruneCommand_DryRun(t *testing.T) {
	cmd := &PruneCommand{
		Policy: &RetentionPolicy{
			KeepBackups:    1,
			KeepDays:       1,
			MinWALSegments: 1,
		},
		DryRun: true,
	}

	result, err := cmd.Execute()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !result.DryRun {
		t.Error("Expected DryRun flag set")
	}
}

func TestPruneResult(t *testing.T) {
	result := &PruneResult{
		DryRun:          true,
		BackupsToDelete: []string{"backup1", "backup2"},
		BytesToFree:     1024 * 1024,
	}

	if len(result.BackupsToDelete) != 2 {
		t.Errorf("Expected 2 backups to delete, got %d", len(result.BackupsToDelete))
	}
	if result.BytesToFree != 1024*1024 {
		t.Errorf("Expected 1MB to free, got %d", result.BytesToFree)
	}
	if !result.DryRun {
		t.Error("Expected DryRun true")
	}
}
