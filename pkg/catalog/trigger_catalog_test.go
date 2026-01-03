package catalog

import (
	"os"
	"testing"
)

func TestTriggerCatalog_CreateTrigger(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create trigger catalog
	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	// Create a trigger
	trigger := &TriggerMeta{
		Name:         "audit_insert",
		TableName:    "users",
		Timing:       TriggerAfter,
		Event:        TriggerInsert,
		ForEachRow:   true,
		FunctionName: "log_insert",
	}

	err = tc.CreateTrigger(trigger)
	if err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}

	// Verify trigger exists
	if !tc.TriggerExists("audit_insert", "users") {
		t.Error("trigger should exist after creation")
	}

	// Verify trigger is enabled by default
	got, exists := tc.GetTrigger("audit_insert", "users")
	if !exists {
		t.Fatal("trigger not found")
	}
	if !got.Enabled {
		t.Error("trigger should be enabled by default")
	}
	if got.FunctionName != "log_insert" {
		t.Errorf("expected function name 'log_insert', got %s", got.FunctionName)
	}
}

func TestTriggerCatalog_DuplicateTrigger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	trigger := &TriggerMeta{
		Name:         "my_trigger",
		TableName:    "orders",
		Timing:       TriggerBefore,
		Event:        TriggerUpdate,
		ForEachRow:   false,
		FunctionName: "check_update",
	}

	// First creation should succeed
	if err := tc.CreateTrigger(trigger); err != nil {
		t.Fatalf("first CreateTrigger failed: %v", err)
	}

	// Second creation should fail
	err = tc.CreateTrigger(trigger)
	if err == nil {
		t.Error("expected error for duplicate trigger")
	}
}

func TestTriggerCatalog_DropTrigger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	trigger := &TriggerMeta{
		Name:         "to_drop",
		TableName:    "products",
		Timing:       TriggerAfter,
		Event:        TriggerDelete,
		ForEachRow:   true,
		FunctionName: "log_delete",
	}

	if err := tc.CreateTrigger(trigger); err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}

	// Drop the trigger
	if err := tc.DropTrigger("to_drop", "products"); err != nil {
		t.Fatalf("DropTrigger failed: %v", err)
	}

	// Verify trigger no longer exists
	if tc.TriggerExists("to_drop", "products") {
		t.Error("trigger should not exist after drop")
	}
}

func TestTriggerCatalog_DropNonexistent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	err = tc.DropTrigger("nonexistent", "some_table")
	if err == nil {
		t.Error("expected error for dropping nonexistent trigger")
	}
}

func TestTriggerCatalog_GetTriggersForTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	// Create multiple triggers on same table
	triggers := []*TriggerMeta{
		{Name: "t1", TableName: "users", Timing: TriggerBefore, Event: TriggerInsert, FunctionName: "f1"},
		{Name: "t2", TableName: "users", Timing: TriggerAfter, Event: TriggerInsert, FunctionName: "f2"},
		{Name: "t3", TableName: "users", Timing: TriggerBefore, Event: TriggerUpdate, FunctionName: "f3"},
		{Name: "t4", TableName: "orders", Timing: TriggerAfter, Event: TriggerDelete, FunctionName: "f4"},
	}

	for _, tr := range triggers {
		if err := tc.CreateTrigger(tr); err != nil {
			t.Fatalf("CreateTrigger failed: %v", err)
		}
	}

	// Get triggers for 'users' table
	userTriggers := tc.GetTriggersForTable("users")
	if len(userTriggers) != 3 {
		t.Errorf("expected 3 triggers for users, got %d", len(userTriggers))
	}

	// Get triggers for 'orders' table
	orderTriggers := tc.GetTriggersForTable("orders")
	if len(orderTriggers) != 1 {
		t.Errorf("expected 1 trigger for orders, got %d", len(orderTriggers))
	}

	// Get triggers for nonexistent table
	noTriggers := tc.GetTriggersForTable("nonexistent")
	if len(noTriggers) != 0 {
		t.Errorf("expected 0 triggers for nonexistent table, got %d", len(noTriggers))
	}
}

func TestTriggerCatalog_GetTriggersForTableEvent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	// Create multiple triggers
	triggers := []*TriggerMeta{
		{Name: "t1", TableName: "users", Timing: TriggerBefore, Event: TriggerInsert, FunctionName: "f1"},
		{Name: "t2", TableName: "users", Timing: TriggerAfter, Event: TriggerInsert, FunctionName: "f2"},
		{Name: "t3", TableName: "users", Timing: TriggerBefore, Event: TriggerUpdate, FunctionName: "f3"},
	}

	for _, tr := range triggers {
		if err := tc.CreateTrigger(tr); err != nil {
			t.Fatalf("CreateTrigger failed: %v", err)
		}
	}

	// Get BEFORE INSERT triggers
	beforeInsert := tc.GetTriggersForTableEvent("users", TriggerBefore, TriggerInsert)
	if len(beforeInsert) != 1 {
		t.Errorf("expected 1 BEFORE INSERT trigger, got %d", len(beforeInsert))
	}
	if len(beforeInsert) > 0 && beforeInsert[0].Name != "t1" {
		t.Errorf("expected trigger t1, got %s", beforeInsert[0].Name)
	}

	// Get AFTER INSERT triggers
	afterInsert := tc.GetTriggersForTableEvent("users", TriggerAfter, TriggerInsert)
	if len(afterInsert) != 1 {
		t.Errorf("expected 1 AFTER INSERT trigger, got %d", len(afterInsert))
	}

	// Get BEFORE UPDATE triggers
	beforeUpdate := tc.GetTriggersForTableEvent("users", TriggerBefore, TriggerUpdate)
	if len(beforeUpdate) != 1 {
		t.Errorf("expected 1 BEFORE UPDATE trigger, got %d", len(beforeUpdate))
	}
}

func TestTriggerCatalog_EnableDisable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	trigger := &TriggerMeta{
		Name:         "toggle_me",
		TableName:    "test",
		Timing:       TriggerBefore,
		Event:        TriggerInsert,
		FunctionName: "f1",
	}

	if err := tc.CreateTrigger(trigger); err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}

	// Trigger should be enabled by default
	tr, _ := tc.GetTrigger("toggle_me", "test")
	if !tr.Enabled {
		t.Error("trigger should be enabled by default")
	}

	// Disable trigger
	if err := tc.DisableTrigger("toggle_me", "test"); err != nil {
		t.Fatalf("DisableTrigger failed: %v", err)
	}

	tr, _ = tc.GetTrigger("toggle_me", "test")
	if tr.Enabled {
		t.Error("trigger should be disabled")
	}

	// Disabled trigger should not be returned by GetTriggersForTableEvent
	triggers := tc.GetTriggersForTableEvent("test", TriggerBefore, TriggerInsert)
	if len(triggers) != 0 {
		t.Error("disabled trigger should not be returned")
	}

	// Enable trigger
	if err := tc.EnableTrigger("toggle_me", "test"); err != nil {
		t.Fatalf("EnableTrigger failed: %v", err)
	}

	tr, _ = tc.GetTrigger("toggle_me", "test")
	if !tr.Enabled {
		t.Error("trigger should be enabled again")
	}
}

func TestTriggerCatalog_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create catalog and add triggers
	tc1, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	trigger := &TriggerMeta{
		Name:         "persist_me",
		TableName:    "table1",
		Timing:       TriggerAfter,
		Event:        TriggerUpdate,
		ForEachRow:   true,
		FunctionName: "audit_fn",
	}

	if err := tc1.CreateTrigger(trigger); err != nil {
		t.Fatalf("CreateTrigger failed: %v", err)
	}

	// Create new catalog instance (simulates restart)
	tc2, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second trigger catalog: %v", err)
	}

	// Verify trigger persisted
	tr, exists := tc2.GetTrigger("persist_me", "table1")
	if !exists {
		t.Fatal("trigger should persist after reload")
	}
	if tr.FunctionName != "audit_fn" {
		t.Errorf("expected function name 'audit_fn', got %s", tr.FunctionName)
	}
	if tr.Timing != TriggerAfter {
		t.Errorf("expected AFTER timing, got %v", tr.Timing)
	}
	if tr.Event != TriggerUpdate {
		t.Errorf("expected UPDATE event, got %v", tr.Event)
	}
	if !tr.ForEachRow {
		t.Error("expected FOR EACH ROW to be true")
	}
}

func TestTriggerCatalog_ListAllTriggers(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	// Create triggers on different tables
	triggers := []*TriggerMeta{
		{Name: "t1", TableName: "table1", Timing: TriggerBefore, Event: TriggerInsert, FunctionName: "f1"},
		{Name: "t2", TableName: "table2", Timing: TriggerAfter, Event: TriggerUpdate, FunctionName: "f2"},
		{Name: "t3", TableName: "table3", Timing: TriggerInsteadOf, Event: TriggerDelete, FunctionName: "f3"},
	}

	for _, tr := range triggers {
		if err := tc.CreateTrigger(tr); err != nil {
			t.Fatalf("CreateTrigger failed: %v", err)
		}
	}

	// List all triggers
	all := tc.ListAllTriggers()
	if len(all) != 3 {
		t.Errorf("expected 3 triggers, got %d", len(all))
	}
}

func TestTriggerCatalog_DropTriggersForTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "trigger_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	tc, err := NewTriggerCatalog(tmpDir)
	if err != nil {
		t.Fatalf("failed to create trigger catalog: %v", err)
	}

	// Create multiple triggers on same table
	triggers := []*TriggerMeta{
		{Name: "t1", TableName: "drop_me", Timing: TriggerBefore, Event: TriggerInsert, FunctionName: "f1"},
		{Name: "t2", TableName: "drop_me", Timing: TriggerAfter, Event: TriggerInsert, FunctionName: "f2"},
		{Name: "t3", TableName: "keep_me", Timing: TriggerBefore, Event: TriggerUpdate, FunctionName: "f3"},
	}

	for _, tr := range triggers {
		if err := tc.CreateTrigger(tr); err != nil {
			t.Fatalf("CreateTrigger failed: %v", err)
		}
	}

	// Drop all triggers for 'drop_me' table
	if err := tc.DropTriggersForTable("drop_me"); err != nil {
		t.Fatalf("DropTriggersForTable failed: %v", err)
	}

	// Verify triggers are gone
	dropped := tc.GetTriggersForTable("drop_me")
	if len(dropped) != 0 {
		t.Errorf("expected 0 triggers for dropped table, got %d", len(dropped))
	}

	// Verify other table's triggers remain
	kept := tc.GetTriggersForTable("keep_me")
	if len(kept) != 1 {
		t.Errorf("expected 1 trigger for kept table, got %d", len(kept))
	}
}

func TestTriggerTiming_String(t *testing.T) {
	tests := []struct {
		timing   TriggerTiming
		expected string
	}{
		{TriggerBefore, "BEFORE"},
		{TriggerAfter, "AFTER"},
		{TriggerInsteadOf, "INSTEAD OF"},
		{TriggerTiming(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		got := tt.timing.String()
		if got != tt.expected {
			t.Errorf("TriggerTiming(%d).String() = %q, want %q", tt.timing, got, tt.expected)
		}
	}
}

func TestTriggerEvent_String(t *testing.T) {
	tests := []struct {
		event    TriggerEvent
		expected string
	}{
		{TriggerInsert, "INSERT"},
		{TriggerUpdate, "UPDATE"},
		{TriggerDelete, "DELETE"},
		{TriggerEvent(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		got := tt.event.String()
		if got != tt.expected {
			t.Errorf("TriggerEvent(%d).String() = %q, want %q", tt.event, got, tt.expected)
		}
	}
}
