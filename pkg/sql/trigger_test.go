package sql

import (
	"testing"
)

func TestParseTrigger_CreateBasic(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantTable  string
		wantTiming TriggerTiming
		wantEvent  TriggerEvent
		wantForRow bool
		wantFunc   string
	}{
		{
			name:       "BEFORE INSERT",
			sql:        "CREATE TRIGGER audit_insert BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION log_insert()",
			wantName:   "audit_insert",
			wantTable:  "users",
			wantTiming: TriggerBefore,
			wantEvent:  TriggerInsert,
			wantForRow: true,
			wantFunc:   "log_insert",
		},
		{
			name:       "AFTER UPDATE",
			sql:        "CREATE TRIGGER audit_update AFTER UPDATE ON orders FOR EACH STATEMENT EXECUTE FUNCTION log_update()",
			wantName:   "audit_update",
			wantTable:  "orders",
			wantTiming: TriggerAfter,
			wantEvent:  TriggerUpdate,
			wantForRow: false,
			wantFunc:   "log_update",
		},
		{
			name:       "AFTER DELETE",
			sql:        "CREATE TRIGGER audit_delete AFTER DELETE ON products EXECUTE FUNCTION log_delete()",
			wantName:   "audit_delete",
			wantTable:  "products",
			wantTiming: TriggerAfter,
			wantEvent:  TriggerDelete,
			wantForRow: false, // Default when FOR EACH not specified
			wantFunc:   "log_delete",
		},
		{
			name:       "INSTEAD OF INSERT",
			sql:        "CREATE TRIGGER view_insert INSTEAD OF INSERT ON my_view FOR EACH ROW EXECUTE PROCEDURE handle_insert()",
			wantName:   "view_insert",
			wantTable:  "my_view",
			wantTiming: TriggerInsteadOf,
			wantEvent:  TriggerInsert,
			wantForRow: true,
			wantFunc:   "handle_insert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			createTrigger, ok := stmt.(*CreateTriggerStmt)
			if !ok {
				t.Fatalf("expected CreateTriggerStmt, got %T", stmt)
			}

			if createTrigger.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", createTrigger.Name, tt.wantName)
			}
			if createTrigger.TableName != tt.wantTable {
				t.Errorf("TableName = %q, want %q", createTrigger.TableName, tt.wantTable)
			}
			if createTrigger.Timing != tt.wantTiming {
				t.Errorf("Timing = %v, want %v", createTrigger.Timing, tt.wantTiming)
			}
			if createTrigger.Event != tt.wantEvent {
				t.Errorf("Event = %v, want %v", createTrigger.Event, tt.wantEvent)
			}
			if createTrigger.ForEachRow != tt.wantForRow {
				t.Errorf("ForEachRow = %v, want %v", createTrigger.ForEachRow, tt.wantForRow)
			}
			if createTrigger.FunctionName != tt.wantFunc {
				t.Errorf("FunctionName = %q, want %q", createTrigger.FunctionName, tt.wantFunc)
			}
		})
	}
}

func TestParseTrigger_CreateIfNotExists(t *testing.T) {
	sql := "CREATE TRIGGER IF NOT EXISTS my_trigger BEFORE INSERT ON users EXECUTE FUNCTION fn()"
	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	createTrigger, ok := stmt.(*CreateTriggerStmt)
	if !ok {
		t.Fatalf("expected CreateTriggerStmt, got %T", stmt)
	}

	if !createTrigger.IfNotExists {
		t.Error("IfNotExists should be true")
	}
	if createTrigger.Name != "my_trigger" {
		t.Errorf("Name = %q, want my_trigger", createTrigger.Name)
	}
}

func TestParseTrigger_DropBasic(t *testing.T) {
	sql := "DROP TRIGGER audit_trigger ON users"
	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	dropTrigger, ok := stmt.(*DropTriggerStmt)
	if !ok {
		t.Fatalf("expected DropTriggerStmt, got %T", stmt)
	}

	if dropTrigger.Name != "audit_trigger" {
		t.Errorf("Name = %q, want audit_trigger", dropTrigger.Name)
	}
	if dropTrigger.TableName != "users" {
		t.Errorf("TableName = %q, want users", dropTrigger.TableName)
	}
	if dropTrigger.IfExists {
		t.Error("IfExists should be false")
	}
}

func TestParseTrigger_DropIfExists(t *testing.T) {
	sql := "DROP TRIGGER IF EXISTS my_trigger ON orders"
	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	dropTrigger, ok := stmt.(*DropTriggerStmt)
	if !ok {
		t.Fatalf("expected DropTriggerStmt, got %T", stmt)
	}

	if !dropTrigger.IfExists {
		t.Error("IfExists should be true")
	}
	if dropTrigger.Name != "my_trigger" {
		t.Errorf("Name = %q, want my_trigger", dropTrigger.Name)
	}
	if dropTrigger.TableName != "orders" {
		t.Errorf("TableName = %q, want orders", dropTrigger.TableName)
	}
}

func TestParseTrigger_ShowTriggers(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
	}{
		{
			name:      "all triggers",
			sql:       "SHOW TRIGGERS",
			wantTable: "",
		},
		{
			name:      "triggers on table",
			sql:       "SHOW TRIGGERS ON users",
			wantTable: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			showStmt, ok := stmt.(*ShowStmt)
			if !ok {
				t.Fatalf("expected ShowStmt, got %T", stmt)
			}

			if showStmt.ShowType != "TRIGGERS" {
				t.Errorf("ShowType = %q, want TRIGGERS", showStmt.ShowType)
			}
			if showStmt.TableName != tt.wantTable {
				t.Errorf("TableName = %q, want %q", showStmt.TableName, tt.wantTable)
			}
		})
	}
}

func TestParseTrigger_Errors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "missing timing",
			sql:  "CREATE TRIGGER tr INSERT ON users EXECUTE FUNCTION fn()",
		},
		{
			name: "missing event",
			sql:  "CREATE TRIGGER tr BEFORE ON users EXECUTE FUNCTION fn()",
		},
		{
			name: "missing ON",
			sql:  "CREATE TRIGGER tr BEFORE INSERT users EXECUTE FUNCTION fn()",
		},
		{
			name: "missing EXECUTE",
			sql:  "CREATE TRIGGER tr BEFORE INSERT ON users FUNCTION fn()",
		},
		{
			name: "DROP missing ON",
			sql:  "DROP TRIGGER tr users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()
			if err == nil {
				t.Error("expected parse error")
			}
		})
	}
}
