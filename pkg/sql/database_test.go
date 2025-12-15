package sql

import (
	"testing"
)

func TestParseDatabaseStatements(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		check   func(t *testing.T, stmt Statement)
	}{
		{
			name:    "CREATE DATABASE simple",
			input:   "CREATE DATABASE mydb",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateDatabaseStmt)
				if !ok {
					t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
				}
				if s.Name != "mydb" {
					t.Errorf("expected name 'mydb', got %q", s.Name)
				}
				if s.IfNotExists {
					t.Error("expected IfNotExists to be false")
				}
				if s.Owner != "" {
					t.Errorf("expected empty owner, got %q", s.Owner)
				}
			},
		},
		{
			name:    "CREATE DATABASE IF NOT EXISTS",
			input:   "CREATE DATABASE IF NOT EXISTS testdb",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateDatabaseStmt)
				if !ok {
					t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
				}
				if s.Name != "testdb" {
					t.Errorf("expected name 'testdb', got %q", s.Name)
				}
				if !s.IfNotExists {
					t.Error("expected IfNotExists to be true")
				}
			},
		},
		{
			name:    "CREATE DATABASE with owner string",
			input:   "CREATE DATABASE mydb WITH OWNER = 'alice'",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateDatabaseStmt)
				if !ok {
					t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
				}
				if s.Name != "mydb" {
					t.Errorf("expected name 'mydb', got %q", s.Name)
				}
				if s.Owner != "alice" {
					t.Errorf("expected owner 'alice', got %q", s.Owner)
				}
			},
		},
		{
			name:    "CREATE DATABASE with owner identifier",
			input:   "CREATE DATABASE mydb WITH OWNER = bob",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*CreateDatabaseStmt)
				if !ok {
					t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
				}
				if s.Owner != "bob" {
					t.Errorf("expected owner 'bob', got %q", s.Owner)
				}
			},
		},
		{
			name:    "DROP DATABASE simple",
			input:   "DROP DATABASE mydb",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropDatabaseStmt)
				if !ok {
					t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
				}
				if s.Name != "mydb" {
					t.Errorf("expected name 'mydb', got %q", s.Name)
				}
				if s.IfExists {
					t.Error("expected IfExists to be false")
				}
			},
		},
		{
			name:    "DROP DATABASE IF EXISTS",
			input:   "DROP DATABASE IF EXISTS olddb",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*DropDatabaseStmt)
				if !ok {
					t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
				}
				if s.Name != "olddb" {
					t.Errorf("expected name 'olddb', got %q", s.Name)
				}
				if !s.IfExists {
					t.Error("expected IfExists to be true")
				}
			},
		},
		{
			name:    "USE DATABASE",
			input:   "USE mydb",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*UseDatabaseStmt)
				if !ok {
					t.Fatalf("expected *UseDatabaseStmt, got %T", stmt)
				}
				if s.Name != "mydb" {
					t.Errorf("expected name 'mydb', got %q", s.Name)
				}
			},
		},
		{
			name:    "SHOW DATABASES",
			input:   "SHOW DATABASES",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.ShowType != "DATABASES" {
					t.Errorf("expected ShowType 'DATABASES', got %q", s.ShowType)
				}
			},
		},
		{
			name:    "SHOW DATABASE (singular)",
			input:   "SHOW DATABASE",
			wantErr: false,
			check: func(t *testing.T, stmt Statement) {
				s, ok := stmt.(*ShowStmt)
				if !ok {
					t.Fatalf("expected *ShowStmt, got %T", stmt)
				}
				if s.ShowType != "DATABASES" {
					t.Errorf("expected ShowType 'DATABASES', got %q", s.ShowType)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.check != nil {
				tt.check(t, stmt)
			}
		})
	}
}
