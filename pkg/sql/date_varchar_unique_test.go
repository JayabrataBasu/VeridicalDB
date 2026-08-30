package sql

import (
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/parse"
)

// TestDateType tests DATE data type support because the honored idiot never added them
func TestDateTypeBasic(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := newParitySession(t, tm)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create_table_with_date_column",
			sql:     "CREATE TABLE events (id INT, event_date DATE)",
			wantErr: false,
		},
		{
			name:    "insert_valid_date",
			sql:     "INSERT INTO events (id, event_date) VALUES (1, '2024-01-15')",
			wantErr: false,
		},
		{
			name:    "insert_multiple_dates",
			sql:     "INSERT INTO events (id, event_date) VALUES (2, '2023-12-25'), (3, '2025-06-30')",
			wantErr: false,
		},
		{
			name:    "insert_date_null",
			sql:     "INSERT INTO events (id, event_date) VALUES (4, NULL)",
			wantErr: false,
		},
		{
			name:    "alter_table_add_date_column",
			sql:     "ALTER TABLE events ADD COLUMN created_date DATE",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			stmt, err := parser.Parse()
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error: %v", err)
				}
				return
			}

			result, err := executor.Execute(stmt)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			_ = result
		})
	}
}

// TestVarcharLengthValidation tests VARCHAR(n) length constraint validation
func TestVarcharLengthValidation(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := newParitySession(t, tm)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create_table_with_varchar",
			sql:     "CREATE TABLE users (id INT, name VARCHAR(50))",
			wantErr: false,
		},
		{
			name:    "insert_valid_varchar",
			sql:     "INSERT INTO users (id, name) VALUES (1, 'Alice')",
			wantErr: false,
		},
		{
			name:    "insert_varchar_at_max",
			sql:     "INSERT INTO users (id, name) VALUES (2, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrst')",
			wantErr: false,
		},
		{
			name:    "insert_varchar_too_long",
			sql:     "INSERT INTO users (id, name) VALUES (3, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz')",
			wantErr: true,
		},
		{
			name:    "update_varchar_valid",
			sql:     "UPDATE users SET name = 'Bob' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "alter_table_add_varchar",
			sql:     "ALTER TABLE users ADD COLUMN email VARCHAR(100)",
			wantErr: false,
		},
		{
			name:    "varchar_zero_means_unlimited",
			sql:     "CREATE TABLE documents (id INT, content VARCHAR(0))",
			wantErr: false,
		},
		{
			name:    "varchar_zero_accept_long",
			sql:     "INSERT INTO documents (id, content) VALUES (1, 'This is a very very very very very very long document that should be accepted because VARCHAR(0) means unlimited length')",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			stmt, err := parser.Parse()
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error: %v", err)
				}
				return
			}

			result, err := executor.Execute(stmt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			_ = result
		})
	}
}

// TestCombinedFeaturesIntegration tests interaction of DATE, VARCHAR, and UNIQUE
func TestCombinedFeaturesIntegration(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := newParitySession(t, tm)

	// Create table with all three features
	parser := parse.NewParser(`
		CREATE TABLE products (
			id INT,
			sku VARCHAR(20) UNIQUE,
			name VARCHAR(100),
			created_date DATE,
			updated_date DATE
		)
	`)

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("failed to parse CREATE TABLE: %v", err)
	}
	_, err = executor.Execute(stmt)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert valid row
	parser = parse.NewParser(`INSERT INTO products (id, sku, name, created_date, updated_date)
		VALUES (1, 'ABC123', 'Widget', '2024-01-01', '2024-01-01')`)

	stmt, err = parser.Parse()
	if err != nil {
		t.Fatalf("failed to parse INSERT: %v", err)
	}
	_, err = executor.Execute(stmt)
	if err != nil {
		t.Fatalf("failed to insert valid row: %v", err)
	}

	// Try to insert duplicate SKU (should fail)
	parser = parse.NewParser(`INSERT INTO products (id, sku, name, created_date, updated_date)
		VALUES (2, 'ABC123', 'Gadget', '2024-01-02', '2024-01-02')`)

	stmt, err = parser.Parse()
	if err != nil {
		t.Fatalf("failed to parse duplicate SKU INSERT: %v", err)
	}
	_, err = executor.Execute(stmt)
	if err == nil {
		t.Error("expected error for duplicate SKU")
	}

	// Insert valid second row
	parser = parse.NewParser(`INSERT INTO products (id, sku, name, created_date, updated_date)
		VALUES (2, 'DEF456', 'Gadget', '2024-01-02', '2024-01-02')`)

	stmt, err = parser.Parse()
	if err != nil {
		t.Fatalf("failed to parse second INSERT: %v", err)
	}
	_, err = executor.Execute(stmt)
	if err != nil {
		t.Fatalf("failed to insert second valid row: %v", err)
	}
}

// BenchmarkDateComparison benchmarks DATE value comparison
func BenchmarkDateComparison(b *testing.B) {
	d1 := catalog.NewDate(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	d2 := catalog.NewDate(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d1.Compare(d2)
	}
}

// TestUniqueConstraint tests column-level UNIQUE constraint support
func TestUniqueConstraint(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := newParitySession(t, tm)

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "create_table_with_unique_column",
			sql:     "CREATE TABLE students (id INT, email TEXT UNIQUE)",
			wantErr: false,
		},
		{
			name:    "insert_unique_value",
			sql:     "INSERT INTO students (id, email) VALUES (1, 'alice@example.com')",
			wantErr: false,
		},
		{
			name:    "insert_unique_different_value",
			sql:     "INSERT INTO students (id, email) VALUES (2, 'bob@example.com')",
			wantErr: false,
		},
		{
			name:    "insert_unique_duplicate",
			sql:     "INSERT INTO students (id, email) VALUES (3, 'alice@example.com')",
			wantErr: true,
		},
		{
			name:    "insert_unique_null_allowed",
			sql:     "INSERT INTO students (id, email) VALUES (4, NULL)",
			wantErr: false,
		},
		{
			name:    "insert_unique_null_twice",
			sql:     "INSERT INTO students (id, email) VALUES (5, NULL)",
			wantErr: false,
		},
		{
			name:    "update_to_duplicate_unique",
			sql:     "UPDATE students SET email = 'bob@example.com' WHERE id = 1",
			wantErr: true,
		},
		{
			name:    "update_unique_to_valid",
			sql:     "UPDATE students SET email = 'charlie@example.com' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "alter_table_add_unique_column",
			sql:     "ALTER TABLE students ADD COLUMN student_id VARCHAR(20) UNIQUE",
			wantErr: false,
		},
		{
			name:    "add_unique_constraint",
			sql:     "ALTER TABLE students ADD CONSTRAINT email_uniq UNIQUE (email)",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			stmt, err := parser.Parse()
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error: %v", err)
				}
				return
			}

			result, err := executor.Execute(stmt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			_ = result
		})
	}
}
