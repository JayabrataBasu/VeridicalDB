package sql

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

func TestInformationSchema(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create some tables
	_, err := executor.Execute(&CreateTableStmt{
		TableName: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "name", Type: catalog.TypeText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = executor.Execute(&CreateTableStmt{
		TableName: "posts",
		Columns: []ColumnDef{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "user_id", Type: catalog.TypeInt32, ReferencesTable: "users", ReferencesColumn: "id"},
			{Name: "title", Type: catalog.TypeText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	// Test information_schema.tables
	result, err := executor.Execute(&SelectStmt{
		TableName: "information_schema.tables",
		Columns: []SelectColumn{
			{Name: "table_name"},
			{Name: "table_type"},
		},
		OrderBy: []OrderByClause{{Column: "table_name"}},
	})
	if err != nil {
		t.Fatalf("Failed to query information_schema.tables: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows in information_schema.tables, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text != "posts" {
		t.Errorf("Expected first table to be posts, got %s", result.Rows[0][0].Text)
	}
	if result.Rows[1][0].Text != "users" {
		t.Errorf("Expected second table to be users, got %s", result.Rows[1][0].Text)
	}

	// Test information_schema.columns
	result, err = executor.Execute(&SelectStmt{
		TableName: "information_schema.columns",
		Columns: []SelectColumn{
			{Name: "table_name"},
			{Name: "column_name"},
			{Name: "data_type"},
		},
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "table_name"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewText("users")},
		},
		OrderBy: []OrderByClause{{Column: "column_name"}},
	})
	if err != nil {
		t.Fatalf("Failed to query information_schema.columns: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 columns for users table, got %d", len(result.Rows))
	}
	// Ordered by column_name: id, name
	if result.Rows[0][1].Text != "id" || result.Rows[0][2].Text != "INT" {
		t.Errorf("Unexpected column info: %v", result.Rows[0])
	}
	if result.Rows[1][1].Text != "name" || result.Rows[1][2].Text != "TEXT" {
		t.Errorf("Unexpected column info: %v", result.Rows[1])
	}

	// Test information_schema.table_constraints
	result, err = executor.Execute(&SelectStmt{
		TableName: "information_schema.table_constraints",
		Columns: []SelectColumn{
			{Name: "constraint_type"},
			{Name: "table_name"},
		},
		OrderBy: []OrderByClause{{Column: "constraint_type"}},
	})
	if err != nil {
		t.Fatalf("Failed to query information_schema.table_constraints: %v", err)
	}

	// Should have:
	// users_pkey (PRIMARY KEY)
	// posts_pkey (PRIMARY KEY)
	// fk_... (FOREIGN KEY)

	foundPK := 0
	foundFK := 0
	for _, row := range result.Rows {
		if row[0].Text == "PRIMARY KEY" {
			foundPK++
		} else if row[0].Text == "FOREIGN KEY" {
			foundFK++
		}
	}

	if foundPK != 2 {
		t.Errorf("Expected 2 PRIMARY KEY constraints, got %d", foundPK)
	}
	if foundFK != 1 {
		t.Errorf("Expected 1 FOREIGN KEY constraint, got %d", foundFK)
	}
}
