package exec

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
)

func TestInformationSchema(t *testing.T) {
	tm := setupTestTableManager(t)
	session := newParitySession(t, tm)

	// Create some tables
	_, err := session.Execute(&ast.CreateTableStmt{
		TableName: "users",
		Columns: []ast.ColumnDef{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "name", Type: catalog.TypeText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	_, err = session.Execute(&ast.CreateTableStmt{
		TableName: "posts",
		Columns: []ast.ColumnDef{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "user_id", Type: catalog.TypeInt32, ReferencesTable: "users", ReferencesColumn: "id"},
			{Name: "title", Type: catalog.TypeText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}

	// Test information_schema.tables
	result, err := session.Execute(&ast.SelectStmt{
		TableName: "information_schema.tables",
		Columns: []ast.SelectColumn{
			{Name: "table_name"},
			{Name: "table_type"},
		},
		OrderBy: []ast.OrderByClause{{Column: "table_name"}},
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
	result, err = session.Execute(&ast.SelectStmt{
		TableName: "information_schema.columns",
		Columns: []ast.SelectColumn{
			{Name: "table_name"},
			{Name: "column_name"},
			{Name: "data_type"},
		},
		Where: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "table_name"},
			Op:    token.TOKEN_EQ,
			Right: &ast.LiteralExpr{Value: catalog.NewText("users")},
		},
		OrderBy: []ast.OrderByClause{{Column: "column_name"}},
	})
	if err != nil {
		t.Fatalf("Failed to query information_schema.columns: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 columns for users table, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "id" || result.Rows[0][2].Text != "INT" {
		t.Errorf("Unexpected column info: %v", result.Rows[0])
	}
	if result.Rows[1][1].Text != "name" || result.Rows[1][2].Text != "TEXT" {
		t.Errorf("Unexpected column info: %v", result.Rows[1])
	}

	// Test information_schema.table_constraints
	result, err = session.Execute(&ast.SelectStmt{
		TableName: "information_schema.table_constraints",
		Columns: []ast.SelectColumn{
			{Name: "constraint_type"},
			{Name: "table_name"},
		},
		OrderBy: []ast.OrderByClause{{Column: "constraint_type"}},
	})
	if err != nil {
		t.Fatalf("Failed to query information_schema.table_constraints: %v", err)
	}

	foundPK, foundFK := 0, 0
	for _, row := range result.Rows {
		switch row[0].Text {
		case "PRIMARY KEY":
			foundPK++
		case "FOREIGN KEY":
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

// TestInformationSchemaViaSQL exercises information_schema through parsed SQL
// strings (not hand-built AST) — the path real clients and introspection tools
// use.
func TestInformationSchemaViaSQL(t *testing.T) {
	session := newParitySession(t, setupTestTableManager(t))
	mustExec := func(sql string) *Result {
		t.Helper()
		r, err := session.ExecuteSQL(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return r
	}

	mustExec("CREATE TABLE authors (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")
	mustExec("CREATE TABLE books (id INT PRIMARY KEY, author_id INT REFERENCES authors(id), title TEXT)")
	mustExec("CREATE VIEW author_names AS SELECT name FROM authors")

	tables := mustExec("SELECT table_name, table_type FROM information_schema.tables ORDER BY table_name")
	if len(tables.Rows) != 3 {
		t.Fatalf("expected 3 rows (authors, books, view), got %d: %v", len(tables.Rows), tables.Rows)
	}
	if tables.Rows[0][0].Text != "author_names" || tables.Rows[0][1].Text != "VIEW" {
		t.Errorf("expected author_names/VIEW first, got %v", tables.Rows[0])
	}

	cols := mustExec("SELECT column_name, is_nullable, data_type FROM information_schema.columns WHERE table_name = 'authors' ORDER BY ordinal_position")
	if len(cols.Rows) != 3 {
		t.Fatalf("expected 3 author columns, got %d", len(cols.Rows))
	}
	if cols.Rows[0][0].Text != "id" || cols.Rows[0][1].Text != "NO" {
		t.Errorf("id column should be NOT NULL (primary key): %v", cols.Rows[0])
	}
	if cols.Rows[1][0].Text != "name" || cols.Rows[1][1].Text != "NO" {
		t.Errorf("name column should be NOT NULL: %v", cols.Rows[1])
	}
	if cols.Rows[2][0].Text != "email" || cols.Rows[2][1].Text != "YES" {
		t.Errorf("email column should be nullable: %v", cols.Rows[2])
	}

	kcu := mustExec("SELECT table_name, column_name FROM information_schema.key_column_usage WHERE constraint_name = 'books_pkey'")
	if len(kcu.Rows) != 1 || kcu.Rows[0][1].Text != "id" {
		t.Errorf("expected books_pkey on id, got %v", kcu.Rows)
	}

	rc := mustExec("SELECT constraint_name, update_rule, delete_rule FROM information_schema.referential_constraints")
	if len(rc.Rows) != 1 {
		t.Fatalf("expected 1 referential constraint, got %d", len(rc.Rows))
	}
	if rc.Rows[0][1].Text != "NO ACTION" || rc.Rows[0][2].Text != "NO ACTION" {
		t.Errorf("unexpected referential rules: %v", rc.Rows[0])
	}

	schemata := mustExec("SELECT schema_name FROM information_schema.schemata")
	if len(schemata.Rows) != 1 || schemata.Rows[0][0].Text != "public" {
		t.Errorf("expected single 'public' schema, got %v", schemata.Rows)
	}

	views := mustExec("SELECT table_name FROM information_schema.views")
	if len(views.Rows) != 1 || views.Rows[0][0].Text != "author_names" {
		t.Errorf("expected author_names view, got %v", views.Rows)
	}
}
