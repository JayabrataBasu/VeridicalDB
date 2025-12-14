package sql

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

func TestForeignKeyConstraints(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create parent table
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

	// Create child table with inline FK
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

	// Insert into parent
	_, err = executor.Execute(&InsertStmt{
		TableName: "users",
		ValuesList: [][]Expression{
			{
				&LiteralExpr{Value: catalog.NewInt32(1)},
				&LiteralExpr{Value: catalog.NewText("Alice")},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to insert into users: %v", err)
	}

	// Insert valid into child
	_, err = executor.Execute(&InsertStmt{
		TableName: "posts",
		ValuesList: [][]Expression{
			{
				&LiteralExpr{Value: catalog.NewInt32(101)},
				&LiteralExpr{Value: catalog.NewInt32(1)},
				&LiteralExpr{Value: catalog.NewText("Hello")},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to insert valid post: %v", err)
	}

	// Insert invalid into child (user_id 999 does not exist)
	_, err = executor.Execute(&InsertStmt{
		TableName: "posts",
		ValuesList: [][]Expression{
			{
				&LiteralExpr{Value: catalog.NewInt32(102)},
				&LiteralExpr{Value: catalog.NewInt32(999)},
				&LiteralExpr{Value: catalog.NewText("Invalid")},
			},
		},
	})
	if err == nil {
		t.Fatalf("Expected error inserting invalid post, got nil")
	}

	// Delete parent row (should fail because it's referenced)
	_, err = executor.Execute(&DeleteStmt{
		TableName: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewInt32(1)},
		},
	})
	if err == nil {
		t.Fatalf("Expected error deleting referenced user, got nil")
	}

	// Delete child row
	_, err = executor.Execute(&DeleteStmt{
		TableName: "posts",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewInt32(101)},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete post: %v", err)
	}

	// Now delete parent row (should succeed)
	_, err = executor.Execute(&DeleteStmt{
		TableName: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewInt32(1)},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete user after deleting posts: %v", err)
	}
}
