package exec

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
)

func TestForeignKeyConstraints(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := newParitySession(t, tm)

	// Create parent table
	_, err := executor.Execute(&ast.CreateTableStmt{
		TableName: "users",
		Columns: []ast.ColumnDef{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "name", Type: catalog.TypeText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create child table with inline FK
	_, err = executor.Execute(&ast.CreateTableStmt{
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

	// Insert into parent
	_, err = executor.Execute(&ast.InsertStmt{
		TableName: "users",
		ValuesList: [][]ast.Expression{
			{
				&ast.LiteralExpr{Value: catalog.NewInt32(1)},
				&ast.LiteralExpr{Value: catalog.NewText("Alice")},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to insert into users: %v", err)
	}

	// Insert valid into child
	_, err = executor.Execute(&ast.InsertStmt{
		TableName: "posts",
		ValuesList: [][]ast.Expression{
			{
				&ast.LiteralExpr{Value: catalog.NewInt32(101)},
				&ast.LiteralExpr{Value: catalog.NewInt32(1)},
				&ast.LiteralExpr{Value: catalog.NewText("Hello")},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to insert valid post: %v", err)
	}

	// Insert invalid into child (user_id 999 does not exist)
	_, err = executor.Execute(&ast.InsertStmt{
		TableName: "posts",
		ValuesList: [][]ast.Expression{
			{
				&ast.LiteralExpr{Value: catalog.NewInt32(102)},
				&ast.LiteralExpr{Value: catalog.NewInt32(999)},
				&ast.LiteralExpr{Value: catalog.NewText("Invalid")},
			},
		},
	})
	if err == nil {
		t.Fatalf("Expected error inserting invalid post, got nil")
	}

	// Delete parent row (should fail because it's referenced)
	_, err = executor.Execute(&ast.DeleteStmt{
		TableName: "users",
		Where: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "id"},
			Op:    token.TOKEN_EQ,
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(1)},
		},
	})
	if err == nil {
		t.Fatalf("Expected error deleting referenced user, got nil")
	}

	// Delete child row
	_, err = executor.Execute(&ast.DeleteStmt{
		TableName: "posts",
		Where: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "id"},
			Op:    token.TOKEN_EQ,
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(101)},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete post: %v", err)
	}

	// Now delete parent row (should succeed)
	_, err = executor.Execute(&ast.DeleteStmt{
		TableName: "users",
		Where: &ast.BinaryExpr{
			Left:  &ast.ColumnRef{Name: "id"},
			Op:    token.TOKEN_EQ,
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(1)},
		},
	})
	if err != nil {
		t.Fatalf("Failed to delete user after deleting posts: %v", err)
	}
}
