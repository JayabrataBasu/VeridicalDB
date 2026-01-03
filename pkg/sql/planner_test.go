package sql

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// TestPlannerTableScan tests that the planner returns TableScan when no index is available.
func TestPlannerTableScan(t *testing.T) {
	dir, err := os.MkdirTemp("", "planner_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	idxMgr, err := btree.NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	planner := NewPlanner(idxMgr)

	// Create a schema
	cols := []catalog.Column{
		{ID: 0, Name: "id", Type: catalog.TypeInt32},
		{ID: 1, Name: "name", Type: catalog.TypeText},
	}
	schema := catalog.NewSchema(cols)
	meta := &catalog.TableMeta{
		Schema: schema,
	}

	// Query without any index
	stmt := &SelectStmt{
		TableName: "test",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewInt32(5)},
		},
	}

	plan := planner.Plan(stmt, meta)

	if plan.Type != PlanTableScan {
		t.Errorf("Expected PlanTableScan, got %v", plan.Type)
	}
	if plan.TableName != "test" {
		t.Errorf("Expected table 'test', got '%s'", plan.TableName)
	}
	t.Logf("Plan: %s", plan.Explain())
}

// TestPlannerIndexScan tests that the planner returns IndexScan when an index is available.
func TestPlannerIndexScan(t *testing.T) {
	dir, err := os.MkdirTemp("", "planner_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	idxMgr, err := btree.NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	// Create an index
	err = idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_users_id",
		TableName: "users",
		Columns:   []string{"id"},
		Unique:    true,
	})
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	planner := NewPlanner(idxMgr)

	// Create a schema
	cols := []catalog.Column{
		{ID: 0, Name: "id", Type: catalog.TypeInt32},
		{ID: 1, Name: "name", Type: catalog.TypeText},
	}
	schema := catalog.NewSchema(cols)
	meta := &catalog.TableMeta{
		Schema: schema,
	}

	// Query on indexed column
	stmt := &SelectStmt{
		TableName: "users",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "id"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewInt32(42)},
		},
	}

	plan := planner.Plan(stmt, meta)

	if plan.Type != PlanIndexScan {
		t.Errorf("Expected PlanIndexScan, got %v", plan.Type)
	}
	if plan.IndexName != "idx_users_id" {
		t.Errorf("Expected index 'idx_users_id', got '%s'", plan.IndexName)
	}
	t.Logf("Plan: %s", plan.Explain())
}

// TestPlannerNonIndexedColumn tests that TableScan is used for non-indexed columns.
func TestPlannerNonIndexedColumn(t *testing.T) {
	dir, err := os.MkdirTemp("", "planner_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	idxMgr, err := btree.NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	// Create an index on 'id'
	err = idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_items_id",
		TableName: "items",
		Columns:   []string{"id"},
		Unique:    true,
	})
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	planner := NewPlanner(idxMgr)

	// Create a schema
	cols := []catalog.Column{
		{ID: 0, Name: "id", Type: catalog.TypeInt32},
		{ID: 1, Name: "name", Type: catalog.TypeText},
	}
	schema := catalog.NewSchema(cols)
	meta := &catalog.TableMeta{
		Schema: schema,
	}

	// Query on NON-indexed column 'name'
	stmt := &SelectStmt{
		TableName: "items",
		Where: &BinaryExpr{
			Left:  &ColumnRef{Name: "name"},
			Op:    TOKEN_EQ,
			Right: &LiteralExpr{Value: catalog.NewText("Widget")},
		},
	}

	plan := planner.Plan(stmt, meta)

	// Should fall back to TableScan because 'name' is not indexed
	if plan.Type != PlanTableScan {
		t.Errorf("Expected PlanTableScan for non-indexed column, got %v", plan.Type)
	}
	t.Logf("Plan: %s", plan.Explain())
}

// TestPlannerNoWhere tests that TableScan is used when there's no WHERE clause.
func TestPlannerNoWhere(t *testing.T) {
	dir, err := os.MkdirTemp("", "planner_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	idxMgr, err := btree.NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	planner := NewPlanner(idxMgr)

	cols := []catalog.Column{
		{ID: 0, Name: "id", Type: catalog.TypeInt32},
	}
	schema := catalog.NewSchema(cols)
	meta := &catalog.TableMeta{
		Schema: schema,
	}

	// Query without WHERE
	stmt := &SelectStmt{
		TableName: "test",
		Where:     nil,
	}

	plan := planner.Plan(stmt, meta)

	if plan.Type != PlanTableScan {
		t.Errorf("Expected PlanTableScan for query without WHERE, got %v", plan.Type)
	}
	t.Logf("Plan: %s", plan.Explain())
}
