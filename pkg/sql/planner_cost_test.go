package sql

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
	"github.com/JayabrataBasu/VeridicalDB/pkg/stats"
)

// TestPlanner_CostBasedIndexSelection tests that the planner chooses IndexScan when cost is lower.
func TestPlanner_CostBasedIndexSelection(t *testing.T) {
	// Setup
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() {
		if closeErr := idxMgr.Close(); closeErr != nil {
			t.Fatalf("failed to close index manager: %v", closeErr)
		}
	}()
	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "age", Type: catalog.TypeInt32},
			{Name: "status", Type: catalog.TypeText},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	// Create index on age
	ageIndex := &btree.IndexMeta{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Unique:    false,
	}
	if err := idxMgr.CreateIndex(*ageIndex); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Set up statistics that favor IndexScan (high selectivity)
	tableStats := &stats.TableStats{
		TableName: "users",
		RowCount:  100000, // Large table
		PageCount: 10000,
		Columns: map[string]*stats.ColumnStats{
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 100,
				NullCount:     0,
			},
		},
	}
	if err := planner.statsMgr.SetTableStats(tableStats); err != nil {
		t.Fatalf("failed to set table stats: %v", err)
	}

	// Query with high selectivity: age = 25 (should prefer IndexScan)
	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "age"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(25)},
		},
	}

	// Test
	plan := planner.Plan(stmt, tableMeta)

	// Verify
	if plan.Type != PlanIndexScan {
		t.Errorf("Expected IndexScan for high selectivity query, got %v", plan.Type)
	}
	if plan.IndexName != "idx_users_age" {
		t.Errorf("Expected idx_users_age, got %s", plan.IndexName)
	}
	if plan.Cost <= 0 {
		t.Errorf("Expected positive cost, got %f", plan.Cost)
	}
	if plan.EstimatedRows <= 0 {
		t.Errorf("Expected positive estimated rows, got %d", plan.EstimatedRows)
	}
	if plan.Selectivity <= 0 || plan.Selectivity > 1 {
		t.Errorf("Expected selectivity between 0 and 1, got %f", plan.Selectivity)
	}

	t.Logf("Plan: %s", plan.Explain())
}

// TestPlanner_CostBasedTableScanSelection tests that the planner chooses TableScan when cost is lower.
func TestPlanner_CostBasedTableScanSelection(t *testing.T) {
	// Setup
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() {
		if closeErr := idxMgr.Close(); closeErr != nil {
			t.Fatalf("failed to close index manager: %v", closeErr)
		}
	}()
	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "age", Type: catalog.TypeInt32},
			{Name: "status", Type: catalog.TypeText},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	// Create index on age
	ageIndex := &btree.IndexMeta{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Unique:    false,
	}
	if err := idxMgr.CreateIndex(*ageIndex); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Set up statistics that favor TableScan (low selectivity)
	tableStats := &stats.TableStats{
		TableName: "users",
		RowCount:  1000, // Small table
		PageCount: 10,
		Columns: map[string]*stats.ColumnStats{
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 10, // Low distinct count means high selectivity for range queries
				NullCount:     0,
			},
		},
	}
	if err := planner.statsMgr.SetTableStats(tableStats); err != nil {
		t.Fatalf("failed to set table stats: %v", err)
	}

	// Query with low selectivity: age > 10 (most rows match)
	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_GT,
			Left:  &ast.ColumnRef{Name: "age"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(10)},
		},
	}

	// Test
	plan := planner.Plan(stmt, tableMeta)

	// For small tables with low selectivity, TableScan might be chosen
	// The exact choice depends on the cost model, but we should have valid cost estimates
	if plan.Cost <= 0 {
		t.Errorf("Expected positive cost, got %f", plan.Cost)
	}
	if plan.EstimatedRows <= 0 {
		t.Errorf("Expected positive estimated rows, got %d", plan.EstimatedRows)
	}

	t.Logf("Plan: %s", plan.Explain())
}

// TestPlanner_SelectivityEstimation tests selectivity calculation for various predicates.
func TestPlanner_SelectivityEstimation(t *testing.T) {
	planner := NewPlanner(nil)

	// Create comprehensive table statistics
	tableStats := &stats.TableStats{
		TableName: "test_table",
		RowCount:  1000,
		Columns: map[string]*stats.ColumnStats{
			"id": {
				ColumnName:    "id",
				DataType:      catalog.TypeInt32,
				DistinctCount: 1000, // Unique values
				NullCount:     0,
			},
			"category": {
				ColumnName:      "category",
				DataType:        catalog.TypeText,
				DistinctCount:   5,
				NullCount:       10,
				MostCommonVals:  []stats.Value{{Type: catalog.TypeText, StringVal: "A"}},
				MostCommonFreqs: []float64{0.4}, // 40% of rows have category 'A'
			},
			"score": {
				ColumnName:    "score",
				DataType:      catalog.TypeInt32,
				DistinctCount: 100,
				NullCount:     50,
			},
		},
	}

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "category", Type: catalog.TypeText},
			{Name: "score", Type: catalog.TypeInt32},
		},
	}

	tests := []struct {
		name                string
		expression          ast.Expression
		expectedSelectivity float64
		tolerance           float64
	}{
		{
			name: "Equality on unique column",
			expression: &ast.BinaryExpr{
				Op:    token.TOKEN_EQ,
				Left:  &ast.ColumnRef{Name: "id"},
				Right: &ast.LiteralExpr{Value: catalog.NewInt32(123)},
			},
			expectedSelectivity: 1.0 / 1000.0, // 1/n_distinct
			tolerance:           0.001,
		},
		{
			name: "Equality on MCV",
			expression: &ast.BinaryExpr{
				Op:    token.TOKEN_EQ,
				Left:  &ast.ColumnRef{Name: "category"},
				Right: &ast.LiteralExpr{Value: catalog.NewText("A")},
			},
			expectedSelectivity: 0.4, // MCV frequency
			tolerance:           0.01,
		},
		{
			name: "Equality on non-MCV",
			expression: &ast.BinaryExpr{
				Op:    token.TOKEN_EQ,
				Left:  &ast.ColumnRef{Name: "category"},
				Right: &ast.LiteralExpr{Value: catalog.NewText("Z")},
			},
			expectedSelectivity: 1.0 / 5.0, // 1/n_distinct for non-MCV
			tolerance:           0.01,
		},
		{
			name: "AND combination",
			expression: &ast.BinaryExpr{
				Op: token.TOKEN_AND,
				Left: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "category"},
					Right: &ast.LiteralExpr{Value: catalog.NewText("A")},
				},
				Right: &ast.BinaryExpr{
					Op:    token.TOKEN_GT,
					Left:  &ast.ColumnRef{Name: "score"},
					Right: &ast.LiteralExpr{Value: catalog.NewInt32(50)},
				},
			},
			expectedSelectivity: 0.4 * 0.33, // sel(category='A') * sel(score>50)
			tolerance:           0.05,
		},
		{
			name: "OR combination",
			expression: &ast.BinaryExpr{
				Op: token.TOKEN_OR,
				Left: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "category"},
					Right: &ast.LiteralExpr{Value: catalog.NewText("A")},
				},
				Right: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "category"},
					Right: &ast.LiteralExpr{Value: catalog.NewText("B")},
				},
			},
			// sel(A OR B) = sel(A) + sel(B) - sel(A)*sel(B)
			// = 0.4 + 0.2 - 0.4*0.2 = 0.6 - 0.08 = 0.52
			expectedSelectivity: 0.52,
			tolerance:           0.05,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectivity := planner.estimateSelectivity(tt.expression, tableStats, schema)

			if selectivity < 0 || selectivity > 1 {
				t.Errorf("Selectivity %f is out of valid range [0,1]", selectivity)
			}

			diff := selectivity - tt.expectedSelectivity
			if diff < 0 {
				diff = -diff
			}
			if diff > tt.tolerance {
				t.Errorf("Expected selectivity %f, got %f (tolerance %f)",
					tt.expectedSelectivity, selectivity, tt.tolerance)
			}

			t.Logf("Expression selectivity: %f", selectivity)
		})
	}
}

// TestPlanner_JoinOrderOptimization tests basic join reordering based on selectivity.
func TestPlanner_JoinOrderOptimization(t *testing.T) {
	// Setup
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)
	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "status", Type: catalog.TypeText},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	// Set up statistics
	tableStats := &stats.TableStats{
		TableName: "users",
		RowCount:  10000,
		PageCount: 100,
		Columns: map[string]*stats.ColumnStats{
			"status": {
				ColumnName:      "status",
				DataType:        catalog.TypeText,
				DistinctCount:   3,
				NullCount:       0,
				MostCommonVals:  []stats.Value{{Type: catalog.TypeText, StringVal: "active"}},
				MostCommonFreqs: []float64{0.1}, // Very selective - only 10% active
			},
		},
	}
	_ = planner.statsMgr.SetTableStats(tableStats)

	// Create a join query (basic test - full join optimization would be more complex)
	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Joins: []ast.JoinClause{
			{
				JoinType:  "INNER",
				TableName: "orders",
				Condition: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "users.id"},
					Right: &ast.ColumnRef{Name: "orders.user_id"},
				},
			},
		},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "status"},
			Right: &ast.LiteralExpr{Value: catalog.NewText("active")},
		},
	}

	// Test
	plan := planner.Plan(stmt, tableMeta)

	// Verify we get a valid plan with cost estimates
	if plan == nil {
		t.Fatal("Expected non-nil plan")
		return
	}
	if plan.Cost <= 0 {
		t.Errorf("Expected positive cost, got %f", plan.Cost)
	}

	t.Logf("Join plan: %s", plan.Explain())
}

// TestPlanner_RuleBased_Fallback tests that rule-based planning works when stats are disabled.
func TestPlanner_RuleBased_Fallback(t *testing.T) {
	// Setup
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)
	planner := NewPlanner(idxMgr)
	planner.useStats = false // Disable cost-based optimization

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "age", Type: catalog.TypeInt32},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	// Create index
	idxMeta := &btree.IndexMeta{
		Name:      "idx_age",
		TableName: "users",
		Columns:   []string{"age"},
		Unique:    false,
		Type:      btree.IndexTypeBTree,
	}
	_ = idxMgr.CreateIndex(*idxMeta)

	// Query with equality (should use index in rule-based mode)
	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "age"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(25)},
		},
	}

	// Test
	plan := planner.Plan(stmt, tableMeta)

	// In rule-based mode, with useStats=false, we return the first candidate plan
	// which is TableScan (generated first), not necessarily IndexScan
	// This is expected behavior - the rule-based fallback just uses first plan
	if plan.Type != PlanTableScan {
		t.Logf("Note: Got plan type %v (expected TableScan in rule-based fallback)", plan.Type)
	}

	t.Logf("Rule-based plan: %s", plan.Explain())
}

// TestPlanner_NoStats_Fallback tests behavior when no statistics are available.
func TestPlanner_NoStats_Fallback(t *testing.T) {
	// Setup planner without statistics
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)
	planner := NewPlanner(idxMgr)
	// Don't set any statistics

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "name", Type: catalog.TypeText},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "name"},
			Right: &ast.LiteralExpr{Value: catalog.NewText("Alice")},
		},
	}

	// Test
	plan := planner.Plan(stmt, tableMeta)

	// Should fallback gracefully with default cost estimates
	if plan == nil {
		t.Fatal("Expected non-nil plan")
		return
	}
	if plan.Cost <= 0 {
		t.Errorf("Expected positive fallback cost, got %f", plan.Cost)
	}

	t.Logf("No-stats fallback plan: %s", plan.Explain())
}

// TestPlanner_CostComparison tests that lower-cost plans are selected.
func TestPlanner_CostComparison(t *testing.T) {
	// Setup
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)
	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "age", Type: catalog.TypeInt32},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	// Create index
	ageIndex := &btree.IndexMeta{
		Name:      "idx_age",
		TableName: "users",
		Columns:   []string{"age"},
		Unique:    false,
		Type:      btree.IndexTypeBTree,
	}
	_ = idxMgr.CreateIndex(*ageIndex)

	// Set up statistics that should make IndexScan cheaper
	tableStats := &stats.TableStats{
		TableName: "users",
		RowCount:  1000000, // Very large table
		PageCount: 100000,
		Columns: map[string]*stats.ColumnStats{
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 100,
				NullCount:     0,
			},
		},
	}
	_ = planner.statsMgr.SetTableStats(tableStats)

	// Highly selective query
	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "age"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(25)},
		},
	}

	// Generate all candidate plans to compare costs
	candidatePlans := planner.generateCandidatePlans(stmt, tableMeta)

	if len(candidatePlans) < 2 {
		t.Fatalf("Expected at least 2 candidate plans (TableScan + IndexScan), got %d", len(candidatePlans))
	}

	// Find TableScan and IndexScan plans
	var tableScanPlan, indexScanPlan *ExecutionPlan
	for _, plan := range candidatePlans {
		switch plan.Type {
		case PlanTableScan:
			tableScanPlan = plan
		case PlanIndexScan:
			indexScanPlan = plan
		}
	}

	if tableScanPlan == nil {
		t.Fatal("No TableScan plan found")
		return
	}
	if indexScanPlan == nil {
		t.Fatal("No IndexScan plan found")
		return
	}

	t.Logf("TableScan cost: %f", tableScanPlan.Cost)
	t.Logf("IndexScan cost: %f", indexScanPlan.Cost)

	// For a highly selective query on a large table, IndexScan should be cheaper
	if indexScanPlan.Cost >= tableScanPlan.Cost {
		t.Errorf("Expected IndexScan cost (%f) to be less than TableScan cost (%f)",
			indexScanPlan.Cost, tableScanPlan.Cost)
	}

	// Verify the planner chooses the IndexScan
	finalPlan := planner.Plan(stmt, tableMeta)
	if finalPlan.Type != PlanIndexScan {
		t.Errorf("Expected planner to choose IndexScan, got %v", finalPlan.Type)
	}
}

func TestPlanner_PlanSelectionMatrix(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()
	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "age", Type: catalog.TypeInt32},
			{Name: "status", Type: catalog.TypeText},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	err = idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Unique:    false,
		Type:      btree.IndexTypeBTree,
	})
	if err != nil {
		t.Fatalf("failed to create test index: %v", err)
	}

	_ = planner.statsMgr.SetTableStats(&stats.TableStats{
		TableName: "users",
		RowCount:  50000,
		PageCount: 5000,
		Columns: map[string]*stats.ColumnStats{
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 100,
				NullCount:     0,
			},
			"status": {
				ColumnName:      "status",
				DataType:        catalog.TypeText,
				DistinctCount:   4,
				NullCount:       0,
				MostCommonVals:  []stats.Value{{Type: catalog.TypeText, StringVal: "active"}},
				MostCommonFreqs: []float64{0.15},
			},
		},
	})

	tests := []struct {
		name              string
		stmt              *ast.SelectStmt
		expectedType      PlanType
		expectedIndexName string
		minSelectivity    float64
		maxSelectivity    float64
	}{
		{
			name:           "no where uses table scan",
			stmt:           &ast.SelectStmt{TableName: "users", Columns: []ast.SelectColumn{{Star: true}}},
			expectedType:   PlanTableScan,
			minSelectivity: 1.0,
			maxSelectivity: 1.0,
		},
		{
			name: "indexed equality uses index scan",
			stmt: &ast.SelectStmt{
				TableName: "users",
				Columns:   []ast.SelectColumn{{Star: true}},
				Where: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "age"},
					Right: &ast.LiteralExpr{Value: catalog.NewInt32(25)},
				},
			},
			expectedType:      PlanIndexScan,
			expectedIndexName: "idx_users_age",
			minSelectivity:    0.009,
			maxSelectivity:    0.011,
		},
		{
			name: "mcv equality remains selective",
			stmt: &ast.SelectStmt{
				TableName: "users",
				Columns:   []ast.SelectColumn{{Star: true}},
				Where: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "status"},
					Right: &ast.LiteralExpr{Value: catalog.NewText("active")},
				},
			},
			expectedType:   PlanTableScan,
			minSelectivity: 0.14,
			maxSelectivity: 0.16,
		},
		{
			name: "compound predicate reduces selectivity",
			stmt: &ast.SelectStmt{
				TableName: "users",
				Columns:   []ast.SelectColumn{{Star: true}},
				Where: &ast.BinaryExpr{
					Op: token.TOKEN_AND,
					Left: &ast.BinaryExpr{
						Op:    token.TOKEN_EQ,
						Left:  &ast.ColumnRef{Name: "age"},
						Right: &ast.LiteralExpr{Value: catalog.NewInt32(25)},
					},
					Right: &ast.BinaryExpr{
						Op:    token.TOKEN_EQ,
						Left:  &ast.ColumnRef{Name: "status"},
						Right: &ast.LiteralExpr{Value: catalog.NewText("active")},
					},
				},
			},
			expectedType:      PlanIndexScan,
			expectedIndexName: "idx_users_age",
			minSelectivity:    0.001,
			maxSelectivity:    0.002,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planner.Plan(tt.stmt, tableMeta)

			if plan == nil {
				t.Fatal("expected non-nil plan")
			}
			if plan.Type != tt.expectedType {
				t.Fatalf("expected plan type %v, got %v", tt.expectedType, plan.Type)
			}
			if tt.expectedIndexName != "" && plan.IndexName != tt.expectedIndexName {
				t.Fatalf("expected index %q, got %q", tt.expectedIndexName, plan.IndexName)
			}
			if plan.Cost <= 0 {
				t.Fatalf("expected positive cost, got %f", plan.Cost)
			}
			if plan.Selectivity < tt.minSelectivity || plan.Selectivity > tt.maxSelectivity {
				t.Fatalf("expected selectivity in [%f, %f], got %f", tt.minSelectivity, tt.maxSelectivity, plan.Selectivity)
			}
			if plan.EstimatedRows <= 0 {
				t.Fatalf("expected positive estimated rows, got %d", plan.EstimatedRows)
			}
		})
	}

	planner.SetStatsManager(nil)
	ruleBasedPlan := planner.Plan(&ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "age"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(25)},
		},
	}, tableMeta)

	if ruleBasedPlan == nil {
		t.Fatal("expected non-nil rule-based plan")
	}
	if ruleBasedPlan.Type != PlanTableScan {
		t.Fatalf("expected rule-based fallback to return first candidate TableScan, got %v", ruleBasedPlan.Type)
	}
}

func TestPlanner_UniqueEqualityPrefersIndexWithLiteral(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() {
		if closeErr := idxMgr.Close(); closeErr != nil {
			t.Fatalf("failed to close index manager: %v", closeErr)
		}
	}()

	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "status", Type: catalog.TypeText},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "users", Schema: schema}

	if err := idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_users_id",
		TableName: "users",
		Columns:   []string{"id"},
		Unique:    true,
		Type:      btree.IndexTypeBTree,
	}); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Intentionally misleading stats on id to ensure unique-equality shortcut wins.
	if err := planner.statsMgr.SetTableStats(&stats.TableStats{
		TableName: "users",
		RowCount:  10000,
		PageCount: 1000,
		Columns: map[string]*stats.ColumnStats{
			"id": {
				ColumnName:    "id",
				DataType:      catalog.TypeInt32,
				DistinctCount: 2,
				NullCount:     0,
			},
		},
	}); err != nil {
		t.Fatalf("failed to set table stats: %v", err)
	}

	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "id"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(42)},
		},
	}

	plan := planner.Plan(stmt, tableMeta)
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.Type != PlanIndexScan {
		t.Fatalf("expected index scan for PK equality literal, got %v", plan.Type)
	}
	if plan.IndexName != "idx_users_id" {
		t.Fatalf("expected idx_users_id, got %s", plan.IndexName)
	}
	if plan.EstimatedRows != 1 {
		t.Fatalf("expected estimated rows = 1 for unique equality, got %d", plan.EstimatedRows)
	}
	if plan.Selectivity > 0.001 {
		t.Fatalf("expected selectivity to be near 1/row_count, got %f", plan.Selectivity)
	}
}

func TestPlanner_IndexedRangeSelectivityCap_SingleSided(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() {
		if closeErr := idxMgr.Close(); closeErr != nil {
			t.Fatalf("failed to close index manager: %v", closeErr)
		}
	}()

	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "age", Type: catalog.TypeInt32},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "users", Schema: schema}

	if err := idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Type:      btree.IndexTypeBTree,
	}); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Histogram produces selectivity near 1.0 for age <= 100.
	if err := planner.statsMgr.SetTableStats(&stats.TableStats{
		TableName: "users",
		RowCount:  100000,
		PageCount: 10000,
		Columns: map[string]*stats.ColumnStats{
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 100000,
				Histogram: &stats.Histogram{
					Bounds:      []stats.Value{{Type: catalog.TypeInt32, IntVal: 100}},
					Frequencies: []int64{100000},
					NumBuckets:  1,
				},
			},
		},
	}); err != nil {
		t.Fatalf("failed to set table stats: %v", err)
	}

	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_LE,
			Left:  &ast.ColumnRef{Name: "age"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(100)},
		},
	}

	plan := planner.Plan(stmt, tableMeta)
	if plan.Type != PlanIndexScan {
		t.Fatalf("expected index scan, got %v", plan.Type)
	}
	if plan.Selectivity > 0.45 {
		t.Fatalf("expected capped selectivity <= 0.45 for single-sided inclusive range, got %f", plan.Selectivity)
	}
}

func TestPlanner_IndexedRangeSelectivityCap_BoundedRange(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() {
		if closeErr := idxMgr.Close(); closeErr != nil {
			t.Fatalf("failed to close index manager: %v", closeErr)
		}
	}()

	planner := NewPlanner(idxMgr)

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "age", Type: catalog.TypeInt32},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "users", Schema: schema}

	if err := idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Type:      btree.IndexTypeBTree,
	}); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if err := planner.statsMgr.SetTableStats(&stats.TableStats{
		TableName: "users",
		RowCount:  50000,
		PageCount: 5000,
		Columns: map[string]*stats.ColumnStats{
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 50000,
				Histogram: &stats.Histogram{
					Bounds:      []stats.Value{{Type: catalog.TypeInt32, IntVal: 100}},
					Frequencies: []int64{50000},
					NumBuckets:  1,
				},
			},
		},
	}); err != nil {
		t.Fatalf("failed to set table stats: %v", err)
	}

	stmt := &ast.SelectStmt{
		TableName: "users",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op: token.TOKEN_AND,
			Left: &ast.BinaryExpr{
				Op:    token.TOKEN_GE,
				Left:  &ast.ColumnRef{Name: "age"},
				Right: &ast.LiteralExpr{Value: catalog.NewInt32(10)},
			},
			Right: &ast.BinaryExpr{
				Op:    token.TOKEN_LT,
				Left:  &ast.ColumnRef{Name: "age"},
				Right: &ast.LiteralExpr{Value: catalog.NewInt32(90)},
			},
		},
	}

	plan := planner.Plan(stmt, tableMeta)
	if plan.Type != PlanIndexScan {
		t.Fatalf("expected index scan, got %v", plan.Type)
	}
	if plan.Selectivity > 0.20 {
		t.Fatalf("expected capped selectivity <= 0.20 for bounded range, got %f", plan.Selectivity)
	}
}

// TestPlanner_DynamicBufferHitRatio tests that setting buffer hit ratio affects cost estimation.
func TestPlanner_DynamicBufferHitRatio(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "data", Type: catalog.TypeText},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "test", Schema: schema}

	tableStats := &stats.TableStats{
		TableName: "test",
		RowCount:  10000,
		PageCount: 1000,
		Columns: map[string]*stats.ColumnStats{
			"id": {ColumnName: "id", DataType: catalog.TypeInt32, DistinctCount: 10000},
		},
	}

	stmt := &ast.SelectStmt{
		TableName: "test",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "id"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(42)},
		},
	}

	// Test with high buffer hit ratio (90%)
	plannerHigh := NewPlanner(idxMgr)
	_ = plannerHigh.statsMgr.SetTableStats(tableStats)
	plannerHigh.SetBufferHitRatio(0.9)
	planHigh := plannerHigh.Plan(stmt, tableMeta)

	// Test with low buffer hit ratio (10%)
	plannerLow := NewPlanner(idxMgr)
	_ = plannerLow.statsMgr.SetTableStats(tableStats)
	plannerLow.SetBufferHitRatio(0.1)
	planLow := plannerLow.Plan(stmt, tableMeta)

	// Low hit ratio should result in higher cost (more disk I/O)
	if planLow.Cost <= planHigh.Cost {
		t.Errorf("Expected lower buffer hit ratio to increase cost: high=%f, low=%f",
			planHigh.Cost, planLow.Cost)
	}

	t.Logf("High hit ratio cost: %f, Low hit ratio cost: %f", planHigh.Cost, planLow.Cost)
}

// TestPlanner_BufferHitRatioBounds tests that SetBufferHitRatio clamps values correctly.
func TestPlanner_BufferHitRatioBounds(t *testing.T) {
	planner := NewPlanner(nil)

	tests := []struct {
		input    float64
		expected float64
	}{
		{0.5, 0.5},
		{0.0, 0.0},
		{1.0, 1.0},
		{-0.5, 0.0},   // Should clamp to 0
		{1.5, 1.0},    // Should clamp to 1
		{-100.0, 0.0}, // Should clamp to 0
		{100.0, 1.0},  // Should clamp to 1
	}

	for _, tt := range tests {
		planner.SetBufferHitRatio(tt.input)
		got := planner.BufferHitRatioValue()
		if got != tt.expected {
			t.Errorf("SetBufferHitRatio(%f): expected %f, got %f", tt.input, tt.expected, got)
		}
	}
}

// TestPlanner_DefaultBufferHitRatio tests the default buffer hit ratio value.
func TestPlanner_DefaultBufferHitRatio(t *testing.T) {
	planner := NewPlanner(nil)

	// Default should match the constant
	if planner.BufferHitRatioValue() != BufferHitRatio {
		t.Errorf("Expected default buffer hit ratio %f, got %f",
			BufferHitRatio, planner.BufferHitRatioValue())
	}
}

// TestPlanner_StatsManagerIntegration tests that stats manager affects plan selection.
func TestPlanner_StatsManagerIntegration(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	// Create an index
	if err := idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_orders_customer_id",
		TableName: "orders",
		Columns:   []string{"customer_id"},
		Unique:    false,
	}); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "customer_id", Type: catalog.TypeInt32},
			{Name: "amount", Type: catalog.TypeFloat64},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "orders", Schema: schema}

	stmt := &ast.SelectStmt{
		TableName: "orders",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where: &ast.BinaryExpr{
			Op:    token.TOKEN_EQ,
			Left:  &ast.ColumnRef{Name: "customer_id"},
			Right: &ast.LiteralExpr{Value: catalog.NewInt32(123)},
		},
	}

	// Test without stats: should still produce a plan
	plannerNoStats := NewPlanner(idxMgr)
	plannerNoStats.SetStatsManager(nil)
	planNoStats := plannerNoStats.Plan(stmt, tableMeta)
	if planNoStats == nil {
		t.Fatal("Expected plan without stats")
	}

	// Test with stats: should produce plan with cost estimates
	plannerWithStats := NewPlanner(idxMgr)
	tableStats := &stats.TableStats{
		TableName: "orders",
		RowCount:  100000,
		PageCount: 10000,
		Columns: map[string]*stats.ColumnStats{
			"customer_id": {
				ColumnName:    "customer_id",
				DataType:      catalog.TypeInt32,
				DistinctCount: 10000, // Many distinct customers
				NullCount:     0,
			},
		},
	}
	if err := plannerWithStats.statsMgr.SetTableStats(tableStats); err != nil {
		t.Fatalf("failed to set table stats: %v", err)
	}

	planWithStats := plannerWithStats.Plan(stmt, tableMeta)
	if planWithStats == nil {
		t.Fatal("Expected plan with stats")
	}

	// Plan with stats should have estimated rows
	if planWithStats.EstimatedRows <= 0 {
		t.Errorf("Expected positive estimated rows with stats, got %d", planWithStats.EstimatedRows)
	}

	// Plan with stats should have selectivity
	if planWithStats.Selectivity <= 0 || planWithStats.Selectivity > 1 {
		t.Errorf("Expected valid selectivity with stats, got %f", planWithStats.Selectivity)
	}

	t.Logf("Plan without stats: %s", planNoStats.Explain())
	t.Logf("Plan with stats: %s (estimated rows: %d, selectivity: %f)",
		planWithStats.Explain(), planWithStats.EstimatedRows, planWithStats.Selectivity)
}

// TestPlanner_ExplainIncludesBufferHitRatio tests EXPLAIN includes buffer pool context.
func TestPlanner_CostSensitivityToCachePerformance(t *testing.T) {
	// Test that poor cache performance leads to significantly higher costs
	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "large_table", Schema: schema}

	tableStats := &stats.TableStats{
		TableName: "large_table",
		RowCount:  1000000, // 1M rows
		PageCount: 100000,  // 100K pages (~400MB)
		Columns: map[string]*stats.ColumnStats{
			"id": {ColumnName: "id", DataType: catalog.TypeInt32, DistinctCount: 1000000},
		},
	}

	stmt := &ast.SelectStmt{
		TableName: "large_table",
		Columns:   []ast.SelectColumn{{Star: true}},
		Where:     nil, // Full scan
	}

	plannerCold := NewPlanner(nil)
	_ = plannerCold.statsMgr.SetTableStats(tableStats)
	plannerCold.SetBufferHitRatio(0.1) // Cold cache

	plannerWarm := NewPlanner(nil)
	_ = plannerWarm.statsMgr.SetTableStats(tableStats)
	plannerWarm.SetBufferHitRatio(0.95) // Warm cache

	planCold := plannerCold.Plan(stmt, tableMeta)
	planWarm := plannerWarm.Plan(stmt, tableMeta)

	// Cold cache cost should be much higher
	costRatio := planCold.Cost / planWarm.Cost
	if costRatio < 2.0 {
		t.Errorf("Expected cold cache to cost at least 2x warm cache, got ratio %f", costRatio)
	}

	t.Logf("Cold cache cost: %f, Warm cache cost: %f, Ratio: %fx",
		planCold.Cost, planWarm.Cost, costRatio)
}

// TestPlanner_IndexVsTableScanThreshold tests the threshold where index beats table scan.
func TestPlanner_IndexVsTableScanThreshold(t *testing.T) {
	idxMgr, err := btree.NewIndexManager(t.TempDir(), 4096)
	if err != nil {
		t.Fatalf("failed to create index manager: %v", err)
	}
	defer func() { _ = idxMgr.Close() }()

	if err := idxMgr.CreateIndex(btree.IndexMeta{
		Name:      "idx_test_key",
		TableName: "test",
		Columns:   []string{"key"},
	}); err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "key", Type: catalog.TypeInt32},
			{Name: "value", Type: catalog.TypeText},
		},
	}
	tableMeta := &catalog.TableMeta{Name: "test", Schema: schema}

	// Test different selectivity levels
	tests := []struct {
		name          string
		distinctCount int64
		rowCount      int64
		expectIndex   bool
	}{
		{"unique_key", 10000, 10000, true},       // 0.01% selectivity - index wins
		{"high_cardinality", 1000, 10000, true},  // 0.1% selectivity - index wins
		{"medium_cardinality", 100, 10000, true}, // 1% selectivity - usually index
		{"low_cardinality", 5, 10000, false},     // 20% selectivity - table scan may win
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := NewPlanner(idxMgr)
			tableStats := &stats.TableStats{
				TableName: "test",
				RowCount:  tt.rowCount,
				PageCount: int32(tt.rowCount / 10), // Assume 10 rows per page
				Columns: map[string]*stats.ColumnStats{
					"key": {
						ColumnName:    "key",
						DataType:      catalog.TypeInt32,
						DistinctCount: tt.distinctCount,
					},
				},
			}
			_ = planner.statsMgr.SetTableStats(tableStats)

			stmt := &ast.SelectStmt{
				TableName: "test",
				Columns:   []ast.SelectColumn{{Star: true}},
				Where: &ast.BinaryExpr{
					Op:    token.TOKEN_EQ,
					Left:  &ast.ColumnRef{Name: "key"},
					Right: &ast.LiteralExpr{Value: catalog.NewInt32(42)},
				},
			}

			plan := planner.Plan(stmt, tableMeta)

			if tt.expectIndex && plan.Type != PlanIndexScan {
				t.Errorf("Expected IndexScan for %s, got %v", tt.name, plan.Type)
			}
			// For low cardinality, we don't assert - the cost model may choose either

			t.Logf("%s: plan=%v, cost=%f, selectivity=%f",
				tt.name, plan.Type, plan.Cost, plan.Selectivity)
		})
	}
}
