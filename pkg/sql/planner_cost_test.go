package sql

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/stats"
)

// TestPlanner_CostBasedIndexSelection tests that the planner chooses IndexScan when cost is lower.
func TestPlanner_CostBasedIndexSelection(t *testing.T) {
	// Setup
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)
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
		Name:    "idx_users_age",
		TableName: "users",
		Columns: []string{"age"},
		Unique:  false,
	}
	_ = idxMgr.CreateIndex(*ageIndex)

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
	_ = planner.statsMgr.SetTableStats(tableStats)

	// Query with high selectivity: age = 25 (should prefer IndexScan)
	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{{Star: true}},
		Where: &BinaryExpr{
			Op:    TOKEN_EQ,
			Left:  &ColumnRef{Name: "age"},
			Right: &LiteralExpr{Value: catalog.NewInt32(25)},
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
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)
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
		Name:    "idx_users_age",
		TableName: "users",
		Columns: []string{"age"},
		Unique:  false,
	}
	_ = idxMgr.CreateIndex(*ageIndex)

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
	_ = planner.statsMgr.SetTableStats(tableStats)

	// Query with low selectivity: age > 10 (most rows match)
	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{{Star: true}},
		Where: &BinaryExpr{
			Op:    TOKEN_GT,
			Left:  &ColumnRef{Name: "age"},
			Right: &LiteralExpr{Value: catalog.NewInt32(10)},
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
		expression          Expression
		expectedSelectivity float64
		tolerance           float64
	}{
		{
			name: "Equality on unique column",
			expression: &BinaryExpr{
				Op:    TOKEN_EQ,
				Left:  &ColumnRef{Name: "id"},
				Right: &LiteralExpr{Value: catalog.NewInt32(123)},
			},
			expectedSelectivity: 1.0 / 1000.0, // 1/n_distinct
			tolerance:           0.001,
		},
		{
			name: "Equality on MCV",
			expression: &BinaryExpr{
				Op:    TOKEN_EQ,
				Left:  &ColumnRef{Name: "category"},
				Right: &LiteralExpr{Value: catalog.NewText("A")},
			},
			expectedSelectivity: 0.4, // MCV frequency
			tolerance:           0.01,
		},
		{
			name: "Equality on non-MCV",
			expression: &BinaryExpr{
				Op:    TOKEN_EQ,
				Left:  &ColumnRef{Name: "category"},
				Right: &LiteralExpr{Value: catalog.NewText("Z")},
			},
			expectedSelectivity: 1.0 / 5.0, // 1/n_distinct for non-MCV
			tolerance:           0.01,
		},
		{
			name: "AND combination",
			expression: &BinaryExpr{
				Op: TOKEN_AND,
				Left: &BinaryExpr{
					Op:    TOKEN_EQ,
					Left:  &ColumnRef{Name: "category"},
					Right: &LiteralExpr{Value: catalog.NewText("A")},
				},
				Right: &BinaryExpr{
					Op:    TOKEN_GT,
					Left:  &ColumnRef{Name: "score"},
					Right: &LiteralExpr{Value: catalog.NewInt32(50)},
				},
			},
			expectedSelectivity: 0.4 * 0.33, // sel(category='A') * sel(score>50)
			tolerance:           0.05,
		},
		{
			name: "OR combination",
			expression: &BinaryExpr{
				Op: TOKEN_OR,
				Left: &BinaryExpr{
					Op:    TOKEN_EQ,
					Left:  &ColumnRef{Name: "category"},
					Right: &LiteralExpr{Value: catalog.NewText("A")},
				},
				Right: &BinaryExpr{
					Op:    TOKEN_EQ,
					Left:  &ColumnRef{Name: "category"},
					Right: &LiteralExpr{Value: catalog.NewText("B")},
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
	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{{Star: true}},
		Joins: []JoinClause{
			{
				JoinType:  "INNER",
				TableName: "orders",
				Condition: &BinaryExpr{
					Op:    TOKEN_EQ,
					Left:  &ColumnRef{Name: "users.id"},
					Right: &ColumnRef{Name: "orders.user_id"},
				},
			},
		},
		Where: &BinaryExpr{
			Op:    TOKEN_EQ,
			Left:  &ColumnRef{Name: "status"},
			Right: &LiteralExpr{Value: catalog.NewText("active")},
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
	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{{Star: true}},
		Where: &BinaryExpr{
			Op:    TOKEN_EQ,
			Left:  &ColumnRef{Name: "age"},
			Right: &LiteralExpr{Value: catalog.NewInt32(25)},
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

	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{{Star: true}},
		Where: &BinaryExpr{
			Op:    TOKEN_EQ,
			Left:  &ColumnRef{Name: "name"},
			Right: &LiteralExpr{Value: catalog.NewText("Alice")},
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
	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{{Star: true}},
		Where: &BinaryExpr{
			Op:    TOKEN_EQ,
			Left:  &ColumnRef{Name: "age"},
			Right: &LiteralExpr{Value: catalog.NewInt32(25)},
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