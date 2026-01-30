package sql

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/stats"
)

// BenchmarkPlanner_CostEstimation benchmarks the cost estimation functionality.
func BenchmarkPlanner_CostEstimation(b *testing.B) {
	planner, tableMeta, stmt := setupPlannerBenchmark()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planner.Plan(stmt, tableMeta)
	}
}

// BenchmarkPlanner_JoinOrdering benchmarks join ordering with multiple tables.
func BenchmarkPlanner_JoinOrdering(b *testing.B) {
	planner, tableMeta, _ := setupPlannerBenchmark()

	// Create a 3-way join query
	joinStmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{
			{Star: true},
		},
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
			{
				JoinType:  "INNER",
				TableName: "products",
				Condition: &BinaryExpr{
					Op:    TOKEN_EQ,
					Left:  &ColumnRef{Name: "orders.product_id"},
					Right: &ColumnRef{Name: "products.id"},
				},
			},
		},
		Where: &BinaryExpr{
			Op:    TOKEN_AND,
			Left: &BinaryExpr{
				Op:    TOKEN_EQ,
				Left:  &ColumnRef{Name: "users.status"},
				Right: &LiteralExpr{Value: catalog.NewText("active")},
			},
			Right: &BinaryExpr{
				Op:    TOKEN_GT,
				Left:  &ColumnRef{Name: "orders.amount"},
				Right: &LiteralExpr{Value: catalog.NewFloat64(100.0)},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planner.Plan(joinStmt, tableMeta)
	}
}

// BenchmarkPlanner_RuleBasedVsCostBased compares rule-based vs cost-based planning.
func BenchmarkPlanner_RuleBasedVsCostBased(b *testing.B) {
	planner, tableMeta, stmt := setupPlannerBenchmark()

	b.Run("CostBased", func(b *testing.B) {
		planner.useStats = true
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = planner.Plan(stmt, tableMeta)
		}
	})

	b.Run("RuleBased", func(b *testing.B) {
		planner.useStats = false
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = planner.Plan(stmt, tableMeta)
		}
	})
}

// BenchmarkPlanner_SelectivityEstimation benchmarks selectivity estimation.
func BenchmarkPlanner_SelectivityEstimation(b *testing.B) {
	planner, _, _ := setupPlannerBenchmark()

	// Create table stats with various column statistics
	tableStats := &stats.TableStats{
		TableName: "users",
		RowCount:  100000,
		PageCount: 1000,
		Columns: map[string]*stats.ColumnStats{
			"id": {
				ColumnName:    "id",
				DataType:      catalog.TypeInt32,
				DistinctCount: 100000,
				NullCount:     0,
			},
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 80,
				NullCount:     100,
			},
			"status": {
				ColumnName:      "status",
				DataType:        catalog.TypeText,
				DistinctCount:   3,
				NullCount:       0,
				MostCommonVals:  []stats.Value{{Type: catalog.TypeText, StringVal: "active"}},
				MostCommonFreqs: []float64{0.7},
			},
		},
	}

	schema := &catalog.Schema{
Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "age", Type: catalog.TypeInt32},
			{Name: "status", Type: catalog.TypeText},
		},
	}

	// Test expression: WHERE age > 25 AND status = 'active'
	whereExpr := &BinaryExpr{
		Op: TOKEN_AND,
		Left: &BinaryExpr{
			Op:    TOKEN_GT,
			Left:  &ColumnRef{Name: "age"},
			Right: &LiteralExpr{Value: catalog.NewInt32(25)},
		},
		Right: &BinaryExpr{
			Op:    TOKEN_EQ,
			Left:  &ColumnRef{Name: "status"},
			Right: &LiteralExpr{Value: catalog.NewText("active")},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planner.estimateSelectivity(whereExpr, tableStats, schema)
	}
}

// setupPlannerBenchmark creates a planner setup for benchmarking.
func setupPlannerBenchmark() (*Planner, *catalog.TableMeta, *SelectStmt) {
	// Create index manager
	idxMgr, _ := btree.NewIndexManager(":memory:", 4096)

	// Create planner
	planner := NewPlanner(idxMgr)

	// Create table metadata
	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32, PrimaryKey: true},
			{Name: "name", Type: catalog.TypeText},
			{Name: "age", Type: catalog.TypeInt32},
			{Name: "status", Type: catalog.TypeText},
		},
	}

	tableMeta := &catalog.TableMeta{
		Name:   "users",
		Schema: schema,
	}

	// Create sample statistics
	tableStats := &stats.TableStats{
		TableName: "users",
		RowCount:  10000,
		PageCount: 100,
		Columns: map[string]*stats.ColumnStats{
			"id": {
				ColumnName:    "id",
				DataType:      catalog.TypeInt32,
				DistinctCount: 10000,
				NullCount:     0,
			},
			"age": {
				ColumnName:    "age",
				DataType:      catalog.TypeInt32,
				DistinctCount: 70,
				NullCount:     50,
			},
			"status": {
				ColumnName:      "status",
				DataType:        catalog.TypeText,
				DistinctCount:   5,
				NullCount:       0,
				MostCommonVals:  []stats.Value{{Type: catalog.TypeText, StringVal: "active"}},
				MostCommonFreqs: []float64{0.6},
			},
		},
	}

	// Set statistics in planner
	_ = planner.statsMgr.SetTableStats(tableStats)

	// Create a test query: SELECT * FROM users WHERE age > 25
	stmt := &SelectStmt{
		TableName: "users",
		Columns: []SelectColumn{
			{Star: true},
		},
		Where: &BinaryExpr{
			Op:    TOKEN_GT,
			Left:  &ColumnRef{Name: "age"},
			Right: &LiteralExpr{Value: catalog.NewInt32(25)},
		},
	}

	// Create an index on age column for testing
	ageIndex := &btree.IndexMeta{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Unique:    false,
		Type:      btree.IndexTypeBTree,
	}
	_ = idxMgr.CreateIndex(*ageIndex)

	return planner, tableMeta, stmt
}