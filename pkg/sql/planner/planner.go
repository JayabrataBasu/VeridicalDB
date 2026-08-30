package planner

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
	"github.com/JayabrataBasu/VeridicalDB/pkg/stats"
)

// Planner provides simple rule-based query optimization.
// This is a basic planner that uses rules rather than a cost model.
//
// Current rules:
// 1. Equality conditions on indexed columns use IndexScan
// 2. Everything else falls back to TableScan
//
// Future enhancements could add:
// - Range scans for comparison operators (<, >, <=, >=)
// - Multi-column index matching for composite conditions
// - Cost-based selection between multiple candidate indexes
// - Join ordering
type Planner struct {
	idxMgr         *btree.IndexManager
	statsMgr       *stats.StatsManager
	useStats       bool    // Enable/disable cost-based optimization
	bufferHitRatio float64 // Dynamic buffer pool hit ratio (0.0-1.0)
}

// NewPlanner creates a new query planner.
func NewPlanner(idxMgr *btree.IndexManager) *Planner {
	return &Planner{
		idxMgr:         idxMgr,
		statsMgr:       stats.NewStatsManager(),
		useStats:       true, // Enable cost-based optimization by default
		bufferHitRatio: BufferHitRatio,
	}
}

// SetStatsManager sets the statistics manager for cost-based optimization.
func (p *Planner) SetStatsManager(mgr *stats.StatsManager) {
	p.statsMgr = mgr
	p.useStats = mgr != nil
}

// SetBufferHitRatio sets the buffer pool hit ratio for cost estimation.
// This allows dynamic adjustment based on actual buffer pool statistics.
// ratio should be between 0.0 and 1.0 (e.g., 0.9 = 90% hit rate).
func (p *Planner) SetBufferHitRatio(ratio float64) {
	if ratio < 0.0 {
		ratio = 0.0
	} else if ratio > 1.0 {
		ratio = 1.0
	}
	p.bufferHitRatio = ratio
}

// BufferHitRatioValue returns the current buffer hit ratio used for cost estimation.
func (p *Planner) BufferHitRatioValue() float64 {
	return p.bufferHitRatio
}

// PlanType indicates the type of execution plan.
type PlanType int

const (
	PlanTableScan PlanType = iota
	PlanIndexScan
)

// Cost model constants for query planning.
const (
	// Page costs
	TableScanCostPerPage = 1.0  // Cost per page for sequential scan
	IndexScanCostPerPage = 0.75 // Cost per page for index scan (better cache locality)
	IndexLookupCost      = 0.1  // Fixed cost per index lookup

	// Row costs
	RowProcessingCost = 0.001 // Cost per row processed
	ConditionEvalCost = 0.001 // Cost per condition evaluation

	// Cache assumptions
	BufferHitRatio         = 0.9  // Assume 90% buffer pool hit ratio
	DiskPageCostMultiplier = 10.0 // Disk access 10x more expensive
)

// ExecutionPlan represents a planned execution strategy.
type ExecutionPlan struct {
	Type      PlanType
	TableName string
	Cost      float64 // Estimated cost of this plan

	// For IndexScan plans
	IndexName string
	IndexCol  string
	ScanKey   []byte          // For equality scans
	ScanOp    token.TokenType // =, <, >, <=, >=

	// For range scans
	StartKey       []byte // Lower bound (nil = unbounded)
	EndKey         []byte // Upper bound (nil = unbounded)
	StartInclusive bool   // Include start key in results
	EndInclusive   bool   // Include end key in results

	// Remaining conditions to evaluate after scan
	RemainingWhere ast.Expression

	// Optimization metadata
	EstimatedRows int64   // Estimated number of rows returned
	Selectivity   float64 // Selectivity of WHERE conditions
}

// Plan creates an execution plan for a SELECT statement using cost-based optimization.
func (p *Planner) Plan(stmt *ast.SelectStmt, tableMeta *catalog.TableMeta) *ExecutionPlan {
	// Generate candidate plans
	candidatePlans := p.generateCandidatePlans(stmt, tableMeta)

	// If cost-based optimization is disabled, use the first plan (rule-based fallback)
	if !p.useStats || p.statsMgr == nil || len(candidatePlans) == 0 {
		if len(candidatePlans) > 0 {
			return candidatePlans[0]
		}
		// Fallback: basic table scan
		return &ExecutionPlan{
			Type:           PlanTableScan,
			TableName:      stmt.TableName,
			RemainingWhere: stmt.Where,
			Cost:           1000.0, // High cost to encourage index usage
		}
	}

	// Cost-based selection: choose plan with lowest cost
	bestPlan := candidatePlans[0]
	for _, plan := range candidatePlans[1:] {
		if plan.Cost < bestPlan.Cost {
			bestPlan = plan
		}
	}

	return bestPlan
}

// generateCandidatePlans creates all possible execution plans and estimates their costs.
func (p *Planner) generateCandidatePlans(stmt *ast.SelectStmt, tableMeta *catalog.TableMeta) []*ExecutionPlan {
	var plans []*ExecutionPlan

	// Plan 1: Table scan
	tableScanPlan := &ExecutionPlan{
		Type:           PlanTableScan,
		TableName:      stmt.TableName,
		RemainingWhere: stmt.Where,
	}
	p.estimateCost(tableScanPlan, tableMeta, stmt.Where)
	plans = append(plans, tableScanPlan)

	// Plan 2+: Index scans (if indexes available)
	if p.idxMgr != nil && stmt.Where != nil {
		indexPlans := p.generateIndexPlans(stmt, tableMeta)
		plans = append(plans, indexPlans...)
	}

	return plans
}

// generateIndexPlans creates index scan plans for all applicable indexes.
func (p *Planner) generateIndexPlans(stmt *ast.SelectStmt, tableMeta *catalog.TableMeta) []*ExecutionPlan {
	var plans []*ExecutionPlan

	// Find all index opportunities
	indexInfos := p.findAllIndexesForCondition(stmt.TableName, stmt.Where, tableMeta.Schema)

	for _, indexInfo := range indexInfos {
		plan := &ExecutionPlan{
			Type:           PlanIndexScan,
			TableName:      stmt.TableName,
			IndexName:      indexInfo.IndexName,
			IndexCol:       indexInfo.Column,
			ScanKey:        indexInfo.Key,
			ScanOp:         indexInfo.Op,
			RemainingWhere: stmt.Where,
		}
		p.estimateCost(plan, tableMeta, stmt.Where)
		plans = append(plans, plan)
	}

	return plans
}

// IndexInfo holds information about an index that can be used for a condition.
type IndexInfo struct {
	IndexName string
	Key       []byte
	Op        token.TokenType
	Column    string
}

// estimateCost calculates the estimated cost for an execution plan.
func (p *Planner) estimateCost(plan *ExecutionPlan, tableMeta *catalog.TableMeta, where ast.Expression) {
	if p.statsMgr == nil {
		p.estimateCostWithoutStats(plan, tableMeta, where)
		return
	}

	// Get table statistics if available
	tableStats, err := p.statsMgr.GetTableStats(plan.TableName)
	if err != nil {
		// No statistics - use fallback estimates
		p.estimateCostWithoutStats(plan, tableMeta, where)
		return
	}

	// Calculate selectivity of WHERE clause
	selectivity := p.estimateSelectivity(where, tableStats, tableMeta.Schema)
	if uniqueSel, ok := p.uniqueEqualitySelectivity(plan, where, tableMeta.Schema, tableStats.RowCount); ok {
		selectivity = uniqueSel
	} else if rangeCap, ok := p.rangeIndexSelectivityCap(plan, where); ok && selectivity > rangeCap {
		selectivity = rangeCap
	}
	plan.Selectivity = selectivity
	plan.EstimatedRows = int64(float64(tableStats.RowCount) * selectivity)
	if selectivity > 0 && plan.EstimatedRows < 1 {
		plan.EstimatedRows = 1
	}

	switch plan.Type {
	case PlanTableScan:
		// Cost = (pages_to_read * page_cost * cache_miss_factor) + (rows * row_processing_cost)
		pageReadCost := float64(tableStats.PageCount) * TableScanCostPerPage

		// Apply cache factor using dynamic buffer hit ratio
		cacheAdjustedCost := pageReadCost * (p.bufferHitRatio + (1.0-p.bufferHitRatio)*DiskPageCostMultiplier)

		// Add row processing cost
		rowProcessCost := float64(tableStats.RowCount) * RowProcessingCost

		// Add condition evaluation cost for filtered rows
		conditionCost := float64(plan.EstimatedRows) * ConditionEvalCost

		plan.Cost = cacheAdjustedCost + rowProcessCost + conditionCost

	case PlanIndexScan:
		// Index scan cost = index_lookup + (filtered_pages * index_scan_cost) + (rows * row_processing_cost)
		indexLookupCost := IndexLookupCost

		// Estimate pages needed for index scan (based on selectivity)
		estimatedPages := int32(float64(tableStats.PageCount) * selectivity)
		if estimatedPages < 1 {
			estimatedPages = 1
		}

		indexScanCost := float64(estimatedPages) * IndexScanCostPerPage
		cacheAdjustedScanCost := indexScanCost * (p.bufferHitRatio + (1.0-p.bufferHitRatio)*DiskPageCostMultiplier)

		rowProcessCost := float64(plan.EstimatedRows) * RowProcessingCost
		conditionCost := float64(plan.EstimatedRows) * ConditionEvalCost

		plan.Cost = indexLookupCost + cacheAdjustedScanCost + rowProcessCost + conditionCost
	}
}

func (p *Planner) uniqueEqualitySelectivity(plan *ExecutionPlan, where ast.Expression, schema *catalog.Schema, rowCount int64) (float64, bool) {
	if plan == nil || plan.Type != PlanIndexScan || plan.ScanOp != token.TOKEN_EQ {
		return 0, false
	}
	if schema == nil || plan.IndexCol == "" {
		return 0, false
	}
	if !isLiteralEqualityOnColumn(where, plan.IndexCol) {
		return 0, false
	}
	if !isUniqueSchemaColumn(schema, plan.IndexCol) {
		return 0, false
	}
	if rowCount <= 0 {
		return 0.001, true
	}
	return 1.0 / float64(rowCount), true
}

func isUniqueSchemaColumn(schema *catalog.Schema, colName string) bool {
	col, _ := schema.ColumnByName(colName)
	if col == nil {
		return false
	}
	return col.PrimaryKey || col.Unique
}

func isLiteralEqualityOnColumn(where ast.Expression, colName string) bool {
	if where == nil {
		return false
	}

	switch expr := where.(type) {
	case *ast.BinaryExpr:
		if expr.Op == token.TOKEN_AND {
			return isLiteralEqualityOnColumn(expr.Left, colName) || isLiteralEqualityOnColumn(expr.Right, colName)
		}
		if expr.Op != token.TOKEN_EQ {
			return false
		}

		if colRef, ok := expr.Left.(*ast.ColumnRef); ok {
			if _, ok := expr.Right.(*ast.LiteralExpr); ok {
				return strings.EqualFold(colRef.Name, colName)
			}
		}
		if colRef, ok := expr.Right.(*ast.ColumnRef); ok {
			if _, ok := expr.Left.(*ast.LiteralExpr); ok {
				return strings.EqualFold(colRef.Name, colName)
			}
		}
	}

	return false
}

func (p *Planner) rangeIndexSelectivityCap(plan *ExecutionPlan, where ast.Expression) (float64, bool) {
	if plan == nil || plan.Type != PlanIndexScan || plan.IndexCol == "" {
		return 0, false
	}

	hasRange, hasLower, hasUpper, hasStrict := collectIndexedRangePredicates(where, plan.IndexCol)
	if !hasRange {
		return 0, false
	}

	// Cap selectivity for indexed range predicates to avoid overly pessimistic estimates
	// from sparse or noisy stats and to keep range index plans competitive.
	if hasLower && hasUpper {
		return 0.20, true
	}
	if hasStrict {
		return 0.35, true
	}
	return 0.45, true
}

func collectIndexedRangePredicates(where ast.Expression, colName string) (hasRange bool, hasLower bool, hasUpper bool, hasStrict bool) {
	if where == nil {
		return false, false, false, false
	}

	switch expr := where.(type) {
	case *ast.BinaryExpr:
		if expr.Op == token.TOKEN_AND {
			lHasRange, lHasLower, lHasUpper, lHasStrict := collectIndexedRangePredicates(expr.Left, colName)
			rHasRange, rHasLower, rHasUpper, rHasStrict := collectIndexedRangePredicates(expr.Right, colName)
			return lHasRange || rHasRange, lHasLower || rHasLower, lHasUpper || rHasUpper, lHasStrict || rHasStrict
		}

		refCol, op, ok := comparisonOnColumnLiteral(expr)
		if !ok || !strings.EqualFold(refCol, colName) {
			return false, false, false, false
		}

		switch op {
		case token.TOKEN_GT:
			return true, true, false, true
		case token.TOKEN_GE:
			return true, true, false, false
		case token.TOKEN_LT:
			return true, false, true, true
		case token.TOKEN_LE:
			return true, false, true, false
		}
	}

	return false, false, false, false
}

func comparisonOnColumnLiteral(expr *ast.BinaryExpr) (colName string, op token.TokenType, ok bool) {
	if expr == nil {
		return "", token.TOKEN_ILLEGAL, false
	}

	if col, isCol := expr.Left.(*ast.ColumnRef); isCol {
		if _, isLit := expr.Right.(*ast.LiteralExpr); isLit {
			return col.Name, expr.Op, true
		}
	}

	if col, isCol := expr.Right.(*ast.ColumnRef); isCol {
		if _, isLit := expr.Left.(*ast.LiteralExpr); isLit {
			return col.Name, flipComparisonOp(expr.Op), true
		}
	}

	return "", token.TOKEN_ILLEGAL, false
}

// estimateCostWithoutStats provides fallback cost estimation when statistics are unavailable.
func (p *Planner) estimateCostWithoutStats(plan *ExecutionPlan, tableMeta *catalog.TableMeta, where ast.Expression) {
	// Rough estimates without statistics
	_ = tableMeta                  // unused, but kept for function signature compatibility
	assumedRowCount := int64(1000) // Assume 1K rows
	assumedPageCount := int32(100) // Assume 100 pages
	assumedSelectivity := 0.1      // Assume 10% selectivity for WHERE clauses

	if where == nil {
		assumedSelectivity = 1.0
	}

	plan.Selectivity = assumedSelectivity
	plan.EstimatedRows = int64(float64(assumedRowCount) * assumedSelectivity)

	switch plan.Type {
	case PlanTableScan:
		plan.Cost = float64(assumedPageCount)*TableScanCostPerPage + float64(assumedRowCount)*RowProcessingCost
	case PlanIndexScan:
		estimatedPages := int32(float64(assumedPageCount) * assumedSelectivity)
		if estimatedPages < 1 {
			estimatedPages = 1
		}
		plan.Cost = IndexLookupCost + float64(estimatedPages)*IndexScanCostPerPage + float64(plan.EstimatedRows)*RowProcessingCost
	}
}

// estimateSelectivity calculates the selectivity of a WHERE expression using statistics.
func (p *Planner) estimateSelectivity(where ast.Expression, tableStats *stats.TableStats, schema *catalog.Schema) float64 {
	if where == nil {
		return 1.0
	}

	switch expr := where.(type) {
	case *ast.BinaryExpr:
		switch expr.Op {
		case token.TOKEN_AND:
			// Assume independence: sel(A AND B) = sel(A) * sel(B)
			leftSel := p.estimateSelectivity(expr.Left, tableStats, schema)
			rightSel := p.estimateSelectivity(expr.Right, tableStats, schema)
			return leftSel * rightSel

		case token.TOKEN_OR:
			// sel(A OR B) = sel(A) + sel(B) - sel(A) * sel(B)
			leftSel := p.estimateSelectivity(expr.Left, tableStats, schema)
			rightSel := p.estimateSelectivity(expr.Right, tableStats, schema)
			return leftSel + rightSel - (leftSel * rightSel)

		case token.TOKEN_EQ, token.TOKEN_NE, token.TOKEN_LT, token.TOKEN_GT, token.TOKEN_LE, token.TOKEN_GE:
			return p.estimateComparisonSelectivity(expr, tableStats, schema)
		}

	case *ast.UnaryExpr:
		if expr.Op == token.TOKEN_NOT {
			// NOT: 1 - selectivity of inner expression
			innerSel := p.estimateSelectivity(expr.Expr, tableStats, schema)
			return 1.0 - innerSel
		}
	}

	// Default fallback for unknown expressions
	return 0.1
}

// estimateComparisonSelectivity estimates selectivity for comparison operators.
func (p *Planner) estimateComparisonSelectivity(expr *ast.BinaryExpr, tableStats *stats.TableStats, schema *catalog.Schema) float64 {
	// Extract column and value from comparison
	_ = schema // unused, but kept for function signature compatibility
	var colName string
	var value *ast.LiteralExpr

	if col, ok := expr.Left.(*ast.ColumnRef); ok {
		if lit, ok := expr.Right.(*ast.LiteralExpr); ok {
			colName = col.Name
			value = lit
		}
	} else if col, ok := expr.Right.(*ast.ColumnRef); ok {
		if lit, ok := expr.Left.(*ast.LiteralExpr); ok {
			colName = col.Name
			value = lit
			// Note: for flipped expressions, we'd need to flip the operator
		}
	}

	if colName == "" || value == nil {
		// Can't analyze - use defaults
		switch expr.Op {
		case token.TOKEN_EQ:
			return 0.1
		case token.TOKEN_NE:
			return 0.9
		case token.TOKEN_LT, token.TOKEN_GT:
			return 0.33
		case token.TOKEN_LE, token.TOKEN_GE:
			return 0.5
		}
	}

	// Look up column statistics
	colStats, exists := tableStats.Columns[colName]
	if !exists {
		return 0.1 // Fallback
	}

	// Convert literal value to stats.Value
	statsValue := convertLiteralToStatsValue(value)

	// Use column statistics to estimate selectivity
	opStr := tokenToString(expr.Op)
	return colStats.EstimateSelectivity(opStr, statsValue)
}

// convertLiteralToStatsValue converts a LiteralExpr to stats.Value.
func convertLiteralToStatsValue(lit *ast.LiteralExpr) stats.Value {
	val := lit.Value
	switch val.Type {
	case catalog.TypeInt32:
		return stats.Value{Type: catalog.TypeInt32, IntVal: int64(val.Int32), IsNull: val.IsNull}
	case catalog.TypeInt64:
		return stats.Value{Type: catalog.TypeInt64, IntVal: val.Int64, IsNull: val.IsNull}
	case catalog.TypeFloat64:
		return stats.Value{Type: catalog.TypeFloat64, FloatVal: val.Float64, IsNull: val.IsNull}
	case catalog.TypeText:
		return stats.Value{Type: catalog.TypeText, StringVal: val.Text, IsNull: val.IsNull}
	case catalog.TypeBool:
		return stats.Value{Type: catalog.TypeBool, BoolVal: val.Bool, IsNull: val.IsNull}
	default:
		return stats.Value{IsNull: true}
	}
}

// tokenToString converts token.TokenType to string for statistics.
func tokenToString(tt token.TokenType) string {
	switch tt {
	case token.TOKEN_EQ:
		return "="
	case token.TOKEN_NE:
		return "!="
	case token.TOKEN_LT:
		return "<"
	case token.TOKEN_GT:
		return ">"
	case token.TOKEN_LE:
		return "<="
	case token.TOKEN_GE:
		return ">="
	default:
		return "="
	}
}

// findAllIndexesForCondition finds all indexes that could be used for a WHERE clause.
func (p *Planner) findAllIndexesForCondition(tableName string, where ast.Expression, schema *catalog.Schema) []*IndexInfo {
	var indexInfos []*IndexInfo

	if where == nil {
		return indexInfos
	}

	// For now, use the existing single-index logic but collect all matches
	if indexInfo := p.findIndexForCondition(tableName, where, schema); indexInfo != nil {
		indexInfos = append(indexInfos, indexInfo)
	}

	return indexInfos
}

// findIndexForCondition checks if an index can be used for a WHERE clause.
func (p *Planner) findIndexForCondition(tableName string, where ast.Expression, schema *catalog.Schema) *IndexInfo {
	if where == nil {
		return nil
	}

	// Handle AND expressions recursively
	if binExpr, ok := where.(*ast.BinaryExpr); ok && binExpr.Op == token.TOKEN_AND {
		if info := p.findIndexForCondition(tableName, binExpr.Left, schema); info != nil {
			return info
		}
		return p.findIndexForCondition(tableName, binExpr.Right, schema)
	}

	// Only handle simple binary expressions for now
	binExpr, ok := where.(*ast.BinaryExpr)
	if !ok {
		return nil
	}

	// Check for equality: column = literal
	if binExpr.Op == token.TOKEN_EQ {
		return p.matchEqualityToIndex(tableName, binExpr, schema)
	}

	// Check for range conditions: <, >, <=, >=
	if binExpr.Op == token.TOKEN_LT || binExpr.Op == token.TOKEN_GT ||
		binExpr.Op == token.TOKEN_LE || binExpr.Op == token.TOKEN_GE {
		return p.matchRangeToIndex(tableName, binExpr, schema)
	}

	return nil
}

// matchRangeToIndex tries to match column <op> literal to an index for range scans.
func (p *Planner) matchRangeToIndex(tableName string, expr *ast.BinaryExpr, _ *catalog.Schema) *IndexInfo {
	var colName string
	var literal *ast.LiteralExpr
	op := expr.Op

	// Check both orderings: column <op> literal and literal <op> column
	if col, ok := expr.Left.(*ast.ColumnRef); ok {
		if lit, ok := expr.Right.(*ast.LiteralExpr); ok {
			colName = col.Name
			literal = lit
			// op stays the same: column < 10 means op is <
		}
	} else if col, ok := expr.Right.(*ast.ColumnRef); ok {
		if lit, ok := expr.Left.(*ast.LiteralExpr); ok {
			colName = col.Name
			literal = lit
			// Flip the operator: 10 < column means column > 10
			op = flipComparisonOp(op)
		}
	}

	if colName == "" || literal == nil {
		return nil
	}

	// Search for an index on this column
	indexes := p.idxMgr.ListIndexes(tableName)
	for _, meta := range indexes {
		// For now, only single-column indexes
		if len(meta.Columns) == 1 && strings.EqualFold(meta.Columns[0], colName) {
			key, err := EncodeValueForIndex(literal.Value)
			if err == nil {
				return &IndexInfo{
					IndexName: meta.Name,
					Key:       key,
					Op:        op,
					Column:    colName,
				}
			}
		}
	}

	return nil
}

// flipComparisonOp flips a comparison operator (for when literal is on left side).
func flipComparisonOp(op token.TokenType) token.TokenType {
	switch op {
	case token.TOKEN_LT:
		return token.TOKEN_GT
	case token.TOKEN_GT:
		return token.TOKEN_LT
	case token.TOKEN_LE:
		return token.TOKEN_GE
	case token.TOKEN_GE:
		return token.TOKEN_LE
	default:
		return op
	}
}

// matchEqualityToIndex tries to match column = literal to an index.
func (p *Planner) matchEqualityToIndex(tableName string, expr *ast.BinaryExpr, _ *catalog.Schema) *IndexInfo {
	var colName string
	var literal *ast.LiteralExpr

	// Check both orderings: column = literal and literal = column
	if col, ok := expr.Left.(*ast.ColumnRef); ok {
		if lit, ok := expr.Right.(*ast.LiteralExpr); ok {
			colName = col.Name
			literal = lit
		}
	} else if col, ok := expr.Right.(*ast.ColumnRef); ok {
		if lit, ok := expr.Left.(*ast.LiteralExpr); ok {
			colName = col.Name
			literal = lit
		}
	}

	if colName == "" || literal == nil {
		return nil
	}

	// Search for an index on this column
	indexes := p.idxMgr.ListIndexes(tableName)
	for _, meta := range indexes {
		// For now, only single-column indexes
		if len(meta.Columns) == 1 && strings.EqualFold(meta.Columns[0], colName) {
			key, err := EncodeValueForIndex(literal.Value)
			if err == nil {
				return &IndexInfo{
					IndexName: meta.Name,
					Key:       key,
					Op:        token.TOKEN_EQ,
					Column:    colName,
				}
			}
		}
	}

	return nil
}

// PlanExplain generates a human-readable explanation of the execution plan.
func (plan *ExecutionPlan) Explain() string {
	costInfo := fmt.Sprintf(" (cost=%.2f, rows=%d, selectivity=%.2f)", plan.Cost, plan.EstimatedRows, plan.Selectivity)

	switch plan.Type {
	case PlanTableScan:
		return "TableScan on " + plan.TableName + costInfo
	case PlanIndexScan:
		return "IndexScan on " + plan.IndexName + " (table: " + plan.TableName + ")" + costInfo
	default:
		return "Unknown plan type" + costInfo
	}
}

// EncodeValueForIndex encodes a catalog.Value for use as an index key.
func EncodeValueForIndex(v catalog.Value) ([]byte, error) {
	if v.IsNull {
		return []byte{0x00}, nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return btree.EncodeIntKey(int64(v.Int32)), nil
	case catalog.TypeInt64:
		return btree.EncodeIntKey(v.Int64), nil
	case catalog.TypeBool:
		if v.Bool {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case catalog.TypeText:
		return append([]byte{0x01}, []byte(v.Text)...), nil
	default:
		return nil, fmt.Errorf("unsupported type for index: %v", v.Type)
	}
}
