package sql

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
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
	idxMgr *btree.IndexManager
}

// NewPlanner creates a new query planner.
func NewPlanner(idxMgr *btree.IndexManager) *Planner {
	return &Planner{idxMgr: idxMgr}
}

// PlanType indicates the type of execution plan.
type PlanType int

const (
	PlanTableScan PlanType = iota
	PlanIndexScan
)

// ExecutionPlan represents a planned execution strategy.
type ExecutionPlan struct {
	Type      PlanType
	TableName string

	// For IndexScan plans
	IndexName string
	ScanKey   []byte
	ScanOp    TokenType // =, <, >, <=, >=

	// Remaining conditions to evaluate after scan
	RemainingWhere Expression
}

// Plan creates an execution plan for a SELECT statement.
func (p *Planner) Plan(stmt *SelectStmt, tableMeta *catalog.TableMeta) *ExecutionPlan {
	plan := &ExecutionPlan{
		Type:           PlanTableScan,
		TableName:      stmt.TableName,
		RemainingWhere: stmt.Where,
	}

	if p.idxMgr == nil || stmt.Where == nil {
		return plan
	}

	// Try to find an index scan opportunity
	indexInfo := p.findIndexForCondition(stmt.TableName, stmt.Where, tableMeta.Schema)
	if indexInfo != nil {
		plan.Type = PlanIndexScan
		plan.IndexName = indexInfo.IndexName
		plan.ScanKey = indexInfo.Key
		plan.ScanOp = indexInfo.Op
		// For now, we still need to verify all conditions
		// (the index only covers one predicate)
		plan.RemainingWhere = stmt.Where
	}

	return plan
}

// IndexInfo holds information about an index that can be used for a condition.
type IndexInfo struct {
	IndexName string
	Key       []byte
	Op        TokenType
	Column    string
}

// findIndexForCondition checks if an index can be used for a WHERE clause.
func (p *Planner) findIndexForCondition(tableName string, where Expression, schema *catalog.Schema) *IndexInfo {
	// Only handle simple binary expressions for now
	binExpr, ok := where.(*BinaryExpr)
	if !ok {
		return nil
	}

	// Check for equality: column = literal
	if binExpr.Op == TOKEN_EQ {
		return p.matchEqualityToIndex(tableName, binExpr, schema)
	}

	// TODO: Handle AND expressions - could match multiple predicates
	// TODO: Handle range conditions (<, >, <=, >=)

	return nil
}

// matchEqualityToIndex tries to match column = literal to an index.
func (p *Planner) matchEqualityToIndex(tableName string, expr *BinaryExpr, _ *catalog.Schema) *IndexInfo {
	var colName string
	var literal *LiteralExpr

	// Check both orderings: column = literal and literal = column
	if col, ok := expr.Left.(*ColumnRef); ok {
		if lit, ok := expr.Right.(*LiteralExpr); ok {
			colName = col.Name
			literal = lit
		}
	} else if col, ok := expr.Right.(*ColumnRef); ok {
		if lit, ok := expr.Left.(*LiteralExpr); ok {
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
			key, err := encodeValueForIndex(literal.Value)
			if err == nil {
				return &IndexInfo{
					IndexName: meta.Name,
					Key:       key,
					Op:        TOKEN_EQ,
					Column:    colName,
				}
			}
		}
	}

	return nil
}

// PlanExplain generates a human-readable explanation of the execution plan.
func (plan *ExecutionPlan) Explain() string {
	switch plan.Type {
	case PlanTableScan:
		return "TableScan on " + plan.TableName
	case PlanIndexScan:
		return "IndexScan on " + plan.IndexName + " (table: " + plan.TableName + ")"
	default:
		return "Unknown plan type"
	}
}
