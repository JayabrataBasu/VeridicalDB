package sql

import "github.com/JayabrataBasu/VeridicalDB/pkg/catalog"

// AST node types for SQL statements

// Statement is the interface for all SQL statements.
type Statement interface {
	statementNode()
}

// Expression is the interface for all SQL expressions.
type Expression interface {
	exprNode()
}

// ColumnDef represents a column definition in CREATE TABLE.
type ColumnDef struct {
	Name          string
	Type          catalog.DataType
	NotNull       bool
	PrimaryKey    bool
	HasDefault    bool
	Default       Expression // literal value or NULL
	AutoIncrement bool
	Check         Expression // CHECK constraint expression
	CheckExprStr  string     // Original CHECK expression string for storage
}

// CreateTableStmt represents CREATE TABLE statement.
type CreateTableStmt struct {
	TableName   string
	Columns     []ColumnDef
	StorageType string // "ROW" (default) or "COLUMN"
}

func (s *CreateTableStmt) statementNode() {}

// DropTableStmt represents DROP TABLE statement.
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) statementNode() {}

// CreateIndexStmt represents CREATE [UNIQUE] INDEX statement.
type CreateIndexStmt struct {
	IndexName string
	TableName string
	Columns   []string
	Unique    bool
}

func (s *CreateIndexStmt) statementNode() {}

// DropIndexStmt represents DROP INDEX statement.
type DropIndexStmt struct {
	IndexName string
}

func (s *DropIndexStmt) statementNode() {}

// InsertStmt represents INSERT INTO statement.
type InsertStmt struct {
	TableName string
	Columns   []string // optional column list
	Values    []Expression
}

func (s *InsertStmt) statementNode() {}

// SelectStmt represents SELECT statement.
type SelectStmt struct {
	Distinct     bool     // SELECT DISTINCT
	DistinctOn   []string // SELECT DISTINCT ON (col1, col2) - PostgreSQL style
	Columns      []SelectColumn
	TableName    string
	TableAlias   string       // optional table alias (FROM table AS t)
	Joins        []JoinClause // JOIN clauses
	Where        Expression
	GroupBy      []string      // column names for simple GROUP BY
	GroupingSets []GroupingSet // GROUPING SETS, CUBE, ROLLUP
	Having       Expression    // HAVING condition
	OrderBy      []OrderByClause
	Limit        *int64     // nil means no limit (static value)
	LimitExpr    Expression // LIMIT expression (for subqueries)
	Offset       *int64     // nil means no offset
}

func (s *SelectStmt) statementNode() {}

// GroupingSet represents a single grouping set (list of columns to group by together).
// For CUBE(a,b), this expands to: (), (a), (b), (a,b)
// For ROLLUP(a,b), this expands to: (a,b), (a), ()
type GroupingSet struct {
	Columns []string // columns in this grouping set (empty for grand total)
}

// GroupingSetType indicates how grouping sets are specified.
type GroupingSetType int

const (
	GroupingSetTypeSimple GroupingSetType = iota // Individual grouping set
	GroupingSetTypeCube                          // CUBE expansion
	GroupingSetTypeRollup                        // ROLLUP expansion
	GroupingSetTypeSets                          // GROUPING SETS explicit list
)

// JoinClause represents a JOIN clause in SELECT.
type JoinClause struct {
	JoinType   string      // "INNER", "LEFT", "RIGHT", "FULL", "CROSS"
	TableName  string      // table to join (for regular joins)
	TableAlias string      // optional table alias
	Condition  Expression  // ON condition (nil for CROSS JOIN)
	Lateral    bool        // true for LATERAL joins
	Subquery   *SelectStmt // subquery for derived table joins (especially LATERAL)
}

// SelectColumn represents a column in SELECT.
type SelectColumn struct {
	Star       bool           // true if *
	Name       string         // column name if not star
	Aggregate  *AggregateFunc // aggregate function if present
	Expression Expression     // general expression (CASE, arithmetic, functions, etc.)
	Alias      string         // optional alias (AS name)
}

// AggregateFunc represents an aggregate function call like COUNT(*), SUM(col).
type AggregateFunc struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Arg      string // column name or "*" for COUNT(*)
}

// OrderByClause represents an ORDER BY column with direction.
type OrderByClause struct {
	Column string
	Desc   bool // true for DESC, false for ASC (default)
}

// UpdateStmt represents UPDATE statement.
type UpdateStmt struct {
	TableName   string
	Assignments []Assignment
	Where       Expression
}

func (s *UpdateStmt) statementNode() {}

// Assignment represents SET column = value.
type Assignment struct {
	Column string
	Value  Expression
}

// DeleteStmt represents DELETE statement.
type DeleteStmt struct {
	TableName string
	Where     Expression
}

func (s *DeleteStmt) statementNode() {}

// Expressions

// LiteralExpr represents a literal value (int, string, bool, null).
type LiteralExpr struct {
	Value catalog.Value
}

func (e *LiteralExpr) exprNode() {}

// ColumnRef represents a column reference in an expression.
type ColumnRef struct {
	Name string
}

func (e *ColumnRef) exprNode() {}

// BinaryExpr represents a binary operation (e.g., a = b, a AND b).
type BinaryExpr struct {
	Left  Expression
	Op    TokenType
	Right Expression
}

func (e *BinaryExpr) exprNode() {}

// UnaryExpr represents a unary operation (e.g., NOT x).
type UnaryExpr struct {
	Op   TokenType
	Expr Expression
}

func (e *UnaryExpr) exprNode() {}

// InExpr represents an IN expression (e.g., col IN (1, 2, 3) or col IN (SELECT ...)).
type InExpr struct {
	Left     Expression   // column or expression being tested
	Values   []Expression // list of values to check against (if not subquery)
	Subquery *SelectStmt  // subquery to check against (if not values)
	Not      bool         // true for NOT IN
}

func (e *InExpr) exprNode() {}

// BetweenExpr represents a BETWEEN expression (e.g., col BETWEEN 1 AND 10).
type BetweenExpr struct {
	Expr Expression // expression being tested
	Low  Expression // lower bound
	High Expression // upper bound
	Not  bool       // true for NOT BETWEEN
}

func (e *BetweenExpr) exprNode() {}

// LikeExpr represents a LIKE/ILIKE expression (e.g., name LIKE 'A%').
type LikeExpr struct {
	Expr            Expression // expression being tested
	Pattern         Expression // pattern to match against
	CaseInsensitive bool       // true for ILIKE
	Not             bool       // true for NOT LIKE
}

func (e *LikeExpr) exprNode() {}

// FunctionExpr represents a function call (e.g., COALESCE(a, b), UPPER(name)).
type FunctionExpr struct {
	Name string       // function name (COALESCE, NULLIF, UPPER, etc.)
	Args []Expression // function arguments
}

func (e *FunctionExpr) exprNode() {}

// Transaction statements

// BeginStmt represents BEGIN statement.
type BeginStmt struct{}

func (s *BeginStmt) statementNode() {}

// CommitStmt represents COMMIT statement.
type CommitStmt struct{}

func (s *CommitStmt) statementNode() {}

// RollbackStmt represents ROLLBACK statement.
type RollbackStmt struct{}

func (s *RollbackStmt) statementNode() {}

// AlterTableStmt represents ALTER TABLE statement.
type AlterTableStmt struct {
	TableName  string
	Action     string     // "ADD COLUMN", "DROP COLUMN", "RENAME TO", "RENAME COLUMN"
	ColumnDef  *ColumnDef // for ADD COLUMN
	ColumnName string     // for DROP COLUMN or RENAME COLUMN (old name)
	NewName    string     // for RENAME TO or RENAME COLUMN (new name)
}

func (s *AlterTableStmt) statementNode() {}

// TruncateTableStmt represents TRUNCATE TABLE statement.
type TruncateTableStmt struct {
	TableName string
}

func (s *TruncateTableStmt) statementNode() {}

// ShowStmt represents SHOW statements (SHOW TABLES, SHOW CREATE TABLE).
type ShowStmt struct {
	ShowType  string // "TABLES", "CREATE TABLE"
	TableName string // for SHOW CREATE TABLE
}

func (s *ShowStmt) statementNode() {}

// ExplainStmt represents EXPLAIN statement for query plan visualization.
type ExplainStmt struct {
	Analyze   bool      // EXPLAIN ANALYZE runs the query and shows actual stats
	Statement Statement // The statement to explain (usually SELECT)
}

func (s *ExplainStmt) statementNode() {}

// CaseExpr represents a CASE WHEN expression.
// Supports both simple CASE (CASE expr WHEN val1 THEN res1 ... END)
// and searched CASE (CASE WHEN cond1 THEN res1 ... END).
type CaseExpr struct {
	// Operand is the expression after CASE (nil for searched CASE).
	// For simple CASE: CASE operand WHEN val1 THEN res1 ...
	Operand Expression

	// Whens is the list of WHEN clauses.
	Whens []WhenClause

	// Else is the ELSE expression (nil if no ELSE clause).
	Else Expression
}

func (e *CaseExpr) exprNode() {}

// WhenClause represents a single WHEN ... THEN ... clause.
type WhenClause struct {
	// Condition is the WHEN expression.
	// For simple CASE, this is compared with the operand.
	// For searched CASE, this is evaluated as a boolean.
	Condition Expression

	// Result is the THEN expression.
	Result Expression
}

// IsNullExpr represents IS NULL or IS NOT NULL expression.
type IsNullExpr struct {
	Expr Expression
	Not  bool // true for IS NOT NULL
}

func (e *IsNullExpr) exprNode() {}

// CastExpr represents CAST(expr AS type) expression.
type CastExpr struct {
	Expr       Expression
	TargetType catalog.DataType
}

func (e *CastExpr) exprNode() {}

// UnionStmt represents UNION/INTERSECT/EXCEPT operations.
type UnionStmt struct {
	Left    *SelectStmt // left SELECT
	Right   *SelectStmt // right SELECT
	Op      string      // "UNION", "INTERSECT", "EXCEPT"
	All     bool        // true for UNION ALL, etc.
	OrderBy []OrderByClause
	Limit   *int64
	Offset  *int64
}

func (s *UnionStmt) statementNode() {}

// CreateViewStmt represents CREATE VIEW statement.
type CreateViewStmt struct {
	ViewName  string
	Columns   []string    // optional column aliases
	Query     *SelectStmt // the SELECT that defines the view
	OrReplace bool        // CREATE OR REPLACE VIEW
}

func (s *CreateViewStmt) statementNode() {}

// DropViewStmt represents DROP VIEW statement.
type DropViewStmt struct {
	ViewName string
	IfExists bool
}

func (s *DropViewStmt) statementNode() {}

// SubqueryExpr represents a subquery in an expression context.
// Used for scalar subqueries: SELECT * FROM t WHERE x = (SELECT MAX(y) FROM t2)
// and subqueries in IN: SELECT * FROM t WHERE x IN (SELECT y FROM t2)
type SubqueryExpr struct {
	Query *SelectStmt
}

func (e *SubqueryExpr) exprNode() {}

// ExistsExpr represents an EXISTS subquery.
// SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)
type ExistsExpr struct {
	Query *SelectStmt
	Not   bool // true for NOT EXISTS
}

func (e *ExistsExpr) exprNode() {}

// WindowSpec defines the OVER clause specification for window functions.
type WindowSpec struct {
	PartitionBy []string        // PARTITION BY columns
	OrderBy     []OrderByClause // ORDER BY within the window
	// Frame specification (optional)
	FrameType  string // "ROWS" or "RANGE" (empty if not specified)
	FrameStart string // "UNBOUNDED PRECEDING", "CURRENT ROW", "n PRECEDING", etc.
	FrameEnd   string // "UNBOUNDED FOLLOWING", "CURRENT ROW", "n FOLLOWING", etc. (empty for single-bound)
}

// WindowFuncExpr represents a window function call.
// Examples:
//   - ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
//   - SUM(amount) OVER (PARTITION BY customer_id)
//   - LAG(value, 1) OVER (ORDER BY date)
type WindowFuncExpr struct {
	Function string       // ROW_NUMBER, RANK, DENSE_RANK, SUM, COUNT, AVG, etc.
	Args     []Expression // function arguments (e.g., column for SUM, offset for LAG)
	Over     *WindowSpec  // the OVER clause
}

func (e *WindowFuncExpr) exprNode() {}

// MergeStmt represents MERGE statement (SQL:2008).
// MERGE INTO target USING source ON condition
// WHEN MATCHED THEN UPDATE SET ...
// WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
type MergeStmt struct {
	TargetTable string            // target table to merge into
	TargetAlias string            // optional alias for target table
	SourceTable string            // source table name (mutually exclusive with SourceQuery)
	SourceQuery *SelectStmt       // source query (mutually exclusive with SourceTable)
	SourceAlias string            // alias for source table/query
	Condition   Expression        // ON condition for matching
	WhenClauses []MergeWhenClause // WHEN MATCHED/NOT MATCHED clauses
}

func (s *MergeStmt) statementNode() {}

// MergeWhenClause represents a WHEN clause in MERGE statement.
type MergeWhenClause struct {
	Matched   bool        // true for WHEN MATCHED, false for WHEN NOT MATCHED
	Condition Expression  // optional AND condition after MATCHED/NOT MATCHED
	Action    MergeAction // UPDATE, DELETE, or INSERT action
}

// MergeAction represents the action in a MERGE WHEN clause.
type MergeAction struct {
	ActionType  string       // "UPDATE", "DELETE", "INSERT", "DO NOTHING"
	Assignments []Assignment // SET assignments for UPDATE
	Columns     []string     // column names for INSERT
	Values      []Expression // values for INSERT
}
