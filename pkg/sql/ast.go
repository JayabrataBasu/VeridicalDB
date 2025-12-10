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
	Distinct   bool // SELECT DISTINCT
	Columns    []SelectColumn
	TableName  string
	TableAlias string       // optional table alias (FROM table AS t)
	Joins      []JoinClause // JOIN clauses
	Where      Expression
	GroupBy    []string   // column names for GROUP BY
	Having     Expression // HAVING condition
	OrderBy    []OrderByClause
	Limit      *int64 // nil means no limit
	Offset     *int64 // nil means no offset
}

func (s *SelectStmt) statementNode() {}

// JoinClause represents a JOIN clause in SELECT.
type JoinClause struct {
	JoinType   string     // "INNER", "LEFT", "RIGHT"
	TableName  string     // table to join
	TableAlias string     // optional table alias
	Condition  Expression // ON condition
}

// SelectColumn represents a column in SELECT.
type SelectColumn struct {
	Star      bool           // true if *
	Name      string         // column name if not star
	Aggregate *AggregateFunc // aggregate function if present
	Alias     string         // optional alias (AS name)
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

// InExpr represents an IN expression (e.g., col IN (1, 2, 3)).
type InExpr struct {
	Left   Expression   // column or expression being tested
	Values []Expression // list of values to check against
	Not    bool         // true for NOT IN
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
