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
	Name             string
	Type             catalog.DataType
	NotNull          bool
	PrimaryKey       bool
	HasDefault       bool
	Default          Expression // literal value or NULL
	AutoIncrement    bool
	Check            Expression // CHECK constraint expression
	CheckExprStr     string     // Original CHECK expression string for storage
	ReferencesTable  string     // Referenced table for inline FK
	ReferencesColumn string     // Referenced column for inline FK
}

// ForeignKeyDef represents a foreign key constraint.
type ForeignKeyDef struct {
	ConstraintName string   // Optional constraint name
	Columns        []string // Columns in the current table
	RefTable       string   // Referenced table name
	RefColumns     []string // Referenced columns in the other table
}

// PartitionType represents the type of partitioning.
type PartitionType int

const (
	PartitionNone PartitionType = iota
	PartitionRange
	PartitionList
	PartitionHash
)

// PartitionBoundDef represents partition boundary definition.
type PartitionBoundDef struct {
	// For RANGE: less than value
	LessThan   Expression
	IsMaxValue bool
	// For LIST: list of values
	Values []Expression
}

// PartitionDef represents a partition definition.
type PartitionDef struct {
	Name  string
	Bound PartitionBoundDef
}

// PartitionSpec represents the partition specification in CREATE TABLE.
type PartitionSpec struct {
	Type       PartitionType
	Columns    []string
	Partitions []PartitionDef
	NumBuckets int // For HASH partitioning
}

// CreateTableStmt represents CREATE TABLE statement.
type CreateTableStmt struct {
	TableName     string
	Columns       []ColumnDef
	ForeignKeys   []ForeignKeyDef
	StorageType   string         // "ROW" (default) or "COLUMN"
	PartitionSpec *PartitionSpec // Optional partition specification
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
	TableName  string
	Columns    []string          // optional column list
	ValuesList [][]Expression    // multiple rows: INSERT INTO t VALUES (...), (...), ...
	Select     *SelectStmt       // INSERT INTO t SELECT ...
	OnConflict *OnConflictClause // optional ON CONFLICT clause
}

// OnConflictClause represents ON CONFLICT ... DO UPDATE/NOTHING
type OnConflictClause struct {
	ConflictColumns []string     // columns that define the conflict (usually PK or unique)
	DoNothing       bool         // ON CONFLICT DO NOTHING
	UpdateSet       []Assignment // ON CONFLICT DO UPDATE SET ...
}

func (s *InsertStmt) statementNode() {}

// SelectStmt represents SELECT statement.
type SelectStmt struct {
	With         *WithClause // CTE definitions (WITH clause)
	Distinct     bool        // SELECT DISTINCT
	DistinctOn   []string    // SELECT DISTINCT ON (col1, col2) - PostgreSQL style
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
	TableAlias  string // optional alias for target table
	Assignments []Assignment
	FromTable   string // UPDATE ... FROM table (PostgreSQL style)
	FromAlias   string // alias for FROM table
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
	TableName  string
	TableAlias string // optional alias for target table
	UsingTable string // DELETE ... USING table (PostgreSQL style)
	UsingAlias string // alias for USING table
	Where      Expression
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

// JSONAccessExpr represents JSON field access operators (-> and ->>).
// Examples: data->'name', data->>'name'
type JSONAccessExpr struct {
	Object Expression // The JSON object/column
	Key    Expression // The key (string literal or expression)
	AsText bool       // true for ->> (returns text), false for -> (returns JSON)
}

func (e *JSONAccessExpr) exprNode() {}

// JSONPathExpr represents JSON path access operators (#> and #>>).
// Examples: data#>'{a,b}', data#>>'{a,b}'
type JSONPathExpr struct {
	Object Expression   // The JSON object/column
	Path   []Expression // Array of path elements
	AsText bool         // true for #>> (returns text), false for #> (returns JSON)
}

func (e *JSONPathExpr) exprNode() {}

// JSONContainsExpr represents JSON containment operators (@> and <@).
// Examples: data @> '{"a":1}', '{"a":1}' <@ data
type JSONContainsExpr struct {
	Left     Expression
	Right    Expression
	Contains bool // true for @> (left contains right), false for <@ (left contained by right)
}

func (e *JSONContainsExpr) exprNode() {}

// JSONExistsExpr represents JSON key existence operators (?, ?|, ?&).
// Examples: data ? 'key', data ?| array['a','b'], data ?& array['a','b']
type JSONExistsExpr struct {
	Object Expression
	Keys   []Expression
	Mode   string // "any" (?), "or" (?|), "and" (?&)
}

func (e *JSONExistsExpr) exprNode() {}

// UnionStmt represents UNION/INTERSECT/EXCEPT operations.
type UnionStmt struct {
	With    *WithClause // CTE definitions (WITH clause)
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

// CTE represents a Common Table Expression definition.
// WITH cte_name [(col1, col2, ...)] AS (SELECT ... [UNION ALL SELECT ...])
type CTE struct {
	Name       string      // CTE name
	Columns    []string    // optional column aliases
	Query      *SelectStmt // simple SELECT query (if not UNION)
	UnionQuery *UnionStmt  // UNION query (for recursive CTEs)
	Recursive  bool        // true for recursive CTE
}

// WithClause represents the WITH clause containing one or more CTEs.
// WITH [RECURSIVE] cte1 AS (...), cte2 AS (...) SELECT ...
type WithClause struct {
	CTEs      []CTE // list of CTE definitions
	Recursive bool  // WITH RECURSIVE (applies to all CTEs)
}

// PrepareStmt represents PREPARE statement.
type PrepareStmt struct {
	Name      string
	Statement Statement
}

func (s *PrepareStmt) statementNode() {}

// ExecuteStmt represents EXECUTE statement.
type ExecuteStmt struct {
	Name   string
	Params []Expression
}

func (s *ExecuteStmt) statementNode() {}

// DeallocateStmt represents DEALLOCATE statement.
type DeallocateStmt struct {
	Name string
}

func (s *DeallocateStmt) statementNode() {}

// PlaceholderExpr represents a parameter placeholder (, , etc).
type PlaceholderExpr struct {
	Index int // 1-based index
}

func (e *PlaceholderExpr) exprNode() {}

// CreateUserStmt represents CREATE USER statement.
// CREATE USER username WITH PASSWORD 'password' [SUPERUSER]
type CreateUserStmt struct {
	Username  string
	Password  string
	Superuser bool
}

func (s *CreateUserStmt) statementNode() {}

// DropUserStmt represents DROP USER statement.
// DROP USER [IF EXISTS] username
type DropUserStmt struct {
	Username string
	IfExists bool
}

func (s *DropUserStmt) statementNode() {}

// AlterUserStmt represents ALTER USER statement.
// ALTER USER username WITH PASSWORD 'newpassword'
// ALTER USER username WITH SUPERUSER / NOSUPERUSER
type AlterUserStmt struct {
	Username       string
	NewPassword    string // empty if not changing password
	SetSuperuser   bool   // true if setting SUPERUSER
	UnsetSuperuser bool   // true if setting NOSUPERUSER
}

func (s *AlterUserStmt) statementNode() {}

// GrantStmt represents GRANT statement (basic version).
// GRANT privilege ON table TO user
type GrantStmt struct {
	Privilege string // SELECT, INSERT, UPDATE, DELETE, ALL
	TableName string
	Username  string
}

func (s *GrantStmt) statementNode() {}

// RevokeStmt represents REVOKE statement (basic version).
// REVOKE privilege ON table FROM user
type RevokeStmt struct {
	Privilege string
	TableName string
	Username  string
}

func (s *RevokeStmt) statementNode() {}

// CreateDatabaseStmt represents CREATE DATABASE statement.
// CREATE DATABASE name [WITH OWNER = 'owner']
type CreateDatabaseStmt struct {
	Name        string
	Owner       string // optional
	IfNotExists bool
}

func (s *CreateDatabaseStmt) statementNode() {}

// DropDatabaseStmt represents DROP DATABASE statement.
// DROP DATABASE [IF EXISTS] name
type DropDatabaseStmt struct {
	Name     string
	IfExists bool
}

func (s *DropDatabaseStmt) statementNode() {}

// UseDatabaseStmt represents USE database statement.
// USE database_name
type UseDatabaseStmt struct {
	Name string
}

func (s *UseDatabaseStmt) statementNode() {}

// ShowDatabasesStmt represents SHOW DATABASES statement.
type ShowDatabasesStmt struct{}

func (s *ShowDatabasesStmt) statementNode() {}

// TriggerTiming specifies when a trigger fires.
type TriggerTiming int

const (
	TriggerBefore TriggerTiming = iota
	TriggerAfter
	TriggerInsteadOf
)

// TriggerEvent specifies the event that fires a trigger.
type TriggerEvent int

const (
	TriggerInsert TriggerEvent = iota
	TriggerUpdate
	TriggerDelete
)

// CreateTriggerStmt represents CREATE TRIGGER statement.
// CREATE TRIGGER name {BEFORE | AFTER | INSTEAD OF} {INSERT | UPDATE | DELETE}
// ON table [FOR EACH {ROW | STATEMENT}]
// EXECUTE FUNCTION function_name(args)
type CreateTriggerStmt struct {
	Name         string
	TableName    string
	Timing       TriggerTiming
	Event        TriggerEvent
	ForEachRow   bool   // true = FOR EACH ROW, false = FOR EACH STATEMENT
	FunctionName string // The trigger function to execute
	FunctionArgs []Expression
	IfNotExists  bool
}

func (s *CreateTriggerStmt) statementNode() {}

// DropTriggerStmt represents DROP TRIGGER statement.
// DROP TRIGGER [IF EXISTS] name ON table
type DropTriggerStmt struct {
	Name      string
	TableName string
	IfExists  bool
}

func (s *DropTriggerStmt) statementNode() {}

// ShowTriggersStmt represents SHOW TRIGGERS statement.
// SHOW TRIGGERS [ON table]
type ShowTriggersStmt struct {
	TableName string // empty for all triggers
}

func (s *ShowTriggersStmt) statementNode() {}

// ==================== Full-Text Search AST Nodes ====================

// TSVectorExpr represents to_tsvector(config, text) or to_tsvector(text).
type TSVectorExpr struct {
	Config string     // Optional configuration (e.g., 'english')
	Text   Expression // The text to convert
}

func (e *TSVectorExpr) exprNode() {}

// TSQueryExpr represents to_tsquery(config, query) or to_tsquery(query).
type TSQueryExpr struct {
	Config    string     // Optional configuration (e.g., 'english')
	Query     Expression // The query text
	PlainText bool       // true for plainto_tsquery (space = AND)
	WebSearch bool       // true for websearch_to_tsquery
}

func (e *TSQueryExpr) exprNode() {}

// TSMatchExpr represents the @@ text search match operator.
// vector @@ query or text @@ query
type TSMatchExpr struct {
	Left  Expression // TSVector or text
	Right Expression // TSQuery or text
}

func (e *TSMatchExpr) exprNode() {}

// TSRankExpr represents ts_rank(vector, query) or ts_rank(vector, query, normalization).
type TSRankExpr struct {
	Vector        Expression // TSVector
	Query         Expression // TSQuery
	Normalization Expression // Optional normalization integer
}

func (e *TSRankExpr) exprNode() {}

// TSHeadlineExpr represents ts_headline(config, text, query, options).
type TSHeadlineExpr struct {
	Config  string     // Optional configuration
	Text    Expression // The document text
	Query   Expression // The search query
	Options Expression // Optional options string
}

func (e *TSHeadlineExpr) exprNode() {}

// CreateFTSIndexStmt represents CREATE FULLTEXT INDEX statement.
// CREATE FULLTEXT INDEX name ON table(columns)
type CreateFTSIndexStmt struct {
	IndexName   string
	TableName   string
	Columns     []string
	IfNotExists bool
}

func (s *CreateFTSIndexStmt) statementNode() {}

// DropFTSIndexStmt represents DROP FULLTEXT INDEX statement.
type DropFTSIndexStmt struct {
	IndexName string
	IfExists  bool
}

func (s *DropFTSIndexStmt) statementNode() {}

// MatchAgainstExpr represents MySQL-style MATCH ... AGAINST syntax.
// MATCH(col1, col2) AGAINST('search terms' IN NATURAL LANGUAGE MODE)
type MatchAgainstExpr struct {
	Columns       []string   // Columns to search
	Query         Expression // Search query
	InBoolMode    bool       // IN BOOLEAN MODE
	WithExpansion bool       // WITH QUERY EXPANSION
}

func (e *MatchAgainstExpr) exprNode() {}

// ================== Stored Procedures / PL/pgSQL AST Types ==================

// ParamMode represents the mode of a procedure/function parameter.
type ParamMode int

const (
	ParamModeIn    ParamMode = iota // IN (default, input only)
	ParamModeOut                    // OUT (output only)
	ParamModeInOut                  // INOUT (input and output)
)

// ProcParam represents a procedure/function parameter.
type ProcParam struct {
	Name    string
	Type    catalog.DataType
	Mode    ParamMode
	Default Expression // optional default value
}

// PLStatement is the interface for all PL/pgSQL statements within a procedure body.
type PLStatement interface {
	plStmtNode()
}

// PLBlock represents a BEGIN...END block in PL/pgSQL.
type PLBlock struct {
	Declarations      []PLVarDecl          // DECLARE section variables
	Statements        []PLStatement        // Statements in the block
	ExceptionHandlers []PLExceptionHandler // EXCEPTION handlers
}

func (s *PLBlock) plStmtNode() {}

// PLVarDecl represents a variable declaration in DECLARE section.
type PLVarDecl struct {
	Name    string
	Type    catalog.DataType
	NotNull bool
	Default Expression // optional default/initial value
}

// PLExceptionHandler represents an exception handler in PL/pgSQL.
type PLExceptionHandler struct {
	Exceptions []string      // exception names (e.g., "OTHERS", "NO_DATA_FOUND")
	Statements []PLStatement // statements to execute
}

// PLAssign represents variable assignment: var := expr;
type PLAssign struct {
	Variable string
	Value    Expression
}

func (s *PLAssign) plStmtNode() {}

// PLIf represents an IF/ELSIF/ELSE statement.
type PLIf struct {
	Condition Expression
	Then      []PLStatement
	ElsIfs    []PLElsIf // ELSIF branches
	Else      []PLStatement
}

func (s *PLIf) plStmtNode() {}

// PLElsIf represents an ELSIF branch.
type PLElsIf struct {
	Condition Expression
	Then      []PLStatement
}

// PLWhile represents a WHILE loop.
type PLWhile struct {
	Condition Expression
	Body      []PLStatement
}

func (s *PLWhile) plStmtNode() {}

// PLLoop represents a simple LOOP (infinite until EXIT).
type PLLoop struct {
	Body []PLStatement
}

func (s *PLLoop) plStmtNode() {}

// PLFor represents a FOR loop (numeric or query).
type PLFor struct {
	Variable   string
	LowerBound Expression  // for numeric FOR
	UpperBound Expression  // for numeric FOR
	Reverse    bool        // FOR i IN REVERSE ...
	Query      *SelectStmt // for FOR rec IN (SELECT ...) LOOP
	Body       []PLStatement
}

func (s *PLFor) plStmtNode() {}

// PLExit represents EXIT [WHEN condition] statement.
type PLExit struct {
	Label     string     // optional label to exit
	Condition Expression // optional WHEN condition
}

func (s *PLExit) plStmtNode() {}

// PLContinue represents CONTINUE [WHEN condition] statement.
type PLContinue struct {
	Label     string     // optional label to continue
	Condition Expression // optional WHEN condition
}

func (s *PLContinue) plStmtNode() {}

// PLReturn represents RETURN [expression] statement.
type PLReturn struct {
	Value Expression // nil for procedures (no return value)
}

func (s *PLReturn) plStmtNode() {}

// PLRaise represents RAISE [level] 'message' [, args...] statement.
type PLRaise struct {
	Level   string       // NOTICE, WARNING, EXCEPTION, etc.
	Message string       // format string
	Args    []Expression // optional format arguments
}

func (s *PLRaise) plStmtNode() {}

// PLPerform represents PERFORM query (execute SELECT without returning).
type PLPerform struct {
	Query *SelectStmt
}

func (s *PLPerform) plStmtNode() {}

// PLSQL represents an embedded SQL statement (SELECT INTO, INSERT, UPDATE, DELETE).
type PLSQL struct {
	Statement Statement // The SQL statement
	Into      []string  // INTO variables for SELECT
}

func (s *PLSQL) plStmtNode() {}

// CreateProcedureStmt represents CREATE PROCEDURE statement.
type CreateProcedureStmt struct {
	Name        string
	Params      []ProcParam
	Body        *PLBlock
	BodyText    string // Original body text for storage
	Language    string // e.g., "plpgsql"
	IfNotExists bool
}

func (s *CreateProcedureStmt) statementNode() {}

// CreateFunctionStmt represents CREATE FUNCTION statement.
type CreateFunctionStmt struct {
	Name        string
	Params      []ProcParam
	ReturnType  catalog.DataType
	Body        *PLBlock
	BodyText    string // Original body text for storage
	Language    string // e.g., "plpgsql"
	IfNotExists bool
}

func (s *CreateFunctionStmt) statementNode() {}

// DropProcedureStmt represents DROP PROCEDURE statement.
type DropProcedureStmt struct {
	Name     string
	IfExists bool
}

func (s *DropProcedureStmt) statementNode() {}

// DropFunctionStmt represents DROP FUNCTION statement.
type DropFunctionStmt struct {
	Name     string
	IfExists bool
}

func (s *DropFunctionStmt) statementNode() {}

// CallStmt represents CALL procedure_name(args...) statement.
type CallStmt struct {
	Name string
	Args []Expression
}

func (s *CallStmt) statementNode() {}

// ShowProceduresStmt represents SHOW PROCEDURES statement.
type ShowProceduresStmt struct{}

func (s *ShowProceduresStmt) statementNode() {}

// ShowFunctionsStmt represents SHOW FUNCTIONS statement.
type ShowFunctionsStmt struct{}

func (s *ShowFunctionsStmt) statementNode() {}
