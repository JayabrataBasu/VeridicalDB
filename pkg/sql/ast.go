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
	Name       string
	Type       catalog.DataType
	NotNull    bool
	PrimaryKey bool
}

// CreateTableStmt represents CREATE TABLE statement.
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) statementNode() {}

// DropTableStmt represents DROP TABLE statement.
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) statementNode() {}

// InsertStmt represents INSERT INTO statement.
type InsertStmt struct {
	TableName string
	Columns   []string // optional column list
	Values    []Expression
}

func (s *InsertStmt) statementNode() {}

// SelectStmt represents SELECT statement.
type SelectStmt struct {
	Columns   []SelectColumn
	TableName string
	Where     Expression
}

func (s *SelectStmt) statementNode() {}

// SelectColumn represents a column in SELECT.
type SelectColumn struct {
	Star bool   // true if *
	Name string // column name if not star
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
