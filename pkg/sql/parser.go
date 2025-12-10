package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// Parser parses SQL statements.
type Parser struct {
	lexer *Lexer
	cur   Token
	peek  Token
}

// NewParser creates a new parser for the input SQL.
func NewParser(input string) *Parser {
	p := &Parser{lexer: NewLexer(input)}
	// Read two tokens to initialize cur and peek
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lexer.NextToken()
}

func (p *Parser) curTokenIs(t TokenType) bool {
	return p.cur.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peek.Type == t
}

func (p *Parser) expectPeek(t TokenType) error {
	if p.peekTokenIs(t) {
		p.nextToken()
		return nil
	}
	return fmt.Errorf("expected %v, got %v at position %d", t, p.peek.Type, p.peek.Pos)
}

func (p *Parser) expect(t TokenType) error {
	if p.curTokenIs(t) {
		p.nextToken()
		return nil
	}
	return fmt.Errorf("expected %v, got %v (%q) at position %d", t, p.cur.Type, p.cur.Literal, p.cur.Pos)
}

// Parse parses a single SQL statement.
func (p *Parser) Parse() (Statement, error) {
	switch p.cur.Type {
	case TOKEN_SELECT:
		return p.parseSelect()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_UPDATE:
		return p.parseUpdate()
	case TOKEN_DELETE:
		return p.parseDelete()
	case TOKEN_CREATE:
		return p.parseCreate()
	case TOKEN_DROP:
		return p.parseDrop()
	case TOKEN_BEGIN:
		return p.parseBegin()
	case TOKEN_COMMIT:
		return p.parseCommit()
	case TOKEN_ROLLBACK:
		return p.parseRollback()
	default:
		return nil, fmt.Errorf("unexpected token %v (%q) at position %d", p.cur.Type, p.cur.Literal, p.cur.Pos)
	}
}

// parseSelect parses: SELECT [DISTINCT] columns FROM table [WHERE expr] [ORDER BY cols] [LIMIT n] [OFFSET n]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	p.nextToken() // consume SELECT

	// Optional DISTINCT
	if p.curTokenIs(TOKEN_DISTINCT) {
		stmt.Distinct = true
		p.nextToken()
	}

	// Parse column list
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	// Optional JOIN clauses
	for p.curTokenIs(TOKEN_JOIN) || p.curTokenIs(TOKEN_INNER) || p.curTokenIs(TOKEN_LEFT) || p.curTokenIs(TOKEN_RIGHT) {
		join, err := p.parseJoinClause()
		if err != nil {
			return nil, err
		}
		stmt.Joins = append(stmt.Joins, join)
	}

	// Optional WHERE
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// Optional GROUP BY
	if p.curTokenIs(TOKEN_GROUP) {
		p.nextToken() // consume GROUP
		if err := p.expect(TOKEN_BY); err != nil {
			return nil, err
		}
		groupBy, err := p.parseGroupByList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// Optional HAVING (only valid with GROUP BY)
	if p.curTokenIs(TOKEN_HAVING) {
		if len(stmt.GroupBy) == 0 {
			return nil, fmt.Errorf("HAVING requires GROUP BY clause")
		}
		p.nextToken() // consume HAVING
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// Optional ORDER BY
	if p.curTokenIs(TOKEN_ORDER) {
		p.nextToken() // consume ORDER
		if err := p.expect(TOKEN_BY); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Optional LIMIT
	if p.curTokenIs(TOKEN_LIMIT) {
		p.nextToken() // consume LIMIT
		if !p.curTokenIs(TOKEN_INT) {
			return nil, fmt.Errorf("expected integer after LIMIT, got %v", p.cur.Type)
		}
		limit, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %s", p.cur.Literal)
		}
		if limit < 0 {
			return nil, fmt.Errorf("LIMIT must be non-negative, got %d", limit)
		}
		stmt.Limit = &limit
		p.nextToken()
	}

	// Optional OFFSET
	if p.curTokenIs(TOKEN_OFFSET) {
		p.nextToken() // consume OFFSET
		if !p.curTokenIs(TOKEN_INT) {
			return nil, fmt.Errorf("expected integer after OFFSET, got %v", p.cur.Type)
		}
		offset, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid OFFSET value: %s", p.cur.Literal)
		}
		if offset < 0 {
			return nil, fmt.Errorf("OFFSET must be non-negative, got %d", offset)
		}
		stmt.Offset = &offset
		p.nextToken()
	}

	return stmt, nil
}

// parseOrderByList parses: column [ASC|DESC] [, column [ASC|DESC] ...]
func (p *Parser) parseOrderByList() ([]OrderByClause, error) {
	var orderBy []OrderByClause

	for {
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name in ORDER BY, got %v", p.cur.Type)
		}
		clause := OrderByClause{Column: p.cur.Literal}
		p.nextToken()

		// Optional ASC/DESC
		if p.curTokenIs(TOKEN_ASC) {
			clause.Desc = false
			p.nextToken()
		} else if p.curTokenIs(TOKEN_DESC) {
			clause.Desc = true
			p.nextToken()
		}

		orderBy = append(orderBy, clause)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return orderBy, nil
}

// parseGroupByList parses: column [, column ...]
func (p *Parser) parseGroupByList() ([]string, error) {
	var groupBy []string

	for {
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name in GROUP BY, got %v", p.cur.Type)
		}
		groupBy = append(groupBy, p.cur.Literal)
		p.nextToken()

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return groupBy, nil
}

// parseJoinClause parses: [INNER|LEFT|RIGHT] JOIN table ON condition
func (p *Parser) parseJoinClause() (JoinClause, error) {
	join := JoinClause{JoinType: "INNER"} // default

	// Parse optional join type
	if p.curTokenIs(TOKEN_INNER) {
		p.nextToken()
	} else if p.curTokenIs(TOKEN_LEFT) {
		join.JoinType = "LEFT"
		p.nextToken()
		if p.curTokenIs(TOKEN_OUTER) {
			p.nextToken() // optional OUTER
		}
	} else if p.curTokenIs(TOKEN_RIGHT) {
		join.JoinType = "RIGHT"
		p.nextToken()
		if p.curTokenIs(TOKEN_OUTER) {
			p.nextToken() // optional OUTER
		}
	}

	// Expect JOIN keyword
	if err := p.expect(TOKEN_JOIN); err != nil {
		return join, err
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return join, fmt.Errorf("expected table name after JOIN, got %v", p.cur.Type)
	}
	join.TableName = p.cur.Literal
	p.nextToken()

	// ON keyword
	if err := p.expect(TOKEN_ON); err != nil {
		return join, err
	}

	// Join condition
	condition, err := p.parseExpression()
	if err != nil {
		return join, fmt.Errorf("expected join condition: %w", err)
	}
	join.Condition = condition

	return join, nil
}

// isAggregateToken returns true if the token is an aggregate function keyword.
func (p *Parser) isAggregateToken() bool {
	switch p.cur.Type {
	case TOKEN_COUNT, TOKEN_SUM, TOKEN_AVG, TOKEN_MIN, TOKEN_MAX:
		return true
	}
	return false
}

// aggregateName returns the name of the current aggregate function token.
func (p *Parser) aggregateName() string {
	switch p.cur.Type {
	case TOKEN_COUNT:
		return "COUNT"
	case TOKEN_SUM:
		return "SUM"
	case TOKEN_AVG:
		return "AVG"
	case TOKEN_MIN:
		return "MIN"
	case TOKEN_MAX:
		return "MAX"
	}
	return ""
}

func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	for {
		if p.curTokenIs(TOKEN_STAR) {
			cols = append(cols, SelectColumn{Star: true})
			p.nextToken()
		} else if p.isAggregateToken() {
			// Parse aggregate function: COUNT(*), SUM(col), etc.
			funcName := p.aggregateName()
			p.nextToken() // consume function name

			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, fmt.Errorf("expected ( after %s", funcName)
			}

			var arg string
			if p.curTokenIs(TOKEN_STAR) {
				arg = "*"
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) {
				arg = p.cur.Literal
				p.nextToken()
			} else {
				return nil, fmt.Errorf("expected column name or * in %s(), got %v", funcName, p.cur.Type)
			}

			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, err
			}

			col := SelectColumn{
				Aggregate: &AggregateFunc{Function: funcName, Arg: arg},
			}
			cols = append(cols, col)
		} else if p.curTokenIs(TOKEN_IDENT) {
			name := p.cur.Literal
			p.nextToken()

			// Check for qualified name (table.column)
			if p.cur.Literal == "." {
				p.nextToken() // consume .
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected column name after '.', got %v", p.cur.Type)
				}
				name = name + "." + p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, SelectColumn{Name: name})
		} else {
			return nil, fmt.Errorf("expected column name or *, got %v", p.cur.Type)
		}

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return cols, nil
}

// parseInsert parses: INSERT INTO table [(columns)] VALUES (values)
func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}

	p.nextToken() // consume INSERT

	if err := p.expect(TOKEN_INTO); err != nil {
		return nil, err
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	// Optional column list
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken()
		for {
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name, got %v", p.cur.Type)
			}
			stmt.Columns = append(stmt.Columns, p.cur.Literal)
			p.nextToken()

			if !p.curTokenIs(TOKEN_COMMA) {
				break
			}
			p.nextToken()
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
	}

	// VALUES
	if err := p.expect(TOKEN_VALUES); err != nil {
		return nil, err
	}

	// (values)
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	for {
		expr, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, expr)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseUpdate parses: UPDATE table SET assignments [WHERE expr]
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{}

	p.nextToken() // consume UPDATE

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	// SET
	if err := p.expect(TOKEN_SET); err != nil {
		return nil, err
	}

	// Assignments
	for {
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name, got %v", p.cur.Type)
		}
		colName := p.cur.Literal
		p.nextToken()

		if err := p.expect(TOKEN_EQ); err != nil {
			return nil, err
		}

		expr, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}

		stmt.Assignments = append(stmt.Assignments, Assignment{Column: colName, Value: expr})

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	// Optional WHERE
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// parseDelete parses: DELETE FROM table [WHERE expr]
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}

	p.nextToken() // consume DELETE

	if err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	// Optional WHERE
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	return stmt, nil
}

// parseCreate parses: CREATE TABLE name (columns) or CREATE [UNIQUE] INDEX name ON table (columns)
func (p *Parser) parseCreate() (Statement, error) {
	p.nextToken() // consume CREATE

	// Check for UNIQUE (for CREATE UNIQUE INDEX)
	isUnique := false
	if p.curTokenIs(TOKEN_UNIQUE) {
		isUnique = true
		p.nextToken()
	}

	if p.curTokenIs(TOKEN_INDEX) {
		return p.parseCreateIndex(isUnique)
	}

	if isUnique {
		return nil, fmt.Errorf("UNIQUE keyword only valid for CREATE INDEX")
	}

	if err := p.expect(TOKEN_TABLE); err != nil {
		return nil, err
	}

	stmt := &CreateTableStmt{}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	// (columns)
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	// Optional USING COLUMN clause
	stmt.StorageType = "ROW" // default
	if p.curTokenIs(TOKEN_USING) {
		p.nextToken() // consume USING
		if p.curTokenIs(TOKEN_COLUMN) {
			stmt.StorageType = "COLUMN"
			p.nextToken()
		} else if p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "ROW" {
			stmt.StorageType = "ROW"
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected ROW or COLUMN after USING, got %v", p.cur.Literal)
		}
	}

	return stmt, nil
}

func (p *Parser) parseColumnDef() (ColumnDef, error) {
	col := ColumnDef{}

	// Column name
	if !p.curTokenIs(TOKEN_IDENT) {
		return col, fmt.Errorf("expected column name, got %v", p.cur.Type)
	}
	col.Name = p.cur.Literal
	p.nextToken()

	// Type
	switch p.cur.Type {
	case TOKEN_INT_TYPE:
		col.Type = catalog.TypeInt32
	case TOKEN_BIGINT:
		col.Type = catalog.TypeInt64
	case TOKEN_TEXT:
		col.Type = catalog.TypeText
	case TOKEN_BOOL:
		col.Type = catalog.TypeBool
	case TOKEN_TIMESTAMP:
		col.Type = catalog.TypeTimestamp
	default:
		return col, fmt.Errorf("expected type, got %v (%q)", p.cur.Type, p.cur.Literal)
	}
	p.nextToken()

	// Optional NOT NULL, PRIMARY KEY, and DEFAULT
	for {
		if p.curTokenIs(TOKEN_NOT) {
			p.nextToken()
			if err := p.expect(TOKEN_NULL); err != nil {
				return col, err
			}
			col.NotNull = true
		} else if p.curTokenIs(TOKEN_PRIMARY) {
			p.nextToken()
			if err := p.expect(TOKEN_KEY); err != nil {
				return col, err
			}
			col.PrimaryKey = true
			col.NotNull = true // primary keys are implicitly not null
		} else if p.curTokenIs(TOKEN_DEFAULT) {
			p.nextToken()
			// Parse default value (literal expression)
			expr, err := p.parsePrimaryExpression()
			if err != nil {
				return col, fmt.Errorf("expected default value: %w", err)
			}
			col.HasDefault = true
			col.Default = expr
		} else if p.curTokenIs(TOKEN_AUTO_INCREMENT) {
			p.nextToken()
			col.AutoIncrement = true
			col.NotNull = true // auto-increment columns are implicitly not null
		} else {
			break
		}
	}

	return col, nil
}

// parseCreateIndex parses: INDEX name ON table (columns)
func (p *Parser) parseCreateIndex(unique bool) (*CreateIndexStmt, error) {
	p.nextToken() // consume INDEX

	stmt := &CreateIndexStmt{Unique: unique}

	// Index name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected index name, got %v", p.cur.Type)
	}
	stmt.IndexName = p.cur.Literal
	p.nextToken()

	// ON
	if err := p.expect(TOKEN_ON); err != nil {
		return nil, err
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	// (columns)
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	for {
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name, got %v", p.cur.Type)
		}
		stmt.Columns = append(stmt.Columns, p.cur.Literal)
		p.nextToken()

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseDrop parses: DROP TABLE name or DROP INDEX name
func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	if p.curTokenIs(TOKEN_INDEX) {
		return p.parseDropIndex()
	}

	if err := p.expect(TOKEN_TABLE); err != nil {
		return nil, err
	}

	stmt := &DropTableStmt{}

	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	stmt.TableName = p.cur.Literal
	p.nextToken()

	return stmt, nil
}

// parseDropIndex parses: INDEX name
func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	p.nextToken() // consume INDEX

	stmt := &DropIndexStmt{}

	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected index name, got %v", p.cur.Type)
	}
	stmt.IndexName = p.cur.Literal
	p.nextToken()

	return stmt, nil
}

// Expression parsing with precedence

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() (Expression, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.curTokenIs(TOKEN_OR) {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAndExpr() (Expression, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}

	for p.curTokenIs(TOKEN_AND) {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseNotExpr() (Expression, error) {
	if p.curTokenIs(TOKEN_NOT) {
		p.nextToken()
		expr, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: TOKEN_NOT, Expr: expr}, nil
	}
	return p.parseComparisonExpr()
}

func (p *Parser) parseComparisonExpr() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	switch p.cur.Type {
	case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
		op := p.cur.Type
		p.nextToken()
		right, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	switch p.cur.Type {
	case TOKEN_INT:
		val, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %s", p.cur.Literal)
		}
		p.nextToken()
		// Use Int32 for smaller values, Int64 for larger
		if val >= -2147483648 && val <= 2147483647 {
			return &LiteralExpr{Value: catalog.NewInt32(int32(val))}, nil
		}
		return &LiteralExpr{Value: catalog.NewInt64(val)}, nil

	case TOKEN_STRING:
		val := p.cur.Literal
		p.nextToken()
		return &LiteralExpr{Value: catalog.NewText(val)}, nil

	case TOKEN_TRUE:
		p.nextToken()
		return &LiteralExpr{Value: catalog.NewBool(true)}, nil

	case TOKEN_FALSE:
		p.nextToken()
		return &LiteralExpr{Value: catalog.NewBool(false)}, nil

	case TOKEN_NULL:
		p.nextToken()
		return &LiteralExpr{Value: catalog.Null(catalog.TypeUnknown)}, nil

	case TOKEN_IDENT:
		name := p.cur.Literal
		p.nextToken()

		// Check for qualified name (table.column)
		if p.cur.Literal == "." {
			p.nextToken() // consume .
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name after '.', got %v", p.cur.Type)
			}
			name = name + "." + p.cur.Literal
			p.nextToken()
		}

		return &ColumnRef{Name: name}, nil

	case TOKEN_LPAREN:
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	default:
		return nil, fmt.Errorf("unexpected token in expression: %v (%q)", p.cur.Type, p.cur.Literal)
	}
}

// parseBegin parses: BEGIN
func (p *Parser) parseBegin() (*BeginStmt, error) {
	p.nextToken() // consume BEGIN
	return &BeginStmt{}, nil
}

// parseCommit parses: COMMIT
func (p *Parser) parseCommit() (*CommitStmt, error) {
	p.nextToken() // consume COMMIT
	return &CommitStmt{}, nil
}

// parseRollback parses: ROLLBACK
func (p *Parser) parseRollback() (*RollbackStmt, error) {
	p.nextToken() // consume ROLLBACK
	return &RollbackStmt{}, nil
}
