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

// Note: peekTokenIs and expectPeek are reserved for future parser extensions.
// They are commented out to avoid unused function warnings.
/*
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
*/

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
		selectStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		// Check for UNION/INTERSECT/EXCEPT
		if p.curTokenIs(TOKEN_UNION) || p.curTokenIs(TOKEN_INTERSECT) || p.curTokenIs(TOKEN_EXCEPT) {
			return p.parseSetOperation(selectStmt)
		}
		return selectStmt, nil
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
	case TOKEN_ALTER:
		return p.parseAlter()
	case TOKEN_TRUNCATE:
		return p.parseTruncate()
	case TOKEN_SHOW:
		return p.parseShow()
	case TOKEN_EXPLAIN:
		return p.parseExplain()
	default:
		return nil, fmt.Errorf("unexpected token %v (%q) at position %d", p.cur.Type, p.cur.Literal, p.cur.Pos)
	}
}

// parseSelect parses: SELECT [DISTINCT [ON (cols)]] columns FROM table [WHERE expr] [ORDER BY cols] [LIMIT n] [OFFSET n]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	p.nextToken() // consume SELECT

	// Optional DISTINCT [ON (col1, col2, ...)]
	if p.curTokenIs(TOKEN_DISTINCT) {
		stmt.Distinct = true
		p.nextToken()

		// Check for DISTINCT ON (PostgreSQL style)
		if p.curTokenIs(TOKEN_ON) {
			p.nextToken() // consume ON
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, fmt.Errorf("expected '(' after DISTINCT ON")
			}

			// Parse column list
			var distinctCols []string
			for {
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected column name in DISTINCT ON, got %v", p.cur.Type)
				}
				distinctCols = append(distinctCols, p.cur.Literal)
				p.nextToken()

				if p.curTokenIs(TOKEN_COMMA) {
					p.nextToken() // consume comma
				} else {
					break
				}
			}

			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after DISTINCT ON columns")
			}
			stmt.DistinctOn = distinctCols
		}
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

	// Optional table alias: AS alias or just alias
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected alias after AS")
		}
		stmt.TableAlias = p.cur.Literal
		p.nextToken()
	} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
		// Implicit alias without AS
		stmt.TableAlias = p.cur.Literal
		p.nextToken()
	}

	// Optional JOIN clauses
	for p.curTokenIs(TOKEN_JOIN) || p.curTokenIs(TOKEN_INNER) || p.curTokenIs(TOKEN_LEFT) || p.curTokenIs(TOKEN_RIGHT) || p.curTokenIs(TOKEN_FULL) || p.curTokenIs(TOKEN_CROSS) {
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

		// Check for integer literal (common case)
		if p.curTokenIs(TOKEN_INT) {
			limit, err := strconv.ParseInt(p.cur.Literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid LIMIT value: %s", p.cur.Literal)
			}
			if limit < 0 {
				return nil, fmt.Errorf("LIMIT must be non-negative, got %d", limit)
			}
			stmt.Limit = &limit
			p.nextToken()
		} else if p.curTokenIs(TOKEN_LPAREN) {
			// LIMIT with subquery or expression: LIMIT (SELECT ...)
			expr, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, fmt.Errorf("error parsing LIMIT expression: %v", err)
			}
			stmt.LimitExpr = expr
		} else {
			return nil, fmt.Errorf("expected integer or expression after LIMIT, got %v", p.cur.Type)
		}
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

// parseJoinClause parses: [INNER|LEFT|RIGHT|FULL|CROSS] JOIN table [ON condition]
func (p *Parser) parseJoinClause() (JoinClause, error) {
	join := JoinClause{JoinType: "INNER"} // default
	isCrossJoin := false

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
	} else if p.curTokenIs(TOKEN_FULL) {
		join.JoinType = "FULL"
		p.nextToken()
		if p.curTokenIs(TOKEN_OUTER) {
			p.nextToken() // optional OUTER
		}
	} else if p.curTokenIs(TOKEN_CROSS) {
		join.JoinType = "CROSS"
		isCrossJoin = true
		p.nextToken()
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

	// Optional table alias: AS alias or just alias (before ON)
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.curTokenIs(TOKEN_IDENT) {
			return join, fmt.Errorf("expected alias after AS")
		}
		join.TableAlias = p.cur.Literal
		p.nextToken()
	} else if p.curTokenIs(TOKEN_IDENT) && !p.curTokenIs(TOKEN_ON) && !p.isKeyword() {
		// Implicit alias without AS
		join.TableAlias = p.cur.Literal
		p.nextToken()
	}

	// CROSS JOIN has no ON condition
	if isCrossJoin {
		return join, nil
	}

	// ON keyword (required for non-CROSS joins)
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

// isKeyword returns true if the current token is a SQL keyword (not suitable for implicit alias).
func (p *Parser) isKeyword() bool {
	switch p.cur.Type {
	case TOKEN_FROM, TOKEN_WHERE, TOKEN_ORDER, TOKEN_GROUP, TOKEN_HAVING,
		TOKEN_LIMIT, TOKEN_OFFSET, TOKEN_JOIN, TOKEN_INNER, TOKEN_LEFT,
		TOKEN_RIGHT, TOKEN_FULL, TOKEN_CROSS, TOKEN_OUTER, TOKEN_ON, TOKEN_AND, TOKEN_OR, TOKEN_AS,
		TOKEN_IN, TOKEN_BETWEEN, TOKEN_COMMA, TOKEN_SEMICOLON:
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

// isDateFunction returns true if the current token is a date/time function.
func (p *Parser) isDateFunction() bool {
	switch p.cur.Type {
	case TOKEN_NOW, TOKEN_CURRENT_TIMESTAMP, TOKEN_CURRENT_DATE,
		TOKEN_YEAR, TOKEN_MONTH, TOKEN_DAY, TOKEN_HOUR, TOKEN_MINUTE, TOKEN_SECOND,
		TOKEN_DATE_ADD, TOKEN_DATE_SUB:
		return true
	}
	return false
}

// isFunctionToken returns true if the current token is any function that can appear in SELECT.
func (p *Parser) isFunctionToken() bool {
	switch p.cur.Type {
	// String functions
	case TOKEN_COALESCE, TOKEN_NULLIF, TOKEN_UPPER, TOKEN_LOWER, TOKEN_LENGTH,
		TOKEN_CONCAT, TOKEN_SUBSTR, TOKEN_SUBSTRING:
		return true
	// Extended string functions
	case TOKEN_TRIM, TOKEN_LTRIM, TOKEN_RTRIM, TOKEN_REPLACE, TOKEN_POSITION,
		TOKEN_REVERSE, TOKEN_REPEAT, TOKEN_LPAD, TOKEN_RPAD:
		return true
	// Math functions
	case TOKEN_ABS, TOKEN_ROUND, TOKEN_FLOOR, TOKEN_CEIL, TOKEN_CEILING,
		TOKEN_MOD, TOKEN_POWER, TOKEN_SQRT:
		return true
	// Special expressions
	case TOKEN_CAST, TOKEN_EXTRACT:
		return true
	}
	return false
}

// parseDateFunctionInSelect parses date functions when they appear in SELECT columns.
func (p *Parser) parseDateFunctionInSelect() (Expression, error) {
	switch p.cur.Type {
	case TOKEN_NOW, TOKEN_CURRENT_TIMESTAMP, TOKEN_CURRENT_DATE:
		return p.parseDateFunction()
	case TOKEN_YEAR, TOKEN_MONTH, TOKEN_DAY, TOKEN_HOUR, TOKEN_MINUTE, TOKEN_SECOND:
		return p.parseDatePartFunction()
	case TOKEN_DATE_ADD, TOKEN_DATE_SUB:
		return p.parseDateAddFunction()
	default:
		return nil, fmt.Errorf("not a date function: %v", p.cur.Type)
	}
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

			// Check for alias: AS name or just name
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken() // consume AS
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				// Implicit alias (without AS)
				col.Alias = p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, col)
		} else if p.curTokenIs(TOKEN_CASE) {
			// Parse CASE expression
			expr, err := p.parseCaseExpression()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expression: expr}

			// Check for alias: AS name or just name
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken() // consume AS
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				// Implicit alias (without AS)
				col.Alias = p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, col)
		} else if p.isDateFunction() {
			// Parse date/time function as expression
			expr, err := p.parseDateFunctionInSelect()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expression: expr}

			// Check for alias: AS name or just name
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken() // consume AS
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				// Implicit alias (without AS)
				col.Alias = p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, col)
		} else if p.isFunctionToken() {
			// Parse general function expression (CAST, EXTRACT, string functions, math functions)
			expr, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expression: expr}

			// Check for alias: AS name or just name
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken() // consume AS
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				// Implicit alias (without AS)
				col.Alias = p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, col)
		} else if p.curTokenIs(TOKEN_INT) || p.curTokenIs(TOKEN_STRING) || p.curTokenIs(TOKEN_TRUE) || p.curTokenIs(TOKEN_FALSE) || p.curTokenIs(TOKEN_NULL) {
			// Parse literal as expression (e.g., SELECT 1 FROM dual)
			expr, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, err
			}

			col := SelectColumn{Expression: expr}

			// Check for alias: AS name or just name
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken() // consume AS
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				// Implicit alias (without AS)
				col.Alias = p.cur.Literal
				p.nextToken()
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

			col := SelectColumn{Name: name}

			// Check for alias: AS name or just name
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken() // consume AS
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				// Implicit alias (without AS)
				col.Alias = p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, col)
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

	// Check for OR REPLACE (for CREATE OR REPLACE VIEW)
	orReplace := false
	if p.curTokenIs(TOKEN_OR) {
		p.nextToken() // consume OR
		if p.cur.Literal != "REPLACE" {
			return nil, fmt.Errorf("expected REPLACE after OR, got %v", p.cur.Literal)
		}
		p.nextToken() // consume REPLACE
		orReplace = true
	}

	// Check for UNIQUE (for CREATE UNIQUE INDEX)
	isUnique := false
	if p.curTokenIs(TOKEN_UNIQUE) {
		isUnique = true
		p.nextToken()
	}

	if p.curTokenIs(TOKEN_INDEX) {
		return p.parseCreateIndex(isUnique)
	}

	if p.curTokenIs(TOKEN_VIEW) {
		return p.parseCreateView(orReplace)
	}

	if isUnique {
		return nil, fmt.Errorf("UNIQUE keyword only valid for CREATE INDEX")
	}
	if orReplace {
		return nil, fmt.Errorf("OR REPLACE only valid for CREATE VIEW")
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

	// Optional NOT NULL, PRIMARY KEY, DEFAULT, CHECK, and AUTO_INCREMENT
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
		} else if p.curTokenIs(TOKEN_CHECK) {
			// Parse CHECK constraint: CHECK(expression)
			p.nextToken() // consume CHECK
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return col, fmt.Errorf("expected ( after CHECK: %w", err)
			}
			// Parse the check expression
			expr, err := p.parseExpression()
			if err != nil {
				return col, fmt.Errorf("error parsing CHECK expression: %w", err)
			}
			col.Check = expr
			// We'll serialize the expression to string when storing in catalog
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return col, fmt.Errorf("expected ) after CHECK expression: %w", err)
			}
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

// parseCreateView parses: CREATE [OR REPLACE] VIEW name [(columns)] AS SELECT ...
func (p *Parser) parseCreateView(orReplace bool) (*CreateViewStmt, error) {
	p.nextToken() // consume VIEW

	stmt := &CreateViewStmt{OrReplace: orReplace}

	// View name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected view name, got %v", p.cur.Type)
	}
	stmt.ViewName = p.cur.Literal
	p.nextToken()

	// Optional column list
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (
		for {
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name in view definition, got %v", p.cur.Type)
			}
			stmt.Columns = append(stmt.Columns, p.cur.Literal)
			p.nextToken()
			if !p.curTokenIs(TOKEN_COMMA) {
				break
			}
			p.nextToken() // consume comma
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
	}

	// AS keyword
	if err := p.expect(TOKEN_AS); err != nil {
		return nil, fmt.Errorf("expected AS after view name")
	}

	// Parse the SELECT statement
	if !p.curTokenIs(TOKEN_SELECT) {
		return nil, fmt.Errorf("expected SELECT after AS in CREATE VIEW")
	}
	selectStmt, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing view query: %w", err)
	}
	stmt.Query = selectStmt

	return stmt, nil
}

// parseSetOperation parses UNION/INTERSECT/EXCEPT between SELECT statements
func (p *Parser) parseSetOperation(left *SelectStmt) (*UnionStmt, error) {
	var op string
	switch p.cur.Type {
	case TOKEN_UNION:
		op = "UNION"
	case TOKEN_INTERSECT:
		op = "INTERSECT"
	case TOKEN_EXCEPT:
		op = "EXCEPT"
	default:
		return nil, fmt.Errorf("expected UNION, INTERSECT, or EXCEPT")
	}
	p.nextToken() // consume the set operator

	// Check for ALL
	all := false
	if p.curTokenIs(TOKEN_ALL) {
		all = true
		p.nextToken()
	}

	// Parse the right SELECT
	if !p.curTokenIs(TOKEN_SELECT) {
		return nil, fmt.Errorf("expected SELECT after %s", op)
	}
	right, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing right side of %s: %w", op, err)
	}

	stmt := &UnionStmt{
		Left:  left,
		Right: right,
		Op:    op,
		All:   all,
	}

	// Optional ORDER BY (applies to the combined result)
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
		p.nextToken()
		if !p.curTokenIs(TOKEN_INT) {
			return nil, fmt.Errorf("expected integer after LIMIT")
		}
		limit, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, err
		}
		stmt.Limit = &limit
		p.nextToken()
	}

	// Optional OFFSET
	if p.curTokenIs(TOKEN_OFFSET) {
		p.nextToken()
		if !p.curTokenIs(TOKEN_INT) {
			return nil, fmt.Errorf("expected integer after OFFSET")
		}
		offset, err := strconv.ParseInt(p.cur.Literal, 10, 64)
		if err != nil {
			return nil, err
		}
		stmt.Offset = &offset
		p.nextToken()
	}

	return stmt, nil
}

// parseDrop parses: DROP TABLE name or DROP INDEX name or DROP VIEW name
func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	if p.curTokenIs(TOKEN_INDEX) {
		return p.parseDropIndex()
	}

	if p.curTokenIs(TOKEN_VIEW) {
		return p.parseDropView()
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

// parseDropView parses: DROP VIEW [IF EXISTS] name
func (p *Parser) parseDropView() (*DropViewStmt, error) {
	p.nextToken() // consume VIEW

	stmt := &DropViewStmt{}

	// Optional IF EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected view name, got %v", p.cur.Type)
	}
	stmt.ViewName = p.cur.Literal
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
		// Check for NOT IN or NOT BETWEEN
		if p.curTokenIs(TOKEN_IN) || p.curTokenIs(TOKEN_BETWEEN) {
			// This is a standalone NOT, parse the rest and negate
			expr, err := p.parseNotExpr()
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: TOKEN_NOT, Expr: expr}, nil
		}
		expr, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: TOKEN_NOT, Expr: expr}, nil
	}
	return p.parseComparisonExpr()
}

func (p *Parser) parseComparisonExpr() (Expression, error) {
	left, err := p.parseAddExpr()
	if err != nil {
		return nil, err
	}

	// Check for NOT IN or NOT BETWEEN first
	isNot := false
	if p.curTokenIs(TOKEN_NOT) {
		p.nextToken()
		isNot = true
	}

	// Handle IN expression
	if p.curTokenIs(TOKEN_IN) {
		p.nextToken() // consume IN
		if err := p.expect(TOKEN_LPAREN); err != nil {
			return nil, fmt.Errorf("expected '(' after IN")
		}

		// Check if it's a subquery: IN (SELECT ...)
		if p.curTokenIs(TOKEN_SELECT) {
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, fmt.Errorf("error parsing IN subquery: %v", err)
			}
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after IN subquery")
			}
			return &InExpr{Left: left, Subquery: subquery, Not: isNot}, nil
		}

		// Parse value list: IN (1, 2, 3)
		var values []Expression
		for {
			val, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, err
			}
			values = append(values, val)

			if p.curTokenIs(TOKEN_COMMA) {
				p.nextToken() // consume comma
			} else {
				break
			}
		}

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, fmt.Errorf("expected ')' after IN list")
		}

		return &InExpr{Left: left, Values: values, Not: isNot}, nil
	}

	// Handle BETWEEN expression
	if p.curTokenIs(TOKEN_BETWEEN) {
		p.nextToken() // consume BETWEEN
		low, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}

		if !p.curTokenIs(TOKEN_AND) {
			return nil, fmt.Errorf("expected AND in BETWEEN expression")
		}
		p.nextToken() // consume AND

		high, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}

		return &BetweenExpr{Expr: left, Low: low, High: high, Not: isNot}, nil
	}

	// Handle LIKE/ILIKE expression
	if p.curTokenIs(TOKEN_LIKE) || p.curTokenIs(TOKEN_ILIKE) {
		caseInsensitive := p.curTokenIs(TOKEN_ILIKE)
		p.nextToken() // consume LIKE/ILIKE

		pattern, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}

		return &LikeExpr{Expr: left, Pattern: pattern, CaseInsensitive: caseInsensitive, Not: isNot}, nil
	}

	// Handle IS NULL / IS NOT NULL
	if p.curTokenIs(TOKEN_IS) {
		p.nextToken() // consume IS
		not := false
		if p.curTokenIs(TOKEN_NOT) {
			not = true
			p.nextToken() // consume NOT
		}
		if !p.curTokenIs(TOKEN_NULL) {
			return nil, fmt.Errorf("expected NULL after IS%s", map[bool]string{true: " NOT", false: ""}[not])
		}
		p.nextToken() // consume NULL
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	// If we saw NOT but it wasn't IN/BETWEEN/LIKE, that's an error
	if isNot {
		return nil, fmt.Errorf("expected IN, BETWEEN, or LIKE after NOT in comparison")
	}

	switch p.cur.Type {
	case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	}

	return left, nil
}

// parseAddExpr parses addition and subtraction expressions.
func (p *Parser) parseAddExpr() (Expression, error) {
	left, err := p.parseMulExpr()
	if err != nil {
		return nil, err
	}

	for p.curTokenIs(TOKEN_PLUS) || p.curTokenIs(TOKEN_MINUS) {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseMulExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

// parseMulExpr parses multiplication and division expressions.
func (p *Parser) parseMulExpr() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	for p.curTokenIs(TOKEN_STAR) || p.curTokenIs(TOKEN_SLASH) {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	// Handle unary minus for negative numbers
	if p.curTokenIs(TOKEN_MINUS) {
		p.nextToken() // consume -
		if p.curTokenIs(TOKEN_INT) {
			val, err := strconv.ParseInt(p.cur.Literal, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer: %s", p.cur.Literal)
			}
			val = -val // negate
			p.nextToken()
			if val >= -2147483648 && val <= 2147483647 {
				return &LiteralExpr{Value: catalog.NewInt32(int32(val))}, nil
			}
			return &LiteralExpr{Value: catalog.NewInt64(val)}, nil
		}
		// For other expressions, create a unary minus expression
		expr, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		// Convert to: 0 - expr
		return &BinaryExpr{
			Left:  &LiteralExpr{Value: catalog.NewInt32(0)},
			Op:    TOKEN_MINUS,
			Right: expr,
		}, nil
	}

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

	case TOKEN_COALESCE, TOKEN_NULLIF, TOKEN_UPPER, TOKEN_LOWER, TOKEN_LENGTH, TOKEN_CONCAT, TOKEN_SUBSTR, TOKEN_SUBSTRING:
		return p.parseFunctionCall()

	// Math functions
	case TOKEN_ABS, TOKEN_ROUND, TOKEN_FLOOR, TOKEN_CEIL, TOKEN_CEILING, TOKEN_MOD, TOKEN_POWER, TOKEN_SQRT:
		return p.parseFunctionCall()

	// Additional string functions
	case TOKEN_TRIM, TOKEN_LTRIM, TOKEN_RTRIM, TOKEN_REPLACE, TOKEN_POSITION, TOKEN_REVERSE, TOKEN_REPEAT, TOKEN_LPAD, TOKEN_RPAD:
		return p.parseFunctionCall()

	case TOKEN_CAST:
		return p.parseCastExpression()

	case TOKEN_EXTRACT:
		return p.parseExtractExpression()

	case TOKEN_NOW, TOKEN_CURRENT_TIMESTAMP, TOKEN_CURRENT_DATE:
		return p.parseDateFunction()

	case TOKEN_YEAR, TOKEN_MONTH, TOKEN_DAY, TOKEN_HOUR, TOKEN_MINUTE, TOKEN_SECOND:
		return p.parseDatePartFunction()

	case TOKEN_DATE_ADD, TOKEN_DATE_SUB:
		return p.parseDateAddFunction()

	case TOKEN_CASE:
		return p.parseCaseExpression()

	case TOKEN_LPAREN:
		p.nextToken()
		// Check if this is a subquery (SELECT inside parentheses)
		if p.curTokenIs(TOKEN_SELECT) {
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, fmt.Errorf("error parsing subquery: %v", err)
			}
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after subquery")
			}
			return &SubqueryExpr{Query: subquery}, nil
		}
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	case TOKEN_EXISTS:
		p.nextToken() // consume EXISTS
		if err := p.expect(TOKEN_LPAREN); err != nil {
			return nil, fmt.Errorf("expected '(' after EXISTS")
		}
		if !p.curTokenIs(TOKEN_SELECT) {
			return nil, fmt.Errorf("expected SELECT after EXISTS (")
		}
		subquery, err := p.parseSelect()
		if err != nil {
			return nil, fmt.Errorf("error parsing EXISTS subquery: %v", err)
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, fmt.Errorf("expected ')' after EXISTS subquery")
		}
		return &ExistsExpr{Query: subquery, Not: false}, nil

	case TOKEN_NOT:
		p.nextToken() // consume NOT
		// Handle NOT EXISTS
		if p.curTokenIs(TOKEN_EXISTS) {
			p.nextToken() // consume EXISTS
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, fmt.Errorf("expected '(' after NOT EXISTS")
			}
			if !p.curTokenIs(TOKEN_SELECT) {
				return nil, fmt.Errorf("expected SELECT after NOT EXISTS (")
			}
			subquery, err := p.parseSelect()
			if err != nil {
				return nil, fmt.Errorf("error parsing NOT EXISTS subquery: %v", err)
			}
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after NOT EXISTS subquery")
			}
			return &ExistsExpr{Query: subquery, Not: true}, nil
		}
		// Handle other NOT expressions (NOT expr)
		expr, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: TOKEN_NOT, Expr: expr}, nil

	case TOKEN_COUNT, TOKEN_SUM, TOKEN_AVG, TOKEN_MIN, TOKEN_MAX:
		// Aggregate functions in expressions (e.g., in HAVING clause)
		return p.parseAggregateExpression()

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

// parseFunctionCall parses function calls like COALESCE(a, b) or NULLIF(a, b).
func (p *Parser) parseFunctionCall() (Expression, error) {
	funcName := p.cur.Literal
	p.nextToken() // consume function name

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after %s", funcName)
	}

	var args []Expression
	for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken() // consume comma
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return &FunctionExpr{Name: funcName, Args: args}, nil
}

// parseAggregateExpression parses aggregate functions like COUNT(*), SUM(column), etc.
func (p *Parser) parseAggregateExpression() (Expression, error) {
	funcName := p.aggregateName()
	p.nextToken() // consume aggregate function name

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected ( after %s", funcName)
	}

	var arg Expression
	if p.curTokenIs(TOKEN_STAR) {
		// COUNT(*)
		arg = &LiteralExpr{Value: catalog.NewText("*")}
		p.nextToken()
	} else if p.curTokenIs(TOKEN_DISTINCT) {
		// COUNT(DISTINCT col)
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		// Wrap in a special marker - for now, just use a function expr
		arg = &FunctionExpr{Name: "DISTINCT", Args: []Expression{expr}}
	} else {
		// SUM(column), AVG(column), etc.
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		arg = expr
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return &FunctionExpr{Name: funcName, Args: []Expression{arg}}, nil
}

// parseDateFunction parses NOW(), CURRENT_TIMESTAMP, CURRENT_DATE
func (p *Parser) parseDateFunction() (Expression, error) {
	funcName := p.cur.Literal
	p.nextToken() // consume function name

	// Optional parentheses with no arguments
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, fmt.Errorf("expected ) after %s(", funcName)
		}
	}

	return &FunctionExpr{Name: funcName, Args: nil}, nil
}

// parseDatePartFunction parses YEAR(date), MONTH(date), DAY(date), etc.
func (p *Parser) parseDatePartFunction() (Expression, error) {
	funcName := p.cur.Literal
	p.nextToken() // consume function name

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected ( after %s", funcName)
	}

	arg, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("error parsing argument to %s: %w", funcName, err)
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return &FunctionExpr{Name: funcName, Args: []Expression{arg}}, nil
}

// parseDateAddFunction parses DATE_ADD(date, INTERVAL n unit) and DATE_SUB(date, INTERVAL n unit)
func (p *Parser) parseDateAddFunction() (Expression, error) {
	funcName := p.cur.Literal
	p.nextToken() // consume DATE_ADD or DATE_SUB

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected ( after %s", funcName)
	}

	// Parse the date expression
	dateExpr, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("error parsing date argument: %w", err)
	}

	if err := p.expect(TOKEN_COMMA); err != nil {
		return nil, fmt.Errorf("expected comma after date in %s", funcName)
	}

	// Expect INTERVAL keyword
	if err := p.expect(TOKEN_INTERVAL); err != nil {
		return nil, fmt.Errorf("expected INTERVAL keyword in %s", funcName)
	}

	// Parse the interval value (should be an integer)
	if !p.curTokenIs(TOKEN_INT) {
		return nil, fmt.Errorf("expected integer interval value in %s", funcName)
	}
	intervalValue := p.cur.Literal
	p.nextToken()

	// Parse the interval unit (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
	var intervalUnit string
	switch p.cur.Type {
	case TOKEN_YEAR:
		intervalUnit = "YEAR"
	case TOKEN_MONTH:
		intervalUnit = "MONTH"
	case TOKEN_DAY:
		intervalUnit = "DAY"
	case TOKEN_HOUR:
		intervalUnit = "HOUR"
	case TOKEN_MINUTE:
		intervalUnit = "MINUTE"
	case TOKEN_SECOND:
		intervalUnit = "SECOND"
	default:
		return nil, fmt.Errorf("expected interval unit (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) in %s", funcName)
	}
	p.nextToken() // consume unit

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	// Create a FunctionExpr with special arguments for interval
	// Args: [dateExpr, intervalValue (as literal), intervalUnit (as literal)]
	return &FunctionExpr{
		Name: funcName,
		Args: []Expression{
			dateExpr,
			&LiteralExpr{Value: catalog.NewText(intervalValue)},
			&LiteralExpr{Value: catalog.NewText(intervalUnit)},
		},
	}, nil
}

// parseCastExpression parses CAST(expr AS type)
func (p *Parser) parseCastExpression() (Expression, error) {
	p.nextToken() // consume CAST

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected ( after CAST")
	}

	// Parse the expression to cast
	expr, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("error parsing CAST expression: %w", err)
	}

	// Expect AS keyword
	if err := p.expect(TOKEN_AS); err != nil {
		return nil, fmt.Errorf("expected AS in CAST expression")
	}

	// Parse target type
	var targetType catalog.DataType
	switch p.cur.Type {
	case TOKEN_INT_TYPE:
		targetType = catalog.TypeInt32
	case TOKEN_BIGINT:
		targetType = catalog.TypeInt64
	case TOKEN_TEXT:
		targetType = catalog.TypeText
	case TOKEN_BOOL:
		targetType = catalog.TypeBool
	case TOKEN_TIMESTAMP:
		targetType = catalog.TypeTimestamp
	default:
		return nil, fmt.Errorf("unsupported CAST target type: %v", p.cur.Literal)
	}
	p.nextToken()

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return &CastExpr{Expr: expr, TargetType: targetType}, nil
}

// parseExtractExpression parses EXTRACT(part FROM date)
func (p *Parser) parseExtractExpression() (Expression, error) {
	p.nextToken() // consume EXTRACT

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected ( after EXTRACT")
	}

	// Parse the part (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
	var part string
	switch p.cur.Type {
	case TOKEN_YEAR:
		part = "YEAR"
	case TOKEN_MONTH:
		part = "MONTH"
	case TOKEN_DAY:
		part = "DAY"
	case TOKEN_HOUR:
		part = "HOUR"
	case TOKEN_MINUTE:
		part = "MINUTE"
	case TOKEN_SECOND:
		part = "SECOND"
	default:
		return nil, fmt.Errorf("expected YEAR, MONTH, DAY, HOUR, MINUTE, or SECOND in EXTRACT, got %v", p.cur.Literal)
	}
	p.nextToken()

	// Expect FROM keyword
	if err := p.expect(TOKEN_FROM); err != nil {
		return nil, fmt.Errorf("expected FROM in EXTRACT expression")
	}

	// Parse the date expression
	dateExpr, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("error parsing EXTRACT date: %w", err)
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	// Return as a function call with the part name and date
	return &FunctionExpr{
		Name: "EXTRACT",
		Args: []Expression{
			&LiteralExpr{Value: catalog.NewText(part)},
			dateExpr,
		},
	}, nil
}

// parseAlter parses: ALTER TABLE table_name action
// Actions: ADD [COLUMN] col_def, DROP COLUMN col_name, RENAME TO new_name, RENAME COLUMN old TO new
func (p *Parser) parseAlter() (*AlterTableStmt, error) {
	p.nextToken() // consume ALTER

	// Expect TABLE
	if err := p.expect(TOKEN_TABLE); err != nil {
		return nil, err
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName := p.cur.Literal
	p.nextToken()

	stmt := &AlterTableStmt{TableName: tableName}

	// Parse action
	switch {
	case p.curTokenIs(TOKEN_ADD):
		p.nextToken() // consume ADD
		// Optional COLUMN keyword
		if p.curTokenIs(TOKEN_COLUMN) {
			p.nextToken()
		}
		// Parse column definition
		colDef, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Action = "ADD COLUMN"
		stmt.ColumnDef = &colDef

	case p.curTokenIs(TOKEN_DROP):
		p.nextToken() // consume DROP
		// Expect COLUMN
		if err := p.expect(TOKEN_COLUMN); err != nil {
			return nil, fmt.Errorf("expected COLUMN after DROP in ALTER TABLE")
		}
		// Column name
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected column name after DROP COLUMN")
		}
		stmt.Action = "DROP COLUMN"
		stmt.ColumnName = p.cur.Literal
		p.nextToken()

	case p.curTokenIs(TOKEN_RENAME):
		p.nextToken() // consume RENAME
		if p.curTokenIs(TOKEN_TO) {
			// RENAME TO new_table_name
			p.nextToken() // consume TO
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected new table name after RENAME TO")
			}
			stmt.Action = "RENAME TO"
			stmt.NewName = p.cur.Literal
			p.nextToken()
		} else if p.curTokenIs(TOKEN_COLUMN) {
			// RENAME COLUMN old_name TO new_name
			p.nextToken() // consume COLUMN
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name after RENAME COLUMN")
			}
			stmt.ColumnName = p.cur.Literal
			p.nextToken()
			if err := p.expect(TOKEN_TO); err != nil {
				return nil, fmt.Errorf("expected TO after column name in RENAME COLUMN")
			}
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected new column name after TO")
			}
			stmt.Action = "RENAME COLUMN"
			stmt.NewName = p.cur.Literal
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected TO or COLUMN after RENAME")
		}

	default:
		return nil, fmt.Errorf("expected ADD, DROP, or RENAME in ALTER TABLE, got %v", p.cur.Type)
	}

	return stmt, nil
}

// parseTruncate parses: TRUNCATE [TABLE] table_name
func (p *Parser) parseTruncate() (*TruncateTableStmt, error) {
	p.nextToken() // consume TRUNCATE

	// Optional TABLE keyword
	if p.curTokenIs(TOKEN_TABLE) {
		p.nextToken()
	}

	// Table name
	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName := p.cur.Literal
	p.nextToken()

	return &TruncateTableStmt{TableName: tableName}, nil
}

// parseShow parses: SHOW TABLES or SHOW CREATE TABLE table_name
func (p *Parser) parseShow() (*ShowStmt, error) {
	p.nextToken() // consume SHOW

	if p.curTokenIs(TOKEN_TABLES) {
		p.nextToken() // consume TABLES
		return &ShowStmt{ShowType: "TABLES"}, nil
	}

	if p.curTokenIs(TOKEN_CREATE) {
		p.nextToken() // consume CREATE
		if err := p.expect(TOKEN_TABLE); err != nil {
			return nil, fmt.Errorf("expected TABLE after SHOW CREATE")
		}
		if !p.curTokenIs(TOKEN_IDENT) {
			return nil, fmt.Errorf("expected table name after SHOW CREATE TABLE")
		}
		tableName := p.cur.Literal
		p.nextToken()
		return &ShowStmt{ShowType: "CREATE TABLE", TableName: tableName}, nil
	}

	return nil, fmt.Errorf("expected TABLES or CREATE after SHOW, got %v", p.cur.Type)
}

// parseExplain parses EXPLAIN [ANALYZE] SELECT ...
func (p *Parser) parseExplain() (Statement, error) {
	p.nextToken() // consume EXPLAIN

	analyze := false
	if p.curTokenIs(TOKEN_ANALYZE) {
		analyze = true
		p.nextToken() // consume ANALYZE
	}

	// Only SELECT statements can be explained
	if !p.curTokenIs(TOKEN_SELECT) {
		return nil, fmt.Errorf("expected SELECT after EXPLAIN, got %v", p.cur.Type)
	}

	stmt, err := p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("error parsing EXPLAIN SELECT: %w", err)
	}

	return &ExplainStmt{
		Statement: stmt,
		Analyze:   analyze,
	}, nil
}

// parseCaseExpression parses CASE WHEN expressions.
// Supports both:
//   - Simple CASE: CASE expr WHEN val1 THEN res1 [WHEN val2 THEN res2...] [ELSE default] END
//   - Searched CASE: CASE WHEN cond1 THEN res1 [WHEN cond2 THEN res2...] [ELSE default] END
func (p *Parser) parseCaseExpression() (Expression, error) {
	p.nextToken() // consume CASE

	caseExpr := &CaseExpr{}

	// Check if this is a simple CASE (has an operand) or searched CASE (starts with WHEN)
	if !p.curTokenIs(TOKEN_WHEN) {
		// Simple CASE - parse the operand expression
		operand, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing CASE operand: %w", err)
		}
		caseExpr.Operand = operand
	}

	// Parse WHEN clauses
	for p.curTokenIs(TOKEN_WHEN) {
		p.nextToken() // consume WHEN

		// Parse the condition
		condition, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing WHEN condition: %w", err)
		}

		// Expect THEN
		if !p.curTokenIs(TOKEN_THEN) {
			return nil, fmt.Errorf("expected THEN after WHEN condition, got %v", p.cur.Type)
		}
		p.nextToken() // consume THEN

		// Parse the result expression
		result, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing THEN result: %w", err)
		}

		caseExpr.Whens = append(caseExpr.Whens, WhenClause{
			Condition: condition,
			Result:    result,
		})
	}

	// Must have at least one WHEN clause
	if len(caseExpr.Whens) == 0 {
		return nil, fmt.Errorf("CASE requires at least one WHEN clause")
	}

	// Parse optional ELSE clause
	if p.curTokenIs(TOKEN_ELSE) {
		p.nextToken() // consume ELSE
		elseExpr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing ELSE expression: %w", err)
		}
		caseExpr.Else = elseExpr
	}

	// Expect END
	if !p.curTokenIs(TOKEN_END) {
		return nil, fmt.Errorf("expected END to close CASE expression, got %v", p.cur.Type)
	}
	p.nextToken() // consume END

	return caseExpr, nil
}
