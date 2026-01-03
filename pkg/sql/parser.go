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
	case TOKEN_WITH:
		return p.parseWith()
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
	case TOKEN_MERGE:
		return p.parseMerge()
	case TOKEN_PREPARE:
		return p.parsePrepare()
	case TOKEN_EXECUTE:
		return p.parseExecute()
	case TOKEN_DEALLOCATE:
		return p.parseDeallocate()
	case TOKEN_GRANT:
		return p.parseGrant()
	case TOKEN_REVOKE:
		return p.parseRevoke()
	case TOKEN_USE:
		return p.parseUseDatabase()
	case TOKEN_CALL:
		return p.parseCall()
	default:
		return nil, fmt.Errorf("unexpected token %v (%q) at position %d", p.cur.Type, p.cur.Literal, p.cur.Pos)
	}
}

// parseWith parses WITH clause (Common Table Expressions).
// WITH [RECURSIVE] cte_name [(col1, col2)] AS (SELECT ...) [, cte_name2 AS (...)] SELECT ...
func (p *Parser) parseWith() (Statement, error) {
	p.nextToken() // consume WITH

	withClause := &WithClause{}

	// Check for RECURSIVE
	if p.curTokenIs(TOKEN_RECURSIVE) {
		withClause.Recursive = true
		p.nextToken() // consume RECURSIVE
	}

	// Parse CTE definitions (at least one required)
	for {
		cte := CTE{}

		// CTE name
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected CTE name, got %v", p.cur.Type)
		}
		name, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		cte.Name = name
		cte.Recursive = withClause.Recursive

		// Optional column list: (col1, col2, ...)
		if p.curTokenIs(TOKEN_LPAREN) {
			p.nextToken() // consume '('
			for {
				if !p.isIdentifierOrContextualKeyword() {
					return nil, fmt.Errorf("expected column name in CTE column list, got %v", p.cur.Type)
				}
				col, err := p.parseIdentifier()
				if err != nil {
					return nil, err
				}
				cte.Columns = append(cte.Columns, col)

				if p.curTokenIs(TOKEN_COMMA) {
					p.nextToken() // consume comma
				} else {
					break
				}
			}
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after CTE column list: %w", err)
			}
		}

		// AS
		if err := p.expect(TOKEN_AS); err != nil {
			return nil, fmt.Errorf("expected AS after CTE name: %w", err)
		}

		// (SELECT ...)
		if err := p.expect(TOKEN_LPAREN); err != nil {
			return nil, fmt.Errorf("expected '(' after AS: %w", err)
		}

		if !p.curTokenIs(TOKEN_SELECT) {
			return nil, fmt.Errorf("expected SELECT in CTE definition, got %v", p.cur.Type)
		}
		cteQuery, err := p.parseSelect()
		if err != nil {
			return nil, fmt.Errorf("failed to parse CTE query: %w", err)
		}

		// Check if there's a UNION/INTERSECT/EXCEPT after the SELECT
		if p.curTokenIs(TOKEN_UNION) || p.curTokenIs(TOKEN_INTERSECT) || p.curTokenIs(TOKEN_EXCEPT) {
			unionStmt, err := p.parseSetOperation(cteQuery)
			if err != nil {
				return nil, fmt.Errorf("failed to parse UNION in CTE: %w", err)
			}
			cte.UnionQuery = unionStmt
		} else {
			cte.Query = cteQuery
		}

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, fmt.Errorf("expected ')' after CTE query: %w", err)
		}

		withClause.CTEs = append(withClause.CTEs, cte)

		// Check for more CTEs
		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken() // consume comma
			continue
		}
		break
	}

	// Now parse the main query (SELECT or UNION/INTERSECT/EXCEPT)
	if !p.curTokenIs(TOKEN_SELECT) {
		return nil, fmt.Errorf("expected SELECT after WITH clause, got %v", p.cur.Type)
	}

	selectStmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	selectStmt.With = withClause

	// Check for UNION/INTERSECT/EXCEPT
	if p.curTokenIs(TOKEN_UNION) || p.curTokenIs(TOKEN_INTERSECT) || p.curTokenIs(TOKEN_EXCEPT) {
		unionStmt, err := p.parseSetOperation(selectStmt)
		if err != nil {
			return nil, err
		}
		// Move WITH clause from SelectStmt to UnionStmt
		unionStmt.With = withClause
		selectStmt.With = nil // Clear from first select
		return unionStmt, nil
	}

	return selectStmt, nil
}

// parseSelect parses: SELECT [DISTINCT [ON (cols)]] columns FROM table [WHERE expr] [ORDER BY cols] [LIMIT n] [OFFSET n]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	return p.parseSelectFull(true)
}

// parseSelectForSetOp parses SELECT without ORDER BY/LIMIT/OFFSET (for UNION/INTERSECT/EXCEPT right side)
func (p *Parser) parseSelectForSetOp() (*SelectStmt, error) {
	return p.parseSelectFull(false)
}

// parseSelectFull parses a SELECT statement. If parseTrailingClauses is false,
// it stops before ORDER BY/LIMIT/OFFSET (used for right side of set operations).
func (p *Parser) parseSelectFull(parseTrailingClauses bool) (*SelectStmt, error) {
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

	// Table name (can be a keyword like "target" used as identifier)
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Optional table alias: AS alias or just alias
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected alias after AS")
		}
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TableAlias = alias
	} else if p.isIdentifierOrContextualKeyword() && !p.isKeyword() {
		// Implicit alias without AS
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TableAlias = alias
	}

	// Optional JOIN clauses (including LATERAL)
	for p.curTokenIs(TOKEN_JOIN) || p.curTokenIs(TOKEN_INNER) || p.curTokenIs(TOKEN_LEFT) || p.curTokenIs(TOKEN_RIGHT) || p.curTokenIs(TOKEN_FULL) || p.curTokenIs(TOKEN_CROSS) || p.curTokenIs(TOKEN_LATERAL) || p.curTokenIs(TOKEN_COMMA) {
		// Handle comma-separated LATERAL subquery: FROM t, LATERAL (SELECT ...)
		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken() // consume comma
			if p.curTokenIs(TOKEN_LATERAL) {
				// Comma + LATERAL: treat as implicit CROSS JOIN LATERAL
				p.nextToken() // consume LATERAL
				if !p.curTokenIs(TOKEN_LPAREN) {
					return nil, fmt.Errorf("expected '(' after LATERAL, got %v", p.cur.Type)
				}
				p.nextToken() // consume '('

				if !p.curTokenIs(TOKEN_SELECT) {
					return nil, fmt.Errorf("expected SELECT after LATERAL '(', got %v", p.cur.Type)
				}
				subquery, err := p.parseSelectStatement()
				if err != nil {
					return nil, fmt.Errorf("failed to parse LATERAL subquery: %w", err)
				}
				selectStmt, ok := subquery.(*SelectStmt)
				if !ok {
					return nil, fmt.Errorf("expected SELECT statement in LATERAL subquery")
				}

				if err := p.expect(TOKEN_RPAREN); err != nil {
					return nil, fmt.Errorf("expected ')' after LATERAL subquery: %w", err)
				}

				join := JoinClause{
					JoinType: "CROSS",
					Lateral:  true,
					Subquery: selectStmt,
				}

				// Subquery alias (required)
				if p.curTokenIs(TOKEN_AS) {
					p.nextToken() // consume AS
				}
				if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
					join.TableAlias = p.cur.Literal
					p.nextToken()
				} else {
					return nil, fmt.Errorf("LATERAL subquery requires an alias")
				}

				stmt.Joins = append(stmt.Joins, join)
				continue
			} else {
				// Regular comma-separated table - treat as implicit CROSS JOIN
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected table name after comma, got %v", p.cur.Type)
				}
				join := JoinClause{
					JoinType:  "CROSS",
					TableName: p.cur.Literal,
				}
				p.nextToken()

				// Optional alias
				if p.curTokenIs(TOKEN_AS) {
					p.nextToken()
					if !p.curTokenIs(TOKEN_IDENT) {
						return nil, fmt.Errorf("expected alias after AS")
					}
					join.TableAlias = p.cur.Literal
					p.nextToken()
				} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
					join.TableAlias = p.cur.Literal
					p.nextToken()
				}

				stmt.Joins = append(stmt.Joins, join)
				continue
			}
		}

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
		groupBy, groupingSets, err := p.parseGroupByClause()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
		stmt.GroupingSets = groupingSets
	}

	// Optional HAVING (only valid with GROUP BY or GROUPING SETS)
	if p.curTokenIs(TOKEN_HAVING) {
		if len(stmt.GroupBy) == 0 && len(stmt.GroupingSets) == 0 {
			return nil, fmt.Errorf("HAVING requires GROUP BY clause")
		}
		p.nextToken() // consume HAVING
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Having = expr
	}

	// Optional ORDER BY, LIMIT, OFFSET - only parsed for standalone SELECTs
	// (not for right side of UNION/INTERSECT/EXCEPT where these apply to the whole set operation)
	if parseTrailingClauses {
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

// parseGroupByClause parses GROUP BY with support for GROUPING SETS, CUBE, ROLLUP.
// Syntax examples:
//
//	GROUP BY col1, col2
//	GROUP BY ROLLUP(col1, col2)
//	GROUP BY CUBE(col1, col2)
//	GROUP BY GROUPING SETS ((col1, col2), (col1), ())
//	GROUP BY col1, ROLLUP(col2, col3)
func (p *Parser) parseGroupByClause() ([]string, []GroupingSet, error) {
	var simpleGroupBy []string
	var groupingSets []GroupingSet

	for {
		if p.curTokenIs(TOKEN_ROLLUP) {
			// ROLLUP(col1, col2, ...) expands to (col1,col2,...), (col1,col2), ..., (col1), ()
			p.nextToken() // consume ROLLUP
			cols, err := p.parseColumnListInParens()
			if err != nil {
				return nil, nil, fmt.Errorf("error parsing ROLLUP: %w", err)
			}
			// Expand ROLLUP: (a,b,c) -> (a,b,c), (a,b), (a), ()
			rollupSets := expandRollup(cols)
			groupingSets = append(groupingSets, rollupSets...)

		} else if p.curTokenIs(TOKEN_CUBE) {
			// CUBE(col1, col2, ...) expands to all combinations
			p.nextToken() // consume CUBE
			cols, err := p.parseColumnListInParens()
			if err != nil {
				return nil, nil, fmt.Errorf("error parsing CUBE: %w", err)
			}
			// Expand CUBE: (a,b) -> (), (a), (b), (a,b)
			cubeSets := expandCube(cols)
			groupingSets = append(groupingSets, cubeSets...)

		} else if p.curTokenIs(TOKEN_GROUPING) {
			// GROUPING SETS ((col1, col2), (col1), ())
			p.nextToken() // consume GROUPING
			if err := p.expect(TOKEN_SETS); err != nil {
				return nil, nil, fmt.Errorf("expected SETS after GROUPING: %w", err)
			}
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, nil, fmt.Errorf("expected '(' after GROUPING SETS: %w", err)
			}

			// Parse list of grouping sets
			for {
				set, err := p.parseGroupingSet()
				if err != nil {
					return nil, nil, err
				}
				groupingSets = append(groupingSets, set)

				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken() // consume comma
			}

			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, nil, fmt.Errorf("expected ')' after GROUPING SETS list: %w", err)
			}

		} else if p.isIdentifierOrContextualKeyword() {
			// Simple column name
			colName, err := p.parseIdentifier()
			if err != nil {
				return nil, nil, err
			}
			simpleGroupBy = append(simpleGroupBy, colName)

		} else {
			return nil, nil, fmt.Errorf("expected column name, ROLLUP, CUBE, or GROUPING SETS in GROUP BY, got %v", p.cur.Type)
		}

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	return simpleGroupBy, groupingSets, nil
}

// parseColumnListInParens parses (col1, col2, ...) and returns the column names.
func (p *Parser) parseColumnListInParens() ([]string, error) {
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	var cols []string
	for !p.curTokenIs(TOKEN_RPAREN) {
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected column name, got %v", p.cur.Type)
		}
		colName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		cols = append(cols, colName)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return cols, nil
}

// parseGroupingSet parses a single grouping set: (col1, col2) or () for grand total
func (p *Parser) parseGroupingSet() (GroupingSet, error) {
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return GroupingSet{}, fmt.Errorf("expected '(' for grouping set: %w", err)
	}

	var cols []string
	for !p.curTokenIs(TOKEN_RPAREN) {
		if !p.isIdentifierOrContextualKeyword() {
			return GroupingSet{}, fmt.Errorf("expected column name in grouping set, got %v", p.cur.Type)
		}
		colName, err := p.parseIdentifier()
		if err != nil {
			return GroupingSet{}, err
		}
		cols = append(cols, colName)

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume comma
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return GroupingSet{}, fmt.Errorf("expected ')' after grouping set: %w", err)
	}

	return GroupingSet{Columns: cols}, nil
}

// expandRollup generates grouping sets for ROLLUP(col1, col2, col3).
// ROLLUP(a, b, c) = (a, b, c), (a, b), (a), ()
func expandRollup(cols []string) []GroupingSet {
	var sets []GroupingSet
	for i := len(cols); i >= 0; i-- {
		sets = append(sets, GroupingSet{Columns: cols[:i]})
	}
	return sets
}

// expandCube generates grouping sets for CUBE(col1, col2, col3).
// CUBE(a, b) = (), (a), (b), (a, b)
func expandCube(cols []string) []GroupingSet {
	n := len(cols)
	count := 1 << n // 2^n combinations
	sets := make([]GroupingSet, 0, count)

	for mask := 0; mask < count; mask++ {
		var setCols []string
		for i := 0; i < n; i++ {
			if mask&(1<<i) != 0 {
				setCols = append(setCols, cols[i])
			}
		}
		sets = append(sets, GroupingSet{Columns: setCols})
	}
	return sets
}

// parseJoinClause parses: [INNER|LEFT|RIGHT|FULL|CROSS] [LATERAL] JOIN table|subquery [ON condition]
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
	} else if p.curTokenIs(TOKEN_LATERAL) {
		// LATERAL without explicit join type defaults to INNER
		join.Lateral = true
		p.nextToken()
	}

	// Check for LATERAL after join type (e.g., LEFT LATERAL JOIN, CROSS LATERAL JOIN)
	if p.curTokenIs(TOKEN_LATERAL) {
		join.Lateral = true
		p.nextToken()
	}

	// Expect JOIN keyword
	if err := p.expect(TOKEN_JOIN); err != nil {
		return join, err
	}

	// Check for LATERAL after JOIN keyword (e.g., JOIN LATERAL)
	if p.curTokenIs(TOKEN_LATERAL) {
		join.Lateral = true
		p.nextToken()
	}

	// Check if joining a subquery (derived table)
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume '('

		// Parse the subquery
		if !p.curTokenIs(TOKEN_SELECT) {
			return join, fmt.Errorf("expected SELECT after '(' in JOIN, got %v", p.cur.Type)
		}
		subquery, err := p.parseSelectStatement()
		if err != nil {
			return join, fmt.Errorf("failed to parse subquery in JOIN: %w", err)
		}
		selectStmt, ok := subquery.(*SelectStmt)
		if !ok {
			return join, fmt.Errorf("expected SELECT statement in JOIN subquery")
		}
		join.Subquery = selectStmt

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return join, fmt.Errorf("expected ')' after subquery in JOIN: %w", err)
		}

		// Subquery alias (required for derived tables)
		if p.curTokenIs(TOKEN_AS) {
			p.nextToken() // consume AS
		}
		if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
			join.TableAlias = p.cur.Literal
			p.nextToken()
		} else if join.Subquery != nil && join.TableAlias == "" {
			return join, fmt.Errorf("derived table (subquery) requires an alias")
		}
	} else {
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
	}

	// CROSS JOIN has no ON condition (LATERAL CROSS JOIN also)
	if isCrossJoin {
		return join, nil
	}

	// For LATERAL without ON clause (implicit join), condition is optional
	if join.Lateral && !p.curTokenIs(TOKEN_ON) {
		return join, nil
	}

	// ON keyword (required for non-CROSS, non-implicit-LATERAL joins)
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

// isWindowFunctionToken returns true if the token is a window-specific function.
func (p *Parser) isWindowFunctionToken() bool {
	switch p.cur.Type {
	case TOKEN_ROW_NUMBER, TOKEN_RANK, TOKEN_DENSE_RANK, TOKEN_NTILE,
		TOKEN_LAG, TOKEN_LEAD, TOKEN_FIRST_VALUE, TOKEN_LAST_VALUE, TOKEN_NTH_VALUE:
		return true
	}
	return false
}

// windowFunctionName returns the name of the current window function token.
func (p *Parser) windowFunctionName() string {
	switch p.cur.Type {
	case TOKEN_ROW_NUMBER:
		return "ROW_NUMBER"
	case TOKEN_RANK:
		return "RANK"
	case TOKEN_DENSE_RANK:
		return "DENSE_RANK"
	case TOKEN_NTILE:
		return "NTILE"
	case TOKEN_LAG:
		return "LAG"
	case TOKEN_LEAD:
		return "LEAD"
	case TOKEN_FIRST_VALUE:
		return "FIRST_VALUE"
	case TOKEN_LAST_VALUE:
		return "LAST_VALUE"
	case TOKEN_NTH_VALUE:
		return "NTH_VALUE"
	}
	return ""
}

// isKeyword returns true if the current token is a SQL keyword (not suitable for implicit alias).
func (p *Parser) isKeyword() bool {
	switch p.cur.Type {
	case TOKEN_FROM, TOKEN_WHERE, TOKEN_ORDER, TOKEN_GROUP, TOKEN_HAVING,
		TOKEN_LIMIT, TOKEN_OFFSET, TOKEN_JOIN, TOKEN_INNER, TOKEN_LEFT,
		TOKEN_RIGHT, TOKEN_FULL, TOKEN_CROSS, TOKEN_OUTER, TOKEN_ON, TOKEN_AND, TOKEN_OR, TOKEN_AS,
		TOKEN_IN, TOKEN_BETWEEN, TOKEN_COMMA, TOKEN_SEMICOLON, TOKEN_OVER, TOKEN_PARTITION,
		TOKEN_WHEN, TOKEN_THEN, TOKEN_MATCHED:
		return true
	}
	return false
}

// isIdentifierOrContextualKeyword returns true if the current token is an identifier
// or a contextual keyword that can be used as an identifier in certain positions.
// This allows table/column names like "target", "source", "matched" etc.
func (p *Parser) isIdentifierOrContextualKeyword() bool {
	if p.curTokenIs(TOKEN_IDENT) {
		return true
	}
	// Contextual keywords that can be used as identifiers
	switch p.cur.Type {
	case TOKEN_TARGET, TOKEN_SOURCE, TOKEN_MATCHED, TOKEN_NOTHING,
		TOKEN_YEAR, TOKEN_MONTH, TOKEN_DAY, TOKEN_HOUR, TOKEN_MINUTE, TOKEN_SECOND,
		TOKEN_DEFAULT: // Allow "default" as database name
		return true
	}
	return false
}

// parseIdentifier parses an identifier, which can be either a regular identifier
// or a contextual keyword being used as an identifier.
func (p *Parser) parseIdentifier() (string, error) {
	if p.isIdentifierOrContextualKeyword() {
		name := p.cur.Literal
		p.nextToken()
		return name, nil
	}
	return "", fmt.Errorf("expected identifier, got %v (%q)", p.cur.Type, p.cur.Literal)
}

// parseSelectStatement parses a SELECT statement when the cursor is on TOKEN_SELECT.
// This is a helper for parsing subqueries.
func (p *Parser) parseSelectStatement() (Statement, error) {
	if !p.curTokenIs(TOKEN_SELECT) {
		return nil, fmt.Errorf("expected SELECT, got %v", p.cur.Type)
	}
	return p.parseSelect()
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
	// Grouping function (for GROUPING SETS/CUBE/ROLLUP)
	case TOKEN_GROUPING:
		return true
	// Full-Text Search functions
	case TOKEN_TO_TSVECTOR, TOKEN_TO_TSQUERY, TOKEN_PLAINTO_TSQUERY, TOKEN_WEBSEARCH,
		TOKEN_TS_RANK, TOKEN_TS_HEADLINE, TOKEN_MATCH:
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
		} else if p.isWindowFunctionToken() {
			// Parse window-specific functions: ROW_NUMBER(), RANK(), etc.
			funcName := p.windowFunctionName()
			p.nextToken() // consume function name

			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, fmt.Errorf("expected ( after %s", funcName)
			}

			// Parse optional arguments (e.g., NTILE(4), LAG(col, 1))
			var args []Expression
			if !p.curTokenIs(TOKEN_RPAREN) {
				for {
					arg, err := p.parseExpression()
					if err != nil {
						return nil, err
					}
					args = append(args, arg)
					if !p.curTokenIs(TOKEN_COMMA) {
						break
					}
					p.nextToken() // consume comma
				}
			}

			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, err
			}

			// Window functions MUST have OVER clause
			if !p.curTokenIs(TOKEN_OVER) {
				return nil, fmt.Errorf("%s requires OVER clause", funcName)
			}

			overClause, err := p.parseOverClause()
			if err != nil {
				return nil, err
			}

			windowExpr := &WindowFuncExpr{
				Function: funcName,
				Args:     args,
				Over:     overClause,
			}

			col := SelectColumn{Expression: windowExpr}

			// Check for alias
			if p.curTokenIs(TOKEN_AS) {
				p.nextToken()
				if !p.curTokenIs(TOKEN_IDENT) {
					return nil, fmt.Errorf("expected alias name after AS")
				}
				col.Alias = p.cur.Literal
				p.nextToken()
			} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
				col.Alias = p.cur.Literal
				p.nextToken()
			}

			cols = append(cols, col)
		} else if p.isAggregateToken() {
			// Parse aggregate function: COUNT(*), SUM(col), etc.
			// May also be a window function if followed by OVER
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

			// Check if this is a window function (aggregate with OVER clause)
			if p.curTokenIs(TOKEN_OVER) {
				overClause, err := p.parseOverClause()
				if err != nil {
					return nil, err
				}

				// Convert to window function expression
				var args []Expression
				if arg != "*" {
					args = append(args, &ColumnRef{Name: arg})
				}

				windowExpr := &WindowFuncExpr{
					Function: funcName,
					Args:     args,
					Over:     overClause,
				}

				col := SelectColumn{Expression: windowExpr}

				// Check for alias
				if p.curTokenIs(TOKEN_AS) {
					p.nextToken()
					if !p.curTokenIs(TOKEN_IDENT) {
						return nil, fmt.Errorf("expected alias name after AS")
					}
					col.Alias = p.cur.Literal
					p.nextToken()
				} else if p.curTokenIs(TOKEN_IDENT) && !p.isKeyword() {
					col.Alias = p.cur.Literal
					p.nextToken()
				}

				cols = append(cols, col)
			} else {
				// Regular aggregate function (no OVER)
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
			}
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

	// Table name (can be a keyword like "target" used as identifier)
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Optional column list
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken()
		for {
			if !p.isIdentifierOrContextualKeyword() {
				return nil, fmt.Errorf("expected column name, got %v", p.cur.Type)
			}
			colName, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, colName)

			if !p.curTokenIs(TOKEN_COMMA) {
				break
			}
			p.nextToken()
		}
		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
	}

	// VALUES or SELECT
	if p.curTokenIs(TOKEN_VALUES) {
		p.nextToken()
		// Parse multiple value rows: (v1, v2), (v3, v4), ...
		for {
			// (values)
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return nil, err
			}

			var rowValues []Expression
			for {
				expr, err := p.parsePrimaryExpression()
				if err != nil {
					return nil, err
				}
				rowValues = append(rowValues, expr)

				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken()
			}

			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, err
			}

			stmt.ValuesList = append(stmt.ValuesList, rowValues)

			// Check for more value rows
			if !p.curTokenIs(TOKEN_COMMA) {
				break
			}
			p.nextToken() // consume comma between value rows
		}
	} else if p.curTokenIs(TOKEN_SELECT) {
		selectStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = selectStmt
	} else {
		return nil, fmt.Errorf("expected VALUES or SELECT, got %v", p.cur.Type)
	}

	// Parse optional ON CONFLICT clause
	if p.curTokenIs(TOKEN_ON) {
		p.nextToken() // consume ON
		if !p.curTokenIs(TOKEN_CONFLICT) {
			return nil, fmt.Errorf("expected CONFLICT after ON, got %v", p.cur.Type)
		}
		p.nextToken() // consume CONFLICT

		onConflict := &OnConflictClause{}

		// Parse optional conflict columns: ON CONFLICT (col1, col2)
		if p.curTokenIs(TOKEN_LPAREN) {
			p.nextToken() // consume (
			for {
				if !p.isIdentifierOrContextualKeyword() {
					return nil, fmt.Errorf("expected column name in ON CONFLICT, got %v", p.cur.Type)
				}
				colName, err := p.parseIdentifier()
				if err != nil {
					return nil, err
				}
				onConflict.ConflictColumns = append(onConflict.ConflictColumns, colName)
				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken() // consume comma
			}
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, err
			}
		}

		// Parse DO NOTHING or DO UPDATE SET ...
		if !p.curTokenIs(TOKEN_DO) {
			return nil, fmt.Errorf("expected DO after ON CONFLICT, got %v", p.cur.Type)
		}
		p.nextToken() // consume DO

		if p.curTokenIs(TOKEN_NOTHING) {
			p.nextToken() // consume NOTHING
			onConflict.DoNothing = true
		} else if p.curTokenIs(TOKEN_UPDATE) {
			p.nextToken() // consume UPDATE
			if err := p.expect(TOKEN_SET); err != nil {
				return nil, fmt.Errorf("expected SET after DO UPDATE, got %v", p.cur.Type)
			}

			// Parse assignments
			for {
				if !p.isIdentifierOrContextualKeyword() {
					return nil, fmt.Errorf("expected column name in SET, got %v", p.cur.Type)
				}
				colName, err := p.parseIdentifier()
				if err != nil {
					return nil, err
				}

				if err := p.expect(TOKEN_EQ); err != nil {
					return nil, err
				}

				expr, err := p.parsePrimaryExpression()
				if err != nil {
					return nil, err
				}

				onConflict.UpdateSet = append(onConflict.UpdateSet, Assignment{Column: colName, Value: expr})

				if !p.curTokenIs(TOKEN_COMMA) {
					break
				}
				p.nextToken() // consume comma
			}
		} else {
			return nil, fmt.Errorf("expected NOTHING or UPDATE after DO, got %v", p.cur.Type)
		}

		stmt.OnConflict = onConflict
	}

	return stmt, nil
}

// parseUpdate parses: UPDATE table SET assignments [WHERE expr]
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{}

	p.nextToken() // consume UPDATE

	// Table name (can be a keyword like "target" used as identifier)
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Optional alias for target table
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected alias after AS, got %v", p.cur.Type)
		}
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TableAlias = alias
	} else if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_SET) {
		// Alias without AS keyword
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TableAlias = alias
	}

	// SET
	if err := p.expect(TOKEN_SET); err != nil {
		return nil, err
	}

	// Assignments
	for {
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected column name, got %v", p.cur.Type)
		}
		colName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		if err := p.expect(TOKEN_EQ); err != nil {
			return nil, err
		}

		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Assignments = append(stmt.Assignments, Assignment{Column: colName, Value: expr})

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken()
	}

	// Optional FROM clause (PostgreSQL style: UPDATE t1 SET ... FROM t2 WHERE ...)
	if p.curTokenIs(TOKEN_FROM) {
		p.nextToken() // consume FROM
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected table name in FROM, got %v", p.cur.Type)
		}
		fromTable, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.FromTable = fromTable

		// Optional alias for FROM table
		if p.curTokenIs(TOKEN_AS) {
			p.nextToken() // consume AS
			if !p.isIdentifierOrContextualKeyword() {
				return nil, fmt.Errorf("expected alias after AS, got %v", p.cur.Type)
			}
			alias, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			stmt.FromAlias = alias
		} else if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_WHERE) {
			// Alias without AS keyword
			alias, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			stmt.FromAlias = alias
		}
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

// parseDelete parses: DELETE FROM table [USING table] [WHERE expr]
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}

	p.nextToken() // consume DELETE

	if err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}

	// Table name (can be a keyword like "target" used as identifier)
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Optional alias for target table
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected alias after AS, got %v", p.cur.Type)
		}
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TableAlias = alias
	} else if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_USING) && !p.curTokenIs(TOKEN_WHERE) {
		// Alias without AS keyword
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TableAlias = alias
	}

	// Optional USING clause (PostgreSQL style: DELETE FROM t1 USING t2 WHERE ...)
	if p.curTokenIs(TOKEN_USING) {
		p.nextToken() // consume USING
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected table name in USING, got %v", p.cur.Type)
		}
		usingTable, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.UsingTable = usingTable

		// Optional alias for USING table
		if p.curTokenIs(TOKEN_AS) {
			p.nextToken() // consume AS
			if !p.isIdentifierOrContextualKeyword() {
				return nil, fmt.Errorf("expected alias after AS, got %v", p.cur.Type)
			}
			alias, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			stmt.UsingAlias = alias
		} else if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_WHERE) {
			// Alias without AS keyword
			alias, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			stmt.UsingAlias = alias
		}
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

// parseMerge parses: MERGE INTO target USING source ON condition
//
//	WHEN MATCHED [AND condition] THEN UPDATE SET ... / DELETE / DO NOTHING
//	WHEN NOT MATCHED [AND condition] THEN INSERT (...) VALUES (...) / DO NOTHING
func (p *Parser) parseMerge() (*MergeStmt, error) {
	stmt := &MergeStmt{}

	p.nextToken() // consume MERGE

	// INTO keyword
	if err := p.expect(TOKEN_INTO); err != nil {
		return nil, fmt.Errorf("expected INTO after MERGE: %w", err)
	}

	// Target table (can be a keyword like "target" used as identifier)
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected target table name after MERGE INTO, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TargetTable = tableName

	// Optional target alias
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected alias after AS")
		}
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TargetAlias = alias
	} else if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_USING) {
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.TargetAlias = alias
	}

	// USING keyword
	if err := p.expect(TOKEN_USING); err != nil {
		return nil, fmt.Errorf("expected USING after target table: %w", err)
	}

	// Source: can be a table name or subquery
	if p.curTokenIs(TOKEN_LPAREN) {
		// Subquery source
		p.nextToken() // consume '('
		if !p.curTokenIs(TOKEN_SELECT) {
			return nil, fmt.Errorf("expected SELECT after '(' in USING, got %v", p.cur.Type)
		}
		subquery, err := p.parseSelectStatement()
		if err != nil {
			return nil, fmt.Errorf("failed to parse source subquery: %w", err)
		}
		selectStmt, ok := subquery.(*SelectStmt)
		if !ok {
			return nil, fmt.Errorf("expected SELECT statement in USING subquery")
		}
		stmt.SourceQuery = selectStmt

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, fmt.Errorf("expected ')' after source subquery: %w", err)
		}
	} else if p.isIdentifierOrContextualKeyword() {
		// Table name source
		tableName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.SourceTable = tableName
	} else {
		return nil, fmt.Errorf("expected table name or subquery after USING, got %v", p.cur.Type)
	}

	// Optional source alias
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken() // consume AS
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected alias after AS")
		}
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.SourceAlias = alias
	} else if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_ON) {
		alias, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.SourceAlias = alias
	}

	// ON keyword
	if err := p.expect(TOKEN_ON); err != nil {
		return nil, fmt.Errorf("expected ON after source: %w", err)
	}

	// Match condition
	condition, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("expected match condition: %w", err)
	}
	stmt.Condition = condition

	// Parse WHEN clauses
	for p.curTokenIs(TOKEN_WHEN) {
		whenClause, err := p.parseMergeWhenClause()
		if err != nil {
			return nil, err
		}
		stmt.WhenClauses = append(stmt.WhenClauses, whenClause)
	}

	if len(stmt.WhenClauses) == 0 {
		return nil, fmt.Errorf("MERGE statement requires at least one WHEN clause")
	}

	return stmt, nil
}

// parseMergeWhenClause parses: WHEN [NOT] MATCHED [AND condition] THEN action
func (p *Parser) parseMergeWhenClause() (MergeWhenClause, error) {
	clause := MergeWhenClause{}

	p.nextToken() // consume WHEN

	// Check for NOT MATCHED
	if p.curTokenIs(TOKEN_NOT) {
		clause.Matched = false
		p.nextToken() // consume NOT
		if err := p.expect(TOKEN_MATCHED); err != nil {
			return clause, fmt.Errorf("expected MATCHED after NOT: %w", err)
		}
	} else if p.curTokenIs(TOKEN_MATCHED) {
		clause.Matched = true
		p.nextToken() // consume MATCHED
	} else {
		return clause, fmt.Errorf("expected MATCHED or NOT MATCHED after WHEN, got %v", p.cur.Type)
	}

	// Optional AND condition
	if p.curTokenIs(TOKEN_AND) {
		p.nextToken() // consume AND
		cond, err := p.parseExpression()
		if err != nil {
			return clause, fmt.Errorf("expected condition after AND: %w", err)
		}
		clause.Condition = cond
	}

	// THEN keyword
	if err := p.expect(TOKEN_THEN); err != nil {
		return clause, fmt.Errorf("expected THEN: %w", err)
	}

	// Parse action
	action, err := p.parseMergeAction(clause.Matched)
	if err != nil {
		return clause, err
	}
	clause.Action = action

	return clause, nil
}

// parseMergeAction parses: UPDATE SET ... / DELETE / INSERT (...) VALUES (...) / DO NOTHING
func (p *Parser) parseMergeAction(matched bool) (MergeAction, error) {
	action := MergeAction{}

	if p.curTokenIs(TOKEN_UPDATE) {
		if !matched {
			return action, fmt.Errorf("UPDATE action is only valid for WHEN MATCHED clause")
		}
		action.ActionType = "UPDATE"
		p.nextToken() // consume UPDATE

		if err := p.expect(TOKEN_SET); err != nil {
			return action, fmt.Errorf("expected SET after UPDATE: %w", err)
		}

		// Parse assignments
		assignments, err := p.parseAssignments()
		if err != nil {
			return action, err
		}
		action.Assignments = assignments

	} else if p.curTokenIs(TOKEN_DELETE) {
		if !matched {
			return action, fmt.Errorf("DELETE action is only valid for WHEN MATCHED clause")
		}
		action.ActionType = "DELETE"
		p.nextToken() // consume DELETE

	} else if p.curTokenIs(TOKEN_INSERT) {
		if matched {
			return action, fmt.Errorf("INSERT action is only valid for WHEN NOT MATCHED clause")
		}
		action.ActionType = "INSERT"
		p.nextToken() // consume INSERT

		// Optional column list
		if p.curTokenIs(TOKEN_LPAREN) {
			p.nextToken() // consume '('
			for {
				if !p.isIdentifierOrContextualKeyword() {
					return action, fmt.Errorf("expected column name in INSERT, got %v", p.cur.Type)
				}
				colName, err := p.parseIdentifier()
				if err != nil {
					return action, err
				}
				action.Columns = append(action.Columns, colName)

				if p.curTokenIs(TOKEN_RPAREN) {
					p.nextToken() // consume ')'
					break
				}
				if err := p.expect(TOKEN_COMMA); err != nil {
					return action, fmt.Errorf("expected ',' or ')' in column list: %w", err)
				}
			}
		}

		// VALUES keyword
		if err := p.expect(TOKEN_VALUES); err != nil {
			return action, fmt.Errorf("expected VALUES after INSERT: %w", err)
		}

		// Value list
		if err := p.expect(TOKEN_LPAREN); err != nil {
			return action, fmt.Errorf("expected '(' after VALUES: %w", err)
		}

		for {
			expr, err := p.parseExpression()
			if err != nil {
				return action, fmt.Errorf("expected value expression: %w", err)
			}
			action.Values = append(action.Values, expr)

			if p.curTokenIs(TOKEN_RPAREN) {
				p.nextToken() // consume ')'
				break
			}
			if err := p.expect(TOKEN_COMMA); err != nil {
				return action, fmt.Errorf("expected ',' or ')' in VALUES list: %w", err)
			}
		}

	} else if p.curTokenIs(TOKEN_DO) {
		p.nextToken() // consume DO
		if err := p.expect(TOKEN_NOTHING); err != nil {
			return action, fmt.Errorf("expected NOTHING after DO: %w", err)
		}
		action.ActionType = "DO NOTHING"

	} else {
		return action, fmt.Errorf("expected UPDATE, DELETE, INSERT, or DO NOTHING after THEN, got %v", p.cur.Type)
	}

	return action, nil
}

// parseAssignments parses: col1 = expr1, col2 = expr2, ...
func (p *Parser) parseAssignments() ([]Assignment, error) {
	var assignments []Assignment

	for {
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected column name in SET, got %v", p.cur.Type)
		}
		colName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}

		if err := p.expect(TOKEN_EQ); err != nil {
			return nil, fmt.Errorf("expected '=' after column name: %w", err)
		}

		expr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("expected value expression: %w", err)
		}

		assignments = append(assignments, Assignment{Column: colName, Value: expr})

		if !p.curTokenIs(TOKEN_COMMA) {
			break
		}
		p.nextToken() // consume ','
	}

	return assignments, nil
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

	// CREATE FULLTEXT INDEX
	if p.curTokenIs(TOKEN_FULLTEXT) {
		if isUnique {
			return nil, fmt.Errorf("UNIQUE not valid for FULLTEXT INDEX")
		}
		p.nextToken() // consume FULLTEXT
		if !p.curTokenIs(TOKEN_INDEX) {
			return nil, fmt.Errorf("expected INDEX after FULLTEXT")
		}
		return p.parseCreateFTSIndex()
	}

	if p.curTokenIs(TOKEN_INDEX) {
		return p.parseCreateIndex(isUnique)
	}

	if p.curTokenIs(TOKEN_VIEW) {
		return p.parseCreateView(orReplace)
	}

	// CREATE USER
	if p.curTokenIs(TOKEN_USER) {
		if isUnique || orReplace {
			return nil, fmt.Errorf("UNIQUE/OR REPLACE not valid for CREATE USER")
		}
		return p.parseCreateUser()
	}

	// CREATE DATABASE
	if p.curTokenIs(TOKEN_DATABASE) {
		if isUnique || orReplace {
			return nil, fmt.Errorf("UNIQUE/OR REPLACE not valid for CREATE DATABASE")
		}
		return p.parseCreateDatabase()
	}

	// CREATE TRIGGER
	if p.curTokenIs(TOKEN_TRIGGER) {
		if isUnique || orReplace {
			return nil, fmt.Errorf("UNIQUE/OR REPLACE not valid for CREATE TRIGGER")
		}
		return p.parseCreateTrigger()
	}

	// CREATE PROCEDURE
	if p.curTokenIs(TOKEN_PROCEDURE) {
		if isUnique {
			return nil, fmt.Errorf("UNIQUE not valid for CREATE PROCEDURE")
		}
		return p.parseCreateProcedure(orReplace)
	}

	// CREATE FUNCTION
	if p.curTokenIs(TOKEN_FUNCTION) {
		if isUnique {
			return nil, fmt.Errorf("UNIQUE not valid for CREATE FUNCTION")
		}
		return p.parseCreateFunction(orReplace)
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

	// Table name (can be a keyword like "target" used as identifier)
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// (columns)
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	for {
		if p.curTokenIs(TOKEN_FOREIGN) {
			fk, err := p.parseForeignKeyDef()
			if err != nil {
				return nil, err
			}
			stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
		} else if p.curTokenIs(TOKEN_CONSTRAINT) {
			p.nextToken() // consume CONSTRAINT
			if !p.isIdentifierOrContextualKeyword() {
				return nil, fmt.Errorf("expected constraint name")
			}
			constraintName, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}

			if p.curTokenIs(TOKEN_FOREIGN) {
				fk, err := p.parseForeignKeyDef()
				if err != nil {
					return nil, err
				}
				fk.ConstraintName = constraintName
				stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
			} else {
				return nil, fmt.Errorf("only FOREIGN KEY constraints are supported for now")
			}
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, col)
		}

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
		} else if p.curTokenIs(TOKEN_ROW) || (p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "ROW") {
			stmt.StorageType = "ROW"
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected ROW or COLUMN after USING, got %v", p.cur.Literal)
		}
	}

	// Optional PARTITION BY clause
	if p.curTokenIs(TOKEN_PARTITION) {
		partSpec, err := p.parsePartitionBy()
		if err != nil {
			return nil, err
		}
		stmt.PartitionSpec = partSpec
	}

	return stmt, nil
}

func (p *Parser) parseForeignKeyDef() (ForeignKeyDef, error) {
	fk := ForeignKeyDef{}
	if err := p.expect(TOKEN_FOREIGN); err != nil {
		return fk, err
	}
	if err := p.expect(TOKEN_KEY); err != nil {
		return fk, err
	}

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return fk, err
	}

	for {
		if !p.isIdentifierOrContextualKeyword() {
			return fk, fmt.Errorf("expected column name in FOREIGN KEY")
		}
		col, err := p.parseIdentifier()
		if err != nil {
			return fk, err
		}
		fk.Columns = append(fk.Columns, col)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return fk, err
	}

	if err := p.expect(TOKEN_REFERENCES); err != nil {
		return fk, err
	}

	if !p.isIdentifierOrContextualKeyword() {
		return fk, fmt.Errorf("expected referenced table name")
	}
	refTable, err := p.parseIdentifier()
	if err != nil {
		return fk, err
	}
	fk.RefTable = refTable

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return fk, err
	}

	for {
		if !p.isIdentifierOrContextualKeyword() {
			return fk, fmt.Errorf("expected referenced column name")
		}
		col, err := p.parseIdentifier()
		if err != nil {
			return fk, err
		}
		fk.RefColumns = append(fk.RefColumns, col)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return fk, err
	}

	return fk, nil
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
	case TOKEN_JSON_TYPE:
		col.Type = catalog.TypeJSON
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
		} else if p.curTokenIs(TOKEN_REFERENCES) {
			p.nextToken() // consume REFERENCES
			if !p.isIdentifierOrContextualKeyword() {
				return col, fmt.Errorf("expected referenced table name")
			}
			refTable, err := p.parseIdentifier()
			if err != nil {
				return col, err
			}
			col.ReferencesTable = refTable

			// Optional (column)
			if p.curTokenIs(TOKEN_LPAREN) {
				p.nextToken() // consume (
				if !p.isIdentifierOrContextualKeyword() {
					return col, fmt.Errorf("expected referenced column name")
				}
				refCol, err := p.parseIdentifier()
				if err != nil {
					return col, err
				}
				col.ReferencesColumn = refCol
				if err := p.expect(TOKEN_RPAREN); err != nil {
					return col, err
				}
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

	// Parse the right SELECT (without ORDER BY/LIMIT/OFFSET - those belong to UNION)
	if !p.curTokenIs(TOKEN_SELECT) {
		return nil, fmt.Errorf("expected SELECT after %s", op)
	}
	right, err := p.parseSelectForSetOp()
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

// parseDrop parses: DROP TABLE name or DROP INDEX name or DROP VIEW name or DROP USER name
func (p *Parser) parseDrop() (Statement, error) {
	p.nextToken() // consume DROP

	if p.curTokenIs(TOKEN_INDEX) {
		return p.parseDropIndex()
	}

	if p.curTokenIs(TOKEN_VIEW) {
		return p.parseDropView()
	}

	if p.curTokenIs(TOKEN_USER) {
		return p.parseDropUser()
	}

	if p.curTokenIs(TOKEN_DATABASE) {
		return p.parseDropDatabase()
	}

	if p.curTokenIs(TOKEN_TRIGGER) {
		return p.parseDropTrigger()
	}

	if p.curTokenIs(TOKEN_PROCEDURE) {
		return p.parseDropProcedure()
	}

	if p.curTokenIs(TOKEN_FUNCTION) {
		return p.parseDropFunction()
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

	// Handle @@ text search match operator
	if p.curTokenIs(TOKEN_MATCH) {
		p.nextToken() // consume @@
		right, err := p.parseAddExpr()
		if err != nil {
			return nil, err
		}
		return &TSMatchExpr{Left: left, Right: right}, nil
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
	left, err := p.parseJSONExpr()
	if err != nil {
		return nil, err
	}

	for p.curTokenIs(TOKEN_STAR) || p.curTokenIs(TOKEN_SLASH) {
		op := p.cur.Type
		p.nextToken()
		right, err := p.parseJSONExpr()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left, nil
}

// parseJSONExpr parses JSON operators (postfix operators like ->, ->>, etc.)
func (p *Parser) parseJSONExpr() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	// Handle JSON postfix operators
	for {
		switch p.cur.Type {
		case TOKEN_ARROW: // ->
			p.nextToken() // consume ->
			key, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, fmt.Errorf("expected key after ->: %v", err)
			}
			left = &JSONAccessExpr{Object: left, Key: key, AsText: false}

		case TOKEN_ARROW_TEXT: // ->>
			p.nextToken() // consume ->>
			key, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, fmt.Errorf("expected key after ->>: %v", err)
			}
			left = &JSONAccessExpr{Object: left, Key: key, AsText: true}

		case TOKEN_HASH_ARROW: // #>
			p.nextToken() // consume #>
			path, err := p.parseJSONPath()
			if err != nil {
				return nil, fmt.Errorf("expected path after #>: %v", err)
			}
			left = &JSONPathExpr{Object: left, Path: path, AsText: false}

		case TOKEN_HASH_ARROW_T: // #>>
			p.nextToken() // consume #>>
			path, err := p.parseJSONPath()
			if err != nil {
				return nil, fmt.Errorf("expected path after #>>: %v", err)
			}
			left = &JSONPathExpr{Object: left, Path: path, AsText: true}

		case TOKEN_AT_GT: // @>
			p.nextToken() // consume @>
			right, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, fmt.Errorf("expected JSON after @>: %v", err)
			}
			left = &JSONContainsExpr{Left: left, Right: right, Contains: true}

		case TOKEN_LT_AT: // <@
			p.nextToken() // consume <@
			right, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, fmt.Errorf("expected JSON after <@: %v", err)
			}
			left = &JSONContainsExpr{Left: left, Right: right, Contains: false}

		case TOKEN_QUESTION: // ?
			p.nextToken() // consume ?
			key, err := p.parsePrimaryExpression()
			if err != nil {
				return nil, fmt.Errorf("expected key after ?: %v", err)
			}
			left = &JSONExistsExpr{Object: left, Keys: []Expression{key}, Mode: "any"}

		case TOKEN_QUESTION_OR: // ?|
			p.nextToken() // consume ?|
			keys, err := p.parseJSONKeyArray()
			if err != nil {
				return nil, fmt.Errorf("expected key array after ?|: %v", err)
			}
			left = &JSONExistsExpr{Object: left, Keys: keys, Mode: "or"}

		case TOKEN_QUESTION_AND: // ?&
			p.nextToken() // consume ?&
			keys, err := p.parseJSONKeyArray()
			if err != nil {
				return nil, fmt.Errorf("expected key array after ?&: %v", err)
			}
			left = &JSONExistsExpr{Object: left, Keys: keys, Mode: "and"}

		default:
			return left, nil
		}
	}
}

// parseJSONPath parses a JSON path like '{a,b,c}'
func (p *Parser) parseJSONPath() ([]Expression, error) {
	// Expect a string literal like '{a,b,c}'
	if p.curTokenIs(TOKEN_STRING) {
		pathStr := p.cur.Literal
		p.nextToken()
		// Parse the path string: {a,b,c} -> ["a", "b", "c"]
		pathStr = strings.TrimPrefix(pathStr, "{")
		pathStr = strings.TrimSuffix(pathStr, "}")
		parts := strings.Split(pathStr, ",")
		var path []Expression
		for _, part := range parts {
			path = append(path, &LiteralExpr{Value: catalog.NewText(strings.TrimSpace(part))})
		}
		return path, nil
	}
	return nil, fmt.Errorf("expected path string like '{a,b,c}'")
}

// parseJSONKeyArray parses an ARRAY['a', 'b'] expression for ?| and ?& operators
func (p *Parser) parseJSONKeyArray() ([]Expression, error) {
	// Check for ARRAY keyword or literal array
	if p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "ARRAY" {
		p.nextToken() // consume ARRAY
	}

	if !p.curTokenIs(TOKEN_LBRACKET) {
		// Maybe it's a single string
		if p.curTokenIs(TOKEN_STRING) {
			key := &LiteralExpr{Value: catalog.NewText(p.cur.Literal)}
			p.nextToken()
			return []Expression{key}, nil
		}
		return nil, fmt.Errorf("expected '[' for array")
	}
	p.nextToken() // consume [

	var keys []Expression
	for !p.curTokenIs(TOKEN_RBRACKET) && !p.curTokenIs(TOKEN_EOF) {
		key, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken() // consume comma
		} else {
			break
		}
	}

	if !p.curTokenIs(TOKEN_RBRACKET) {
		return nil, fmt.Errorf("expected ']' after array elements")
	}
	p.nextToken() // consume ]

	return keys, nil
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

	case TOKEN_MAXVALUE:
		p.nextToken()
		// MAXVALUE is a special marker - we represent it as a special column reference
		// that can be detected during partition evaluation
		return &ColumnRef{Name: "MAXVALUE"}, nil

	case TOKEN_IDENT,
		// Contextual keywords that can be used as identifiers
		TOKEN_TARGET, TOKEN_SOURCE, TOKEN_MATCHED, TOKEN_NOTHING, TOKEN_EXCLUDED:
		name := p.cur.Literal
		p.nextToken()

		// Check for qualified name (table.column)
		if p.cur.Literal == "." {
			p.nextToken() // consume .
			if !p.curTokenIs(TOKEN_IDENT) && !p.isIdentifierOrContextualKeyword() {
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

	// GROUPING() function for grouping sets
	case TOKEN_GROUPING:
		return p.parseFunctionCall()

	// Full-Text Search functions
	case TOKEN_TO_TSVECTOR:
		return p.parseTSVectorFunc()
	case TOKEN_TO_TSQUERY, TOKEN_PLAINTO_TSQUERY, TOKEN_WEBSEARCH:
		return p.parseTSQueryFunc()
	case TOKEN_TS_RANK:
		return p.parseTSRankFunc()
	case TOKEN_TS_HEADLINE:
		return p.parseTSHeadlineFunc()
	case TOKEN_MATCH:
		return p.parseMatchAgainst()

	case TOKEN_CAST:
		return p.parseCastExpression()

	case TOKEN_EXTRACT:
		return p.parseExtractExpression()

	case TOKEN_PLACEHOLDER:
		return p.parsePlaceholder()

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

// parseAlter parses: ALTER TABLE table_name action or ALTER USER username ...
// Actions: ADD [COLUMN] col_def, DROP COLUMN col_name, RENAME TO new_name, RENAME COLUMN old TO new
func (p *Parser) parseAlter() (Statement, error) {
	p.nextToken() // consume ALTER

	// Check for ALTER USER
	if p.curTokenIs(TOKEN_USER) {
		return p.parseAlterUser()
	}

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
func (p *Parser) parseShow() (Statement, error) {
	p.nextToken() // consume SHOW

	if p.curTokenIs(TOKEN_TABLES) {
		p.nextToken() // consume TABLES
		return &ShowStmt{ShowType: "TABLES"}, nil
	}

	if p.curTokenIs(TOKEN_DATABASE) {
		p.nextToken() // consume DATABASE (or DATABASES)
		return &ShowStmt{ShowType: "DATABASES"}, nil
	}

	// Also accept plural DATABASES as identifier
	if p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "DATABASES" {
		p.nextToken()
		return &ShowStmt{ShowType: "DATABASES"}, nil
	}

	// SHOW TRIGGERS [ON table]
	if p.curTokenIs(TOKEN_TRIGGER) || (p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "TRIGGERS") {
		p.nextToken() // consume TRIGGER(S)
		tableName := ""
		if p.curTokenIs(TOKEN_ON) {
			p.nextToken() // consume ON
			if !p.isIdentifierOrContextualKeyword() {
				return nil, fmt.Errorf("expected table name after ON")
			}
			var err error
			tableName, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		return &ShowStmt{ShowType: "TRIGGERS", TableName: tableName}, nil
	}

	// SHOW PROCEDURES
	if p.curTokenIs(TOKEN_PROCEDURES) || p.curTokenIs(TOKEN_PROCEDURE) ||
		(p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "PROCEDURES") {
		p.nextToken()
		return &ShowProceduresStmt{}, nil
	}

	// SHOW FUNCTIONS
	if p.curTokenIs(TOKEN_FUNCTION) ||
		(p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "FUNCTIONS") {
		p.nextToken()
		return &ShowFunctionsStmt{}, nil
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

// parseOverClause parses OVER (PARTITION BY ... ORDER BY ...) for window functions.
func (p *Parser) parseOverClause() (*WindowSpec, error) {
	if !p.curTokenIs(TOKEN_OVER) {
		return nil, fmt.Errorf("expected OVER, got %v", p.cur.Type)
	}
	p.nextToken() // consume OVER

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected ( after OVER")
	}

	spec := &WindowSpec{}

	// Parse PARTITION BY clause (optional)
	if p.curTokenIs(TOKEN_PARTITION) {
		p.nextToken() // consume PARTITION
		if !p.curTokenIs(TOKEN_BY) {
			return nil, fmt.Errorf("expected BY after PARTITION")
		}
		p.nextToken() // consume BY

		// Parse partition columns
		for {
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name in PARTITION BY, got %v", p.cur.Type)
			}
			spec.PartitionBy = append(spec.PartitionBy, p.cur.Literal)
			p.nextToken()

			if !p.curTokenIs(TOKEN_COMMA) {
				break
			}
			p.nextToken() // consume comma
		}
	}

	// Parse ORDER BY clause (optional)
	if p.curTokenIs(TOKEN_ORDER) {
		p.nextToken() // consume ORDER
		if !p.curTokenIs(TOKEN_BY) {
			return nil, fmt.Errorf("expected BY after ORDER")
		}
		p.nextToken() // consume BY

		// Parse order by columns
		for {
			if !p.curTokenIs(TOKEN_IDENT) {
				return nil, fmt.Errorf("expected column name in ORDER BY, got %v", p.cur.Type)
			}
			orderCol := OrderByClause{Column: p.cur.Literal}
			p.nextToken()

			// Check for ASC/DESC
			if p.curTokenIs(TOKEN_ASC) {
				orderCol.Desc = false
				p.nextToken()
			} else if p.curTokenIs(TOKEN_DESC) {
				orderCol.Desc = true
				p.nextToken()
			}

			// Check for NULLS FIRST/LAST
			if p.curTokenIs(TOKEN_NULL) && p.peek.Literal == "S" {
				// This would be NULLS FIRST/LAST - not yet implemented, skip
				_ = true // placeholder to avoid empty branch
			}

			spec.OrderBy = append(spec.OrderBy, orderCol)

			if !p.curTokenIs(TOKEN_COMMA) {
				break
			}
			p.nextToken() // consume comma
		}
	}

	// Parse frame specification (optional): ROWS/RANGE BETWEEN ... AND ...
	if p.curTokenIs(TOKEN_ROWS) || p.curTokenIs(TOKEN_RANGE) {
		if p.curTokenIs(TOKEN_ROWS) {
			spec.FrameType = "ROWS"
		} else {
			spec.FrameType = "RANGE"
		}
		p.nextToken() // consume ROWS/RANGE

		// Check for BETWEEN keyword (optional, for full syntax: ROWS BETWEEN ... AND ...)
		if p.curTokenIs(TOKEN_BETWEEN) {
			p.nextToken() // consume BETWEEN
		}

		// Parse frame start bound
		frameBound, err := p.parseFrameBound()
		if err != nil {
			return nil, err
		}
		spec.FrameStart = frameBound

		// Check for AND ... (end bound)
		if p.curTokenIs(TOKEN_AND) {
			p.nextToken() // consume AND
			endBound, err := p.parseFrameBound()
			if err != nil {
				return nil, err
			}
			spec.FrameEnd = endBound
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ) to close OVER clause")
	}

	return spec, nil
}

// parseFrameBound parses a frame bound like UNBOUNDED PRECEDING, CURRENT ROW, 5 PRECEDING, etc.
func (p *Parser) parseFrameBound() (string, error) {
	if p.curTokenIs(TOKEN_UNBOUNDED) {
		p.nextToken() // consume UNBOUNDED
		if p.curTokenIs(TOKEN_PRECEDING) {
			p.nextToken()
			return "UNBOUNDED PRECEDING", nil
		} else if p.curTokenIs(TOKEN_FOLLOWING) {
			p.nextToken()
			return "UNBOUNDED FOLLOWING", nil
		}
		return "", fmt.Errorf("expected PRECEDING or FOLLOWING after UNBOUNDED")
	}

	if p.curTokenIs(TOKEN_CURRENT) {
		p.nextToken() // consume CURRENT
		if p.curTokenIs(TOKEN_ROW) {
			p.nextToken()
			return "CURRENT ROW", nil
		}
		return "", fmt.Errorf("expected ROW after CURRENT")
	}

	if p.curTokenIs(TOKEN_INT) {
		n := p.cur.Literal
		p.nextToken()
		if p.curTokenIs(TOKEN_PRECEDING) {
			p.nextToken()
			return n + " PRECEDING", nil
		} else if p.curTokenIs(TOKEN_FOLLOWING) {
			p.nextToken()
			return n + " FOLLOWING", nil
		}
		return "", fmt.Errorf("expected PRECEDING or FOLLOWING after number")
	}

	return "", fmt.Errorf("invalid frame bound: %v", p.cur.Type)
}

func (p *Parser) parsePrepare() (*PrepareStmt, error) {
	p.nextToken() // consume PREPARE

	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected identifier for prepared statement name")
	}
	name := p.cur.Literal
	p.nextToken()

	// Optional AS
	if p.curTokenIs(TOKEN_AS) {
		p.nextToken()
	}

	// Parse the statement
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}

	return &PrepareStmt{Name: name, Statement: stmt}, nil
}

func (p *Parser) parseExecute() (*ExecuteStmt, error) {
	p.nextToken() // consume EXECUTE

	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected identifier for prepared statement name")
	}
	name := p.cur.Literal
	p.nextToken()

	var params []Expression
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (

		if !p.curTokenIs(TOKEN_RPAREN) {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			params = append(params, expr)

			for p.curTokenIs(TOKEN_COMMA) {
				p.nextToken()
				expr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				params = append(params, expr)
			}
		}

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
	}

	return &ExecuteStmt{Name: name, Params: params}, nil
}

func (p *Parser) parseDeallocate() (*DeallocateStmt, error) {
	p.nextToken() // consume DEALLOCATE

	// Optional PREPARE keyword
	if p.curTokenIs(TOKEN_PREPARE) {
		p.nextToken()
	}

	if !p.curTokenIs(TOKEN_IDENT) {
		return nil, fmt.Errorf("expected identifier for prepared statement name")
	}
	name := p.cur.Literal
	p.nextToken()

	return &DeallocateStmt{Name: name}, nil
}

func (p *Parser) parsePlaceholder() (Expression, error) {
	idxStr := p.cur.Literal
	// Strip the leading $
	if len(idxStr) > 0 && idxStr[0] == '$' {
		idxStr = idxStr[1:]
	}
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return nil, fmt.Errorf("invalid placeholder index: %s", p.cur.Literal)
	}
	if idx < 1 {
		return nil, fmt.Errorf("placeholder index must be >= 1")
	}
	p.nextToken()
	return &PlaceholderExpr{Index: idx}, nil
}

// parseCreateUser parses: CREATE USER username WITH PASSWORD 'password' [SUPERUSER]
func (p *Parser) parseCreateUser() (*CreateUserStmt, error) {
	p.nextToken() // consume USER

	stmt := &CreateUserStmt{}

	// Username
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected username, got %v", p.cur.Type)
	}
	username, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Username = username

	// Expect WITH
	if !p.curTokenIs(TOKEN_WITH) {
		return nil, fmt.Errorf("expected WITH after username")
	}
	p.nextToken() // consume WITH

	// Expect PASSWORD
	if !p.curTokenIs(TOKEN_PASSWORD) {
		return nil, fmt.Errorf("expected PASSWORD after WITH")
	}
	p.nextToken() // consume PASSWORD

	// Password string
	if !p.curTokenIs(TOKEN_STRING) {
		return nil, fmt.Errorf("expected password string")
	}
	stmt.Password = p.cur.Literal
	p.nextToken()

	// Optional SUPERUSER
	if p.curTokenIs(TOKEN_SUPERUSER) {
		stmt.Superuser = true
		p.nextToken()
	}

	return stmt, nil
}

// parseDropUser parses: DROP USER [IF EXISTS] username
func (p *Parser) parseDropUser() (*DropUserStmt, error) {
	p.nextToken() // consume USER

	stmt := &DropUserStmt{}

	// Optional IF EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	// Username
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected username, got %v", p.cur.Type)
	}
	username, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Username = username

	return stmt, nil
}

// parseAlterUser parses: ALTER USER username WITH PASSWORD 'newpassword' | SUPERUSER | NOSUPERUSER
func (p *Parser) parseAlterUser() (*AlterUserStmt, error) {
	p.nextToken() // consume USER

	stmt := &AlterUserStmt{}

	// Username
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected username, got %v", p.cur.Type)
	}
	username, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Username = username

	// Expect WITH
	if !p.curTokenIs(TOKEN_WITH) {
		return nil, fmt.Errorf("expected WITH after username")
	}
	p.nextToken() // consume WITH

	// PASSWORD or SUPERUSER/NOSUPERUSER
	if p.curTokenIs(TOKEN_PASSWORD) {
		p.nextToken() // consume PASSWORD
		if !p.curTokenIs(TOKEN_STRING) {
			return nil, fmt.Errorf("expected password string")
		}
		stmt.NewPassword = p.cur.Literal
		p.nextToken()
	} else if p.curTokenIs(TOKEN_SUPERUSER) {
		stmt.SetSuperuser = true
		p.nextToken()
	} else if p.cur.Literal == "NOSUPERUSER" {
		stmt.UnsetSuperuser = true
		p.nextToken()
	} else {
		return nil, fmt.Errorf("expected PASSWORD, SUPERUSER, or NOSUPERUSER after WITH")
	}

	return stmt, nil
}

// parseGrant parses: GRANT privilege ON table TO user
func (p *Parser) parseGrant() (*GrantStmt, error) {
	p.nextToken() // consume GRANT

	stmt := &GrantStmt{}

	// Privilege (SELECT, INSERT, UPDATE, DELETE, ALL)
	if !p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_SELECT) && !p.curTokenIs(TOKEN_INSERT) &&
		!p.curTokenIs(TOKEN_UPDATE) && !p.curTokenIs(TOKEN_DELETE) && !p.curTokenIs(TOKEN_ALL) {
		return nil, fmt.Errorf("expected privilege (SELECT, INSERT, UPDATE, DELETE, ALL)")
	}
	stmt.Privilege = strings.ToUpper(p.cur.Literal)
	p.nextToken()

	// Expect ON
	if err := p.expect(TOKEN_ON); err != nil {
		return nil, fmt.Errorf("expected ON after privilege")
	}

	// Table name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Expect TO
	if err := p.expect(TOKEN_TO); err != nil {
		return nil, fmt.Errorf("expected TO after table name")
	}

	// Username
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected username, got %v", p.cur.Type)
	}
	username, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Username = username

	return stmt, nil
}

// parseRevoke parses: REVOKE privilege ON table FROM user
func (p *Parser) parseRevoke() (*RevokeStmt, error) {
	p.nextToken() // consume REVOKE

	stmt := &RevokeStmt{}

	// Privilege
	if !p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_SELECT) && !p.curTokenIs(TOKEN_INSERT) &&
		!p.curTokenIs(TOKEN_UPDATE) && !p.curTokenIs(TOKEN_DELETE) && !p.curTokenIs(TOKEN_ALL) {
		return nil, fmt.Errorf("expected privilege (SELECT, INSERT, UPDATE, DELETE, ALL)")
	}
	stmt.Privilege = strings.ToUpper(p.cur.Literal)
	p.nextToken()

	// Expect ON
	if err := p.expect(TOKEN_ON); err != nil {
		return nil, fmt.Errorf("expected ON after privilege")
	}

	// Table name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name, got %v", p.cur.Type)
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Expect FROM
	if !p.curTokenIs(TOKEN_FROM) {
		return nil, fmt.Errorf("expected FROM after table name")
	}
	p.nextToken() // consume FROM

	// Username
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected username, got %v", p.cur.Type)
	}
	username, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Username = username

	return stmt, nil
}

// parseCreateDatabase parses: CREATE DATABASE [IF NOT EXISTS] name [WITH OWNER = 'owner']
func (p *Parser) parseCreateDatabase() (*CreateDatabaseStmt, error) {
	p.nextToken() // consume DATABASE

	stmt := &CreateDatabaseStmt{}

	// Optional IF NOT EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if !p.curTokenIs(TOKEN_NOT) {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF NOT")
		}
		stmt.IfNotExists = true
	}

	// Database name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected database name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	// Optional WITH OWNER = 'owner'
	if p.curTokenIs(TOKEN_WITH) {
		p.nextToken() // consume WITH
		if !p.curTokenIs(TOKEN_OWNER) {
			return nil, fmt.Errorf("expected OWNER after WITH")
		}
		p.nextToken() // consume OWNER
		if !p.curTokenIs(TOKEN_EQ) {
			return nil, fmt.Errorf("expected = after OWNER")
		}
		p.nextToken() // consume =
		if !p.curTokenIs(TOKEN_STRING) && !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected owner name, got %v", p.cur.Type)
		}
		if p.curTokenIs(TOKEN_STRING) {
			stmt.Owner = p.cur.Literal
		} else {
			stmt.Owner, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		p.nextToken()
	}

	return stmt, nil
}

// parseDropDatabase parses: DROP DATABASE [IF EXISTS] name
func (p *Parser) parseDropDatabase() (*DropDatabaseStmt, error) {
	p.nextToken() // consume DATABASE

	stmt := &DropDatabaseStmt{}

	// Optional IF EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	// Database name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected database name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	return stmt, nil
}

// parseUseDatabase parses: USE database_name
func (p *Parser) parseUseDatabase() (*UseDatabaseStmt, error) {
	p.nextToken() // consume USE

	stmt := &UseDatabaseStmt{}

	// Database name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected database name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	return stmt, nil
}

// parseCreateTrigger parses: CREATE TRIGGER name {BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE|DELETE}
// ON table [FOR EACH {ROW|STATEMENT}] EXECUTE FUNCTION func_name(args)
func (p *Parser) parseCreateTrigger() (*CreateTriggerStmt, error) {
	p.nextToken() // consume TRIGGER

	stmt := &CreateTriggerStmt{}

	// Optional IF NOT EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if !p.curTokenIs(TOKEN_NOT) {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF NOT")
		}
		stmt.IfNotExists = true
	}

	// Trigger name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected trigger name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	// Timing: BEFORE, AFTER, or INSTEAD OF
	switch {
	case p.curTokenIs(TOKEN_BEFORE):
		stmt.Timing = TriggerBefore
		p.nextToken()
	case p.curTokenIs(TOKEN_AFTER):
		stmt.Timing = TriggerAfter
		p.nextToken()
	case p.curTokenIs(TOKEN_INSTEAD):
		p.nextToken() // consume INSTEAD
		if !p.curTokenIs(TOKEN_OF) {
			return nil, fmt.Errorf("expected OF after INSTEAD")
		}
		p.nextToken() // consume OF
		stmt.Timing = TriggerInsteadOf
	default:
		return nil, fmt.Errorf("expected BEFORE, AFTER, or INSTEAD OF, got %v", p.cur.Type)
	}

	// Event: INSERT, UPDATE, or DELETE
	switch {
	case p.curTokenIs(TOKEN_INSERT):
		stmt.Event = TriggerInsert
		p.nextToken()
	case p.curTokenIs(TOKEN_UPDATE):
		stmt.Event = TriggerUpdate
		p.nextToken()
	case p.curTokenIs(TOKEN_DELETE):
		stmt.Event = TriggerDelete
		p.nextToken()
	default:
		return nil, fmt.Errorf("expected INSERT, UPDATE, or DELETE, got %v", p.cur.Type)
	}

	// ON table_name
	if !p.curTokenIs(TOKEN_ON) {
		return nil, fmt.Errorf("expected ON after event, got %v", p.cur.Type)
	}
	p.nextToken() // consume ON

	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name after ON")
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Optional: FOR EACH {ROW|STATEMENT}
	// Default to FOR EACH STATEMENT if not specified
	stmt.ForEachRow = false
	if p.curTokenIs(TOKEN_FOR) {
		p.nextToken() // consume FOR
		if !p.curTokenIs(TOKEN_EACH) {
			return nil, fmt.Errorf("expected EACH after FOR")
		}
		p.nextToken() // consume EACH
		if p.curTokenIs(TOKEN_ROW) {
			stmt.ForEachRow = true
			p.nextToken()
		} else if p.curTokenIs(TOKEN_STATEMENT) {
			stmt.ForEachRow = false
			p.nextToken()
		} else {
			return nil, fmt.Errorf("expected ROW or STATEMENT after FOR EACH")
		}
	}

	// EXECUTE FUNCTION func_name(args) or EXECUTE PROCEDURE func_name(args)
	if !p.curTokenIs(TOKEN_EXECUTE) {
		return nil, fmt.Errorf("expected EXECUTE, got %v", p.cur.Type)
	}
	p.nextToken() // consume EXECUTE

	// Accept either FUNCTION or PROCEDURE (they're synonyms in PostgreSQL)
	// These are now keywords, not identifiers
	if p.curTokenIs(TOKEN_FUNCTION) || p.curTokenIs(TOKEN_PROCEDURE) {
		p.nextToken()
	} else {
		return nil, fmt.Errorf("expected FUNCTION or PROCEDURE after EXECUTE")
	}

	// Function name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected function name")
	}
	funcName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.FunctionName = funcName

	// Optional parentheses for function args ()
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (
		// For now, skip any arguments until we reach )
		for !p.curTokenIs(TOKEN_RPAREN) && !p.curTokenIs(TOKEN_EOF) {
			p.nextToken()
		}
		if p.curTokenIs(TOKEN_RPAREN) {
			p.nextToken() // consume )
		}
	}

	return stmt, nil
}

// parseDropTrigger parses: DROP TRIGGER [IF EXISTS] name ON table_name
func (p *Parser) parseDropTrigger() (*DropTriggerStmt, error) {
	p.nextToken() // consume TRIGGER

	stmt := &DropTriggerStmt{}

	// Optional IF EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	// Trigger name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected trigger name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	// ON table_name
	if !p.curTokenIs(TOKEN_ON) {
		return nil, fmt.Errorf("expected ON after trigger name")
	}
	p.nextToken() // consume ON

	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name after ON")
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	return stmt, nil
}

// ==================== Full-Text Search Parser Functions ====================

// parseTSVectorFunc parses to_tsvector([config,] text).
func (p *Parser) parseTSVectorFunc() (Expression, error) {
	p.nextToken() // consume TO_TSVECTOR

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after to_tsvector")
	}

	// Parse first argument
	arg1, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	result := &TSVectorExpr{}

	if p.curTokenIs(TOKEN_COMMA) {
		// Two arguments: config and text
		p.nextToken() // consume comma
		// First arg is config (must be a string literal)
		if lit, ok := arg1.(*LiteralExpr); ok && lit.Value.Type == catalog.TypeText {
			result.Config = lit.Value.Text
		} else {
			return nil, fmt.Errorf("to_tsvector config must be a string literal")
		}
		// Parse text argument
		result.Text, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	} else {
		// One argument: just text
		result.Text = arg1
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after to_tsvector arguments")
	}

	return result, nil
}

// parseTSQueryFunc parses to_tsquery, plainto_tsquery, or websearch_to_tsquery.
func (p *Parser) parseTSQueryFunc() (Expression, error) {
	funcType := p.cur.Type
	p.nextToken() // consume function name

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after ts query function")
	}

	// Parse first argument
	arg1, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	result := &TSQueryExpr{
		PlainText: funcType == TOKEN_PLAINTO_TSQUERY,
		WebSearch: funcType == TOKEN_WEBSEARCH,
	}

	if p.curTokenIs(TOKEN_COMMA) {
		// Two arguments: config and query
		p.nextToken() // consume comma
		if lit, ok := arg1.(*LiteralExpr); ok && lit.Value.Type == catalog.TypeText {
			result.Config = lit.Value.Text
		} else {
			return nil, fmt.Errorf("ts query config must be a string literal")
		}
		result.Query, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	} else {
		// One argument: just query
		result.Query = arg1
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after ts query function arguments")
	}

	return result, nil
}

// parseTSRankFunc parses ts_rank(vector, query [, normalization]).
func (p *Parser) parseTSRankFunc() (Expression, error) {
	p.nextToken() // consume TS_RANK

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after ts_rank")
	}

	result := &TSRankExpr{}
	var err error

	// Parse vector argument
	result.Vector, err = p.parseExpression()
	if err != nil {
		return nil, err
	}

	if !p.curTokenIs(TOKEN_COMMA) {
		return nil, fmt.Errorf("expected ',' after ts_rank vector argument")
	}
	p.nextToken() // consume comma

	// Parse query argument
	result.Query, err = p.parseExpression()
	if err != nil {
		return nil, err
	}

	// Optional normalization argument
	if p.curTokenIs(TOKEN_COMMA) {
		p.nextToken() // consume comma
		result.Normalization, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after ts_rank arguments")
	}

	return result, nil
}

// parseTSHeadlineFunc parses ts_headline([config,] text, query [, options]).
func (p *Parser) parseTSHeadlineFunc() (Expression, error) {
	p.nextToken() // consume TS_HEADLINE

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after ts_headline")
	}

	result := &TSHeadlineExpr{}
	args := make([]Expression, 0, 4)

	// Parse all arguments
	for {
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
		return nil, fmt.Errorf("expected ')' after ts_headline arguments")
	}

	// Determine which arguments are which based on count
	switch len(args) {
	case 2:
		// ts_headline(text, query)
		result.Text = args[0]
		result.Query = args[1]
	case 3:
		// ts_headline(config, text, query) or ts_headline(text, query, options)
		if lit, ok := args[0].(*LiteralExpr); ok && lit.Value.Type == catalog.TypeText {
			// First arg looks like a config
			result.Config = lit.Value.Text
			result.Text = args[1]
			result.Query = args[2]
		} else {
			// First arg is text
			result.Text = args[0]
			result.Query = args[1]
			result.Options = args[2]
		}
	case 4:
		// ts_headline(config, text, query, options)
		if lit, ok := args[0].(*LiteralExpr); ok && lit.Value.Type == catalog.TypeText {
			result.Config = lit.Value.Text
		}
		result.Text = args[1]
		result.Query = args[2]
		result.Options = args[3]
	default:
		return nil, fmt.Errorf("ts_headline requires 2-4 arguments, got %d", len(args))
	}

	return result, nil
}

// parseMatchAgainst parses MySQL-style MATCH(cols) AGAINST(query).
func (p *Parser) parseMatchAgainst() (Expression, error) {
	p.nextToken() // consume MATCH

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after MATCH")
	}

	result := &MatchAgainstExpr{}
	var err error

	// Parse column list
	for {
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected column name in MATCH")
		}
		col, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		result.Columns = append(result.Columns, col)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken() // consume comma
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after MATCH columns")
	}

	// Parse AGAINST
	if !p.curTokenIs(TOKEN_AGAINST) {
		return nil, fmt.Errorf("expected AGAINST after MATCH()")
	}
	p.nextToken() // consume AGAINST

	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after AGAINST")
	}

	// Parse query
	result.Query, err = p.parseExpression()
	if err != nil {
		return nil, err
	}

	// Check for optional modifiers: IN BOOLEAN MODE, WITH QUERY EXPANSION
	if p.curTokenIs(TOKEN_IN) {
		p.nextToken() // consume IN
		// Expect BOOLEAN
		if p.cur.Literal == "BOOLEAN" || strings.ToUpper(p.cur.Literal) == "BOOLEAN" {
			p.nextToken() // consume BOOLEAN
			// Expect MODE
			if p.cur.Literal == "MODE" || strings.ToUpper(p.cur.Literal) == "MODE" {
				p.nextToken() // consume MODE
				result.InBoolMode = true
			}
		} else if strings.ToUpper(p.cur.Literal) == "NATURAL" {
			p.nextToken() // consume NATURAL
			// NATURAL LANGUAGE MODE - default, just skip
			if strings.ToUpper(p.cur.Literal) == "LANGUAGE" {
				p.nextToken()
				if strings.ToUpper(p.cur.Literal) == "MODE" {
					p.nextToken()
				}
			}
		}
	}

	if p.curTokenIs(TOKEN_WITH) {
		p.nextToken() // consume WITH
		// Expect QUERY EXPANSION
		if strings.ToUpper(p.cur.Literal) == "QUERY" {
			p.nextToken()
			if strings.ToUpper(p.cur.Literal) == "EXPANSION" {
				p.nextToken()
				result.WithExpansion = true
			}
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after AGAINST query")
	}

	return result, nil
}

// parseCreateFTSIndex parses: CREATE FULLTEXT INDEX [IF NOT EXISTS] name ON table(columns)
func (p *Parser) parseCreateFTSIndex() (*CreateFTSIndexStmt, error) {
	p.nextToken() // consume INDEX

	stmt := &CreateFTSIndexStmt{}

	// Optional IF NOT EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if !p.curTokenIs(TOKEN_NOT) {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF NOT")
		}
		stmt.IfNotExists = true
	}

	// Index name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected index name")
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.IndexName = name

	// ON
	if !p.curTokenIs(TOKEN_ON) {
		return nil, fmt.Errorf("expected ON after index name")
	}
	p.nextToken() // consume ON

	// Table name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected table name after ON")
	}
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.TableName = tableName

	// Column list
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after table name")
	}

	for {
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected column name")
		}
		col, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken() // consume comma
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after column list")
	}

	return stmt, nil
}

// parsePartitionBy parses PARTITION BY clause.
// Syntax:
//
//	PARTITION BY RANGE (column) (partition_defs)
//	PARTITION BY LIST (column) (partition_defs)
//	PARTITION BY HASH (column) PARTITIONS n
func (p *Parser) parsePartitionBy() (*PartitionSpec, error) {
	if err := p.expect(TOKEN_PARTITION); err != nil {
		return nil, err
	}
	if err := p.expect(TOKEN_BY); err != nil {
		return nil, fmt.Errorf("expected BY after PARTITION")
	}

	spec := &PartitionSpec{}

	// Parse partition type: RANGE, LIST, or HASH
	switch {
	case p.curTokenIs(TOKEN_RANGE):
		spec.Type = PartitionRange
		p.nextToken()
	case p.curTokenIs(TOKEN_LIST):
		spec.Type = PartitionList
		p.nextToken()
	case p.curTokenIs(TOKEN_HASH):
		spec.Type = PartitionHash
		p.nextToken()
	default:
		return nil, fmt.Errorf("expected RANGE, LIST, or HASH after PARTITION BY, got %v", p.cur.Literal)
	}

	// Parse partition columns
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after partition type")
	}

	for {
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected column name in partition key")
		}
		col, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		spec.Columns = append(spec.Columns, col)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after partition columns")
	}

	// For HASH partitioning, parse PARTITIONS n
	if spec.Type == PartitionHash {
		if p.curTokenIs(TOKEN_PARTITIONS) {
			p.nextToken()
			if !p.curTokenIs(TOKEN_INT) {
				return nil, fmt.Errorf("expected number after PARTITIONS")
			}
			n, err := strconv.Atoi(p.cur.Literal)
			if err != nil {
				return nil, fmt.Errorf("invalid partition count: %v", p.cur.Literal)
			}
			spec.NumBuckets = n
			p.nextToken()

			// Auto-generate partition names for HASH
			for i := 0; i < n; i++ {
				spec.Partitions = append(spec.Partitions, PartitionDef{
					Name: fmt.Sprintf("p%d", i),
				})
			}
		} else {
			return nil, fmt.Errorf("HASH partitioning requires PARTITIONS clause")
		}
		return spec, nil
	}

	// For RANGE and LIST, parse partition definitions
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' before partition definitions")
	}

	for {
		partDef, err := p.parsePartitionDef(spec.Type)
		if err != nil {
			return nil, err
		}
		spec.Partitions = append(spec.Partitions, partDef)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after partition definitions")
	}

	return spec, nil
}

// parsePartitionDef parses a single partition definition.
// RANGE: PARTITION name VALUES LESS THAN (value) or PARTITION name VALUES LESS THAN MAXVALUE
// LIST:  PARTITION name VALUES IN (value1, value2, ...)
func (p *Parser) parsePartitionDef(partType PartitionType) (PartitionDef, error) {
	def := PartitionDef{}

	if err := p.expect(TOKEN_PARTITION); err != nil {
		return def, fmt.Errorf("expected PARTITION keyword")
	}

	// Partition name
	if !p.isIdentifierOrContextualKeyword() {
		return def, fmt.Errorf("expected partition name")
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return def, err
	}
	def.Name = name

	if err := p.expect(TOKEN_VALUES); err != nil {
		return def, fmt.Errorf("expected VALUES after partition name")
	}

	switch partType {
	case PartitionRange:
		// VALUES LESS THAN (value) or VALUES LESS THAN MAXVALUE
		if err := p.expect(TOKEN_LESS); err != nil {
			return def, fmt.Errorf("expected LESS after VALUES for RANGE partition")
		}
		if err := p.expect(TOKEN_THAN); err != nil {
			return def, fmt.Errorf("expected THAN after LESS")
		}

		if p.curTokenIs(TOKEN_MAXVALUE) {
			def.Bound.IsMaxValue = true
			p.nextToken()
		} else {
			if err := p.expect(TOKEN_LPAREN); err != nil {
				return def, fmt.Errorf("expected '(' or MAXVALUE after LESS THAN")
			}
			expr, err := p.parseExpression()
			if err != nil {
				return def, fmt.Errorf("error parsing partition bound: %v", err)
			}
			def.Bound.LessThan = expr
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return def, fmt.Errorf("expected ')' after partition bound value")
			}
		}

	case PartitionList:
		// VALUES IN (value1, value2, ...)
		if !p.curTokenIs(TOKEN_IN) {
			return def, fmt.Errorf("expected IN after VALUES for LIST partition")
		}
		p.nextToken() // consume IN

		if err := p.expect(TOKEN_LPAREN); err != nil {
			return def, fmt.Errorf("expected '(' after IN")
		}

		for {
			expr, err := p.parseExpression()
			if err != nil {
				return def, fmt.Errorf("error parsing list value: %v", err)
			}
			def.Bound.Values = append(def.Bound.Values, expr)

			if p.curTokenIs(TOKEN_COMMA) {
				p.nextToken()
			} else {
				break
			}
		}

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return def, fmt.Errorf("expected ')' after list values")
		}

	default:
		return def, fmt.Errorf("unexpected partition type in parsePartitionDef")
	}

	return def, nil
}

// ================== Stored Procedures / PL/pgSQL Parsing ==================

// parsePLDataType parses a data type for PL/pgSQL (INT, BIGINT, TEXT, BOOL, TIMESTAMP, etc.)
func (p *Parser) parsePLDataType() (catalog.DataType, error) {
	switch p.cur.Type {
	case TOKEN_INT_TYPE:
		p.nextToken()
		return catalog.TypeInt32, nil
	case TOKEN_BIGINT:
		p.nextToken()
		return catalog.TypeInt64, nil
	case TOKEN_TEXT:
		p.nextToken()
		return catalog.TypeText, nil
	case TOKEN_BOOL:
		p.nextToken()
		return catalog.TypeBool, nil
	case TOKEN_TIMESTAMP:
		p.nextToken()
		return catalog.TypeTimestamp, nil
	case TOKEN_JSON_TYPE:
		p.nextToken()
		return catalog.TypeJSON, nil
	case TOKEN_TRIGGER:
		p.nextToken()
		return catalog.TypeTrigger, nil
	default:
		// Also check for identifier-style types
		if p.curTokenIs(TOKEN_IDENT) {
			typeName := strings.ToUpper(p.cur.Literal)
			p.nextToken()
			switch typeName {
			case "INTEGER", "INT4":
				return catalog.TypeInt32, nil
			case "BIGINT", "INT8":
				return catalog.TypeInt64, nil
			case "TEXT", "VARCHAR", "STRING":
				return catalog.TypeText, nil
			case "BOOLEAN", "BOOL":
				return catalog.TypeBool, nil
			case "TIMESTAMP", "DATETIME":
				return catalog.TypeTimestamp, nil
			case "JSON", "JSONB":
				return catalog.TypeJSON, nil
			case "TRIGGER":
				return catalog.TypeTrigger, nil
			default:
				return catalog.TypeUnknown, fmt.Errorf("unknown type: %s", typeName)
			}
		}
		return catalog.TypeUnknown, fmt.Errorf("expected type, got %v (%q)", p.cur.Type, p.cur.Literal)
	}
}

// parseCreateProcedure parses CREATE [OR REPLACE] PROCEDURE name(params) AS $$ body $$ LANGUAGE plpgsql
func (p *Parser) parseCreateProcedure(_ bool) (*CreateProcedureStmt, error) {
	p.nextToken() // consume PROCEDURE

	stmt := &CreateProcedureStmt{}

	// Check for IF NOT EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if !p.curTokenIs(TOKEN_NOT) {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after NOT")
		}
		stmt.IfNotExists = true
	}

	// Procedure name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected procedure name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	// Parameters
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after procedure name")
	}

	params, err := p.parseProcParams()
	if err != nil {
		return nil, err
	}
	stmt.Params = params

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after parameters")
	}

	// AS keyword
	if !p.curTokenIs(TOKEN_AS) {
		return nil, fmt.Errorf("expected AS after parameters, got %v", p.cur.Type)
	}
	p.nextToken() // consume AS

	// Dollar-quoted body $$...$$
	if !p.curTokenIs(TOKEN_DOLLAR_QUOTE) {
		return nil, fmt.Errorf("expected $$ body $$, got %v", p.cur.Type)
	}
	stmt.BodyText = p.cur.Literal
	p.nextToken() // consume dollar-quoted body

	// LANGUAGE plpgsql
	if !p.curTokenIs(TOKEN_LANGUAGE) {
		return nil, fmt.Errorf("expected LANGUAGE after body, got %v", p.cur.Type)
	}
	p.nextToken() // consume LANGUAGE

	if p.curTokenIs(TOKEN_PLPGSQL) || (p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "PLPGSQL") {
		stmt.Language = "plpgsql"
	} else if p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "SQL" {
		stmt.Language = "sql"
	} else {
		return nil, fmt.Errorf("expected PLPGSQL or SQL after LANGUAGE, got %v", p.cur.Literal)
	}
	p.nextToken()

	// Parse the body into PLBlock
	if stmt.Language == "plpgsql" {
		body, err := p.parsePLBody(stmt.BodyText)
		if err != nil {
			return nil, fmt.Errorf("error parsing procedure body: %v", err)
		}
		stmt.Body = body
	}

	return stmt, nil
}

// parseCreateFunction parses CREATE [OR REPLACE] FUNCTION name(params) RETURNS type AS $$ body $$ LANGUAGE plpgsql
func (p *Parser) parseCreateFunction(_ bool) (*CreateFunctionStmt, error) {
	p.nextToken() // consume FUNCTION

	stmt := &CreateFunctionStmt{}

	// Check for IF NOT EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if !p.curTokenIs(TOKEN_NOT) {
			return nil, fmt.Errorf("expected NOT after IF")
		}
		p.nextToken() // consume NOT
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after NOT")
		}
		stmt.IfNotExists = true
	}

	// Function name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected function name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	// Parameters
	if err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, fmt.Errorf("expected '(' after function name")
	}

	params, err := p.parseProcParams()
	if err != nil {
		return nil, err
	}
	stmt.Params = params

	if err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, fmt.Errorf("expected ')' after parameters")
	}

	// RETURNS type
	if !p.curTokenIs(TOKEN_RETURNS) {
		return nil, fmt.Errorf("expected RETURNS after parameters, got %v", p.cur.Type)
	}
	p.nextToken() // consume RETURNS

	// Return type
	if p.curTokenIs(TOKEN_VOID) {
		stmt.ReturnType = catalog.TypeInt32 // VOID represented as int with special handling
		p.nextToken()
	} else {
		retType, err := p.parsePLDataType()
		if err != nil {
			return nil, fmt.Errorf("error parsing return type: %v", err)
		}
		stmt.ReturnType = retType
	}

	// AS keyword
	if !p.curTokenIs(TOKEN_AS) {
		return nil, fmt.Errorf("expected AS after return type, got %v", p.cur.Type)
	}
	p.nextToken() // consume AS

	// Dollar-quoted body $$...$$
	if !p.curTokenIs(TOKEN_DOLLAR_QUOTE) {
		return nil, fmt.Errorf("expected $$ body $$, got %v", p.cur.Type)
	}
	stmt.BodyText = p.cur.Literal
	p.nextToken() // consume dollar-quoted body

	// LANGUAGE plpgsql
	if !p.curTokenIs(TOKEN_LANGUAGE) {
		return nil, fmt.Errorf("expected LANGUAGE after body, got %v", p.cur.Type)
	}
	p.nextToken() // consume LANGUAGE

	if p.curTokenIs(TOKEN_PLPGSQL) || (p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "PLPGSQL") {
		stmt.Language = "plpgsql"
	} else if p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "SQL" {
		stmt.Language = "sql"
	} else {
		return nil, fmt.Errorf("expected PLPGSQL or SQL after LANGUAGE, got %v", p.cur.Literal)
	}
	p.nextToken()

	// Parse the body into PLBlock
	if stmt.Language == "plpgsql" {
		body, err := p.parsePLBody(stmt.BodyText)
		if err != nil {
			return nil, fmt.Errorf("error parsing function body: %v", err)
		}
		stmt.Body = body
	}

	return stmt, nil
}

// parseProcParams parses procedure/function parameters: (name TYPE [DEFAULT expr], ...)
func (p *Parser) parseProcParams() ([]ProcParam, error) {
	var params []ProcParam

	if p.curTokenIs(TOKEN_RPAREN) {
		return params, nil // empty parameter list
	}

	for {
		param := ProcParam{Mode: ParamModeIn} // default mode is IN

		// Check for mode: IN, OUT, INOUT
		if p.curTokenIs(TOKEN_IN) {
			p.nextToken()
			if p.curTokenIs(TOKEN_OUT) {
				param.Mode = ParamModeInOut
				p.nextToken()
			}
		} else if p.curTokenIs(TOKEN_OUT) {
			param.Mode = ParamModeOut
			p.nextToken()
		} else if p.curTokenIs(TOKEN_INOUT) {
			param.Mode = ParamModeInOut
			p.nextToken()
		}

		// Parameter name
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected parameter name, got %v", p.cur.Type)
		}
		name, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		param.Name = name

		// Parameter type
		dataType, err := p.parsePLDataType()
		if err != nil {
			return nil, fmt.Errorf("error parsing parameter type: %v", err)
		}
		param.Type = dataType

		// Optional DEFAULT
		if p.curTokenIs(TOKEN_DEFAULT) || p.curTokenIs(TOKEN_COLON_EQ) {
			p.nextToken() // consume DEFAULT or :=
			defVal, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("error parsing default value: %v", err)
			}
			param.Default = defVal
		}

		params = append(params, param)

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	return params, nil
}

// parsePLBody parses the PL/pgSQL body text into a PLBlock AST.
func (p *Parser) parsePLBody(bodyText string) (*PLBlock, error) {
	// Create a new parser for the body text
	plParser := NewParser(bodyText)
	return plParser.parsePLBlock()
}

// parsePLBlock parses a PL/pgSQL BEGIN...END block with optional DECLARE section.
func (p *Parser) parsePLBlock() (*PLBlock, error) {
	block := &PLBlock{}

	// Optional DECLARE section
	if p.curTokenIs(TOKEN_DECLARE) {
		p.nextToken() // consume DECLARE
		decls, err := p.parsePLDeclarations()
		if err != nil {
			return nil, err
		}
		block.Declarations = decls
	}

	// BEGIN
	if !p.curTokenIs(TOKEN_BEGIN) {
		return nil, fmt.Errorf("expected BEGIN, got %v (%q)", p.cur.Type, p.cur.Literal)
	}
	p.nextToken() // consume BEGIN

	// Statements until END or EXCEPTION
	stmts, err := p.parsePLStatements()
	if err != nil {
		return nil, err
	}
	block.Statements = stmts

	// Optional EXCEPTION section
	if p.curTokenIs(TOKEN_EXCEPTION) {
		p.nextToken() // consume EXCEPTION
		handlers, err := p.parsePLExceptionHandlers()
		if err != nil {
			return nil, err
		}
		block.ExceptionHandlers = handlers
	}

	// END
	if !p.curTokenIs(TOKEN_END) {
		return nil, fmt.Errorf("expected END, got %v (%q)", p.cur.Type, p.cur.Literal)
	}
	p.nextToken() // consume END

	// Optional semicolon after END
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return block, nil
}

// parsePLDeclarations parses variable declarations in DECLARE section.
func (p *Parser) parsePLDeclarations() ([]PLVarDecl, error) {
	var decls []PLVarDecl

	for !p.curTokenIs(TOKEN_BEGIN) && !p.curTokenIs(TOKEN_EOF) {
		decl := PLVarDecl{}

		// Variable name
		if !p.isIdentifierOrContextualKeyword() {
			return nil, fmt.Errorf("expected variable name in DECLARE, got %v", p.cur.Type)
		}
		name, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		decl.Name = name

		// Data type
		dataType, err := p.parsePLDataType()
		if err != nil {
			return nil, fmt.Errorf("error parsing variable type: %v", err)
		}
		decl.Type = dataType

		// Optional NOT NULL
		if p.curTokenIs(TOKEN_NOT) {
			p.nextToken()
			if err := p.expect(TOKEN_NULL); err != nil {
				return nil, fmt.Errorf("expected NULL after NOT")
			}
			decl.NotNull = true
		}

		// Optional DEFAULT or :=
		if p.curTokenIs(TOKEN_DEFAULT) || p.curTokenIs(TOKEN_COLON_EQ) {
			p.nextToken()
			defVal, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("error parsing default value: %v", err)
			}
			decl.Default = defVal
		}

		// Semicolon
		if p.curTokenIs(TOKEN_SEMICOLON) {
			p.nextToken()
		}

		decls = append(decls, decl)
	}

	return decls, nil
}

// parsePLStatements parses PL/pgSQL statements until END, EXCEPTION, ELSIF, ELSE, or LOOP terminator.
func (p *Parser) parsePLStatements() ([]PLStatement, error) {
	var stmts []PLStatement

	for !p.curTokenIs(TOKEN_END) && !p.curTokenIs(TOKEN_EXCEPTION) &&
		!p.curTokenIs(TOKEN_ELSIF) && !p.curTokenIs(TOKEN_ELSE) &&
		!p.curTokenIs(TOKEN_EOF) {

		stmt, err := p.parsePLStatement()
		if err != nil {
			return nil, err
		}
		if stmt != nil {
			stmts = append(stmts, stmt)
		}
	}

	return stmts, nil
}

// parsePLStatement parses a single PL/pgSQL statement.
func (p *Parser) parsePLStatement() (PLStatement, error) {
	switch {
	case p.curTokenIs(TOKEN_IF):
		return p.parsePLIf()

	case p.curTokenIs(TOKEN_WHILE):
		return p.parsePLWhile()

	case p.curTokenIs(TOKEN_LOOP):
		return p.parsePLLoop()

	case p.curTokenIs(TOKEN_FOR):
		return p.parsePLFor()

	case p.curTokenIs(TOKEN_EXIT):
		return p.parsePLExit()

	case p.curTokenIs(TOKEN_CONTINUE):
		return p.parsePLContinue()

	case p.curTokenIs(TOKEN_RETURN):
		return p.parsePLReturn()

	case p.curTokenIs(TOKEN_RAISE):
		return p.parsePLRaise()

	case p.curTokenIs(TOKEN_PERFORM):
		return p.parsePLPerform()

	case p.curTokenIs(TOKEN_SELECT), p.curTokenIs(TOKEN_INSERT),
		p.curTokenIs(TOKEN_UPDATE), p.curTokenIs(TOKEN_DELETE):
		return p.parsePLSQL()

	case p.curTokenIs(TOKEN_NULL):
		// NULL statement (do nothing)
		p.nextToken()
		if p.curTokenIs(TOKEN_SEMICOLON) {
			p.nextToken()
		}
		return nil, nil

	case p.isIdentifierOrContextualKeyword():
		// Could be assignment: var := expr;
		return p.parsePLAssignOrSQL()

	default:
		return nil, fmt.Errorf("unexpected token in PL/pgSQL body: %v (%q)", p.cur.Type, p.cur.Literal)
	}
}

// parsePLIf parses IF...ELSIF...ELSE...END IF statement.
func (p *Parser) parsePLIf() (*PLIf, error) {
	p.nextToken() // consume IF

	stmt := &PLIf{}

	// Condition
	cond, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("error parsing IF condition: %v", err)
	}
	stmt.Condition = cond

	// THEN
	if !p.curTokenIs(TOKEN_THEN) {
		return nil, fmt.Errorf("expected THEN after IF condition, got %v", p.cur.Type)
	}
	p.nextToken() // consume THEN

	// THEN statements
	thenStmts, err := p.parsePLStatements()
	if err != nil {
		return nil, err
	}
	stmt.Then = thenStmts

	// ELSIF branches
	for p.curTokenIs(TOKEN_ELSIF) {
		p.nextToken() // consume ELSIF
		elsif := PLElsIf{}

		cond, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing ELSIF condition: %v", err)
		}
		elsif.Condition = cond

		if !p.curTokenIs(TOKEN_THEN) {
			return nil, fmt.Errorf("expected THEN after ELSIF condition")
		}
		p.nextToken() // consume THEN

		stmts, err := p.parsePLStatements()
		if err != nil {
			return nil, err
		}
		elsif.Then = stmts

		stmt.ElsIfs = append(stmt.ElsIfs, elsif)
	}

	// ELSE branch
	if p.curTokenIs(TOKEN_ELSE) {
		p.nextToken() // consume ELSE
		elseStmts, err := p.parsePLStatements()
		if err != nil {
			return nil, err
		}
		stmt.Else = elseStmts
	}

	// END IF
	if !p.curTokenIs(TOKEN_END) {
		return nil, fmt.Errorf("expected END IF, got %v", p.cur.Type)
	}
	p.nextToken() // consume END

	if !p.curTokenIs(TOKEN_IF) {
		return nil, fmt.Errorf("expected IF after END, got %v", p.cur.Type)
	}
	p.nextToken() // consume IF

	// Semicolon
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLWhile parses WHILE condition LOOP ... END LOOP statement.
func (p *Parser) parsePLWhile() (*PLWhile, error) {
	p.nextToken() // consume WHILE

	stmt := &PLWhile{}

	// Condition
	cond, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("error parsing WHILE condition: %v", err)
	}
	stmt.Condition = cond

	// LOOP
	if !p.curTokenIs(TOKEN_LOOP) {
		return nil, fmt.Errorf("expected LOOP after WHILE condition, got %v", p.cur.Type)
	}
	p.nextToken() // consume LOOP

	// Body statements until END LOOP
	body, err := p.parsePLLoopBody()
	if err != nil {
		return nil, err
	}
	stmt.Body = body

	return stmt, nil
}

// parsePLLoop parses simple LOOP ... END LOOP statement.
func (p *Parser) parsePLLoop() (*PLLoop, error) {
	p.nextToken() // consume LOOP

	stmt := &PLLoop{}

	body, err := p.parsePLLoopBody()
	if err != nil {
		return nil, err
	}
	stmt.Body = body

	return stmt, nil
}

// parsePLLoopBody parses loop body until END LOOP.
func (p *Parser) parsePLLoopBody() ([]PLStatement, error) {
	var stmts []PLStatement

	for !p.curTokenIs(TOKEN_EOF) {
		// Check for END LOOP
		if p.curTokenIs(TOKEN_END) {
			p.nextToken() // consume END
			if !p.curTokenIs(TOKEN_LOOP) {
				return nil, fmt.Errorf("expected LOOP after END in loop body")
			}
			p.nextToken() // consume LOOP
			if p.curTokenIs(TOKEN_SEMICOLON) {
				p.nextToken()
			}
			return stmts, nil
		}

		stmt, err := p.parsePLStatement()
		if err != nil {
			return nil, err
		}
		if stmt != nil {
			stmts = append(stmts, stmt)
		}
	}

	return nil, fmt.Errorf("unexpected EOF in loop body")
}

// parsePLFor parses FOR loop (numeric or query).
func (p *Parser) parsePLFor() (*PLFor, error) {
	p.nextToken() // consume FOR

	stmt := &PLFor{}

	// Loop variable
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected loop variable, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Variable = name

	// IN keyword
	if !p.curTokenIs(TOKEN_IN) {
		return nil, fmt.Errorf("expected IN after loop variable, got %v", p.cur.Type)
	}
	p.nextToken() // consume IN

	// Check for REVERSE
	if p.curTokenIs(TOKEN_IDENT) && strings.ToUpper(p.cur.Literal) == "REVERSE" {
		stmt.Reverse = true
		p.nextToken()
	}

	// Check if it's a query loop: FOR rec IN (SELECT ...) or FOR rec IN query
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (
		if p.curTokenIs(TOKEN_SELECT) {
			query, err := p.parseSelect()
			if err != nil {
				return nil, fmt.Errorf("error parsing FOR query: %v", err)
			}
			stmt.Query = query
			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after FOR query")
			}
		} else {
			// Numeric range in parentheses
			lower, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.LowerBound = lower

			// .. or TO for range
			if p.cur.Literal == ".." {
				p.nextToken()
			}

			upper, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.UpperBound = upper

			if err := p.expect(TOKEN_RPAREN); err != nil {
				return nil, fmt.Errorf("expected ')' after numeric range")
			}
		}
	} else {
		// Numeric range: lower..upper or lower TO upper
		lower, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.LowerBound = lower

		// .. separator
		if p.cur.Literal == ".." {
			p.nextToken()
		}

		upper, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.UpperBound = upper
	}

	// LOOP
	if !p.curTokenIs(TOKEN_LOOP) {
		return nil, fmt.Errorf("expected LOOP, got %v", p.cur.Type)
	}
	p.nextToken() // consume LOOP

	// Body
	body, err := p.parsePLLoopBody()
	if err != nil {
		return nil, err
	}
	stmt.Body = body

	return stmt, nil
}

// parsePLExit parses EXIT [label] [WHEN condition] statement.
func (p *Parser) parsePLExit() (*PLExit, error) {
	p.nextToken() // consume EXIT

	stmt := &PLExit{}

	// Optional label
	if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_WHEN) {
		label, _ := p.parseIdentifier()
		stmt.Label = label
	}

	// Optional WHEN condition
	if p.curTokenIs(TOKEN_WHEN) {
		p.nextToken() // consume WHEN
		cond, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing EXIT WHEN condition: %v", err)
		}
		stmt.Condition = cond
	}

	// Semicolon
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLContinue parses CONTINUE [label] [WHEN condition] statement.
func (p *Parser) parsePLContinue() (*PLContinue, error) {
	p.nextToken() // consume CONTINUE

	stmt := &PLContinue{}

	// Optional label
	if p.isIdentifierOrContextualKeyword() && !p.curTokenIs(TOKEN_WHEN) {
		label, _ := p.parseIdentifier()
		stmt.Label = label
	}

	// Optional WHEN condition
	if p.curTokenIs(TOKEN_WHEN) {
		p.nextToken() // consume WHEN
		cond, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing CONTINUE WHEN condition: %v", err)
		}
		stmt.Condition = cond
	}

	// Semicolon
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLReturn parses RETURN [expression] statement.
func (p *Parser) parsePLReturn() (*PLReturn, error) {
	p.nextToken() // consume RETURN

	stmt := &PLReturn{}

	// Optional return value (not followed immediately by semicolon or END)
	if !p.curTokenIs(TOKEN_SEMICOLON) && !p.curTokenIs(TOKEN_END) && !p.curTokenIs(TOKEN_EOF) {
		val, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing RETURN expression: %v", err)
		}
		stmt.Value = val
	}

	// Semicolon
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLRaise parses RAISE [level] 'message' [, args] statement.
func (p *Parser) parsePLRaise() (*PLRaise, error) {
	p.nextToken() // consume RAISE

	stmt := &PLRaise{Level: "EXCEPTION"} // default level

	// Optional level: NOTICE, WARNING, EXCEPTION, etc.
	if p.curTokenIs(TOKEN_NOTICE) || p.curTokenIs(TOKEN_EXCEPTION) ||
		(p.curTokenIs(TOKEN_IDENT) && (strings.ToUpper(p.cur.Literal) == "WARNING" ||
			strings.ToUpper(p.cur.Literal) == "INFO" ||
			strings.ToUpper(p.cur.Literal) == "DEBUG")) {
		stmt.Level = strings.ToUpper(p.cur.Literal)
		p.nextToken()
	}

	// Message string
	if p.curTokenIs(TOKEN_STRING) {
		stmt.Message = p.cur.Literal
		p.nextToken()
	}

	// Optional format arguments
	for p.curTokenIs(TOKEN_COMMA) {
		p.nextToken() // consume comma
		arg, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing RAISE argument: %v", err)
		}
		stmt.Args = append(stmt.Args, arg)
	}

	// Semicolon
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLPerform parses PERFORM query statement.
func (p *Parser) parsePLPerform() (*PLPerform, error) {
	p.nextToken() // consume PERFORM

	stmt := &PLPerform{}

	// The rest is a SELECT query (without SELECT keyword)
	// PERFORM expr is equivalent to SELECT expr with results discarded
	// For simplicity, we'll parse as if it were a SELECT
	// We need to construct a pseudo-SELECT

	// Save position and manually construct SELECT
	query := &SelectStmt{
		Columns: []SelectColumn{},
	}

	// Parse expression list as columns
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing PERFORM expression: %v", err)
		}
		query.Columns = append(query.Columns, SelectColumn{Expression: expr})

		if p.curTokenIs(TOKEN_COMMA) {
			p.nextToken()
		} else {
			break
		}
	}

	// Optional FROM
	if p.curTokenIs(TOKEN_FROM) {
		p.nextToken()
		if p.isIdentifierOrContextualKeyword() {
			name, _ := p.parseIdentifier()
			query.TableName = name
		}
	}

	// Optional WHERE
	if p.curTokenIs(TOKEN_WHERE) {
		p.nextToken()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		query.Where = where
	}

	stmt.Query = query

	// Semicolon
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLSQL parses embedded SQL (SELECT INTO, INSERT, UPDATE, DELETE).
func (p *Parser) parsePLSQL() (*PLSQL, error) {
	stmt := &PLSQL{}

	// Parse the SQL statement
	sqlStmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("error parsing embedded SQL: %v", err)
	}
	stmt.Statement = sqlStmt

	// Consume trailing semicolon if present
	if p.curTokenIs(TOKEN_SEMICOLON) {
		p.nextToken()
	}

	return stmt, nil
}

// parsePLAssignOrSQL parses variable assignment or SQL statement starting with identifier.
func (p *Parser) parsePLAssignOrSQL() (PLStatement, error) {
	// Save the identifier
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	// Check for := assignment
	if p.curTokenIs(TOKEN_COLON_EQ) {
		p.nextToken() // consume :=
		expr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error parsing assignment value: %v", err)
		}

		if p.curTokenIs(TOKEN_SEMICOLON) {
			p.nextToken()
		}

		return &PLAssign{Variable: name, Value: expr}, nil
	}

	// Could be a procedure/function call or other statement
	// For now, treat as an error
	return nil, fmt.Errorf("unexpected identifier %q in PL/pgSQL body (expected := for assignment)", name)
}

// parsePLExceptionHandlers parses EXCEPTION WHEN handlers.
func (p *Parser) parsePLExceptionHandlers() ([]PLExceptionHandler, error) {
	var handlers []PLExceptionHandler

	for p.curTokenIs(TOKEN_WHEN) {
		p.nextToken() // consume WHEN

		handler := PLExceptionHandler{}

		// Exception names (e.g., OTHERS, NO_DATA_FOUND)
		for {
			if !p.isIdentifierOrContextualKeyword() {
				return nil, fmt.Errorf("expected exception name")
			}
			name, _ := p.parseIdentifier()
			handler.Exceptions = append(handler.Exceptions, name)

			if p.curTokenIs(TOKEN_OR) {
				p.nextToken() // consume OR
			} else {
				break
			}
		}

		// THEN
		if !p.curTokenIs(TOKEN_THEN) {
			return nil, fmt.Errorf("expected THEN after exception names")
		}
		p.nextToken() // consume THEN

		// Handler statements until next WHEN or END
		var stmts []PLStatement
		for !p.curTokenIs(TOKEN_WHEN) && !p.curTokenIs(TOKEN_END) && !p.curTokenIs(TOKEN_EOF) {
			stmt, err := p.parsePLStatement()
			if err != nil {
				return nil, err
			}
			if stmt != nil {
				stmts = append(stmts, stmt)
			}
		}
		handler.Statements = stmts

		handlers = append(handlers, handler)
	}

	return handlers, nil
}

// parseDropProcedure parses DROP PROCEDURE [IF EXISTS] name
func (p *Parser) parseDropProcedure() (*DropProcedureStmt, error) {
	p.nextToken() // consume PROCEDURE

	stmt := &DropProcedureStmt{}

	// Optional IF EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	// Procedure name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected procedure name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	return stmt, nil
}

// parseDropFunction parses DROP FUNCTION [IF EXISTS] name
func (p *Parser) parseDropFunction() (*DropFunctionStmt, error) {
	p.nextToken() // consume FUNCTION

	stmt := &DropFunctionStmt{}

	// Optional IF EXISTS
	if p.curTokenIs(TOKEN_IF) {
		p.nextToken() // consume IF
		if err := p.expect(TOKEN_EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		stmt.IfExists = true
	}

	// Function name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected function name, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	return stmt, nil
}

// parseCall parses CALL procedure_name(args...)
func (p *Parser) parseCall() (*CallStmt, error) {
	p.nextToken() // consume CALL

	stmt := &CallStmt{}

	// Procedure name
	if !p.isIdentifierOrContextualKeyword() {
		return nil, fmt.Errorf("expected procedure name after CALL, got %v", p.cur.Type)
	}
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	stmt.Name = name

	// Arguments (optional parentheses)
	if p.curTokenIs(TOKEN_LPAREN) {
		p.nextToken() // consume (

		if !p.curTokenIs(TOKEN_RPAREN) {
			for {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, fmt.Errorf("error parsing CALL argument: %v", err)
				}
				stmt.Args = append(stmt.Args, arg)

				if p.curTokenIs(TOKEN_COMMA) {
					p.nextToken()
				} else {
					break
				}
			}
		}

		if err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, fmt.Errorf("expected ')' after CALL arguments")
		}
	}

	return stmt, nil
}
