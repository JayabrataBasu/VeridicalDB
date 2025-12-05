package sql

import (
	"fmt"
	"strconv"

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

// parseSelect parses: SELECT columns FROM table [WHERE expr]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}

	p.nextToken() // consume SELECT

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

func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	for {
		if p.curTokenIs(TOKEN_STAR) {
			cols = append(cols, SelectColumn{Star: true})
			p.nextToken()
		} else if p.curTokenIs(TOKEN_IDENT) {
			cols = append(cols, SelectColumn{Name: p.cur.Literal})
			p.nextToken()
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

// parseCreate parses: CREATE TABLE name (columns)
func (p *Parser) parseCreate() (*CreateTableStmt, error) {
	p.nextToken() // consume CREATE

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

	// Optional NOT NULL and PRIMARY KEY
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
		} else {
			break
		}
	}

	return col, nil
}

// parseDrop parses: DROP TABLE name
func (p *Parser) parseDrop() (*DropTableStmt, error) {
	p.nextToken() // consume DROP

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
