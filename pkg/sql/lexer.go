// Package sql provides SQL parsing and execution for VeridicalDB.
package sql

import (
	"strings"
	"unicode"
)

// TokenType represents the type of a token.
type TokenType int

const (
	// Special tokens
	TOKEN_EOF TokenType = iota
	TOKEN_ILLEGAL

	// Literals
	TOKEN_IDENT  // identifiers: table names, column names
	TOKEN_INT    // integer literals
	TOKEN_STRING // string literals 'hello'

	// Operators and delimiters
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )
	TOKEN_STAR      // *
	TOKEN_PLUS      // +
	TOKEN_MINUS     // -
	TOKEN_SLASH     // /
	TOKEN_EQ        // =
	TOKEN_NE        // != or <>
	TOKEN_LT        // <
	TOKEN_LE        // <=
	TOKEN_GT        // >
	TOKEN_GE        // >=

	// Keywords
	TOKEN_SELECT
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_INSERT
	TOKEN_INTO
	TOKEN_VALUES
	TOKEN_UPDATE
	TOKEN_SET
	TOKEN_DELETE
	TOKEN_CREATE
	TOKEN_TABLE
	TOKEN_DROP
	TOKEN_AND
	TOKEN_OR
	TOKEN_NOT
	TOKEN_NULL
	TOKEN_TRUE
	TOKEN_FALSE
	TOKEN_PRIMARY
	TOKEN_KEY
	TOKEN_INT_TYPE
	TOKEN_BIGINT
	TOKEN_TEXT
	TOKEN_BOOL
	TOKEN_TIMESTAMP

	// Transaction keywords
	TOKEN_BEGIN
	TOKEN_COMMIT
	TOKEN_ROLLBACK

	// Index keywords
	TOKEN_INDEX
	TOKEN_UNIQUE
	TOKEN_ON

	// Storage keywords
	TOKEN_USING
	TOKEN_COLUMN

	// ORDER BY and LIMIT keywords
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_ASC
	TOKEN_DESC
	TOKEN_LIMIT
	TOKEN_OFFSET

	// Aggregate function keywords
	TOKEN_COUNT
	TOKEN_SUM
	TOKEN_AVG
	TOKEN_MIN
	TOKEN_MAX

	// GROUP BY and HAVING keywords
	TOKEN_GROUP
	TOKEN_HAVING

	// DISTINCT keyword
	TOKEN_DISTINCT

	// DEFAULT keyword
	TOKEN_DEFAULT

	// AUTO_INCREMENT keyword
	TOKEN_AUTO_INCREMENT

	// JOIN keywords
	TOKEN_JOIN
	TOKEN_INNER
	TOKEN_LEFT
	TOKEN_RIGHT
	TOKEN_FULL
	TOKEN_CROSS
	TOKEN_OUTER

	// IN / BETWEEN / AS keywords
	TOKEN_IN
	TOKEN_BETWEEN
	TOKEN_AS

	// LIKE keyword
	TOKEN_LIKE
	TOKEN_ILIKE

	// NULL handling functions
	TOKEN_COALESCE
	TOKEN_NULLIF

	// String functions
	TOKEN_UPPER
	TOKEN_LOWER
	TOKEN_LENGTH
	TOKEN_CONCAT
	TOKEN_SUBSTR
	TOKEN_SUBSTRING

	// DDL keywords
	TOKEN_ALTER
	TOKEN_ADD
	TOKEN_RENAME
	TOKEN_TO
	TOKEN_TRUNCATE
	TOKEN_SHOW
	TOKEN_TABLES
	TOKEN_IF
	TOKEN_EXISTS

	// Query analysis
	TOKEN_EXPLAIN
	TOKEN_ANALYZE

	// CASE WHEN
	TOKEN_CASE
	TOKEN_WHEN
	TOKEN_THEN
	TOKEN_ELSE
	TOKEN_END

	// CHECK constraint
	TOKEN_CHECK

	// Foreign Key constraint
	TOKEN_REFERENCES
	TOKEN_FOREIGN
	TOKEN_CONSTRAINT

	// Date/Time functions
	TOKEN_NOW
	TOKEN_CURRENT_TIMESTAMP
	TOKEN_CURRENT_DATE
	TOKEN_YEAR
	TOKEN_MONTH
	TOKEN_DAY
	TOKEN_HOUR
	TOKEN_MINUTE
	TOKEN_SECOND
	TOKEN_DATE_ADD
	TOKEN_DATE_SUB
	TOKEN_INTERVAL
	TOKEN_EXTRACT

	// IS NULL / IS NOT NULL
	TOKEN_IS

	// Type casting
	TOKEN_CAST

	// Set operations
	TOKEN_UNION
	TOKEN_INTERSECT
	TOKEN_EXCEPT
	TOKEN_ALL

	// View
	TOKEN_VIEW

	// Math functions
	TOKEN_ABS
	TOKEN_ROUND
	TOKEN_FLOOR
	TOKEN_CEIL
	TOKEN_CEILING
	TOKEN_MOD
	TOKEN_POWER
	TOKEN_SQRT

	// String functions
	TOKEN_TRIM
	TOKEN_LTRIM
	TOKEN_RTRIM
	TOKEN_REPLACE
	TOKEN_POSITION
	TOKEN_REVERSE
	TOKEN_REPEAT
	TOKEN_LPAD
	TOKEN_RPAD

	// Window functions
	TOKEN_OVER
	TOKEN_PARTITION
	TOKEN_ROW_NUMBER
	TOKEN_RANK
	TOKEN_DENSE_RANK
	TOKEN_NTILE
	TOKEN_LAG
	TOKEN_LEAD
	TOKEN_FIRST_VALUE
	TOKEN_LAST_VALUE
	TOKEN_NTH_VALUE
	TOKEN_ROWS
	TOKEN_RANGE
	TOKEN_UNBOUNDED
	TOKEN_PRECEDING
	TOKEN_FOLLOWING
	TOKEN_CURRENT
	TOKEN_ROW

	// LATERAL join
	TOKEN_LATERAL

	// MERGE statement
	TOKEN_MERGE
	TOKEN_MATCHED
	TOKEN_TARGET
	TOKEN_SOURCE
	TOKEN_DO
	TOKEN_NOTHING
	TOKEN_CONFLICT
	TOKEN_EXCLUDED

	// Grouping sets
	TOKEN_GROUPING
	TOKEN_SETS
	TOKEN_CUBE
	TOKEN_ROLLUP

	// CTE (Common Table Expressions)
	TOKEN_WITH
	TOKEN_RECURSIVE
)

var keywords = map[string]TokenType{
	"SELECT":            TOKEN_SELECT,
	"FROM":              TOKEN_FROM,
	"WHERE":             TOKEN_WHERE,
	"INSERT":            TOKEN_INSERT,
	"INTO":              TOKEN_INTO,
	"VALUES":            TOKEN_VALUES,
	"UPDATE":            TOKEN_UPDATE,
	"SET":               TOKEN_SET,
	"DELETE":            TOKEN_DELETE,
	"CREATE":            TOKEN_CREATE,
	"TABLE":             TOKEN_TABLE,
	"DROP":              TOKEN_DROP,
	"AND":               TOKEN_AND,
	"OR":                TOKEN_OR,
	"NOT":               TOKEN_NOT,
	"NULL":              TOKEN_NULL,
	"TRUE":              TOKEN_TRUE,
	"FALSE":             TOKEN_FALSE,
	"PRIMARY":           TOKEN_PRIMARY,
	"KEY":               TOKEN_KEY,
	"INT":               TOKEN_INT_TYPE,
	"INTEGER":           TOKEN_INT_TYPE,
	"BIGINT":            TOKEN_BIGINT,
	"TEXT":              TOKEN_TEXT,
	"VARCHAR":           TOKEN_TEXT,
	"STRING":            TOKEN_TEXT,
	"BOOL":              TOKEN_BOOL,
	"BOOLEAN":           TOKEN_BOOL,
	"TIMESTAMP":         TOKEN_TIMESTAMP,
	"DATETIME":          TOKEN_TIMESTAMP,
	"BEGIN":             TOKEN_BEGIN,
	"COMMIT":            TOKEN_COMMIT,
	"ROLLBACK":          TOKEN_ROLLBACK,
	"INDEX":             TOKEN_INDEX,
	"UNIQUE":            TOKEN_UNIQUE,
	"ON":                TOKEN_ON,
	"USING":             TOKEN_USING,
	"COLUMN":            TOKEN_COLUMN,
	"ORDER":             TOKEN_ORDER,
	"BY":                TOKEN_BY,
	"ASC":               TOKEN_ASC,
	"DESC":              TOKEN_DESC,
	"LIMIT":             TOKEN_LIMIT,
	"OFFSET":            TOKEN_OFFSET,
	"COUNT":             TOKEN_COUNT,
	"SUM":               TOKEN_SUM,
	"AVG":               TOKEN_AVG,
	"MIN":               TOKEN_MIN,
	"MAX":               TOKEN_MAX,
	"GROUP":             TOKEN_GROUP,
	"HAVING":            TOKEN_HAVING,
	"DISTINCT":          TOKEN_DISTINCT,
	"DEFAULT":           TOKEN_DEFAULT,
	"AUTO_INCREMENT":    TOKEN_AUTO_INCREMENT,
	"JOIN":              TOKEN_JOIN,
	"INNER":             TOKEN_INNER,
	"LEFT":              TOKEN_LEFT,
	"RIGHT":             TOKEN_RIGHT,
	"FULL":              TOKEN_FULL,
	"CROSS":             TOKEN_CROSS,
	"OUTER":             TOKEN_OUTER,
	"IN":                TOKEN_IN,
	"BETWEEN":           TOKEN_BETWEEN,
	"AS":                TOKEN_AS,
	"LIKE":              TOKEN_LIKE,
	"ILIKE":             TOKEN_ILIKE,
	"COALESCE":          TOKEN_COALESCE,
	"NULLIF":            TOKEN_NULLIF,
	"UPPER":             TOKEN_UPPER,
	"LOWER":             TOKEN_LOWER,
	"LENGTH":            TOKEN_LENGTH,
	"LEN":               TOKEN_LENGTH,
	"CONCAT":            TOKEN_CONCAT,
	"SUBSTR":            TOKEN_SUBSTR,
	"SUBSTRING":         TOKEN_SUBSTRING,
	"ALTER":             TOKEN_ALTER,
	"ADD":               TOKEN_ADD,
	"RENAME":            TOKEN_RENAME,
	"TO":                TOKEN_TO,
	"TRUNCATE":          TOKEN_TRUNCATE,
	"SHOW":              TOKEN_SHOW,
	"TABLES":            TOKEN_TABLES,
	"IF":                TOKEN_IF,
	"EXISTS":            TOKEN_EXISTS,
	"EXPLAIN":           TOKEN_EXPLAIN,
	"ANALYZE":           TOKEN_ANALYZE,
	"CASE":              TOKEN_CASE,
	"WHEN":              TOKEN_WHEN,
	"THEN":              TOKEN_THEN,
	"ELSE":              TOKEN_ELSE,
	"END":               TOKEN_END,
	"CHECK":             TOKEN_CHECK,
	"REFERENCES":        TOKEN_REFERENCES,
	"FOREIGN":           TOKEN_FOREIGN,
	"CONSTRAINT":        TOKEN_CONSTRAINT,
	"NOW":               TOKEN_NOW,
	"CURRENT_TIMESTAMP": TOKEN_CURRENT_TIMESTAMP,
	"CURRENT_DATE":      TOKEN_CURRENT_DATE,
	"YEAR":              TOKEN_YEAR,
	"MONTH":             TOKEN_MONTH,
	"DAY":               TOKEN_DAY,
	"HOUR":              TOKEN_HOUR,
	"MINUTE":            TOKEN_MINUTE,
	"SECOND":            TOKEN_SECOND,
	"DATE_ADD":          TOKEN_DATE_ADD,
	"DATE_SUB":          TOKEN_DATE_SUB,
	"INTERVAL":          TOKEN_INTERVAL,
	"EXTRACT":           TOKEN_EXTRACT,
	"IS":                TOKEN_IS,
	"CAST":              TOKEN_CAST,
	"UNION":             TOKEN_UNION,
	"INTERSECT":         TOKEN_INTERSECT,
	"EXCEPT":            TOKEN_EXCEPT,
	"ALL":               TOKEN_ALL,
	"VIEW":              TOKEN_VIEW,
	"ABS":               TOKEN_ABS,
	"ROUND":             TOKEN_ROUND,
	"FLOOR":             TOKEN_FLOOR,
	"CEIL":              TOKEN_CEIL,
	"CEILING":           TOKEN_CEILING,
	"MOD":               TOKEN_MOD,
	"POWER":             TOKEN_POWER,
	"POW":               TOKEN_POWER,
	"SQRT":              TOKEN_SQRT,
	"TRIM":              TOKEN_TRIM,
	"LTRIM":             TOKEN_LTRIM,
	"RTRIM":             TOKEN_RTRIM,
	"REPLACE":           TOKEN_REPLACE,
	"POSITION":          TOKEN_POSITION,
	"REVERSE":           TOKEN_REVERSE,
	"REPEAT":            TOKEN_REPEAT,
	"LPAD":              TOKEN_LPAD,
	"RPAD":              TOKEN_RPAD,
	"OVER":              TOKEN_OVER,
	"PARTITION":         TOKEN_PARTITION,
	"ROW_NUMBER":        TOKEN_ROW_NUMBER,
	"RANK":              TOKEN_RANK,
	"DENSE_RANK":        TOKEN_DENSE_RANK,
	"NTILE":             TOKEN_NTILE,
	"LAG":               TOKEN_LAG,
	"LEAD":              TOKEN_LEAD,
	"FIRST_VALUE":       TOKEN_FIRST_VALUE,
	"LAST_VALUE":        TOKEN_LAST_VALUE,
	"NTH_VALUE":         TOKEN_NTH_VALUE,
	"ROWS":              TOKEN_ROWS,
	"RANGE":             TOKEN_RANGE,
	"UNBOUNDED":         TOKEN_UNBOUNDED,
	"PRECEDING":         TOKEN_PRECEDING,
	"FOLLOWING":         TOKEN_FOLLOWING,
	"CURRENT":           TOKEN_CURRENT,
	"ROW":               TOKEN_ROW,
	"LATERAL":           TOKEN_LATERAL,
	"MERGE":             TOKEN_MERGE,
	"MATCHED":           TOKEN_MATCHED,
	"TARGET":            TOKEN_TARGET,
	"SOURCE":            TOKEN_SOURCE,
	"DO":                TOKEN_DO,
	"NOTHING":           TOKEN_NOTHING,
	"CONFLICT":          TOKEN_CONFLICT,
	"EXCLUDED":          TOKEN_EXCLUDED,
	"GROUPING":          TOKEN_GROUPING,
	"SETS":              TOKEN_SETS,
	"CUBE":              TOKEN_CUBE,
	"ROLLUP":            TOKEN_ROLLUP,
	"WITH":              TOKEN_WITH,
	"RECURSIVE":         TOKEN_RECURSIVE,
}

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input   string
	pos     int  // current position
	readPos int  // next position to read
	ch      byte // current character
}

// NewLexer creates a new Lexer for the input string.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	var tok Token
	tok.Pos = l.pos

	switch l.ch {
	case 0:
		tok.Type = TOKEN_EOF
		tok.Literal = ""
	case ',':
		tok = Token{Type: TOKEN_COMMA, Literal: ",", Pos: l.pos}
		l.readChar()
	case ';':
		tok = Token{Type: TOKEN_SEMICOLON, Literal: ";", Pos: l.pos}
		l.readChar()
	case '(':
		tok = Token{Type: TOKEN_LPAREN, Literal: "(", Pos: l.pos}
		l.readChar()
	case ')':
		tok = Token{Type: TOKEN_RPAREN, Literal: ")", Pos: l.pos}
		l.readChar()
	case '*':
		tok = Token{Type: TOKEN_STAR, Literal: "*", Pos: l.pos}
		l.readChar()
	case '+':
		tok = Token{Type: TOKEN_PLUS, Literal: "+", Pos: l.pos}
		l.readChar()
	case '-':
		tok = Token{Type: TOKEN_MINUS, Literal: "-", Pos: l.pos}
		l.readChar()
	case '/':
		tok = Token{Type: TOKEN_SLASH, Literal: "/", Pos: l.pos}
		l.readChar()
	case '=':
		tok = Token{Type: TOKEN_EQ, Literal: "=", Pos: l.pos}
		l.readChar()
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_LE, Literal: "<=", Pos: l.pos - 1}
			l.readChar()
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "<>", Pos: l.pos - 1}
			l.readChar()
		} else {
			tok = Token{Type: TOKEN_LT, Literal: "<", Pos: l.pos}
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_GE, Literal: ">=", Pos: l.pos - 1}
			l.readChar()
		} else {
			tok = Token{Type: TOKEN_GT, Literal: ">", Pos: l.pos}
			l.readChar()
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TOKEN_NE, Literal: "!=", Pos: l.pos - 1}
			l.readChar()
		} else {
			tok = Token{Type: TOKEN_ILLEGAL, Literal: string(l.ch), Pos: l.pos}
			l.readChar()
		}
	case '\'':
		tok.Type = TOKEN_STRING
		tok.Literal = l.readString()
		tok.Pos = l.pos
	default:
		if isLetter(l.ch) {
			tok.Pos = l.pos
			tok.Literal = l.readIdentifier()
			tok.Type = lookupKeyword(tok.Literal)
			return tok
		} else if isDigit(l.ch) || (l.ch == '-' && isDigit(l.peekChar())) {
			tok.Pos = l.pos
			tok.Literal = l.readNumber()
			tok.Type = TOKEN_INT
			return tok
		} else {
			tok = Token{Type: TOKEN_ILLEGAL, Literal: string(l.ch), Pos: l.pos}
			l.readChar()
		}
	}
	return tok
}

func (l *Lexer) readIdentifier() string {
	pos := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readNumber() string {
	pos := l.pos
	if l.ch == '-' {
		l.readChar()
	}
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readString() string {
	l.readChar() // consume opening quote
	pos := l.pos
	for l.ch != '\'' && l.ch != 0 {
		if l.ch == '\\' && l.peekChar() == '\'' {
			l.readChar() // skip escape
		}
		l.readChar()
	}
	str := l.input[pos:l.pos]
	if l.ch == '\'' {
		l.readChar() // consume closing quote
	}
	return str
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func lookupKeyword(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TOKEN_IDENT
}
