// Package token defines the lexical tokens of the VeridicalDB SQL dialect.
package token

import "strings"

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
	TOKEN_LBRACKET  // [
	TOKEN_RBRACKET  // ]
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
	TOKEN_DATE

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

	// Prepared Statements
	TOKEN_PREPARE
	TOKEN_EXECUTE
	TOKEN_DEALLOCATE
	TOKEN_PLACEHOLDER // $1, $2, etc.
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

	// User Authentication
	TOKEN_USER
	TOKEN_PASSWORD
	TOKEN_GRANT
	TOKEN_REVOKE
	TOKEN_ROLE
	TOKEN_SUPERUSER

	// Multi-Database Namespaces
	TOKEN_DATABASE
	TOKEN_USE
	TOKEN_OWNER

	// Triggers
	TOKEN_TRIGGER
	TOKEN_BEFORE
	TOKEN_AFTER
	TOKEN_INSTEAD
	TOKEN_OF
	TOKEN_FOR
	TOKEN_EACH
	TOKEN_STATEMENT
	TOKEN_NEW
	TOKEN_OLD

	// JSON operators
	TOKEN_JSON_TYPE    // JSON keyword for type
	TOKEN_ARROW        // -> (JSON object field access)
	TOKEN_ARROW_TEXT   // ->> (JSON object field as text)
	TOKEN_HASH_ARROW   // #> (JSON path access)
	TOKEN_HASH_ARROW_T // #>> (JSON path as text)
	TOKEN_AT_GT        // @> (JSON contains)
	TOKEN_LT_AT        // <@ (JSON contained by)
	TOKEN_QUESTION     // ? (JSON key exists)
	TOKEN_QUESTION_OR  // ?| (JSON any key exists)
	TOKEN_QUESTION_AND // ?& (JSON all keys exist)
	// TOKEN_CONCAT is already defined earlier for || concatenation

	// Full-Text Search - please be kind here, I'm only human (post realization that I did not add Date makes me question everything)
	TOKEN_TSVECTOR        // tsvector type
	TOKEN_TSQUERY         // tsquery type
	TOKEN_TO_TSVECTOR     // to_tsvector() function
	TOKEN_TO_TSQUERY      // to_tsquery() function
	TOKEN_PLAINTO_TSQUERY // plainto_tsquery() function
	TOKEN_MATCH           // @@ operator (text search match)
	TOKEN_FULLTEXT        // FULLTEXT keyword for index
	TOKEN_AGAINST         // AGAINST keyword (MySQL-style)
	TOKEN_WEBSEARCH       // websearch_to_tsquery
	TOKEN_TS_RANK         // ts_rank() function
	TOKEN_TS_HEADLINE     // ts_headline() function

	// Table Partitioning
	TOKEN_LIST       // LIST partitioning
	TOKEN_HASH       // HASH partitioning
	TOKEN_MAXVALUE   // MAXVALUE for range partitions
	TOKEN_LESS       // LESS for LESS THAN
	TOKEN_THAN       // THAN for LESS THAN
	TOKEN_PARTITIONS // PARTITIONS keyword

	// Stored Procedures / PL/pgSQL
	TOKEN_PROCEDURE    // PROCEDURE keyword
	TOKEN_CALL         // CALL keyword
	TOKEN_LANGUAGE     // LANGUAGE keyword
	TOKEN_PLPGSQL      // PLPGSQL keyword
	TOKEN_DECLARE      // DECLARE keyword
	TOKEN_LOOP         // LOOP keyword
	TOKEN_WHILE        // WHILE keyword
	TOKEN_EXIT         // EXIT keyword
	TOKEN_CONTINUE     // CONTINUE keyword
	TOKEN_RETURN       // RETURN keyword
	TOKEN_ELSIF        // ELSIF keyword
	TOKEN_RAISE        // RAISE keyword
	TOKEN_NOTICE       // NOTICE keyword
	TOKEN_EXCEPTION    // EXCEPTION keyword
	TOKEN_PERFORM      // PERFORM keyword
	TOKEN_DOLLAR_QUOTE // $$ dollar quoting
	TOKEN_COLON_EQ     // := assignment
	TOKEN_PROCEDURES   // PROCEDURES keyword for SHOW
	TOKEN_INOUT        // INOUT parameter mode
	TOKEN_OUT          // OUT parameter mode
	TOKEN_FUNCTION     // FUNCTION keyword
	TOKEN_RETURNS      // RETURNS keyword
	TOKEN_VOID         // VOID return type
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
	"DATE":              TOKEN_DATE,
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
	"PREPARE":           TOKEN_PREPARE,
	"EXECUTE":           TOKEN_EXECUTE,
	"DEALLOCATE":        TOKEN_DEALLOCATE,
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
	"USER":              TOKEN_USER,
	"PASSWORD":          TOKEN_PASSWORD,
	"GRANT":             TOKEN_GRANT,
	"REVOKE":            TOKEN_REVOKE,
	"ROLE":              TOKEN_ROLE,
	"SUPERUSER":         TOKEN_SUPERUSER,
	"DATABASE":          TOKEN_DATABASE,
	"USE":               TOKEN_USE,
	"OWNER":             TOKEN_OWNER,
	"TRIGGER":           TOKEN_TRIGGER,
	"BEFORE":            TOKEN_BEFORE,
	"AFTER":             TOKEN_AFTER,
	"INSTEAD":           TOKEN_INSTEAD,
	"OF":                TOKEN_OF,
	"FOR":               TOKEN_FOR,
	"EACH":              TOKEN_EACH,
	"STATEMENT":         TOKEN_STATEMENT,
	"NEW":               TOKEN_NEW,
	"OLD":               TOKEN_OLD,
	"JSON":              TOKEN_JSON_TYPE,
	"JSONB":             TOKEN_JSON_TYPE,
	// Full-Text Search keywords
	"TSVECTOR":             TOKEN_TSVECTOR,
	"TSQUERY":              TOKEN_TSQUERY,
	"TO_TSVECTOR":          TOKEN_TO_TSVECTOR,
	"TO_TSQUERY":           TOKEN_TO_TSQUERY,
	"PLAINTO_TSQUERY":      TOKEN_PLAINTO_TSQUERY,
	"FULLTEXT":             TOKEN_FULLTEXT,
	"AGAINST":              TOKEN_AGAINST,
	"WEBSEARCH_TO_TSQUERY": TOKEN_WEBSEARCH,
	"TS_RANK":              TOKEN_TS_RANK,
	"TS_HEADLINE":          TOKEN_TS_HEADLINE,
	"MATCH":                TOKEN_MATCH,
	// Table Partitioning
	"LIST":       TOKEN_LIST,
	"HASH":       TOKEN_HASH,
	"MAXVALUE":   TOKEN_MAXVALUE,
	"LESS":       TOKEN_LESS,
	"THAN":       TOKEN_THAN,
	"PARTITIONS": TOKEN_PARTITIONS,
	// Stored Procedures / PL/pgSQL
	"PROCEDURE":  TOKEN_PROCEDURE,
	"CALL":       TOKEN_CALL,
	"LANGUAGE":   TOKEN_LANGUAGE,
	"PLPGSQL":    TOKEN_PLPGSQL,
	"DECLARE":    TOKEN_DECLARE,
	"LOOP":       TOKEN_LOOP,
	"WHILE":      TOKEN_WHILE,
	"EXIT":       TOKEN_EXIT,
	"CONTINUE":   TOKEN_CONTINUE,
	"RETURN":     TOKEN_RETURN,
	"ELSIF":      TOKEN_ELSIF,
	"RAISE":      TOKEN_RAISE,
	"NOTICE":     TOKEN_NOTICE,
	"EXCEPTION":  TOKEN_EXCEPTION,
	"PERFORM":    TOKEN_PERFORM,
	"PROCEDURES": TOKEN_PROCEDURES,
	"INOUT":      TOKEN_INOUT,
	"OUT":        TOKEN_OUT,
	"FUNCTION":   TOKEN_FUNCTION,
	"RETURNS":    TOKEN_RETURNS,
	"VOID":       TOKEN_VOID,
}

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

// LookupKeyword returns the keyword token for ident, or TOKEN_IDENT if ident is
// not a reserved word. The lookup is case-insensitive.
func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TOKEN_IDENT
}
