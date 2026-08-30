// Package lex implements the SQL lexer for VeridicalDB.
package lex

import (
	"unicode"

	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
)

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

func (l *Lexer) skipComments() {
	for {
		l.skipWhitespace()
		if l.ch == '-' && l.peekChar() == '-' {
			// Skip until end of line
			for l.ch != '\n' && l.ch != 0 {
				l.readChar()
			}
			continue
		}
		break
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() token.Token {
	l.skipComments()

	var tok token.Token
	tok.Pos = l.pos

	switch l.ch {
	case 0:
		tok.Type = token.TOKEN_EOF
		tok.Literal = ""
	case ',':
		tok = token.Token{Type: token.TOKEN_COMMA, Literal: ",", Pos: l.pos}
		l.readChar()
	case ';':
		tok = token.Token{Type: token.TOKEN_SEMICOLON, Literal: ";", Pos: l.pos}
		l.readChar()
	case '(':
		tok = token.Token{Type: token.TOKEN_LPAREN, Literal: "(", Pos: l.pos}
		l.readChar()
	case ')':
		tok = token.Token{Type: token.TOKEN_RPAREN, Literal: ")", Pos: l.pos}
		l.readChar()
	case '[':
		tok = token.Token{Type: token.TOKEN_LBRACKET, Literal: "[", Pos: l.pos}
		l.readChar()
	case ']':
		tok = token.Token{Type: token.TOKEN_RBRACKET, Literal: "]", Pos: l.pos}
		l.readChar()
	case '*':
		tok = token.Token{Type: token.TOKEN_STAR, Literal: "*", Pos: l.pos}
		l.readChar()
	case '+':
		tok = token.Token{Type: token.TOKEN_PLUS, Literal: "+", Pos: l.pos}
		l.readChar()
	case '-':
		// Check for JSON operators -> and ->>
		if l.peekChar() == '>' {
			startPos := l.pos
			l.readChar() // consume -
			if l.peekChar() == '>' {
				l.readChar() // consume first >
				tok = token.Token{Type: token.TOKEN_ARROW_TEXT, Literal: "->>", Pos: startPos}
				l.readChar() // consume second >
			} else {
				tok = token.Token{Type: token.TOKEN_ARROW, Literal: "->", Pos: startPos}
				l.readChar() // consume >
			}
		} else {
			tok = token.Token{Type: token.TOKEN_MINUS, Literal: "-", Pos: l.pos}
			l.readChar()
		}
	case '#':
		// Check for JSON operators #> and #>>
		if l.peekChar() == '>' {
			startPos := l.pos
			l.readChar() // consume #
			if l.peekChar() == '>' {
				l.readChar() // consume first >
				tok = token.Token{Type: token.TOKEN_HASH_ARROW_T, Literal: "#>>", Pos: startPos}
				l.readChar() // consume second >
			} else {
				tok = token.Token{Type: token.TOKEN_HASH_ARROW, Literal: "#>", Pos: startPos}
				l.readChar() // consume >
			}
		} else {
			tok = token.Token{Type: token.TOKEN_ILLEGAL, Literal: "#", Pos: l.pos}
			l.readChar()
		}
	case '@':
		startPos := l.pos
		if l.peekChar() == '@' {
			// @@ text search match operator
			l.readChar() // consume first @
			tok = token.Token{Type: token.TOKEN_MATCH, Literal: "@@", Pos: startPos}
			l.readChar() // consume second @
		} else if l.peekChar() == '>' {
			// @> JSON contains operator
			l.readChar() // consume @
			tok = token.Token{Type: token.TOKEN_AT_GT, Literal: "@>", Pos: startPos}
			l.readChar() // consume >
		} else {
			tok = token.Token{Type: token.TOKEN_ILLEGAL, Literal: "@", Pos: l.pos}
			l.readChar()
		}
	case '?':
		// Check for JSON operators ?, ?|, ?&
		startPos := l.pos
		if l.peekChar() == '|' {
			l.readChar() // consume ?
			tok = token.Token{Type: token.TOKEN_QUESTION_OR, Literal: "?|", Pos: startPos}
			l.readChar() // consume |
		} else if l.peekChar() == '&' {
			l.readChar() // consume ?
			tok = token.Token{Type: token.TOKEN_QUESTION_AND, Literal: "?&", Pos: startPos}
			l.readChar() // consume &
		} else {
			tok = token.Token{Type: token.TOKEN_QUESTION, Literal: "?", Pos: l.pos}
			l.readChar()
		}
	case '|':
		// Check for || concatenation operator
		if l.peekChar() == '|' {
			startPos := l.pos
			l.readChar() // consume first |
			tok = token.Token{Type: token.TOKEN_CONCAT, Literal: "||", Pos: startPos}
			l.readChar() // consume second |
		} else {
			tok = token.Token{Type: token.TOKEN_ILLEGAL, Literal: "|", Pos: l.pos}
			l.readChar()
		}
	case '/':
		tok = token.Token{Type: token.TOKEN_SLASH, Literal: "/", Pos: l.pos}
		l.readChar()
	case '=':
		tok = token.Token{Type: token.TOKEN_EQ, Literal: "=", Pos: l.pos}
		l.readChar()
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			tok = token.Token{Type: token.TOKEN_LE, Literal: "<=", Pos: l.pos - 1}
			l.readChar()
		} else if l.peekChar() == '>' {
			l.readChar()
			tok = token.Token{Type: token.TOKEN_NE, Literal: "<>", Pos: l.pos - 1}
			l.readChar()
		} else if l.peekChar() == '@' {
			// JSON contained by operator
			startPos := l.pos
			l.readChar() // consume <
			tok = token.Token{Type: token.TOKEN_LT_AT, Literal: "<@", Pos: startPos}
			l.readChar() // consume @
		} else {
			tok = token.Token{Type: token.TOKEN_LT, Literal: "<", Pos: l.pos}
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = token.Token{Type: token.TOKEN_GE, Literal: ">=", Pos: l.pos - 1}
			l.readChar()
		} else {
			tok = token.Token{Type: token.TOKEN_GT, Literal: ">", Pos: l.pos}
			l.readChar()
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			tok = token.Token{Type: token.TOKEN_NE, Literal: "!=", Pos: l.pos - 1}
			l.readChar()
		} else {
			tok = token.Token{Type: token.TOKEN_ILLEGAL, Literal: "!", Pos: l.pos}
			l.readChar()
		}
	case '$':
		tok.Pos = l.pos
		if l.peekChar() == '$' {
			// Dollar quoting $$...$$
			tok.Literal = l.readDollarQuote()
			tok.Type = token.TOKEN_DOLLAR_QUOTE
		} else {
			// Placeholder $1, $2, etc.
			tok.Literal = l.readPlaceholder()
			tok.Type = token.TOKEN_PLACEHOLDER
		}
		return tok
	case ':':
		tok.Pos = l.pos
		if l.peekChar() == '=' {
			l.readChar() // consume :
			tok = token.Token{Type: token.TOKEN_COLON_EQ, Literal: ":=", Pos: tok.Pos}
			l.readChar() // consume =
		} else {
			tok = token.Token{Type: token.TOKEN_ILLEGAL, Literal: ":", Pos: l.pos}
			l.readChar()
		}
		return tok
	case '\'':
		tok.Pos = l.pos
		tok.Literal = l.readString()
		tok.Type = token.TOKEN_STRING
		return tok
	default:
		if isLetter(l.ch) {
			tok.Pos = l.pos
			tok.Literal = l.readIdentifier()
			tok.Type = token.LookupKeyword(tok.Literal)
			return tok
		} else if isDigit(l.ch) || (l.ch == '-' && isDigit(l.peekChar())) {
			tok.Pos = l.pos
			tok.Literal = l.readNumber()
			tok.Type = token.TOKEN_INT
			return tok
		} else {
			tok = token.Token{Type: token.TOKEN_ILLEGAL, Literal: string(l.ch), Pos: l.pos}
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

func (l *Lexer) readPlaceholder() string {
	pos := l.pos
	l.readChar() // consume '$'
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

// readDollarQuote reads a dollar-quoted string $$content$$ and returns just the content.
func (l *Lexer) readDollarQuote() string {
	l.readChar() // consume first $
	l.readChar() // consume second $
	pos := l.pos
	// Read until we find closing $$ or EOF
	for l.ch != 0 && (l.ch != '$' || l.peekChar() != '$') {
		l.readChar()
	}
	content := l.input[pos:l.pos]
	if l.ch == '$' {
		l.readChar() // consume first closing $
		l.readChar() // consume second closing $
	}
	return content
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
