package edn

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes EDN input
type Lexer struct {
	input   string
	pos     int
	line    int
	col     int
	tokens  []Token
	current int
}

// NewLexer creates a new lexer for the given input
func NewLexer(input string) *Lexer {
	return &Lexer{
		input:   input,
		pos:     0,
		line:    1,
		col:     1,
		tokens:  []Token{},
		current: 0,
	}
}

// Lex tokenizes the entire input
func (l *Lexer) Lex() error {
	for l.pos < len(l.input) {
		l.skipWhitespaceAndComments()
		if l.pos >= len(l.input) {
			break
		}

		startLine := l.line
		startCol := l.col

		ch := l.peek()
		switch ch {
		case '"':
			str, err := l.readString()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, Token{
				Type:  TokenString,
				Value: str,
				Line:  startLine,
				Col:   startCol,
			})
		case '(':
			l.advance()
			l.tokens = append(l.tokens, Token{
				Type: TokenLeftParen,
				Line: startLine,
				Col:  startCol,
			})
		case ')':
			l.advance()
			l.tokens = append(l.tokens, Token{
				Type: TokenRightParen,
				Line: startLine,
				Col:  startCol,
			})
		case '[':
			l.advance()
			l.tokens = append(l.tokens, Token{
				Type: TokenLeftBracket,
				Line: startLine,
				Col:  startCol,
			})
		case ']':
			l.advance()
			l.tokens = append(l.tokens, Token{
				Type: TokenRightBracket,
				Line: startLine,
				Col:  startCol,
			})
		case '{':
			l.advance()
			l.tokens = append(l.tokens, Token{
				Type: TokenLeftBrace,
				Line: startLine,
				Col:  startCol,
			})
		case '}':
			l.advance()
			l.tokens = append(l.tokens, Token{
				Type: TokenRightBrace,
				Line: startLine,
				Col:  startCol,
			})
		default:
			atom := l.readAtom()
			if atom != "" {
				l.tokens = append(l.tokens, Token{
					Type:  TokenAtom,
					Value: atom,
					Line:  startLine,
					Col:   startCol,
				})
			} else {
				return fmt.Errorf("unexpected character '%c' at %d:%d", ch, l.line, l.col)
			}
		}
	}

	// Add EOF token
	l.tokens = append(l.tokens, Token{
		Type: TokenEOF,
		Line: l.line,
		Col:  l.col,
	})

	return nil
}

// NextToken returns the next token
func (l *Lexer) NextToken() Token {
	if l.current >= len(l.tokens) {
		return Token{Type: TokenEOF, Line: l.line, Col: l.col}
	}
	token := l.tokens[l.current]
	l.current++
	return token
}

// PeekToken returns the next token without advancing
func (l *Lexer) PeekToken() Token {
	if l.current >= len(l.tokens) {
		return Token{Type: TokenEOF, Line: l.line, Col: l.col}
	}
	return l.tokens[l.current]
}

// peek returns the current character without advancing
func (l *Lexer) peek() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

// advance moves to the next character
func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// skipWhitespaceAndComments skips whitespace and comments
func (l *Lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.input) {
		ch := l.peek()
		if unicode.IsSpace(rune(ch)) || ch == ',' {
			l.advance()
		} else if ch == ';' {
			// Skip comment until end of line
			for l.pos < len(l.input) && l.peek() != '\n' {
				l.advance()
			}
		} else {
			break
		}
	}
}

// readString reads a string literal
func (l *Lexer) readString() (string, error) {
	var result strings.Builder
	l.advance() // skip opening quote

	for l.pos < len(l.input) {
		ch := l.peek()
		if ch == '"' {
			l.advance() // skip closing quote
			return result.String(), nil
		} else if ch == '\\' {
			l.advance()
			if l.pos >= len(l.input) {
				return "", fmt.Errorf("unexpected end of input in string at %d:%d", l.line, l.col)
			}
			escaped := l.peek()
			switch escaped {
			case 't':
				result.WriteByte('\t')
			case 'r':
				result.WriteByte('\r')
			case 'n':
				result.WriteByte('\n')
			case '\\':
				result.WriteByte('\\')
			case '"':
				result.WriteByte('"')
			default:
				return "", fmt.Errorf("invalid escape sequence '\\%c' at %d:%d", escaped, l.line, l.col)
			}
			l.advance()
		} else {
			result.WriteByte(ch)
			l.advance()
		}
	}

	return "", fmt.Errorf("unterminated string at %d:%d", l.line, l.col)
}

// readAtom reads an atom (non-string, non-delimiter token)
func (l *Lexer) readAtom() string {
	var result strings.Builder
	start := l.pos

	// Special case for #{
	if l.pos+1 < len(l.input) && l.input[l.pos] == '#' && l.input[l.pos+1] == '{' {
		l.advance() // skip #
		l.advance() // skip {
		return "#{"
	}

	for l.pos < len(l.input) {
		ch := l.peek()
		if isDelimiter(ch) || unicode.IsSpace(rune(ch)) || ch == ',' {
			break
		}
		result.WriteByte(ch)
		l.advance()
	}

	// Handle special case for character literals
	if l.pos-start > 1 && l.input[start] == '\\' {
		// Character literal
		return result.String()
	}

	return result.String()
}

// isDelimiter checks if a character is a delimiter
func isDelimiter(ch byte) bool {
	return ch == '(' || ch == ')' || ch == '[' || ch == ']' || ch == '{' || ch == '}' || ch == '"' || ch == ';'
}
