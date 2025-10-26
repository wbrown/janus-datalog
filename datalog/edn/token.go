package edn

import "fmt"

// TokenType represents the type of EDN token
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenString
	TokenAtom
	TokenLeftParen
	TokenRightParen
	TokenLeftBracket
	TokenRightBracket
	TokenLeftBrace
	TokenRightBrace
)

// Token represents a lexical token in EDN
type Token struct {
	Type  TokenType
	Value string
	Line  int
	Col   int
}

// String returns a string representation of the token
func (t Token) String() string {
	switch t.Type {
	case TokenEOF:
		return fmt.Sprintf("EOF[%d:%d]", t.Line, t.Col)
	case TokenString:
		return fmt.Sprintf("String[%d:%d]:%q", t.Line, t.Col, t.Value)
	case TokenAtom:
		return fmt.Sprintf("Atom[%d:%d]:%s", t.Line, t.Col, t.Value)
	case TokenLeftParen:
		return fmt.Sprintf("LeftParen[%d:%d]", t.Line, t.Col)
	case TokenRightParen:
		return fmt.Sprintf("RightParen[%d:%d]", t.Line, t.Col)
	case TokenLeftBracket:
		return fmt.Sprintf("LeftBracket[%d:%d]", t.Line, t.Col)
	case TokenRightBracket:
		return fmt.Sprintf("RightBracket[%d:%d]", t.Line, t.Col)
	case TokenLeftBrace:
		return fmt.Sprintf("LeftBrace[%d:%d]", t.Line, t.Col)
	case TokenRightBrace:
		return fmt.Sprintf("RightBrace[%d:%d]", t.Line, t.Col)
	default:
		return fmt.Sprintf("Unknown[%d:%d]:%s", t.Line, t.Col, t.Value)
	}
}
