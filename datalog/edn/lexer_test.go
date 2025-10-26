package edn

import (
	"reflect"
	"testing"
)

func TestLexerBasic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:  "empty input",
			input: "",
			expected: []Token{
				{Type: TokenEOF, Line: 1, Col: 1},
			},
		},
		{
			name:  "whitespace only",
			input: "   \n  \t  ",
			expected: []Token{
				{Type: TokenEOF, Line: 2, Col: 6},
			},
		},
		{
			name:  "single atom",
			input: "hello",
			expected: []Token{
				{Type: TokenAtom, Value: "hello", Line: 1, Col: 1},
				{Type: TokenEOF, Line: 1, Col: 6},
			},
		},
		{
			name:  "multiple atoms",
			input: "foo bar baz",
			expected: []Token{
				{Type: TokenAtom, Value: "foo", Line: 1, Col: 1},
				{Type: TokenAtom, Value: "bar", Line: 1, Col: 5},
				{Type: TokenAtom, Value: "baz", Line: 1, Col: 9},
				{Type: TokenEOF, Line: 1, Col: 12},
			},
		},
		{
			name:  "string literal",
			input: `"hello world"`,
			expected: []Token{
				{Type: TokenString, Value: "hello world", Line: 1, Col: 1},
				{Type: TokenEOF, Line: 1, Col: 14},
			},
		},
		{
			name:  "string with escapes",
			input: `"hello\nworld\t\"quoted\""`,
			expected: []Token{
				{Type: TokenString, Value: "hello\nworld\t\"quoted\"", Line: 1, Col: 1},
				{Type: TokenEOF, Line: 1, Col: 27},
			},
		},
		{
			name:  "parentheses",
			input: "()",
			expected: []Token{
				{Type: TokenLeftParen, Line: 1, Col: 1},
				{Type: TokenRightParen, Line: 1, Col: 2},
				{Type: TokenEOF, Line: 1, Col: 3},
			},
		},
		{
			name:  "brackets",
			input: "[]",
			expected: []Token{
				{Type: TokenLeftBracket, Line: 1, Col: 1},
				{Type: TokenRightBracket, Line: 1, Col: 2},
				{Type: TokenEOF, Line: 1, Col: 3},
			},
		},
		{
			name:  "braces",
			input: "{}",
			expected: []Token{
				{Type: TokenLeftBrace, Line: 1, Col: 1},
				{Type: TokenRightBrace, Line: 1, Col: 2},
				{Type: TokenEOF, Line: 1, Col: 3},
			},
		},
		{
			name:  "nested structures",
			input: "[1 (2 3) {4 5}]",
			expected: []Token{
				{Type: TokenLeftBracket, Line: 1, Col: 1},
				{Type: TokenAtom, Value: "1", Line: 1, Col: 2},
				{Type: TokenLeftParen, Line: 1, Col: 4},
				{Type: TokenAtom, Value: "2", Line: 1, Col: 5},
				{Type: TokenAtom, Value: "3", Line: 1, Col: 7},
				{Type: TokenRightParen, Line: 1, Col: 8},
				{Type: TokenLeftBrace, Line: 1, Col: 10},
				{Type: TokenAtom, Value: "4", Line: 1, Col: 11},
				{Type: TokenAtom, Value: "5", Line: 1, Col: 13},
				{Type: TokenRightBrace, Line: 1, Col: 14},
				{Type: TokenRightBracket, Line: 1, Col: 15},
				{Type: TokenEOF, Line: 1, Col: 16},
			},
		},
		{
			name:  "comments",
			input: "foo ; this is a comment\nbar",
			expected: []Token{
				{Type: TokenAtom, Value: "foo", Line: 1, Col: 1},
				{Type: TokenAtom, Value: "bar", Line: 2, Col: 1},
				{Type: TokenEOF, Line: 2, Col: 4},
			},
		},
		{
			name:  "commas as whitespace",
			input: "foo, bar, baz",
			expected: []Token{
				{Type: TokenAtom, Value: "foo", Line: 1, Col: 1},
				{Type: TokenAtom, Value: "bar", Line: 1, Col: 6},
				{Type: TokenAtom, Value: "baz", Line: 1, Col: 11},
				{Type: TokenEOF, Line: 1, Col: 14},
			},
		},
		{
			name:  "keywords",
			input: ":foo :bar/baz",
			expected: []Token{
				{Type: TokenAtom, Value: ":foo", Line: 1, Col: 1},
				{Type: TokenAtom, Value: ":bar/baz", Line: 1, Col: 6},
				{Type: TokenEOF, Line: 1, Col: 14},
			},
		},
		{
			name:  "character literals",
			input: `\a \newline`,
			expected: []Token{
				{Type: TokenAtom, Value: `\a`, Line: 1, Col: 1},
				{Type: TokenAtom, Value: `\newline`, Line: 1, Col: 4},
				{Type: TokenEOF, Line: 1, Col: 12},
			},
		},
		{
			name:  "tagged values",
			input: "#inst \"2024-01-01\"",
			expected: []Token{
				{Type: TokenAtom, Value: "#inst", Line: 1, Col: 1},
				{Type: TokenString, Value: "2024-01-01", Line: 1, Col: 7},
				{Type: TokenEOF, Line: 1, Col: 19},
			},
		},
		{
			name:  "sets",
			input: "#{1 2 3}",
			expected: []Token{
				{Type: TokenAtom, Value: "#{", Line: 1, Col: 1},
				{Type: TokenAtom, Value: "1", Line: 1, Col: 3},
				{Type: TokenAtom, Value: "2", Line: 1, Col: 5},
				{Type: TokenAtom, Value: "3", Line: 1, Col: 7},
				{Type: TokenRightBrace, Line: 1, Col: 8},
				{Type: TokenEOF, Line: 1, Col: 9},
			},
		},
		{
			name:  "discard",
			input: "#_ foo bar",
			expected: []Token{
				{Type: TokenAtom, Value: "#_", Line: 1, Col: 1},
				{Type: TokenAtom, Value: "foo", Line: 1, Col: 4},
				{Type: TokenAtom, Value: "bar", Line: 1, Col: 8},
				{Type: TokenEOF, Line: 1, Col: 11},
			},
		},
		{
			name:  "multiline",
			input: "foo\nbar\nbaz",
			expected: []Token{
				{Type: TokenAtom, Value: "foo", Line: 1, Col: 1},
				{Type: TokenAtom, Value: "bar", Line: 2, Col: 1},
				{Type: TokenAtom, Value: "baz", Line: 3, Col: 1},
				{Type: TokenEOF, Line: 3, Col: 4},
			},
		},
		{
			name: "complex query",
			input: `[:find ?e ?name
 :where [?e :person/name ?name]]`,
			expected: []Token{
				{Type: TokenLeftBracket, Line: 1, Col: 1},
				{Type: TokenAtom, Value: ":find", Line: 1, Col: 2},
				{Type: TokenAtom, Value: "?e", Line: 1, Col: 8},
				{Type: TokenAtom, Value: "?name", Line: 1, Col: 11},
				{Type: TokenAtom, Value: ":where", Line: 2, Col: 2},
				{Type: TokenLeftBracket, Line: 2, Col: 9},
				{Type: TokenAtom, Value: "?e", Line: 2, Col: 10},
				{Type: TokenAtom, Value: ":person/name", Line: 2, Col: 13},
				{Type: TokenAtom, Value: "?name", Line: 2, Col: 26},
				{Type: TokenRightBracket, Line: 2, Col: 31},
				{Type: TokenRightBracket, Line: 2, Col: 32},
				{Type: TokenEOF, Line: 2, Col: 33},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			err := lexer.Lex()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(lexer.tokens, tt.expected) {
				t.Errorf("tokens mismatch\ngot:  %v\nwant: %v", formatTokens(lexer.tokens), formatTokens(tt.expected))
			}
		})
	}
}

func TestLexerErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name:  "unterminated string",
			input: `"hello`,
			error: "unterminated string",
		},
		{
			name:  "invalid escape",
			input: `"hello\x"`,
			error: "invalid escape sequence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			err := lexer.Lex()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.error)
			}
			if !contains(err.Error(), tt.error) {
				t.Errorf("expected error containing %q, got %q", tt.error, err.Error())
			}
		})
	}
}

func formatTokens(tokens []Token) string {
	var result string
	for _, tok := range tokens {
		result += tok.String() + "\n"
	}
	return result
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr || len(s) > len(substr) && contains(s[1:], substr)
}
