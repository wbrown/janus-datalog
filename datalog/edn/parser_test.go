package edn

import (
	"reflect"
	"testing"
)

func TestParserAtoms(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Node
	}{
		{
			name:     "nil",
			input:    "nil",
			expected: Node{Type: NodeNil, Line: 1, Col: 1},
		},
		{
			name:     "true",
			input:    "true",
			expected: Node{Type: NodeBool, Value: "true", Line: 1, Col: 1},
		},
		{
			name:     "false",
			input:    "false",
			expected: Node{Type: NodeBool, Value: "false", Line: 1, Col: 1},
		},
		{
			name:     "integer",
			input:    "42",
			expected: Node{Type: NodeInt, Value: "42", Line: 1, Col: 1},
		},
		{
			name:     "negative integer",
			input:    "-42",
			expected: Node{Type: NodeInt, Value: "-42", Line: 1, Col: 1},
		},
		{
			name:     "integer with suffix",
			input:    "42N",
			expected: Node{Type: NodeInt, Value: "42N", Line: 1, Col: 1},
		},
		{
			name:     "float",
			input:    "3.14",
			expected: Node{Type: NodeFloat, Value: "3.14", Line: 1, Col: 1},
		},
		{
			name:     "scientific notation",
			input:    "1.23e-4",
			expected: Node{Type: NodeFloat, Value: "1.23e-4", Line: 1, Col: 1},
		},
		{
			name:     "string",
			input:    `"hello world"`,
			expected: Node{Type: NodeString, Value: "hello world", Line: 1, Col: 1},
		},
		{
			name:     "string with escapes",
			input:    `"line1\nline2"`,
			expected: Node{Type: NodeString, Value: "line1\nline2", Line: 1, Col: 1},
		},
		{
			name:     "character",
			input:    `\a`,
			expected: Node{Type: NodeChar, Value: `\a`, Line: 1, Col: 1},
		},
		{
			name:     "newline character",
			input:    `\newline`,
			expected: Node{Type: NodeChar, Value: `\n`, Line: 1, Col: 1},
		},
		{
			name:     "symbol",
			input:    "foo",
			expected: Node{Type: NodeSymbol, Value: "foo", Line: 1, Col: 1},
		},
		{
			name:     "symbol with special chars",
			input:    "foo-bar*",
			expected: Node{Type: NodeSymbol, Value: "foo-bar*", Line: 1, Col: 1},
		},
		{
			name:     "variable symbol",
			input:    "?x",
			expected: Node{Type: NodeSymbol, Value: "?x", Line: 1, Col: 1},
		},
		{
			name:     "keyword",
			input:    ":foo",
			expected: Node{Type: NodeKeyword, Value: ":foo", Line: 1, Col: 1},
		},
		{
			name:     "namespaced keyword",
			input:    ":foo/bar",
			expected: Node{Type: NodeKeyword, Value: ":foo/bar", Line: 1, Col: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(*node, tt.expected) {
				t.Errorf("node mismatch\ngot:  %+v\nwant: %+v", *node, tt.expected)
			}
		})
	}
}

func TestParserCollections(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Node
	}{
		{
			name:  "empty list",
			input: "()",
			expected: Node{
				Type:  NodeList,
				Nodes: []Node{},
				Line:  1,
				Col:   1,
			},
		},
		{
			name:  "list with atoms",
			input: "(1 2 3)",
			expected: Node{
				Type: NodeList,
				Nodes: []Node{
					{Type: NodeInt, Value: "1", Line: 1, Col: 2},
					{Type: NodeInt, Value: "2", Line: 1, Col: 4},
					{Type: NodeInt, Value: "3", Line: 1, Col: 6},
				},
				Line: 1,
				Col:  1,
			},
		},
		{
			name:  "empty vector",
			input: "[]",
			expected: Node{
				Type:  NodeVector,
				Nodes: []Node{},
				Line:  1,
				Col:   1,
			},
		},
		{
			name:  "vector with mixed types",
			input: "[1 :foo \"bar\"]",
			expected: Node{
				Type: NodeVector,
				Nodes: []Node{
					{Type: NodeInt, Value: "1", Line: 1, Col: 2},
					{Type: NodeKeyword, Value: ":foo", Line: 1, Col: 4},
					{Type: NodeString, Value: "bar", Line: 1, Col: 9},
				},
				Line: 1,
				Col:  1,
			},
		},
		{
			name:  "empty map",
			input: "{}",
			expected: Node{
				Type:  NodeMap,
				Nodes: []Node{},
				Line:  1,
				Col:   1,
			},
		},
		{
			name:  "map with key-value pairs",
			input: "{:a 1 :b 2}",
			expected: Node{
				Type: NodeMap,
				Nodes: []Node{
					{Type: NodeKeyword, Value: ":a", Line: 1, Col: 2},
					{Type: NodeInt, Value: "1", Line: 1, Col: 5},
					{Type: NodeKeyword, Value: ":b", Line: 1, Col: 7},
					{Type: NodeInt, Value: "2", Line: 1, Col: 10},
				},
				Line: 1,
				Col:  1,
			},
		},
		{
			name:  "set",
			input: "#{1 2 3}",
			expected: Node{
				Type: NodeSet,
				Nodes: []Node{
					{Type: NodeInt, Value: "1", Line: 1, Col: 3},
					{Type: NodeInt, Value: "2", Line: 1, Col: 5},
					{Type: NodeInt, Value: "3", Line: 1, Col: 7},
				},
				Line: 1,
				Col:  1,
			},
		},
		{
			name:  "nested collections",
			input: "[1 (2 3) {:a 4}]",
			expected: Node{
				Type: NodeVector,
				Nodes: []Node{
					{Type: NodeInt, Value: "1", Line: 1, Col: 2},
					{
						Type: NodeList,
						Nodes: []Node{
							{Type: NodeInt, Value: "2", Line: 1, Col: 5},
							{Type: NodeInt, Value: "3", Line: 1, Col: 7},
						},
						Line: 1,
						Col:  4,
					},
					{
						Type: NodeMap,
						Nodes: []Node{
							{Type: NodeKeyword, Value: ":a", Line: 1, Col: 11},
							{Type: NodeInt, Value: "4", Line: 1, Col: 14},
						},
						Line: 1,
						Col:  10,
					},
				},
				Line: 1,
				Col:  1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !nodesEqual(*node, tt.expected) {
				t.Errorf("node mismatch\ngot:  %s\nwant: %s", nodeToString(*node), nodeToString(tt.expected))
			}
		})
	}
}

func TestParserSpecialForms(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *Node
	}{
		{
			name:  "tagged value",
			input: `#inst "2024-01-01"`,
			expected: &Node{
				Type: NodeTagged,
				Tag:  "inst",
				Tagged: &Node{
					Type:  NodeString,
					Value: "2024-01-01",
					Line:  1,
					Col:   7,
				},
				Line: 1,
				Col:  1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !nodesEqual(*node, *tt.expected) {
				t.Errorf("node mismatch\ngot:  %s\nwant: %s", nodeToString(*node), nodeToString(*tt.expected))
			}
		})
	}
}

func TestParserDatalogQuery(t *testing.T) {
	input := `[:find ?e ?name
 :where [?e :person/name ?name]
        [?e :person/age ?age]
        [(> ?age 21)]]`

	expected := Node{
		Type: NodeVector,
		Nodes: []Node{
			{Type: NodeKeyword, Value: ":find", Line: 1, Col: 2},
			{Type: NodeSymbol, Value: "?e", Line: 1, Col: 8},
			{Type: NodeSymbol, Value: "?name", Line: 1, Col: 11},
			{Type: NodeKeyword, Value: ":where", Line: 2, Col: 2},
			{
				Type: NodeVector,
				Nodes: []Node{
					{Type: NodeSymbol, Value: "?e", Line: 2, Col: 10},
					{Type: NodeKeyword, Value: ":person/name", Line: 2, Col: 13},
					{Type: NodeSymbol, Value: "?name", Line: 2, Col: 26},
				},
				Line: 2,
				Col:  9,
			},
			{
				Type: NodeVector,
				Nodes: []Node{
					{Type: NodeSymbol, Value: "?e", Line: 3, Col: 10},
					{Type: NodeKeyword, Value: ":person/age", Line: 3, Col: 13},
					{Type: NodeSymbol, Value: "?age", Line: 3, Col: 25},
				},
				Line: 3,
				Col:  9,
			},
			{
				Type: NodeVector,
				Nodes: []Node{
					{
						Type: NodeList,
						Nodes: []Node{
							{Type: NodeSymbol, Value: ">", Line: 4, Col: 11},
							{Type: NodeSymbol, Value: "?age", Line: 4, Col: 13},
							{Type: NodeInt, Value: "21", Line: 4, Col: 19},
						},
						Line: 4,
						Col:  10,
					},
				},
				Line: 4,
				Col:  9,
			},
		},
		Line: 1,
		Col:  1,
	}

	node, err := Parse(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !nodesEqual(*node, expected) {
		t.Errorf("node mismatch\ngot:  %s\nwant: %s", nodeToString(*node), nodeToString(expected))
	}
}

func TestParserErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name:  "invalid keyword",
			input: ":123",
			error: "symbol cannot start with digit",
		},
		{
			name:  "invalid symbol",
			input: "123abc",
			error: "symbol cannot start with digit",
		},
		{
			name:  "unterminated list",
			input: "(1 2 3",
			error: "unterminated list",
		},
		{
			name:  "unterminated vector",
			input: "[1 2 3",
			error: "unterminated vector",
		},
		{
			name:  "unterminated map",
			input: "{:a 1",
			error: "unterminated map",
		},
		{
			name:  "odd map elements",
			input: "{:a}",
			error: "map missing value",
		},
		{
			name:  "invalid character literal",
			input: `\invalid`,
			error: "invalid character literal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.error)
			}
			if !contains(err.Error(), tt.error) {
				t.Errorf("expected error containing %q, got %q", tt.error, err.Error())
			}
		})
	}
}

func TestParseAll(t *testing.T) {
	input := `1 2 3
:foo
"bar"`

	lexer := NewLexer(input)
	if err := lexer.Lex(); err != nil {
		t.Fatalf("unexpected lex error: %v", err)
	}

	parser := NewParser(lexer)
	nodes, err := parser.ParseAll()
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	expected := []Node{
		{Type: NodeInt, Value: "1", Line: 1, Col: 1},
		{Type: NodeInt, Value: "2", Line: 1, Col: 3},
		{Type: NodeInt, Value: "3", Line: 1, Col: 5},
		{Type: NodeKeyword, Value: ":foo", Line: 2, Col: 1},
		{Type: NodeString, Value: "bar", Line: 3, Col: 1},
	}

	if len(nodes) != len(expected) {
		t.Errorf("expected %d nodes, got %d", len(expected), len(nodes))
	}

	for i, node := range nodes {
		if !nodesEqual(node, expected[i]) {
			t.Errorf("node %d mismatch\ngot:  %+v\nwant: %+v", i, node, expected[i])
		}
	}
}

// Helper functions

func nodesEqual(a, b Node) bool {
	if a.Type != b.Type || a.Value != b.Value || a.Tag != b.Tag {
		return false
	}

	if len(a.Nodes) != len(b.Nodes) {
		return false
	}

	for i := range a.Nodes {
		if !nodesEqual(a.Nodes[i], b.Nodes[i]) {
			return false
		}
	}

	if (a.Tagged == nil) != (b.Tagged == nil) {
		return false
	}

	if a.Tagged != nil && !nodesEqual(*a.Tagged, *b.Tagged) {
		return false
	}

	return true
}

func nodeToString(n Node) string {
	return n.String()
}
