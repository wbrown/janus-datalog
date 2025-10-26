package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPatternMatching(t *testing.T) {
	// Create test datoms
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	friendAttr := datalog.NewKeyword(":user/friend")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: friendAttr, V: bob, Tx: 2},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
		{E: bob, A: ageAttr, V: int64(25), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	tests := []struct {
		name     string
		pattern  *query.DataPattern
		expected int
	}{
		{
			name: "match all with variables",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
			expected: 5,
		},
		{
			name: "match specific entity",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: alice},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
			expected: 3,
		},
		{
			name: "match specific attribute",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: nameAttr},
					query.Variable{Name: "?v"},
				},
			},
			expected: 2,
		},
		{
			name: "match specific value",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Constant{Value: "Alice"},
				},
			},
			expected: 1,
		},
		{
			name: "match entity reference value",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Constant{Value: friendAttr},
					query.Constant{Value: bob},
				},
			},
			expected: 1,
		},
		{
			name: "match with blanks",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Blank{},
					query.Constant{Value: ageAttr},
					query.Variable{Name: "?age"},
				},
			},
			expected: 2,
		},
		{
			name: "match specific transaction",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?e"},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
					query.Constant{Value: uint64(2)},
				},
			},
			expected: 1,
		},
		{
			name: "no matches",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: alice},
					query.Constant{Value: nameAttr},
					query.Constant{Value: "Bob"}, // Alice's name is not Bob
				},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := matcher.Match(tt.pattern, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			count := 0
			var tuples []Tuple
			it := results.Iterator()
			for it.Next() {
				tuples = append(tuples, it.Tuple())
				count++
			}
			it.Close()

			if count != tt.expected {
				t.Errorf("expected %d matches, got %d", tt.expected, count)
				for _, tuple := range tuples {
					t.Logf("  %v", tuple)
				}
			}
		})
	}
}

func TestDatomToRelationExtraction(t *testing.T) {
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datom := datalog.Datom{
		E:  alice,
		A:  nameAttr,
		V:  "Alice",
		Tx: 1,
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?user"},
			query.Constant{Value: nameAttr},
			query.Variable{Name: "?name"},
			query.Variable{Name: "?tx"},
		},
	}

	// Convert datom to relation using the pattern
	rel := PatternToRelation([]datalog.Datom{datom}, pattern)

	// Check relation has correct columns
	columns := rel.Columns()
	if len(columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(columns))
	}

	// Check column names
	expectedCols := []query.Symbol{"?user", "?name", "?tx"}
	for i, col := range columns {
		if col != expectedCols[i] {
			t.Errorf("expected column %d to be %s, got %s", i, expectedCols[i], col)
		}
	}

	// Check tuple values
	it := rel.Iterator()
	if !it.Next() {
		t.Fatal("expected one tuple")
	}
	tuple := it.Tuple()
	it.Close()

	// Check specific values
	if !tuple[0].(datalog.Identity).Equal(alice) {
		t.Errorf("expected ?user to be alice")
	}

	if tuple[1] != "Alice" {
		t.Errorf("expected ?name to be 'Alice'")
	}

	if tuple[2] != uint64(1) {
		t.Errorf("expected ?tx to be 1")
	}
}

func TestPatternToRelation(t *testing.T) {
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: bob, A: nameAttr, V: "Bob", Tx: 1},
	}

	// Pattern: [?user :user/name ?name]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?user"},
			query.Constant{Value: nameAttr},
			query.Variable{Name: "?name"},
		},
	}

	rel := PatternToRelation(datoms, pattern)

	// Check columns
	cols := rel.Columns()
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "?user" || cols[1] != "?name" {
		t.Errorf("unexpected columns: %v", cols)
	}

	// Verify tuples contain correct data
	it := rel.Iterator()
	defer it.Close()

	count := 0
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) != 2 {
			t.Errorf("expected tuple length 2, got %d", len(tuple))
		}

		// First element should be an Identity
		if _, ok := tuple[0].(datalog.Identity); !ok {
			t.Errorf("expected Identity in position 0, got %T", tuple[0])
		}

		// Second element should be a string
		if _, ok := tuple[1].(string); !ok {
			t.Errorf("expected string in position 1, got %T", tuple[1])
		}

		count++
	}

	if count != 2 {
		t.Errorf("expected 2 tuples, got %d", count)
	}
}

func TestMatchWithStringConstants(t *testing.T) {
	// Test that we can use string constants for matching keywords and identities
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Match using string constants (convenience feature)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: "user:alice"}, // String instead of Identity
			query.Constant{Value: ":user/name"}, // String instead of Keyword
			query.Variable{Name: "?name"},
		},
	}

	results, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count := 0
	it := results.Iterator()
	for it.Next() {
		count++
	}
	it.Close()

	if count != 1 {
		t.Errorf("expected 1 match using string constants, got %d", count)
	}
}
