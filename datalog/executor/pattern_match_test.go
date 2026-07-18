package executor

import (
	"strings"
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
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: ageAttr, V: int64(30), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: friendAttr, V: bob, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: ageAttr, V: int64(25), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
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
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			expected: 5,
		},
		{
			name: "match specific entity",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: alice},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			expected: 3,
		},
		{
			name: "match specific attribute",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: nameAttr},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			expected: 2,
		},
		{
			name: "match specific value",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Constant{Value: "Alice"},
				},
			},
			expected: 1,
		},
		{
			name: "match entity reference value",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
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
					query.Variable{Name: datalog.NewSymbol("?age")},
				},
			},
			expected: 2,
		},
		{
			name: "match specific transaction",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
					query.Constant{Value: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
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
			results, err := matcher.Match(query.PatternQuery(tt.pattern), nil)
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
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?user")},
			query.Constant{Value: nameAttr},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Variable{Name: datalog.NewSymbol("?tx")},
		},
	}

	// Convert datom to relation using the pattern
	rel := PatternToRelation([]datalog.Datom{datom}, pattern)

	// Check relation has correct symbols
	symbols := rel.Symbols()
	if len(symbols) != 3 {
		t.Errorf("expected 3 symbols, got %d", len(symbols))
	}

	// Check symbol names
	expectedSymbols := []query.Symbol{datalog.NewSymbol("?user"), datalog.NewSymbol("?name"), datalog.NewSymbol("?tx")}
	for i, symbol := range symbols {
		if symbol != expectedSymbols[i] {
			t.Errorf("expected symbol %d to be %s, got %s", i, expectedSymbols[i], symbol)
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

	expectedTx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	if tuple[2] != expectedTx {
		t.Errorf("expected ?tx to be %v, got %v", expectedTx, tuple[2])
	}
}

func TestPatternToRelation(t *testing.T) {
	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: bob, A: nameAttr, V: "Bob", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	// Pattern: [?user :user/name ?name]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?user")},
			query.Constant{Value: nameAttr},
			query.Variable{Name: datalog.NewSymbol("?name")},
		},
	}

	rel := PatternToRelation(datoms, pattern)

	// Check symbols
	symbols := rel.Symbols()
	if len(symbols) != 2 {
		t.Errorf("expected 2 symbols, got %d", len(symbols))
	}
	if symbols[0] != datalog.NewSymbol("?user") || symbols[1] != datalog.NewSymbol("?name") {
		t.Errorf("unexpected symbols: %v", symbols)
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
	// String constants match keywords (the attribute convenience is retained),
	// but never identities: the entity position requires an Identity and a
	// string there is a loud query defect, not a convenience.
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Identity in entity position, string for the attribute keyword
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: alice},
			query.Constant{Value: ":user/name"}, // String instead of Keyword
			query.Variable{Name: datalog.NewSymbol("?name")},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
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
		t.Errorf("expected 1 match using string attribute constant, got %d", count)
	}

	// A string constant in the entity position is a validation error
	stringEPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: "user:alice"},
			query.Constant{Value: ":user/name"},
			query.Variable{Name: datalog.NewSymbol("?name")},
		},
	}

	if _, err := matcher.Match(query.PatternQuery(stringEPattern), nil); err == nil {
		t.Fatal("expected an error for a string constant in entity position, got none")
	} else if !strings.Contains(err.Error(), "entity position") {
		t.Errorf("error should name the entity position; got: %v", err)
	}
}
