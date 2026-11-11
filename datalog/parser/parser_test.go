package parser

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseSimpleQuery(t *testing.T) {
	input := `[:find ?e ?name
              :where [?e :person/name ?name]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check find clause
	if len(q.Find) != 2 {
		t.Errorf("Expected 2 find elements, got %d", len(q.Find))
	}

	// Check each find element
	for i, elem := range q.Find {
		v, ok := elem.(query.FindVariable)
		if !ok {
			t.Errorf("Find element %d is not a FindVariable", i)
			continue
		}

		switch i {
		case 0:
			if v.Symbol != datalog.NewSymbol("?e") {
				t.Errorf("Find[0]: expected ?e, got %s", v.Symbol)
			}
		case 1:
			if v.Symbol != datalog.NewSymbol("?name") {
				t.Errorf("Find[1]: expected ?name, got %s", v.Symbol)
			}
		}
	}

	// Check where clause
	if len(q.Where) != 1 {
		t.Fatalf("expected 1 pattern, got %d", len(q.Where))
	}

	pattern := q.Where[0].(*query.DataPattern)
	if len(pattern.Elements) != 3 {
		t.Fatalf("expected 3 elements in pattern, got %d", len(pattern.Elements))
	}

	// Check pattern elements
	if !pattern.Elements[0].IsVariable() {
		t.Errorf("expected first element to be variable")
	}
	if pattern.Elements[0].String() != "?e" {
		t.Errorf("expected first element to be ?e, got %s", pattern.Elements[0].String())
	}

	// Check attribute
	if pattern.Elements[1].IsVariable() || pattern.Elements[1].IsBlank() {
		t.Errorf("expected second element to be constant")
	}

	// Check third element
	if !pattern.Elements[2].IsVariable() {
		t.Errorf("expected third element to be variable")
	}
	if pattern.Elements[2].String() != "?name" {
		t.Errorf("expected third element to be ?name, got %s", pattern.Elements[2].String())
	}
}

func TestParseComplexQuery(t *testing.T) {
	input := `[:find ?e ?name ?age
              :where [?e :person/name ?name]
                     [?e :person/age ?age]
                     [?e :person/active true]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check find clause
	if len(q.Find) != 3 {
		t.Errorf("expected 3 find variables, got %d", len(q.Find))
	}

	// Check where clause
	if len(q.Where) != 3 {
		t.Errorf("expected 3 patterns, got %d", len(q.Where))
	}

	// Check last pattern has boolean value
	lastPattern := q.Where[2].(*query.DataPattern)
	if lastPattern.Elements[2].IsVariable() {
		t.Errorf("expected constant boolean value")
	}
}

func TestParseWithBlank(t *testing.T) {
	input := `[:find ?name
              :where [_ :person/name ?name]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pattern := q.Where[0].(*query.DataPattern)
	if !pattern.Elements[0].IsBlank() {
		t.Errorf("expected blank, got %v", pattern.Elements[0])
	}
}

func TestParseWithLiterals(t *testing.T) {
	tests := []struct {
		name  string
		input string
		value query.PatternElement // the pattern's value position
	}{
		{
			name: "string literal",
			input: `[:find ?e
                     :where [?e :person/name "Alice"]]`,
			value: query.Constant{Value: "Alice"},
		},
		{
			name: "integer literal",
			input: `[:find ?e
                     :where [?e :person/age 42]]`,
			value: query.Constant{Value: int64(42)},
		},
		{
			name: "float literal",
			input: `[:find ?e
                     :where [?e :score/value 3.14]]`,
			value: query.Constant{Value: 3.14},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			require.NoError(t, err)
			require.Len(t, q.Where, 1)
			pattern, ok := q.Where[0].(*query.DataPattern)
			require.Truef(t, ok, "the clause is a data pattern, got %T", q.Where[0])
			assert.Equal(t, tt.value, pattern.Elements[2])
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name:  "not a vector",
			input: `(:find ?e)`,
			error: "query must be a vector",
		},
		{
			name:  "missing find",
			input: `[:where [?e :foo ?v]]`,
			error: "query must have at least one find variable",
		},
		{
			name:  "missing where",
			input: `[:find ?e]`,
			error: "query must have at least one where pattern",
		},
		{
			name:  "non-variable in find",
			input: `[:find foo :where [?e :bar ?v]]`,
			error: "find clause must contain variables",
		},
		{
			name:  "invalid pattern length",
			input: `[:find ?e :where [?e ?a ?v ?tx ?added ?extra]]`,
			error: "data pattern must have 1 to 5 elements, got 6",
		},
		{
			name:  "non-vector pattern",
			input: `[:find ?e :where (?e :foo ?v)]`,
			error: "rule invocation name must be a symbol, not the variable ?e",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseQuery(tt.input)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.error)
			}
			if !contains(err.Error(), tt.error) {
				t.Errorf("expected error containing %q, got %q", tt.error, err.Error())
			}
		})
	}
}

// TestAFiveElementPatternParsesRendersAndParsesBack pins that a data pattern
// of five elements, the fifth whether the datom asserts, parses with each
// element at its position, renders, and parses back to the same clauses,
// whether the fifth is a variable or the constant true or false.
func TestAFiveElementPatternParsesRendersAndParsesBack(t *testing.T) {
	for _, fifth := range []struct {
		text    string
		element query.PatternElement
	}{
		{"?added", query.Variable{Name: datalog.NewSymbol("?added")}},
		{"true", query.Constant{Value: true}},
		{"false", query.Constant{Value: false}},
	} {
		t.Run(fifth.text, func(t *testing.T) {
			q, err := ParseQuery(`[:find ?e ?v :where [?e :w/one ?v ?tx ` + fifth.text + `]]`)
			require.NoError(t, err)
			require.Len(t, q.Where, 1)
			pattern, ok := q.Where[0].(*query.DataPattern)
			require.Truef(t, ok, "the clause is a data pattern, got %T", q.Where[0])
			require.Len(t, pattern.Elements, 5)
			assert.Equal(t, fifth.element, pattern.Elements[4], "the fifth element")

			back, err := ParseQuery(q.String())
			require.NoError(t, err, q.String())
			assert.Equal(t, q.Where, back.Where, "the rendering parses back to the same clauses")
		})
	}
}

func TestQueryString(t *testing.T) {
	input := `[:find ?e ?name :where [?e :person/name ?name]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	formatted := q.String()

	// Parse the formatted output to ensure it's valid
	q2, err := ParseQuery(formatted)
	if err != nil {
		t.Fatalf("formatted query failed to parse: %v\nformatted: %s", err, formatted)
	}

	// Compare the queries
	if !reflect.DeepEqual(q.Find, q2.Find) {
		t.Errorf("find clauses don't match after formatting")
	}
	if len(q.Where) != len(q2.Where) {
		t.Errorf("where clauses don't match after formatting")
	}
}

func TestParseSubqueryPatterns(t *testing.T) {
	tests := []struct {
		name  string
		input string
		// where is the number of :where clauses and at the subquery's place
		// among them; find is the inner query's :find, inputs what the call
		// passes, binding the form its result binds under.
		where   int
		at      int
		find    []query.FindElement
		inputs  []query.PatternElement
		binding query.BindingForm
	}{
		{
			name: "subquery bound by Datomic's relation form [[?high]]",
			input: `[:find ?date ?high
			         :where
			           [?s :symbol/ticker "AAPL"]
			           [(ground "2025-06-02") ?date]
			           [(q [:find (max ?h)
			                :where [?p :price/symbol ?symbol]
			                       [?p :price/high ?h]]
			               ?s) [[?high]]]]`,
			where:   3,
			at:      2,
			find:    []query.FindElement{query.FindAggregate{Function: datalog.SymMax, Arg: datalog.NewSymbol("?h")}},
			inputs:  []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?s")}},
			binding: query.RelationBinding{Variables: []query.Symbol{datalog.NewSymbol("?high")}},
		},
		{
			// ?var is Datomic's scalar binding.
			name: "subquery with scalar binding",
			input: `[:find ?symbol ?max-price
			         :where
			           [?s :symbol/ticker ?symbol]
			           [(q [:find (max ?price)
			                :where [?p :price/symbol ?sym]
			                       [?p :price/value ?price]]
			               ?s) ?max-price]]`,
			where:   2,
			at:      1,
			find:    []query.FindElement{query.FindAggregate{Function: datalog.SymMax, Arg: datalog.NewSymbol("?price")}},
			inputs:  []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?s")}},
			binding: query.ScalarBinding{Variable: datalog.NewSymbol("?max-price")},
		},
		{
			name: "subquery with collection binding",
			input: `[:find ?symbol ?prices
			         :where
			           [?s :symbol/ticker ?symbol]
			           [(q [:find ?price
			                :where [?p :price/symbol ?sym]
			                       [?p :price/value ?price]]
			               ?s) [?prices ...]]]`,
			where:   2,
			at:      1,
			find:    []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?price")}},
			inputs:  []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?s")}},
			binding: query.CollectionBinding{Variable: datalog.NewSymbol("?prices")},
		},
		{
			name: "subquery with relation binding",
			input: `[:find ?symbol ?data
			         :where
			           [?s :symbol/ticker ?symbol]
			           [(q [:find ?price ?time
			                :where [?p :price/symbol ?sym]
			                       [?p :price/value ?price]
			                       [?p :price/time ?time]]
			               ?s) [[?price ?time] ...]]]`,
			where: 2,
			at:    1,
			find: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?price")},
				query.FindVariable{Symbol: datalog.NewSymbol("?time")},
			},
			inputs:  []query.PatternElement{query.Variable{Name: datalog.NewSymbol("?s")}},
			binding: query.RelationBinding{Variables: []query.Symbol{datalog.NewSymbol("?price"), datalog.NewSymbol("?time")}},
		},
		{
			// The source, a constant holding its symbol, and the two
			// variables, one input per :in position.
			name: "subquery with multiple inputs",
			input: `[:find ?result
			         :where
			           [?s :symbol/ticker "AAPL"]
			           [(ground "2025-06-02") ?date]
			           [(q [:find (avg ?v)
			                :in $ ?sym ?date
			                :where [?p :price/symbol ?sym]
			                       [?p :price/time ?t]
			                       [(= ?t ?date)]
			                       [?p :price/value ?v]]
			               $ ?s ?date) [[?result]]]]`,
			where: 3,
			at:    2,
			find:  []query.FindElement{query.FindAggregate{Function: datalog.SymAvg, Arg: datalog.NewSymbol("?v")}},
			inputs: []query.PatternElement{
				query.Constant{Value: datalog.NewSymbol("$")},
				query.Variable{Name: datalog.NewSymbol("?s")},
				query.Variable{Name: datalog.NewSymbol("?date")},
			},
			binding: query.RelationBinding{Variables: []query.Symbol{datalog.NewSymbol("?result")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			require.NoError(t, err)
			require.Len(t, q.Where, tt.where)
			subq, ok := q.Where[tt.at].(*query.SubqueryPattern)
			require.Truef(t, ok, "clause %d is a subquery, got %T", tt.at, q.Where[tt.at])
			assert.Equal(t, tt.find, subq.Query.Find, "the inner query's :find")
			assert.Equal(t, tt.inputs, subq.Inputs, "the inputs the call passes")
			assert.Equal(t, tt.binding, subq.Binding, "the form the result binds under")
		})
	}
}

func TestParseSubqueryErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name: "missing binding",
			input: `[:find ?x
			         :where [(q [:find ?y :where [?e :foo ?y]])]]`,
			error: "subquery pattern must have exactly 2 elements",
		},
		{
			name: "invalid binding form",
			input: `[:find ?x
			         :where [(q [:find ?y :where [?e :foo ?y]]) 123]]`,
			error: "binding form must be a symbol or vector",
		},
		{
			name: "empty relation binding",
			input: `[:find ?x
			         :where [(q [:find ?y :where [?e :foo ?y]]) [[]]]]`,
			error: "relation binding cannot be empty",
		},
		{
			name: "non-variable in relation binding",
			input: `[:find ?x
			         :where [(q [:find ?y :where [?e :foo ?y]]) [[foo]]]]`,
			error: "relation binding element 0 must be a variable",
		},
		{
			name: "invalid nested query",
			input: `[:find ?x
			         :where [(q (:find ?y) ?s) [[?x]]]]`,
			error: "query form must be a vector",
		},
		{
			name: "missing q symbol",
			input: `[:find ?x
			         :where [(foo [:find ?y :where [?e :foo ?y]] ?s) ?x]]`,
			error: "unsupported function: foo",
		},
		{
			name: "a non-vector constant at a collection parameter",
			input: `[:find ?x
			         :where [(q [:find ?y :in $ [?v ...] :where [?e :foo ?v] [?e :bar ?y]] $ "x") [[?x]]]]`,
			error: `argument 2 is "x", and the collection parameter [?v ...] takes a vector`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseQuery(tt.input)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.error)
			}
			if !contains(err.Error(), tt.error) {
				t.Errorf("expected error containing %q, got %q", tt.error, err.Error())
			}
		})
	}
}

func TestFormatSubquery(t *testing.T) {
	input := `[:find ?date ?high
	           :where 
	             [?s :symbol/ticker "AAPL"]
	             [(q [:find (max ?h)
	                  :where [?p :price/symbol ?symbol]
	                         [?p :price/high ?h]]
	                 ?s) [[?high]]]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	formatted := q.String()

	// Parse the formatted output to ensure it's valid
	q2, err := ParseQuery(formatted)
	if err != nil {
		t.Fatalf("formatted query failed to parse: %v\nformatted: %s", err, formatted)
	}

	// Basic structure check
	if len(q2.Where) != 2 {
		t.Errorf("expected 2 patterns after formatting, got %d", len(q2.Where))
	}

	// Check subquery pattern preserved
	_, ok := q2.Where[1].(*query.SubqueryPattern)
	if !ok {
		t.Errorf("expected SubqueryPattern in formatted query")
	}
}

// A relation binding has one rendering, [[?a ?b] ...], whichever spelling it
// was written with; Datomic's [[?a ?b]] is read as the same binding.
func TestQueryStringRendersTheRelationBindingWithTheEllipsis(t *testing.T) {
	q, err := ParseQuery(`[:find ?symbol ?price ?time
	                       :where [?s :symbol/ticker ?symbol]
	                              [(q [:find ?p ?t
	                                   :in $ ?sym
	                                   :where [?x :price/symbol ?sym]
	                                          [?x :price/value ?p]
	                                          [?x :price/time ?t]]
	                                  $ ?s) [[?price ?time]]]]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	written, ok := q.Where[1].(*query.SubqueryPattern).Binding.(query.RelationBinding)
	if !ok {
		t.Fatalf("Datomic's spelling parses to the relation binding, got %T", q.Where[1].(*query.SubqueryPattern).Binding)
	}

	formatted := q.String()
	if !contains(formatted, "[[?price ?time] ...]") {
		t.Fatalf("the relation binding renders as [[?price ?time] ...], got:\n%s", formatted)
	}

	q2, err := ParseQuery(formatted)
	if err != nil {
		t.Fatalf("formatted query failed to parse: %v\nformatted: %s", err, formatted)
	}
	rendered, ok := q2.Where[1].(*query.SubqueryPattern).Binding.(query.RelationBinding)
	if !ok {
		t.Fatalf("the rendering parses to the relation binding, got %T", q2.Where[1].(*query.SubqueryPattern).Binding)
	}
	if len(rendered.Variables) != len(written.Variables) || rendered.Variables[0] != written.Variables[0] || rendered.Variables[1] != written.Variables[1] {
		t.Fatalf("the rendering binds %v, the written query bound %v", rendered.Variables, written.Variables)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s[:len(substr)] == substr || (len(s) > len(substr) && contains(s[1:], substr)))
}

func TestParseScalarFindSpec(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		scalarReturn bool
		findCount    int
		shouldError  bool
		errorMsg     string
	}{
		{
			name:         "scalar find spec with aggregate",
			input:        `[:find (max ?v) . :where [?e :event/value ?v]]`,
			scalarReturn: true,
			findCount:    1,
		},
		{
			name:         "scalar find spec with variable",
			input:        `[:find ?name . :where [?e :person/name ?name]]`,
			scalarReturn: true,
			findCount:    1,
		},
		{
			name:         "normal find (no scalar spec)",
			input:        `[:find ?name :where [?e :person/name ?name]]`,
			scalarReturn: false,
			findCount:    1,
		},
		{
			name:         "normal find with multiple variables",
			input:        `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`,
			scalarReturn: false,
			findCount:    2,
		},
		{
			name:        "scalar find spec with multiple elements fails",
			input:       `[:find ?name ?age . :where [?e :person/name ?name] [?e :person/age ?age]]`,
			shouldError: true,
			errorMsg:    "scalar find spec (.) requires exactly one find element",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)

			if tt.shouldError {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errorMsg)
				}
				if !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if q.ScalarReturn != tt.scalarReturn {
				t.Errorf("expected ScalarReturn=%v, got %v", tt.scalarReturn, q.ScalarReturn)
			}

			if len(q.Find) != tt.findCount {
				t.Errorf("expected %d find elements, got %d", tt.findCount, len(q.Find))
			}
		})
	}
}

func TestParseSymbolValue(t *testing.T) {
	// Bare symbols in value position should parse as Symbol constants
	input := `[:find ?e
              :where [?e :fn/name my-function]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(q.Where) != 1 {
		t.Fatalf("expected 1 where clause, got %d", len(q.Where))
	}

	pattern := q.Where[0].(*query.DataPattern)
	elem := pattern.Elements[2]

	if elem.IsVariable() {
		t.Fatal("expected constant, got variable")
	}
	if elem.IsBlank() {
		t.Fatal("expected constant, got blank")
	}

	c, ok := elem.(query.Constant)
	if !ok {
		t.Fatalf("expected Constant, got %T", elem)
	}

	sym, ok := c.Value.(datalog.Symbol)
	if !ok {
		t.Fatalf("expected Symbol value, got %T", c.Value)
	}

	if sym.String() != "my-function" {
		t.Errorf("expected symbol value 'my-function', got %q", sym.String())
	}

	// Verify it's interned
	expected := datalog.NewSymbol("my-function")
	if sym != expected {
		t.Error("parsed symbol should be pointer-equal to NewSymbol result (interning)")
	}
}
