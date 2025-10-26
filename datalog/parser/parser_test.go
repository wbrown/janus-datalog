package parser

import (
	"fmt"
	"reflect"
	"testing"

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
			if v.Symbol != "?e" {
				t.Errorf("Find[0]: expected ?e, got %s", v.Symbol)
			}
		case 1:
			if v.Symbol != "?name" {
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
		check func(*query.Query) error
	}{
		{
			name: "string literal",
			input: `[:find ?e
                     :where [?e :person/name "Alice"]]`,
			check: func(q *query.Query) error {
				pattern := q.Where[0].(*query.DataPattern)
				elem := pattern.Elements[2]
				if elem.IsVariable() {
					return fmt.Errorf("expected constant, got variable")
				}
				return nil
			},
		},
		{
			name: "integer literal",
			input: `[:find ?e
                     :where [?e :person/age 42]]`,
			check: func(q *query.Query) error {
				pattern := q.Where[0].(*query.DataPattern)
				elem := pattern.Elements[2]
				if elem.IsVariable() {
					return fmt.Errorf("expected constant, got variable")
				}
				return nil
			},
		},
		{
			name: "float literal",
			input: `[:find ?e
                     :where [?e :score/value 3.14]]`,
			check: func(q *query.Query) error {
				pattern := q.Where[0].(*query.DataPattern)
				elem := pattern.Elements[2]
				if elem.IsVariable() {
					return fmt.Errorf("expected constant, got variable")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := tt.check(q); err != nil {
				t.Error(err)
			}
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
			input: `[:find ?e :where [?e ?a]]`,
			error: "pattern must have 3 or 4 elements",
		},
		{
			name:  "non-vector pattern",
			input: `[:find ?e :where (?e :foo ?v)]`,
			error: "expected vector in :where clause",
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

func TestValidateQuery(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
		error string
	}{
		{
			name: "valid query",
			input: `[:find ?e ?name
                     :where [?e :person/name ?name]]`,
			valid: true,
		},
		{
			name: "unbound find variable",
			input: `[:find ?e ?x
                     :where [?e :person/name ?name]]`,
			valid: false,
			error: "find variable ?x not bound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			err = ValidateQuery(q)
			if tt.valid && err != nil {
				t.Errorf("expected valid query, got error: %v", err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected validation error")
			}
			if !tt.valid && !contains(err.Error(), tt.error) {
				t.Errorf("expected error containing %q, got %q", tt.error, err.Error())
			}
		})
	}
}

func TestExtractVariables(t *testing.T) {
	input := `[:find ?e ?name
              :where [?e :person/name ?name]
                     [?e :person/age ?age]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vars := ExtractVariables(q.Where)
	expected := []query.Symbol{"?e", "?name", "?age"}

	if len(vars) != len(expected) {
		t.Errorf("expected %d variables, got %d", len(expected), len(vars))
	}

	varSet := make(map[query.Symbol]bool)
	for _, v := range vars {
		varSet[v] = true
	}

	for _, exp := range expected {
		if !varSet[exp] {
			t.Errorf("missing variable %s", exp)
		}
	}
}

func TestFormatQuery(t *testing.T) {
	input := `[:find ?e ?name :where [?e :person/name ?name]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	formatted := FormatQuery(q)

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
		check func(*query.Query) error
	}{
		{
			name: "simple subquery with tuple binding",
			input: `[:find ?date ?high
			         :where 
			           [?s :symbol/ticker "AAPL"]
			           [(ground "2025-06-02") ?date]
			           [(q [:find (max ?h)
			                :where [?p :price/symbol ?symbol]
			                       [?p :price/high ?h]]
			               ?s) [[?high]]]]`,
			check: func(q *query.Query) error {
				if len(q.Where) != 3 {
					return fmt.Errorf("expected 3 patterns, got %d", len(q.Where))
				}

				// Check the subquery pattern
				subq, ok := q.Where[2].(*query.SubqueryPattern)
				if !ok {
					return fmt.Errorf("expected SubqueryPattern, got %T", q.Where[2])
				}

				// Check inputs
				if len(subq.Inputs) != 1 {
					return fmt.Errorf("expected 1 input, got %d", len(subq.Inputs))
				}

				// Check binding is tuple binding
				binding, ok := subq.Binding.(query.TupleBinding)
				if !ok {
					return fmt.Errorf("expected TupleBinding, got %T", subq.Binding)
				}

				if len(binding.Variables) != 1 || binding.Variables[0] != "?high" {
					return fmt.Errorf("expected [[?high]] binding, got %v", binding.Variables)
				}

				// Check nested query
				if len(subq.Query.Find) != 1 {
					return fmt.Errorf("expected 1 find element in nested query")
				}

				agg, ok := subq.Query.Find[0].(query.FindAggregate)
				if !ok || agg.Function != "max" || agg.Arg != "?h" {
					return fmt.Errorf("expected (max ?h) in nested query")
				}

				return nil
			},
		},
		{
			name: "subquery with collection binding",
			input: `[:find ?symbol ?prices
			         :where 
			           [?s :symbol/ticker ?symbol]
			           [(q [:find ?price ?time
			                :where [?p :price/symbol ?sym]
			                       [?p :price/value ?price]
			                       [?p :price/time ?time]]
			               ?s) ?prices]]`,
			check: func(q *query.Query) error {
				subq, ok := q.Where[1].(*query.SubqueryPattern)
				if !ok {
					return fmt.Errorf("expected SubqueryPattern")
				}

				// Check collection binding
				binding, ok := subq.Binding.(query.CollectionBinding)
				if !ok {
					return fmt.Errorf("expected CollectionBinding, got %T", subq.Binding)
				}

				if binding.Variable != "?prices" {
					return fmt.Errorf("expected ?prices binding, got %s", binding.Variable)
				}

				return nil
			},
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
			check: func(q *query.Query) error {
				subq, ok := q.Where[1].(*query.SubqueryPattern)
				if !ok {
					return fmt.Errorf("expected SubqueryPattern")
				}

				// Check relation binding
				binding, ok := subq.Binding.(query.RelationBinding)
				if !ok {
					return fmt.Errorf("expected RelationBinding, got %T", subq.Binding)
				}

				if len(binding.Variables) != 2 {
					return fmt.Errorf("expected 2 variables in binding, got %d", len(binding.Variables))
				}

				if binding.Variables[0] != "?price" || binding.Variables[1] != "?time" {
					return fmt.Errorf("expected [[?price ?time] ...], got %v", binding.Variables)
				}

				return nil
			},
		},
		{
			name: "subquery with multiple inputs",
			input: `[:find ?result
			         :where 
			           [?s :symbol/ticker "AAPL"]
			           [(ground "2025-06-02") ?date]
			           [(q [:find (avg ?v)
			                :where [?p :price/symbol ?sym]
			                       [?p :price/time ?t]
			                       [(= ?t ?date)]
			                       [?p :price/value ?v]]
			               ?s ?date) [[?result]]]]`,
			check: func(q *query.Query) error {
				subq, ok := q.Where[2].(*query.SubqueryPattern)
				if !ok {
					return fmt.Errorf("expected SubqueryPattern")
				}

				// Check multiple inputs
				if len(subq.Inputs) != 2 {
					return fmt.Errorf("expected 2 inputs, got %d", len(subq.Inputs))
				}

				// First input should be variable ?s
				v1, ok := subq.Inputs[0].(query.Variable)
				if !ok || v1.Name != "?s" {
					return fmt.Errorf("expected ?s as first input")
				}

				// Second input should be variable ?date
				v2, ok := subq.Inputs[1].(query.Variable)
				if !ok || v2.Name != "?date" {
					return fmt.Errorf("expected ?date as second input")
				}

				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}

			if err := tt.check(q); err != nil {
				t.Error(err)
			}
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
			name: "empty tuple binding",
			input: `[:find ?x
			         :where [(q [:find ?y :where [?e :foo ?y]]) [[]]]]`,
			error: "tuple binding cannot be empty",
		},
		{
			name: "non-variable in tuple binding",
			input: `[:find ?x
			         :where [(q [:find ?y :where [?e :foo ?y]]) [[foo]]]]`,
			error: "tuple binding element 0 must be a variable",
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
			error: "unsupported pattern element type",
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

	formatted := FormatQuery(q)

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

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s[:len(substr)] == substr || (len(s) > len(substr) && contains(s[1:], substr)))
}
