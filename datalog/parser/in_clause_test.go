package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseInputClause(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(*query.Query) error
	}{
		{
			name: "database input only",
			input: `[:find ?e
			         :in $
			         :where [?e :person/name "Alice"]]`,
			check: func(q *query.Query) error {
				if len(q.In) != 1 {
					t.Errorf("expected 1 input, got %d", len(q.In))
				}
				if _, ok := q.In[0].(query.DatabaseInput); !ok {
					t.Errorf("expected DatabaseInput, got %T", q.In[0])
				}
				return nil
			},
		},
		{
			name: "database and scalar input",
			input: `[:find ?e
			         :in $ ?name
			         :where [?e :person/name ?name]]`,
			check: func(q *query.Query) error {
				if len(q.In) != 2 {
					t.Errorf("expected 2 inputs, got %d", len(q.In))
				}
				if _, ok := q.In[0].(query.DatabaseInput); !ok {
					t.Errorf("expected DatabaseInput at position 0, got %T", q.In[0])
				}
				if scalar, ok := q.In[1].(query.ScalarInput); !ok {
					t.Errorf("expected ScalarInput at position 1, got %T", q.In[1])
				} else if scalar.Symbol != "?name" {
					t.Errorf("expected ?name, got %s", scalar.Symbol)
				}
				return nil
			},
		},
		{
			name: "collection input",
			input: `[:find ?e ?food
			         :in $ [?food ...]
			         :where [?e :person/likes ?food]]`,
			check: func(q *query.Query) error {
				if len(q.In) != 2 {
					t.Errorf("expected 2 inputs, got %d", len(q.In))
				}
				if coll, ok := q.In[1].(query.CollectionInput); !ok {
					t.Errorf("expected CollectionInput at position 1, got %T", q.In[1])
				} else if coll.Symbol != "?food" {
					t.Errorf("expected ?food, got %s", coll.Symbol)
				}
				return nil
			},
		},
		{
			name: "tuple input",
			input: `[:find ?e
			         :in $ [[?name ?age]]
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]]`,
			check: func(q *query.Query) error {
				if len(q.In) != 2 {
					t.Errorf("expected 2 inputs, got %d", len(q.In))
				}
				if tuple, ok := q.In[1].(query.TupleInput); !ok {
					t.Errorf("expected TupleInput at position 1, got %T", q.In[1])
				} else {
					if len(tuple.Symbols) != 2 {
						t.Errorf("expected 2 symbols in tuple, got %d", len(tuple.Symbols))
					}
					if tuple.Symbols[0] != "?name" || tuple.Symbols[1] != "?age" {
						t.Errorf("expected [?name ?age], got %v", tuple.Symbols)
					}
				}
				return nil
			},
		},
		{
			name: "relation input",
			input: `[:find ?e ?name
			         :in $ [[?name ?email] ...]
			         :where [?e :person/name ?name]
			                [?e :person/email ?email]]`,
			check: func(q *query.Query) error {
				if len(q.In) != 2 {
					t.Errorf("expected 2 inputs, got %d", len(q.In))
				}
				if rel, ok := q.In[1].(query.RelationInput); !ok {
					t.Errorf("expected RelationInput at position 1, got %T", q.In[1])
				} else {
					if len(rel.Symbols) != 2 {
						t.Errorf("expected 2 symbols in relation, got %d", len(rel.Symbols))
					}
					if rel.Symbols[0] != "?name" || rel.Symbols[1] != "?email" {
						t.Errorf("expected [?name ?email], got %v", rel.Symbols)
					}
				}
				return nil
			},
		},
		{
			name: "multiple scalar inputs",
			input: `[:find ?e
			         :in $ ?name ?min-age ?max-age
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]
			                [(>= ?age ?min-age)]
			                [(<= ?age ?max-age)]]`,
			check: func(q *query.Query) error {
				if len(q.In) != 4 {
					t.Errorf("expected 4 inputs, got %d", len(q.In))
				}

				expectedVars := []string{"?name", "?min-age", "?max-age"}
				for i, expected := range expectedVars {
					if scalar, ok := q.In[i+1].(query.ScalarInput); !ok {
						t.Errorf("expected ScalarInput at position %d, got %T", i+1, q.In[i+1])
					} else if scalar.Symbol != query.Symbol(expected) {
						t.Errorf("expected %s at position %d, got %s", expected, i+1, scalar.Symbol)
					}
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

func TestParseInputClauseErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name: "invalid input symbol",
			input: `[:find ?e
			         :in foo
			         :where [?e :bar ?v]]`,
			error: "input must be $ or a variable",
		},
		{
			name: "empty input vector",
			input: `[:find ?e
			         :in []
			         :where [?e :bar ?v]]`,
			error: "input vector cannot be empty",
		},
		{
			name: "non-variable in collection",
			input: `[:find ?e
			         :in [foo ...]
			         :where [?e :bar ?v]]`,
			error: "collection input must contain a variable",
		},
		{
			name: "non-variable in tuple",
			input: `[:find ?e
			         :in [[?x foo]]
			         :where [?e :bar ?v]]`,
			error: "tuple input element 1 must be a variable",
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

func TestFormatQueryWithInputClause(t *testing.T) {
	input := `[:find ?e ?name
	           :in $ ?age [?food ...]
	           :where [?e :person/name ?name]
	                  [?e :person/age ?age]
	                  [?e :person/likes ?food]]`

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

	// Check that inputs are preserved
	if len(q2.In) != 3 {
		t.Errorf("expected 3 inputs after formatting, got %d", len(q2.In))
	}

	// Verify the formatted string contains :in clause
	if !contains(formatted, ":in $") {
		t.Errorf("formatted query missing :in clause")
	}
}

func TestSubqueryWithInputClause(t *testing.T) {
	input := `[:find ?date ?high
	           :where 
	             [?s :symbol/ticker "AAPL"]
	             [(ground "2025-06-02") ?date]
	             [(q [:find (max ?h)
	                  :in $ ?symbol ?date
	                  :where [?p :price/symbol ?symbol]
	                         [?p :price/time ?t]
	                         [(same-date? ?t ?date)]
	                         [?p :price/high ?h]]
	                 ?s ?date) [[?high]]]]`

	q, err := ParseQuery(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check the subquery pattern
	if len(q.Where) != 3 {
		t.Fatalf("expected 3 patterns, got %d", len(q.Where))
	}

	subq, ok := q.Where[2].(*query.SubqueryPattern)
	if !ok {
		t.Fatalf("expected SubqueryPattern, got %T", q.Where[2])
	}

	// Check that the nested query has :in clause
	if len(subq.Query.In) != 3 {
		t.Errorf("expected 3 inputs in subquery, got %d", len(subq.Query.In))
	}

	// Verify input types
	if _, ok := subq.Query.In[0].(query.DatabaseInput); !ok {
		t.Errorf("expected DatabaseInput at position 0")
	}
	if scalar, ok := subq.Query.In[1].(query.ScalarInput); !ok || scalar.Symbol != "?symbol" {
		t.Errorf("expected ScalarInput ?symbol at position 1")
	}
	if scalar, ok := subq.Query.In[2].(query.ScalarInput); !ok || scalar.Symbol != "?date" {
		t.Errorf("expected ScalarInput ?date at position 2")
	}
}

func TestNestedQueryInputParsing(t *testing.T) {
	// Test that a nested query's :in clause is parsed correctly
	nestedQuery := `[:find (max ?h)
	                 :in $ ?sym ?d
	                 :where [?p :price/symbol ?sym]
	                        [?p :price/time ?t]
	                        [(same-date? ?t ?d)]
	                        [?p :price/high ?h]]`

	q, err := ParseQuery(nestedQuery)
	if err != nil {
		t.Fatalf("Error parsing: %v", err)
	}

	// Check inputs
	if len(q.In) != 3 {
		t.Fatalf("Expected 3 inputs, got %d", len(q.In))
	}

	// Check database input
	if _, ok := q.In[0].(query.DatabaseInput); !ok {
		t.Errorf("Input 0: expected DatabaseInput, got %T", q.In[0])
	}

	// Check first scalar input
	if scalar, ok := q.In[1].(query.ScalarInput); !ok {
		t.Errorf("Input 1: expected ScalarInput, got %T", q.In[1])
	} else if scalar.Symbol != "?sym" {
		t.Errorf("Input 1: expected symbol ?sym, got %s", scalar.Symbol)
	}

	// Check second scalar input
	if scalar, ok := q.In[2].(query.ScalarInput); !ok {
		t.Errorf("Input 2: expected ScalarInput, got %T", q.In[2])
	} else if scalar.Symbol != "?d" {
		t.Errorf("Input 2: expected symbol ?d, got %s", scalar.Symbol)
	}
}
