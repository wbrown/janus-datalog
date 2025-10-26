package parser

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseComparatorPatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		validate func(*query.Query) error
	}{
		{
			name: "less than comparison",
			input: `[:find ?e ?age
                     :where [?e :person/age ?age]
                            [(< ?age 30)]]`,
			validate: func(q *query.Query) error {
				if len(q.Where) != 2 {
					return fmt.Errorf("expected 2 patterns, got %d", len(q.Where))
				}

				// Check comparison
				comp, ok := q.Where[1].(*query.Comparison)
				if !ok {
					return fmt.Errorf("expected Comparison, got %T", q.Where[1])
				}

				if comp.Op != query.OpLT {
					return fmt.Errorf("expected '<' operator, got %s", comp.Op)
				}

				// Check that left is variable and right is constant
				if _, ok := comp.Left.(query.VariableTerm); !ok {
					return fmt.Errorf("expected left to be variable, got %T", comp.Left)
				}
				if _, ok := comp.Right.(query.ConstantTerm); !ok {
					return fmt.Errorf("expected right to be constant, got %T", comp.Right)
				}

				return nil
			},
		},
		{
			name: "greater than comparison",
			input: `[:find ?price
                     :where [?e :stock/price ?price]
                            [(> ?price 100.50)]]`,
			validate: func(q *query.Query) error {
				comp := q.Where[1].(*query.Comparison)
				if comp.Op != query.OpGT {
					return fmt.Errorf("expected '>' operator, got %s", comp.Op)
				}
				return nil
			},
		},
		{
			name: "equality comparison",
			input: `[:find ?e
                     :where [?e :person/name ?name]
                            [(= ?name "Alice")]]`,
			validate: func(q *query.Query) error {
				comp := q.Where[1].(*query.Comparison)
				if comp.Op != query.OpEQ {
					return fmt.Errorf("expected '=' operator, got %s", comp.Op)
				}
				return nil
			},
		},
		{
			name: "not equal comparison",
			input: `[:find ?e ?status
                     :where [?e :order/status ?status]
                            [(!= ?status "cancelled")]]`,
			validate: func(q *query.Query) error {
				// != is actually parsed as NotEqualPredicate
				_, ok := q.Where[1].(*query.NotEqualPredicate)
				if !ok {
					return fmt.Errorf("expected NotEqualPredicate, got %T", q.Where[1])
				}
				return nil
			},
		},
		{
			name: "range comparison",
			input: `[:find ?e ?age
                     :where [?e :person/age ?age]
                            [(>= ?age 18)]
                            [(<= ?age 65)]]`,
			validate: func(q *query.Query) error {
				if len(q.Where) != 3 {
					return fmt.Errorf("expected 3 patterns, got %d", len(q.Where))
				}

				comp1 := q.Where[1].(*query.Comparison)
				comp2 := q.Where[2].(*query.Comparison)

				if comp1.Op != query.OpGTE {
					return fmt.Errorf("expected '>=' operator, got %s", comp1.Op)
				}
				if comp2.Op != query.OpLTE {
					return fmt.Errorf("expected '<=' operator, got %s", comp2.Op)
				}

				return nil
			},
		},
		{
			name: "multiple variable comparison",
			input: `[:find ?p1 ?p2
                     :where [?p1 :person/age ?age1]
                            [?p2 :person/age ?age2]
                            [(< ?age1 ?age2)]]`,
			validate: func(q *query.Query) error {
				comp := q.Where[2].(*query.Comparison)

				// Both terms should be variables
				if _, ok := comp.Left.(query.VariableTerm); !ok {
					return fmt.Errorf("expected left to be variable")
				}
				if _, ok := comp.Right.(query.VariableTerm); !ok {
					return fmt.Errorf("expected right to be variable")
				}

				return nil
			},
		},
		{
			name: "string operations",
			input: `[:find ?e ?name
                     :where [?e :person/name ?name]
                            [(str/starts-with? ?name "Dr.")]]`,
			validate: func(q *query.Query) error {
				fnPred, ok := q.Where[1].(*query.FunctionPredicate)
				if !ok {
					return fmt.Errorf("expected FunctionPredicate, got %T", q.Where[1])
				}
				if fnPred.Fn != "str/starts-with?" {
					return fmt.Errorf("expected 'str/starts-with?' function, got %s", fnPred.Fn)
				}
				return nil
			},
		},
		{
			name: "mathematical operations",
			input: `[:find ?qty ?price
                     :where [?order :order/quantity ?qty]
                            [?order :order/price ?price]
                            [(* ?qty ?price)]]`,
			validate: func(q *query.Query) error {
				// Mathematical operations without binding are predicates
				fnPred, ok := q.Where[2].(*query.FunctionPredicate)
				if !ok {
					return fmt.Errorf("expected FunctionPredicate, got %T", q.Where[2])
				}
				if fnPred.Fn != "*" {
					return fmt.Errorf("expected '*' function, got %s", fnPred.Fn)
				}
				if len(fnPred.Args) != 2 {
					return fmt.Errorf("expected 2 args, got %d", len(fnPred.Args))
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

			if err := tt.validate(q); err != nil {
				t.Error(err)
			}

			// Validate that all variables are bound
			if err := ValidateQuery(q); err != nil {
				t.Errorf("validation error: %v", err)
			}
		})
	}
}

func TestComparatorErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		error string
	}{
		{
			name: "function not in vector",
			input: `[:find ?x
                     :where (< ?x 10)]`,
			error: "expected vector in :where clause",
		},
		{
			name: "empty function",
			input: `[:find ?x
                     :where [?x :foo ?y]
                            [()]]`,
			error: "predicate must have at least function name and one argument",
		},
		{
			name: "non-symbol function name",
			input: `[:find ?x
                     :where [?x :foo ?y]
                            [(42 ?x)]]`,
			error: "function name must be a symbol",
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
