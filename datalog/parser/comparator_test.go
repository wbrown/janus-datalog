package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseComparatorPatterns(t *testing.T) {
	tests := []struct {
		name  string
		input string
		// where is the number of :where clauses, and predicates the clauses
		// the :where ends with, after its data patterns.
		where      int
		predicates []query.Clause
	}{
		{
			name: "less than comparison",
			input: `[:find ?e ?age
                     :where [?e :person/age ?age]
                            [(< ?age 30)]]`,
			where: 2,
			predicates: []query.Clause{&query.Comparison{
				Op:    datalog.SymLT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
				Right: query.ConstantTerm{Value: int64(30)},
			}},
		},
		{
			name: "greater than comparison",
			input: `[:find ?price
                     :where [?e :stock/price ?price]
                            [(> ?price 100.50)]]`,
			where: 2,
			predicates: []query.Clause{&query.Comparison{
				Op:    datalog.SymGT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?price")},
				Right: query.ConstantTerm{Value: 100.50},
			}},
		},
		{
			name: "equality comparison",
			input: `[:find ?e
                     :where [?e :person/name ?name]
                            [(= ?name "Alice")]]`,
			where: 2,
			predicates: []query.Clause{&query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?name")},
				Right: query.ConstantTerm{Value: "Alice"},
			}},
		},
		{
			// != is the equality comparison, inverted.
			name: "not equal comparison",
			input: `[:find ?e ?status
                     :where [?e :order/status ?status]
                            [(!= ?status "cancelled")]]`,
			where: 2,
			predicates: []query.Clause{&query.NotEqualPredicate{Comparison: query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?status")},
				Right: query.ConstantTerm{Value: "cancelled"},
			}}},
		},
		{
			name: "range comparison",
			input: `[:find ?e ?age
                     :where [?e :person/age ?age]
                            [(>= ?age 18)]
                            [(<= ?age 65)]]`,
			where: 3,
			predicates: []query.Clause{
				&query.Comparison{
					Op:    datalog.SymGTE,
					Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
					Right: query.ConstantTerm{Value: int64(18)},
				},
				&query.Comparison{
					Op:    datalog.SymLTE,
					Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
					Right: query.ConstantTerm{Value: int64(65)},
				},
			},
		},
		{
			name: "multiple variable comparison",
			input: `[:find ?p1 ?p2
                     :where [?p1 :person/age ?age1]
                            [?p2 :person/age ?age2]
                            [(< ?age1 ?age2)]]`,
			where: 3,
			predicates: []query.Clause{&query.Comparison{
				Op:    datalog.SymLT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age1")},
				Right: query.VariableTerm{Symbol: datalog.NewSymbol("?age2")},
			}},
		},
		{
			name: "string operations",
			input: `[:find ?e ?name
                     :where [?e :person/name ?name]
                            [(str/starts-with? ?name "Dr.")]]`,
			where: 2,
			predicates: []query.Clause{&query.StrStartsWithPredicate{
				Value:  query.VariableTerm{Symbol: datalog.NewSymbol("?name")},
				Prefix: query.ConstantTerm{Value: "Dr."},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			require.NoError(t, err)
			require.Len(t, q.Where, tt.where)
			assert.Equal(t, tt.predicates, q.Where[tt.where-len(tt.predicates):])
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
			error: "rule invocation (< ?x 10) requires % in :in",
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
		{
			// Previously parsed to a FunctionPredicate and errored per tuple
			// at Eval ("unknown predicate function: *") — never reached when
			// upstream matched nothing, yielding silent empty results. A
			// non-boolean/unregistered function in predicate position is now
			// rejected at the parse boundary.
			name: "bare arithmetic in predicate position",
			input: `[:find ?qty ?price
                     :where [?order :order/quantity ?qty]
                            [?order :order/price ?price]
                            [(* ?qty ?price)]]`,
			error: "unknown predicate function: *",
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
