package parser

import (
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestParserCreatesClauseStructure(t *testing.T) {
	queryStr := `
	[:find ?x ?sum
	 :where
	 [?e :foo/x ?x]
	 [?e :foo/y ?y]
	 [(> ?x 5)]
	 [(+ ?x ?y) ?sum]]
	`

	q, err := ParseQuery(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	if len(q.Where) != 4 {
		t.Errorf("Expected 4 clauses, got %d", len(q.Where))
	}

	// Check clause types
	for i, clause := range q.Where {
		switch c := clause.(type) {
		case *query.DataPattern:
			t.Logf("Clause %d: DataPattern with %d elements", i, len(c.Elements))
		case *query.Comparison:
			t.Logf("Clause %d: Comparison (%s)", i, c.Op)
		case *query.Expression:
			t.Logf("Clause %d: Expression with binding %s", i, c.Binding)
			if fn, ok := c.Function.(*query.ArithmeticFunction); ok {
				t.Logf("  Function: Arithmetic (%s)", fn.Op)
			}
		case *query.FunctionPredicate:
			t.Logf("Clause %d: FunctionPredicate %s", i, c.Fn)
		default:
			t.Logf("Clause %d: Unknown type %T", i, c)
		}
	}

	// Verify specific clauses
	if _, ok := q.Where[0].(*query.DataPattern); !ok {
		t.Errorf("Expected clause 0 to be DataPattern, got %T", q.Where[0])
	}

	if _, ok := q.Where[2].(*query.Comparison); !ok {
		t.Errorf("Expected clause 2 to be Comparison, got %T", q.Where[2])
	}

	if expr, ok := q.Where[3].(*query.Expression); ok {
		if expr.Binding != "?sum" {
			t.Errorf("Expected binding ?sum, got %s", expr.Binding)
		}
		if _, ok := expr.Function.(*query.ArithmeticFunction); !ok {
			t.Errorf("Expected ArithmeticFunction, got %T", expr.Function)
		}
	} else {
		t.Errorf("Expected clause 3 to be Expression, got %T", q.Where[3])
	}
}
