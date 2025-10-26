package parser

import (
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestParserCreatesCorrectTypes(t *testing.T) {
	queryStr := `
	[:find ?x ?sum (sum ?y)
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

	// Check find elements
	if len(q.Find) != 3 {
		t.Errorf("Expected 3 find elements, got %d", len(q.Find))
	}

	// Check aggregate in find
	if agg, ok := q.Find[2].(query.FindAggregate); ok {
		t.Logf("Find aggregate: %s(%s)", agg.Function, agg.Arg)
	} else {
		t.Errorf("Expected FindAggregate, got %T", q.Find[2])
	}

	// Check where patterns
	predicateFound := false
	expressionFound := false

	for i, pattern := range q.Where {
		switch p := pattern.(type) {
		case *query.DataPattern:
			t.Logf("Pattern %d: DataPattern", i)
		case query.Predicate:
			t.Logf("Pattern %d: Predicate type %T", i, p)
			predicateFound = true
		case *query.Expression:
			t.Logf("Pattern %d: Expression with binding %s", i, p.Binding)
			expressionFound = true
		case query.Function:
			t.Logf("Pattern %d: Function type %T", i, p)
		default:
			t.Logf("Pattern %d: Unknown type %T", i, p)
		}
	}

	if !predicateFound {
		t.Error("No Predicate found in where clause")
	}

	if !expressionFound {
		t.Error("No Expression found in where clause")
	}
}
