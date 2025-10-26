package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestPredicateTypeAssignment(t *testing.T) {
	tests := []struct {
		name         string
		predicate    string
		expectedType string
	}{
		{"Variable equality", "[(= ?x ?y)]", "equality"},
		{"Variable-constant equality", "[(= ?x 5)]", "equality"},
		{"Constant-variable equality", "[(= 5 ?x)]", "equality"},
		{"Greater than", "[(> ?x 5)]", "comparison"},
		{"Less than equal", "[(<= ?x 10)]", "comparison"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryStr := `[:find ?x :where [?e :attr ?x] ` + tt.predicate + `]`
			q, err := parser.ParseQuery(queryStr)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			p := NewPlanner(nil, PlannerOptions{})

			// Find the predicate in the query
			var pred query.Predicate
			for _, clause := range q.Where {
				if pr, ok := clause.(query.Predicate); ok {
					pred = pr
					break
				}
			}

			if pred == nil {
				t.Fatal("No predicate found in query")
			}

			// Create predicate plan
			predPlan := p.createPredicatePlan(pred)

			if predPlan.Type.String() != tt.expectedType {
				t.Errorf("Expected type %s, got %s for predicate %s",
					tt.expectedType, predPlan.Type.String(), pred.String())
			}

			t.Logf("Predicate %s: Type=%s, Variable=%s, RequiredVars=%v",
				pred.String(), predPlan.Type, predPlan.Variable, predPlan.RequiredVars)
		})
	}
}
