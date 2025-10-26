package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"testing"
)

// TestWhyMetadataNotPopulated investigates why [(= ?d 20)] doesn't get metadata
func TestWhyMetadataNotPopulated(t *testing.T) {
	// First, test the simple case that works
	t.Run("SimpleCase", func(t *testing.T) {
		queryStr := `[:find ?d :where [(= ?d 20)]]`
		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatal(err)
		}

		p := NewPlanner(nil, PlannerOptions{})
		plan, err := p.Plan(q)
		if err != nil {
			t.Fatal(err)
		}

		// Check if metadata is populated
		for i, phase := range plan.Phases {
			for j, pred := range phase.Predicates {
				t.Logf("Phase %d, Pred %d: Type=%s, Var=%s, Val=%v",
					i, j, pred.Type, pred.Variable, pred.Value)
			}
		}
	})

	// Now test with a pattern that provides ?d first
	t.Run("WithProvidingPattern", func(t *testing.T) {
		queryStr := `[:find ?d 
		              :where 
		              [?x :foo/bar ?y]
		              [(= ?d 20)]]`
		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatal(err)
		}

		p := NewPlanner(nil, PlannerOptions{})
		plan, err := p.Plan(q)
		if err != nil {
			t.Fatal(err)
		}

		// Check if metadata is populated
		for i, phase := range plan.Phases {
			for j, pred := range phase.Predicates {
				t.Logf("Phase %d, Pred %d: Type=%s, Var=%s, Val=%v",
					i, j, pred.Type, pred.Variable, pred.Value)
			}
		}
	})

	// Test with expression that binds ?d
	t.Run("WithExpressionBinding", func(t *testing.T) {
		queryStr := `[:find ?d 
		              :where 
		              [?b :price/time ?t]
		              [(day ?t) ?d]
		              [(= ?d 20)]]`
		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatal(err)
		}

		p := NewPlanner(nil, PlannerOptions{})
		plan, err := p.Plan(q)
		if err != nil {
			t.Fatal(err)
		}

		// Check if metadata is populated
		for i, phase := range plan.Phases {
			t.Logf("Phase %d:", i)
			t.Logf("  Provides: %v", phase.Provides)
			for j, pred := range phase.Predicates {
				t.Logf("  Pred %d: Type=%s, Var=%s, Val=%v",
					j, pred.Type, pred.Variable, pred.Value)

				// Also check if it's being treated as an equality binding
				if pred.Predicate != nil {
					t.Logf("    Predicate type: %T", pred.Predicate)
					t.Logf("    String: %s", pred.Predicate.String())
					t.Logf("    Required vars: %v", pred.RequiredVars)
				}
			}
		}
	})
}
