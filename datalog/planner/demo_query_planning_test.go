package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestDemoQueryPlanning(t *testing.T) {
	queryStr := `[:find ?year ?month ?day (max ?high) (min ?low) ?open ?close
				 :where
						[?s :symbol/ticker "CRWV"]
						[?p :price/symbol ?s]
						[?p :price/time ?time]
						[?p :price/minute-of-day ?mod]
						[(>= ?mod 570)]
						[(<= ?mod 960)]
						[(year ?time) ?year]
						[(month ?time) ?month]
						[(day ?time) ?day]
						[?p :price/high ?high]
						[?p :price/low ?low]
						
						[?p-open :price/symbol ?s]
						[?p-open :price/time ?time-open]
						[(year ?time-open) ?year-open]
						[(month ?time-open) ?month-open]
						[(day ?time-open) ?day-open]
						[(= ?year ?year-open)]
						[(= ?month ?month-open)]
						[(= ?day ?day-open)]
						[?p-open :price/minute-of-day 570]
						[?p-open :price/open ?open]
						
						[?p-close :price/symbol ?s]
						[?p-close :price/time ?time-close]
						[(year ?time-close) ?year-close]
						[(month ?time-close) ?month-close]
						[(day ?time-close) ?day-close]
						[(= ?year ?year-close)]
						[(= ?month ?month-close)]
						[(= ?day ?day-close)]
						[?p-close :price/minute-of-day 960]
						[?p-close :price/close ?close]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	p := NewPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}

	// Check all phases
	for i, phase := range plan.Phases {
		t.Logf("Phase %d has %d predicates, %d expressions", i+1, len(phase.Predicates), len(phase.Expressions))
		for _, pred := range phase.Predicates {
			if comp, ok := pred.Predicate.(*query.Comparison); ok && comp.Op == query.OpEQ {
				t.Logf("  Phase %d equality predicate: %s", i+1, pred.Predicate.String())
			}
		}
	}

	// Check that phases have the equality predicates
	if len(plan.Phases) < 4 {
		t.Fatalf("Expected at least 4 phases, got %d", len(plan.Phases))
	}

	// Count total equality predicates across all phases
	totalEqualityPredicates := 0
	for i, phase := range plan.Phases {
		phaseEqualityCount := 0
		for _, pred := range phase.Predicates {
			if comp, ok := pred.Predicate.(*query.Comparison); ok && comp.Op == query.OpEQ {
				phaseEqualityCount++
				totalEqualityPredicates++
			}
		}
		if phaseEqualityCount > 0 {
			t.Logf("Phase %d has %d equality predicates", i+1, phaseEqualityCount)
		}
	}

	// We expect 6 equality predicates total:
	// 3 for open: [(= ?year ?year-open)], [(= ?month ?month-open)], [(= ?day ?day-open)]
	// 3 for close: [(= ?year ?year-close)], [(= ?month ?month-close)], [(= ?day ?day-close)]
	if totalEqualityPredicates != 6 {
		t.Errorf("Expected 6 total equality predicates, got %d", totalEqualityPredicates)
	}

	// Find which phases have the equality predicates
	openPhaseIndex := -1
	closePhaseIndex := -1

	for i, phase := range plan.Phases {
		eqCount := 0
		for _, pred := range phase.Predicates {
			if comp, ok := pred.Predicate.(*query.Comparison); ok && comp.Op == query.OpEQ {
				eqCount++
			}
		}
		if eqCount == 3 {
			// This phase has 3 equality predicates
			if openPhaseIndex == -1 {
				openPhaseIndex = i
			} else if closePhaseIndex == -1 {
				closePhaseIndex = i
			}
		}
	}

	if openPhaseIndex == -1 {
		t.Error("Could not find phase with open equality predicates")
	}
	if closePhaseIndex == -1 {
		t.Error("Could not find phase with close equality predicates")
	}
}
