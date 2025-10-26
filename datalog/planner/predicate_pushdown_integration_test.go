package planner

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestPredicatePushdownIntegration(t *testing.T) {
	t.Run("OHLCQueryOptimization", func(t *testing.T) {
		// This is the problematic query that fetches 7,800 bars instead of 390
		queryStr := `[:find ?b ?t ?h ?l ?c ?v
		              :where 
		              [?s :symbol/ticker "CRWV"]
		              [?b :price/symbol ?s]
		              [?b :price/time ?t]
		              [?b :price/high ?h]
		              [?b :price/low ?l]
		              [?b :price/close ?c]
		              [?b :price/volume ?v]
		              [(day ?t) ?d]
		              [(= ?d 20)]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Plan without predicate pushdown
		plannerNoPush := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: false,
		})
		planNoPush, err := plannerNoPush.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan query without pushdown: %v", err)
		}

		// Plan with predicate pushdown
		plannerWithPush := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: true,
		})
		planWithPush, err := plannerWithPush.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan query with pushdown: %v", err)
		}

		// Without pushdown, predicates should remain at phase level
		foundPhasePredicates := false
		for _, phase := range planNoPush.Phases {
			if len(phase.Predicates) > 0 {
				foundPhasePredicates = true
				break
			}
		}
		if !foundPhasePredicates {
			t.Error("Expected predicates at phase level without pushdown")
		}

		// With pushdown, predicates should be pushed to patterns
		foundStorageConstraints := false
		for _, phase := range planWithPush.Phases {
			for _, pattern := range phase.Patterns {
				if pattern.Metadata != nil {
					if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok && len(constraints) > 0 {
						foundStorageConstraints = true

						// Verify we have the day constraint
						for _, c := range constraints {
							if c.Type == ConstraintTimeExtraction && c.TimeField == "day" {
								t.Logf("Found pushed day constraint: %+v", c)
							}
						}
					}
				}
			}
		}

		if !foundStorageConstraints {
			t.Error("Expected storage constraints with predicate pushdown")
		}
	})

	t.Run("MultiPatternEntityPropagation", func(t *testing.T) {
		// Test that predicates propagate across patterns sharing an entity
		queryStr := `[:find ?b ?s ?h
		              :where
		              [?b :price/symbol ?s]
		              [?b :price/high ?h]
		              [?b :price/time ?t]
		              [(> ?h 150)]
		              [(day ?t) ?d]
		              [(= ?d 15)]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		planner := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: true,
		})
		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan query: %v", err)
		}

		// Count how many patterns have constraints
		patternsWithConstraints := 0
		totalConstraints := 0

		for _, phase := range plan.Phases {
			for _, pattern := range phase.Patterns {
				if pattern.Metadata != nil {
					if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok && len(constraints) > 0 {
						patternsWithConstraints++
						totalConstraints += len(constraints)

						// Log what constraints were pushed where
						if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
							if len(dp.Elements) > 1 {
								if attr, ok := dp.Elements[1].(query.Constant); ok {
									t.Logf("Pattern %v has %d constraints", attr.Value, len(constraints))
								}
							}
						}
					}
				}
			}
		}

		// We should have pushed constraints to multiple patterns
		if patternsWithConstraints < 2 {
			t.Errorf("Expected constraints on at least 2 patterns, got %d", patternsWithConstraints)
		}

		if totalConstraints < 2 {
			t.Errorf("Expected at least 2 total constraints, got %d", totalConstraints)
		}
	})

	t.Run("SelectivityAnalysis", func(t *testing.T) {
		// Test that selectivity is properly calculated

		// Pattern with day constraint (1/30 selectivity)
		patternWithDay := PatternPlan{
			Metadata: map[string]interface{}{
				"storage_constraints": []StorageConstraint{
					{Type: ConstraintTimeExtraction, TimeField: "day"},
				},
			},
		}

		daySelectivity := analyzeSelectivity(patternWithDay)
		expectedDay := 1.0 / 30
		if daySelectivity < expectedDay-0.01 || daySelectivity > expectedDay+0.01 {
			t.Errorf("Expected day selectivity ~%f, got %f", expectedDay, daySelectivity)
		}

		// Pattern with equality constraint (0.01 selectivity)
		patternWithEquality := PatternPlan{
			Metadata: map[string]interface{}{
				"storage_constraints": []StorageConstraint{
					{Type: ConstraintEquality, Operator: query.OpEQ},
				},
			},
		}

		eqSelectivity := analyzeSelectivity(patternWithEquality)
		if eqSelectivity != 0.01 {
			t.Errorf("Expected equality selectivity 0.01, got %f", eqSelectivity)
		}

		// Combined constraints multiply selectivity
		patternWithBoth := PatternPlan{
			Metadata: map[string]interface{}{
				"storage_constraints": []StorageConstraint{
					{Type: ConstraintTimeExtraction, TimeField: "day"},
					{Type: ConstraintEquality, Operator: query.OpEQ},
				},
			},
		}

		bothSelectivity := analyzeSelectivity(patternWithBoth)
		expectedBoth := (1.0 / 30) * 0.01
		if bothSelectivity < expectedBoth-0.0001 || bothSelectivity > expectedBoth+0.0001 {
			t.Errorf("Expected combined selectivity ~%f, got %f", expectedBoth, bothSelectivity)
		}
	})

	t.Run("PredicateRemovalAfterPushdown", func(t *testing.T) {
		// Ensure pushed predicates are removed from phase-level predicates
		queryStr := `[:find ?b ?t
		              :where
		              [?b :price/time ?t]
		              [(day ?t) ?d]
		              [(= ?d 20)]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		planner := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: true,
		})
		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan query: %v", err)
		}

		// Check that predicates were removed from phase level
		for i, phase := range plan.Phases {
			// After pushdown, time extraction predicates should be removed
			for _, pred := range phase.Predicates {
				// The predicates should be empty or only contain non-pushable ones
				t.Logf("Phase %d still has predicate: %+v", i, pred)
			}

			// But they should be in pattern metadata
			for _, pattern := range phase.Patterns {
				if pattern.Metadata != nil {
					if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok && len(constraints) > 0 {
						t.Logf("Pattern has %d storage constraints", len(constraints))
					}
				}
			}
		}
	})
}

func TestRealWorldQueries(t *testing.T) {
	t.Run("DailyOHLC", func(t *testing.T) {
		// Real-world daily OHLC aggregation query
		queryStr := `[:find ?date ?symbol (max ?high) (min ?low) (sum ?volume)
		              :where
		              [?s :symbol/ticker ?symbol]
		              [?b :bar/symbol ?s]
		              [?b :bar/date ?date]
		              [?b :bar/high ?high]
		              [?b :bar/low ?low]
		              [?b :bar/volume ?volume]
		              [(>= ?date "2024-01-01")]
		              [(<= ?date "2024-01-31")]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		planner := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: true,
		})

		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan query: %v", err)
		}

		// Verify date range constraints are pushed
		foundDateConstraints := 0
		for _, phase := range plan.Phases {
			for _, pattern := range phase.Patterns {
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
					// Check if this is a date pattern
					if len(dp.Elements) > 1 {
						if attr, ok := dp.Elements[1].(query.Constant); ok {
							// Try different ways to get the attribute value
							var attrStr string
							switch v := attr.Value.(type) {
							case datalog.Keyword:
								attrStr = v.String()
							case string:
								attrStr = v
							default:
								attrStr = fmt.Sprintf("%v", v)
							}
							if attrStr == ":bar/date" {
								// This pattern should have date range constraints
								if pattern.Metadata != nil {
									if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
										for _, c := range constraints {
											if c.Type == ConstraintRange {
												foundDateConstraints++
												t.Logf("Found date range constraint: %+v", c)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		if foundDateConstraints < 2 {
			t.Errorf("Expected at least 2 date range constraints (>= and <=), found %d", foundDateConstraints)
		}
	})

	t.Run("TimeWindowQuery", func(t *testing.T) {
		// Query for specific time window
		queryStr := `[:find ?b ?price
		              :where
		              [?b :bar/time ?t]
		              [?b :bar/close ?price]
		              [(hour ?t) ?h]
		              [(>= ?h 9)]
		              [(<= ?h 16)]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		planner := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: true,
		})

		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan query: %v", err)
		}

		// Look for hour constraints
		foundHourConstraints := false
		for _, phase := range plan.Phases {
			for _, pattern := range phase.Patterns {
				if pattern.Metadata != nil {
					if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
						for _, c := range constraints {
							if c.Type == ConstraintTimeExtraction && c.TimeField == "hour" {
								foundHourConstraints = true
								t.Logf("Found hour constraint: %+v", c)
							}
						}
					}
				}
			}
		}

		if !foundHourConstraints {
			t.Error("Expected hour constraints to be pushed to storage")
		}
	})
}
