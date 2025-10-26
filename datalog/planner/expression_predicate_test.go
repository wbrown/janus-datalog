package planner

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

// TestTimeExtractionExpressionWithEquality tests that time extraction expressions
// combined with equality predicates create proper storage constraints
func TestTimeExtractionExpressionWithEquality(t *testing.T) {
	tests := []struct {
		name               string
		queryStr           string
		expectedConstraint bool
		constraintField    string
		constraintValue    interface{}
	}{
		{
			name: "DayExtractionWithEquality",
			queryStr: `[:find ?b ?t
			            :where
			            [?b :price/time ?t]
			            [(day ?t) ?d]
			            [(= ?d 20)]]`,
			expectedConstraint: true,
			constraintField:    "day",
			constraintValue:    int64(20),
		},
		{
			name: "MonthExtractionWithEquality",
			queryStr: `[:find ?b ?t
			            :where
			            [?b :price/time ?t]
			            [(month ?t) ?m]
			            [(= ?m 6)]]`,
			expectedConstraint: true,
			constraintField:    "month",
			constraintValue:    int64(6),
		},
		{
			name: "YearExtractionWithEquality",
			queryStr: `[:find ?b ?t
			            :where
			            [?b :price/time ?t]
			            [(year ?t) ?y]
			            [(= ?y 2025)]]`,
			expectedConstraint: true,
			constraintField:    "year",
			constraintValue:    int64(2025),
		},
		{
			name: "HourExtractionWithEquality",
			queryStr: `[:find ?b ?t
			            :where
			            [?b :price/time ?t]
			            [(hour ?t) ?h]
			            [(= ?h 14)]]`,
			expectedConstraint: true,
			constraintField:    "hour",
			constraintValue:    int64(14),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.queryStr)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			// Plan with predicate pushdown enabled
			planner := NewPlanner(nil, PlannerOptions{
				EnablePredicatePushdown: true,
			})
			plan, err := planner.Plan(q)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Look for storage constraints on the time pattern
			foundConstraint := false
			for _, phase := range plan.Phases {
				for _, pattern := range phase.Patterns {
					if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
						// Check if this is the time pattern
						if len(dp.Elements) > 1 {
							if attr, ok := dp.Elements[1].(query.Constant); ok {
								if kw, ok := attr.Value.(datalog.Keyword); ok && kw.String() == ":price/time" {
									// This is the time pattern - check for constraints
									if pattern.Metadata != nil {
										if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
											for _, c := range constraints {
												if c.Type == ConstraintTimeExtraction {
													foundConstraint = true
													if c.TimeField != tt.constraintField {
														t.Errorf("Expected time field %s, got %s", tt.constraintField, c.TimeField)
													}
													if c.Value != tt.constraintValue {
														t.Errorf("Expected value %v, got %v", tt.constraintValue, c.Value)
													}
													if c.Operator != query.OpEQ {
														t.Errorf("Expected = operator, got %s", c.Operator)
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
			}

			if tt.expectedConstraint && !foundConstraint {
				t.Error("Expected time extraction constraint to be created from expression + equality")
			}
			if !tt.expectedConstraint && foundConstraint {
				t.Error("Did not expect constraint but found one")
			}
		})
	}
}

// TestOHLCQueryWithExpressionHandling tests the full OHLC query pattern
func TestOHLCQueryWithExpressionHandling(t *testing.T) {
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

	// Plan with predicate pushdown
	planner := NewPlanner(nil, PlannerOptions{
		EnablePredicatePushdown: true,
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Check that the time pattern has a day=20 constraint
	foundDayConstraint := false
	for _, phase := range plan.Phases {
		for _, pattern := range phase.Patterns {
			if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
				if len(dp.Elements) > 1 {
					if attr, ok := dp.Elements[1].(query.Constant); ok {
						if kw, ok := attr.Value.(datalog.Keyword); ok && kw.String() == ":price/time" {
							// Found the time pattern
							if pattern.Metadata != nil {
								if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
									for _, c := range constraints {
										if c.Type == ConstraintTimeExtraction && c.TimeField == "day" && c.Value == int64(20) {
											foundDayConstraint = true
											t.Logf("Found day=20 constraint on :price/time pattern")
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

	if !foundDayConstraint {
		t.Error("Expected day=20 constraint to be pushed to :price/time pattern")
	}

	// Also verify that the equality predicate was removed
	for _, phase := range plan.Phases {
		for _, pred := range phase.Predicates {
			if pred.Type == PredicateEquality && pred.Variable == "?d" {
				t.Error("Equality predicate on ?d should have been removed after combining with time extraction")
			}
		}
	}
}
