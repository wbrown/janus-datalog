package planner

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestPlannerRejectsUnknownFunctions ensures we fail at planning time, not runtime
func TestPlannerRejectsUnknownFunctions(t *testing.T) {
	planner := NewPlanner(nil, PlannerOptions{})

	tests := []struct {
		name          string
		query         string
		shouldFail    bool
		errorContains string
	}{
		{
			name: "valid str/starts-with?",
			query: `[:find ?x
                     :where [?e :attr ?x]
                            [(str/starts-with? ?x "foo")]]`,
			shouldFail: false,
		},
		{
			name: "unknown function foo/bar",
			query: `[:find ?x
                     :where [?e :attr ?x]
                            [(foo/bar ?x "test")]]`,
			shouldFail:    true,
			errorContains: "unknown function 'foo/bar'",
		},
		{
			name: "unknown function custom-pred",
			query: `[:find ?x
                     :where [?e :attr ?x]
                            [(custom-pred ?x 5)]]`,
			shouldFail:    true,
			errorContains: "unknown function 'custom-pred'",
		},
		{
			name: "str/starts-with? with wrong arg count",
			query: `[:find ?x
                     :where [?e :attr ?x]
                            [(str/starts-with? ?x)]]`,
			shouldFail:    true,
			errorContains: "at least 2 arguments",
		},
		{
			name: "valid year extraction",
			query: `[:find ?y
                     :where [?e :time ?t]
                            [(year ?t) ?y]]`,
			shouldFail: false, // year is an expression, not a predicate
		},
		{
			name: "not= should NOT be treated as function",
			query: `[:find ?x
                     :where [?e :attr ?x]
                            [(not= ?x 5)]]`,
			shouldFail: false, // not= is parsed as NotEqualPredicate, not FunctionPredicate
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			_, err = planner.Plan(q)

			if tt.shouldFail {
				if err == nil {
					t.Errorf("Expected planning to fail, but it succeeded")
					return
				}
				if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				} else {
					t.Logf("Correctly rejected with error: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Planning failed unexpectedly: %v", err)
				}
			}
		})
	}
}

// TestNotEqualIsNotFunction ensures not= doesn't get parsed as a FunctionPredicate
func TestNotEqualIsNotFunction(t *testing.T) {
	planner := NewPlanner(nil, PlannerOptions{})

	// Both != and not= should parse as NotEqualPredicate, not FunctionPredicate
	queries := []string{
		`[:find ?x :where [?e :attr ?x] [(!= ?x 5)]]`,
		`[:find ?x :where [?e :attr ?x] [(not= ?x 5)]]`,
	}

	for _, queryStr := range queries {
		t.Run(queryStr, func(t *testing.T) {
			q, err := parser.ParseQuery(queryStr)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			plan, err := planner.Plan(q)
			if err != nil {
				t.Fatalf("Planning error: %v", err)
			}

			// Verify that the plan doesn't trigger function validation errors
			if plan == nil {
				t.Error("Expected valid plan, got nil")
			}
		})
	}
}
