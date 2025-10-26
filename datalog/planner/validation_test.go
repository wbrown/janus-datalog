package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestValidatePlan_InputSymbols(t *testing.T) {
	planner := NewPlanner(nil, PlannerOptions{})

	tests := []struct {
		name         string
		findVars     []query.Symbol
		inputSymbols map[query.Symbol]bool
		phases       []Phase
		expressions  []*query.Expression
		subqueries   []*query.SubqueryPattern
		shouldError  bool
		errorMsg     string
	}{
		{
			name:     "Input parameter in find clause - valid",
			findVars: []query.Symbol{"?x"},
			inputSymbols: map[query.Symbol]bool{
				"?x": true,
			},
			phases:      []Phase{},
			expressions: []*query.Expression{},
			subqueries:  []*query.SubqueryPattern{},
			shouldError: false,
		},
		{
			name:     "Unbound variable in find clause - invalid",
			findVars: []query.Symbol{"?day"},
			inputSymbols: map[query.Symbol]bool{
				"?target-day": true, // Different variable
			},
			phases:      []Phase{},
			expressions: []*query.Expression{},
			subqueries:  []*query.SubqueryPattern{},
			shouldError: true,
			errorMsg:    "find variable ?day will not be bound by query",
		},
		{
			name:         "Pattern binds variable - valid",
			findVars:     []query.Symbol{"?e"},
			inputSymbols: map[query.Symbol]bool{},
			phases: []Phase{
				{
					Provides: []query.Symbol{"?e"},
					Patterns: []PatternPlan{
						{
							Bindings: map[query.Symbol]bool{
								"?e": true,
							},
						},
					},
				},
			},
			expressions: []*query.Expression{},
			subqueries:  []*query.SubqueryPattern{},
			shouldError: false,
		},
		{
			name:         "Expression binds variable - valid",
			findVars:     []query.Symbol{"?result"},
			inputSymbols: map[query.Symbol]bool{},
			phases:       []Phase{},
			expressions: []*query.Expression{
				{
					Binding: "?result",
				},
			},
			subqueries:  []*query.SubqueryPattern{},
			shouldError: false,
		},
		{
			name:         "Subquery TupleBinding - valid",
			findVars:     []query.Symbol{"?high", "?low"},
			inputSymbols: map[query.Symbol]bool{},
			phases:       []Phase{},
			expressions:  []*query.Expression{},
			subqueries: []*query.SubqueryPattern{
				{
					Binding: query.TupleBinding{
						Variables: []query.Symbol{"?high", "?low"},
					},
				},
			},
			shouldError: false,
		},
		{
			name:     "Multiple inputs and pattern - valid",
			findVars: []query.Symbol{"?sym", "?target-day", "?s"},
			inputSymbols: map[query.Symbol]bool{
				"?sym":        true,
				"?target-day": true,
			},
			phases: []Phase{
				{
					Provides: []query.Symbol{"?s"},
					Patterns: []PatternPlan{
						{
							Bindings: map[query.Symbol]bool{
								"?s": true,
							},
						},
					},
				},
			},
			expressions: []*query.Expression{},
			subqueries:  []*query.SubqueryPattern{},
			shouldError: false,
		},
		{
			name:     "Mixed resolved and unresolved - invalid",
			findVars: []query.Symbol{"?x", "?y"},
			inputSymbols: map[query.Symbol]bool{
				"?x": true,
			},
			// ?y is never bound
			phases:      []Phase{},
			expressions: []*query.Expression{},
			subqueries:  []*query.SubqueryPattern{},
			shouldError: true,
			errorMsg:    "find variable ?y will not be bound by query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := planner.validatePlan(tt.phases, tt.expressions, tt.subqueries, tt.findVars, tt.inputSymbols)

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("Expected error message %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}
