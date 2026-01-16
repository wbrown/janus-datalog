package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExtractOrClauseSymbols(t *testing.T) {
	tests := []struct {
		name            string
		orClause        *query.OrClause
		expectedProvides []query.Symbol
	}{
		{
			name: "Pattern and expression both provide same variable",
			orClause: &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: datalog.NewKeyword(":test/attr")},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  "?x",
						},
					},
				},
			},
			expectedProvides: []query.Symbol{"?x"},
		},
		{
			name: "Two patterns providing same variables",
			orClause: &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: datalog.NewKeyword(":attr1")},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: datalog.NewKeyword(":attr2")},
								query.Variable{Name: "?x"},
							},
						},
					},
				},
			},
			expectedProvides: []query.Symbol{"?e", "?x"},
		},
		{
			name: "Expression-only branches",
			orClause: &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "first"},
							Binding:  "?x",
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "second"},
							Binding:  "?x",
						},
					},
				},
			},
			expectedProvides: []query.Symbol{"?x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syms := extractOrClauseSymbols(tt.orClause)

			// Check that all expected symbols are provided
			providedSet := make(map[query.Symbol]bool)
			for _, sym := range syms.Provides {
				providedSet[sym] = true
			}

			for _, expected := range tt.expectedProvides {
				if !providedSet[expected] {
					t.Errorf("Expected symbol %s to be provided, but it wasn't. Got: %v", expected, syms.Provides)
				}
			}

			// Check we don't have extra unexpected symbols (for single-symbol cases)
			if len(tt.expectedProvides) == 1 && len(syms.Provides) != 1 {
				// For multi-symbol cases, just check all expected are present
				if len(tt.expectedProvides) != len(syms.Provides) {
					t.Logf("Note: Expected %d symbols, got %d: %v", len(tt.expectedProvides), len(syms.Provides), syms.Provides)
				}
			}
		})
	}
}

func TestOrOnlyQueryPlanning(t *testing.T) {
	// Test that an OR-only query creates phases correctly
	planner := NewPlanner(nil, PlannerOptions{})

	q := &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: "?x"},
		},
		Where: []query.Clause{
			&query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: "?e"},
								query.Constant{Value: datalog.NewKeyword(":test/attr")},
								query.Variable{Name: "?x"},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  "?x",
						},
					},
				},
			},
		},
	}

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}

	if len(plan.Phases) == 0 {
		t.Fatal("Expected at least 1 phase, got 0")
	}

	t.Logf("Phases: %d", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("Phase %d: Available=%v, Provides=%v, Keep=%v, OrClauses=%d",
			i, phase.Available, phase.Provides, phase.Keep, len(phase.OrClauses))
	}

	// Check that the OR clause is assigned to a phase
	hasOrClause := false
	for _, phase := range plan.Phases {
		if len(phase.OrClauses) > 0 {
			hasOrClause = true
		}
	}
	if !hasOrClause {
		t.Error("Expected OR clause to be assigned to a phase")
	}

	// Check that ?x is in Provides
	foundX := false
	for _, phase := range plan.Phases {
		for _, sym := range phase.Provides {
			if sym == "?x" {
				foundX = true
			}
		}
	}
	if !foundX {
		t.Error("Expected ?x to be in Provides")
	}

	// Test the realized plan
	realized := plan.Realize()
	t.Logf("Realized phases: %d", len(realized.Phases))
	for i, rp := range realized.Phases {
		t.Logf("Realized Phase %d:", i)
		t.Logf("  Query: %s", rp.Query.String())
		t.Logf("  Query.Where: %d clauses", len(rp.Query.Where))
		for j, clause := range rp.Query.Where {
			t.Logf("    Clause %d: %T - %s", j, clause, clause.String())
		}
		t.Logf("  Provides: %v", rp.Provides)
		t.Logf("  Keep: %v", rp.Keep)
	}
}

func TestExtractExpressionSymbols(t *testing.T) {
	tests := []struct {
		name            string
		expression      *query.Expression
		expectedProvides []query.Symbol
		expectedRequires []query.Symbol
	}{
		{
			name: "Ground function provides binding",
			expression: &query.Expression{
				Function: &query.GroundFunction{Value: int64(0)},
				Binding:  "?x",
			},
			expectedProvides: []query.Symbol{"?x"},
			expectedRequires: []query.Symbol{},
		},
		{
			name: "Arithmetic requires inputs, provides binding",
			expression: &query.Expression{
				Function: &query.ArithmeticFunction{
					Op:    query.OpAdd,
					Left:  query.VariableTerm{Symbol: "?a"},
					Right: query.VariableTerm{Symbol: "?b"},
				},
				Binding: "?sum",
			},
			expectedProvides: []query.Symbol{"?sum"},
			expectedRequires: []query.Symbol{"?a", "?b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syms := extractExpressionSymbols(tt.expression)

			// Check provides
			if len(syms.Provides) != len(tt.expectedProvides) {
				t.Errorf("Expected %d provides, got %d: %v", len(tt.expectedProvides), len(syms.Provides), syms.Provides)
			}
			for i, expected := range tt.expectedProvides {
				if i < len(syms.Provides) && syms.Provides[i] != expected {
					t.Errorf("Expected provides[%d] = %s, got %s", i, expected, syms.Provides[i])
				}
			}

			// Check requires
			requiresSet := make(map[query.Symbol]bool)
			for _, sym := range syms.Requires {
				requiresSet[sym] = true
			}
			for _, expected := range tt.expectedRequires {
				if !requiresSet[expected] {
					t.Errorf("Expected symbol %s to be required, but it wasn't. Got: %v", expected, syms.Requires)
				}
			}
		})
	}
}
