package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExtractSubqueryPatternSymbols(t *testing.T) {
	tests := []struct {
		name             string
		pattern          *query.SubqueryPattern
		expectedProvides []query.Symbol
		expectedRequires []query.Symbol
	}{
		{
			name: "RelationBinding of one variable provides it",
			pattern: &query.SubqueryPattern{
				Query: &query.Query{
					Find: []query.FindElement{
						query.FindAggregate{Function: datalog.SymCount, Arg: datalog.NewSymbol("?t")},
					},
				},
				Inputs: []query.PatternElement{
					query.Constant{Value: "db"},
					query.Variable{Name: datalog.NewSymbol("?scenario")},
				},
				Binding: query.RelationBinding{Variables: []query.Symbol{datalog.NewSymbol("?openingCount")}},
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?openingCount")},
			expectedRequires: []query.Symbol{datalog.NewSymbol("?scenario")},
		},
		{
			name: "RelationBinding provides multiple variables",
			pattern: &query.SubqueryPattern{
				Query: &query.Query{
					Find: []query.FindElement{
						query.FindVariable{Symbol: datalog.NewSymbol("?a")},
						query.FindVariable{Symbol: datalog.NewSymbol("?b")},
					},
				},
				Inputs:  []query.PatternElement{},
				Binding: query.RelationBinding{Variables: []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")}},
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			expectedRequires: []query.Symbol{},
		},
		{
			name: "ScalarBinding provides single variable",
			pattern: &query.SubqueryPattern{
				Query: &query.Query{
					Find: []query.FindElement{
						query.FindVariable{Symbol: datalog.NewSymbol("?x")},
					},
				},
				Inputs:  []query.PatternElement{},
				Binding: query.ScalarBinding{Variable: datalog.NewSymbol("?result")},
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?result")},
			expectedRequires: []query.Symbol{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syms := query.ScopeOf(tt.pattern, nil)

			// Check provides
			providedSet := make(map[query.Symbol]bool)
			for _, sym := range syms.Provides {
				providedSet[sym] = true
			}
			for _, expected := range tt.expectedProvides {
				if !providedSet[expected] {
					t.Errorf("Expected symbol %s to be provided, but it wasn't. Got: %v", expected, syms.Provides)
				}
			}

			// Check requires
			requiresSet := make(map[query.Symbol]bool)
			for _, sym := range syms.Correlates {
				requiresSet[sym] = true
			}
			for _, expected := range tt.expectedRequires {
				if !requiresSet[expected] {
					t.Errorf("Expected symbol %s to be required, but it wasn't. Got: %v", expected, syms.Correlates)
				}
			}
		})
	}
}

func TestOrJoinClauseRequiresCorrelatedInputs(t *testing.T) {
	// An or-join's header is its whole interface: the required run names the
	// input a branch's subquery reads from the enclosing scope.
	t.Run("OR-JOIN with correlated subquery requires the input its header names", func(t *testing.T) {
		orJoin := &query.OrJoinClause{
			RequiredVars: []query.Symbol{datalog.NewSymbol("?entity")},
			OutputVars:   []query.Symbol{datalog.NewSymbol("?count")},
			Branches: [][]query.Clause{
				{
					&query.SubqueryPattern{
						Query: &query.Query{
							Find: []query.FindElement{
								query.FindAggregate{Function: datalog.SymCount, Arg: datalog.NewSymbol("?t")},
							},
						},
						Inputs: []query.PatternElement{
							query.Constant{Value: "db"},
							query.Variable{Name: datalog.NewSymbol("?entity")}, // Correlated input
						},
						Binding: query.RelationBinding{Variables: []query.Symbol{datalog.NewSymbol("?count")}},
					},
				},
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(0)},
						Binding:  datalog.NewSymbol("?count"),
					},
				},
			},
		}

		syms := query.ScopeOf(orJoin, nil)

		requiresSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Correlates {
			requiresSet[sym] = true
		}

		if !requiresSet[datalog.NewSymbol("?entity")] {
			t.Errorf("OR-JOIN with correlated subquery must require ?entity, got Requires: %v", syms.Correlates)
		}

		// Should provide the JoinVars
		providesSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Provides {
			providesSet[sym] = true
		}

		if !providesSet[datalog.NewSymbol("?count")] {
			t.Errorf("OR-JOIN should provide ?count (from JoinVars), got Provides: %v", syms.Provides)
		}
	})
}

// TestOrJoinClauseRequiredVarNotProvided verifies that a required run
// correlates with the enclosing scope and is not provided, and that branch
// locals outside the header never escape.
// This is the Rule 5 get-else pattern: (or-join [[?rxn]] [?rxn :attr ?v] [(ground "") ?v])
// Branch 1 reads ?rxn, branch 2 does not: ?rxn is bound from outside, and
// ?v is branch-local — the header is the complete interface.
func TestOrJoinClauseRequiredVarNotProvided(t *testing.T) {
	orJoin := &query.OrJoinClause{
		RequiredVars: []query.Symbol{datalog.NewSymbol("?rxn")},
		Branches: [][]query.Clause{
			{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?rxn")},
						query.Constant{Value: datalog.NewKeyword(":item/note")},
						query.Variable{Name: datalog.NewSymbol("?reason")},
					},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: ""},
					Binding:  datalog.NewSymbol("?reason"),
				},
			},
		},
	}

	syms := query.ScopeOf(orJoin, nil)

	requiresSet := make(map[query.Symbol]bool)
	for _, sym := range syms.Correlates {
		requiresSet[sym] = true
	}
	providesSet := make(map[query.Symbol]bool)
	for _, sym := range syms.Provides {
		providesSet[sym] = true
	}

	if !requiresSet[datalog.NewSymbol("?rxn")] {
		t.Errorf("join var ?rxn should be REQUIRED (not produced by all branches), got Requires: %v", syms.Correlates)
	}
	if providesSet[datalog.NewSymbol("?rxn")] {
		t.Errorf("join var ?rxn should NOT be in provides (branch 2 doesn't produce it), got Provides: %v", syms.Provides)
	}
	if providesSet[datalog.NewSymbol("?reason")] {
		t.Errorf("?reason is branch-local outside the header and must not escape, got Provides: %v", syms.Provides)
	}
}

func TestExpressionScope(t *testing.T) {
	tests := []struct {
		name             string
		expression       *query.Expression
		expectedProvides []query.Symbol
		expectedRequires []query.Symbol
	}{
		{
			name: "Ground function provides binding",
			expression: &query.Expression{
				Function: &query.GroundFunction{Value: int64(0)},
				Binding:  datalog.NewSymbol("?x"),
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?x")},
			expectedRequires: []query.Symbol{},
		},
		{
			name: "Arithmetic requires inputs, provides binding",
			expression: &query.Expression{
				Function: &query.ArithmeticFunction{
					Op: datalog.SymAdd,
					Args: []query.Term{
						query.VariableTerm{Symbol: datalog.NewSymbol("?a")},
						query.VariableTerm{Symbol: datalog.NewSymbol("?b")},
					},
				},
				Binding: datalog.NewSymbol("?sum"),
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?sum")},
			expectedRequires: []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syms := query.ScopeOf(tt.expression, nil)

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
			for _, sym := range syms.Correlates {
				requiresSet[sym] = true
			}
			for _, expected := range tt.expectedRequires {
				if !requiresSet[expected] {
					t.Errorf("Expected symbol %s to be required, but it wasn't. Got: %v", expected, syms.Correlates)
				}
			}
		})
	}
}
