package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExtractOrClauseSymbols(t *testing.T) {
	tests := []struct {
		name             string
		orClause         *query.OrClause
		expectedProvides []query.Symbol
	}{
		{
			name: "Pattern and expression both provide same variable",
			orClause: &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?e")},
								query.Constant{Value: datalog.NewKeyword(":test/attr")},
								query.Variable{Name: datalog.NewSymbol("?x")},
							},
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: int64(0)},
							Binding:  datalog.NewSymbol("?x"),
						},
					},
				},
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?x")},
		},
		{
			name: "Two patterns providing same variables",
			orClause: &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?e")},
								query.Constant{Value: datalog.NewKeyword(":attr1")},
								query.Variable{Name: datalog.NewSymbol("?x")},
							},
						},
					},
					{
						&query.DataPattern{
							Elements: []query.PatternElement{
								query.Variable{Name: datalog.NewSymbol("?e")},
								query.Constant{Value: datalog.NewKeyword(":attr2")},
								query.Variable{Name: datalog.NewSymbol("?x")},
							},
						},
					},
				},
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?x")},
		},
		{
			name: "Expression-only branches",
			orClause: &query.OrClause{
				Branches: [][]query.Clause{
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "first"},
							Binding:  datalog.NewSymbol("?x"),
						},
					},
					{
						&query.Expression{
							Function: &query.GroundFunction{Value: "second"},
							Binding:  datalog.NewSymbol("?x"),
						},
					},
				},
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?x")},
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

func TestExtractSubqueryPatternSymbols(t *testing.T) {
	tests := []struct {
		name             string
		pattern          *query.SubqueryPattern
		expectedProvides []query.Symbol
		expectedRequires []query.Symbol
	}{
		{
			name: "TupleBinding provides variables",
			pattern: &query.SubqueryPattern{
				Query: &query.Query{
					Find: []query.FindElement{
						query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?t")},
					},
				},
				Inputs: []query.PatternElement{
					query.Constant{Value: "db"},
					query.Variable{Name: datalog.NewSymbol("?scenario")},
				},
				Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?openingCount")}},
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
			syms := extractSubqueryPatternSymbols(tt.pattern)

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

func TestOrClauseSymbolsUnionVsIntersection(t *testing.T) {
	// Test that pattern-only OR uses intersection semantics
	// and OR with expressions uses union semantics

	t.Run("Pattern-only OR uses intersection", func(t *testing.T) {
		// Branch 1 provides: ?e, ?x
		// Branch 2 provides: ?f, ?x
		// Intersection should be: ?x only
		orClause := &query.OrClause{
			Branches: [][]query.Clause{
				{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: datalog.NewKeyword(":attr1")},
							query.Variable{Name: datalog.NewSymbol("?x")},
						},
					},
				},
				{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?f")},
							query.Constant{Value: datalog.NewKeyword(":attr2")},
							query.Variable{Name: datalog.NewSymbol("?x")},
						},
					},
				},
			},
		}

		syms := extractOrClauseSymbols(orClause)

		// Should only have ?x (intersection)
		if len(syms.Provides) != 1 {
			t.Errorf("Expected 1 symbol (intersection), got %d: %v", len(syms.Provides), syms.Provides)
		}
		if len(syms.Provides) == 1 && syms.Provides[0] != datalog.NewSymbol("?x") {
			t.Errorf("Expected ?x, got %v", syms.Provides[0])
		}
	})

	t.Run("OR with expression uses intersection", func(t *testing.T) {
		// Branch 1 (pattern) provides: ?e, ?x
		// Branch 2 (expression) provides: ?x
		// OrClause always uses intersection now (all branches execute in union mode)
		orClause := &query.OrClause{
			Branches: [][]query.Clause{
				{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: datalog.NewKeyword(":test/attr")},
							query.Variable{Name: datalog.NewSymbol("?x")},
						},
					},
				},
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(0)},
						Binding:  datalog.NewSymbol("?x"),
					},
				},
			},
		}

		syms := extractOrClauseSymbols(orClause)

		// Intersection: only ?x is common to both branches
		providedSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Provides {
			providedSet[sym] = true
		}

		if !providedSet[datalog.NewSymbol("?x")] {
			t.Errorf("Expected ?x to be provided (intersection), got: %v", syms.Provides)
		}
		if providedSet[datalog.NewSymbol("?e")] {
			t.Errorf("?e should NOT be provided (intersection: not in branch 2), got: %v", syms.Provides)
		}
	})

	t.Run("OR-DEFAULT with expression uses union", func(t *testing.T) {
		// Same branches but OrDefaultClause — uses union provides (fallback: one branch executes)
		orDefaultClause := &query.OrDefaultClause{
			Branches: [][]query.Clause{
				{
					&query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: datalog.NewSymbol("?e")},
							query.Constant{Value: datalog.NewKeyword(":test/attr")},
							query.Variable{Name: datalog.NewSymbol("?x")},
						},
					},
				},
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(0)},
						Binding:  datalog.NewSymbol("?x"),
					},
				},
			},
		}

		syms := extractOrDefaultClauseSymbols(orDefaultClause)

		providedSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Provides {
			providedSet[sym] = true
		}

		if !providedSet[datalog.NewSymbol("?x")] {
			t.Errorf("Expected ?x to be provided, got: %v", syms.Provides)
		}
		if !providedSet[datalog.NewSymbol("?e")] {
			t.Errorf("Expected ?e to be provided (union semantics), got: %v", syms.Provides)
		}
	})

	t.Run("OrDefault SubqueryPattern with ground fallback uses union", func(t *testing.T) {
		// Branch 1 (SubqueryPattern) provides: ?openingCount
		// Branch 2 (Expression) provides: ?openingCount
		// Union should be: ?openingCount
		// This is the real-world use case — uses OrDefaultClause for fallback
		orClause := &query.OrDefaultClause{
			Branches: [][]query.Clause{
				{
					&query.SubqueryPattern{
						Query: &query.Query{
							Find: []query.FindElement{
								query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?t")},
							},
						},
						Inputs: []query.PatternElement{
							query.Constant{Value: "db"},
							query.Variable{Name: datalog.NewSymbol("?scenario")},
						},
						Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?openingCount")}},
					},
				},
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(0)},
						Binding:  datalog.NewSymbol("?openingCount"),
					},
				},
			},
		}

		syms := extractOrDefaultClauseSymbols(orClause)

		foundOpeningCount := false
		for _, sym := range syms.Provides {
			if sym == datalog.NewSymbol("?openingCount") {
				foundOpeningCount = true
			}
		}

		if !foundOpeningCount {
			t.Errorf("Expected ?openingCount to be provided, got: %v", syms.Provides)
		}
	})
}

func TestOrClauseRequiresCorrelatedInputs(t *testing.T) {
	// This test verifies that OR clauses with correlated subqueries
	// correctly report their required input symbols.
	// This was the root cause of the v0.5.1 regression where OR clauses
	// were scheduled before the patterns that provide their inputs.

	t.Run("SubqueryPattern with variable input requires that variable", func(t *testing.T) {
		orClause := &query.OrClause{
			Branches: [][]query.Clause{
				{
					&query.SubqueryPattern{
						Query: &query.Query{
							Find: []query.FindElement{
								query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?t")},
							},
						},
						Inputs: []query.PatternElement{
							query.Constant{Value: "db"},
							query.Variable{Name: datalog.NewSymbol("?scenario")}, // This is a correlated input
						},
						Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?count")}},
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

		syms := extractOrClauseSymbols(orClause)

		// The OR clause MUST require ?scenario because the subquery needs it
		requiresSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Requires {
			requiresSet[sym] = true
		}

		if !requiresSet[datalog.NewSymbol("?scenario")] {
			t.Errorf("OR clause with correlated subquery must require ?scenario, got Requires: %v", syms.Requires)
		}

		// Should still provide ?count
		providesSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Provides {
			providesSet[sym] = true
		}

		if !providesSet[datalog.NewSymbol("?count")] {
			t.Errorf("OR clause should provide ?count, got Provides: %v", syms.Provides)
		}
	})

	t.Run("Ground-only branch requires nothing", func(t *testing.T) {
		orClause := &query.OrClause{
			Branches: [][]query.Clause{
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(0)},
						Binding:  datalog.NewSymbol("?x"),
					},
				},
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(1)},
						Binding:  datalog.NewSymbol("?x"),
					},
				},
			},
		}

		syms := extractOrClauseSymbols(orClause)

		if len(syms.Requires) != 0 {
			t.Errorf("OR with only ground expressions should require nothing, got: %v", syms.Requires)
		}
	})

	t.Run("Expression requiring variable propagates requirement", func(t *testing.T) {
		// Branch with arithmetic that needs ?input
		orClause := &query.OrClause{
			Branches: [][]query.Clause{
				{
					&query.Expression{
						Function: &query.ArithmeticFunction{
							Op:    query.OpAdd,
							Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?input")},
							Right: query.ConstantTerm{Value: int64(1)},
						},
						Binding: datalog.NewSymbol("?output"),
					},
				},
				{
					&query.Expression{
						Function: &query.GroundFunction{Value: int64(0)},
						Binding:  datalog.NewSymbol("?output"),
					},
				},
			},
		}

		syms := extractOrClauseSymbols(orClause)

		requiresSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Requires {
			requiresSet[sym] = true
		}

		if !requiresSet[datalog.NewSymbol("?input")] {
			t.Errorf("OR clause should require ?input from arithmetic expression, got: %v", syms.Requires)
		}
	})
}

func TestOrWithSubqueryPatternAndFallback(t *testing.T) {
	// This test verifies the fix for the planner not recognizing
	// that SubqueryPattern provides symbols in OR fallback branches
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.SubqueryPattern{
					Query: &query.Query{
						Find: []query.FindElement{
							query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?t")},
						},
					},
					Inputs: []query.PatternElement{
						query.Constant{Value: "db"},
						query.Variable{Name: datalog.NewSymbol("?scenario")},
					},
					Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?openingCount")}},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?openingCount"),
				},
			},
		},
	}

	syms := extractOrClauseSymbols(orClause)

	// Both branches provide ?openingCount, so the OR should provide it
	foundOpeningCount := false
	for _, sym := range syms.Provides {
		if sym == datalog.NewSymbol("?openingCount") {
			foundOpeningCount = true
		}
	}

	if !foundOpeningCount {
		t.Errorf("Expected ?openingCount to be provided by OR clause, but got: %v", syms.Provides)
	}
}

func TestOrJoinClauseRequiresCorrelatedInputs(t *testing.T) {
	// Verify OR-JOIN also correctly propagates requirements from branches
	t.Run("OR-JOIN with correlated subquery requires input variable", func(t *testing.T) {
		orJoin := &query.OrJoinClause{
			JoinVars: []query.Symbol{datalog.NewSymbol("?count")},
			Branches: [][]query.Clause{
				{
					&query.SubqueryPattern{
						Query: &query.Query{
							Find: []query.FindElement{
								query.FindAggregate{Function: "count", Arg: datalog.NewSymbol("?t")},
							},
						},
						Inputs: []query.PatternElement{
							query.Constant{Value: "db"},
							query.Variable{Name: datalog.NewSymbol("?entity")}, // Correlated input
						},
						Binding: query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?count")}},
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

		syms := extractOrJoinClauseSymbols(orJoin)

		requiresSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Requires {
			requiresSet[sym] = true
		}

		if !requiresSet[datalog.NewSymbol("?entity")] {
			t.Errorf("OR-JOIN with correlated subquery must require ?entity, got Requires: %v", syms.Requires)
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

// TestOrJoinClauseJoinVarRequiredNotProvided verifies that join vars
// not produced by all branches are in requires (not provides).
// This is the Rule 5 get-else pattern: (or-join [?rxn] [?rxn :attr ?v] [(ground "") ?v])
// Branch 1 produces ?rxn, branch 2 does not. ?rxn must be required from outside.
func TestOrJoinClauseJoinVarRequiredNotProvided(t *testing.T) {
	orJoin := &query.OrJoinClause{
		JoinVars: []query.Symbol{datalog.NewSymbol("?rxn")},
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

	syms := extractOrJoinClauseSymbols(orJoin)

	requiresSet := make(map[query.Symbol]bool)
	for _, sym := range syms.Requires {
		requiresSet[sym] = true
	}
	providesSet := make(map[query.Symbol]bool)
	for _, sym := range syms.Provides {
		providesSet[sym] = true
	}

	if !requiresSet[datalog.NewSymbol("?rxn")] {
		t.Errorf("join var ?rxn should be REQUIRED (not produced by all branches), got Requires: %v", syms.Requires)
	}
	if providesSet[datalog.NewSymbol("?rxn")] {
		t.Errorf("join var ?rxn should NOT be in provides (branch 2 doesn't produce it), got Provides: %v", syms.Provides)
	}
	if !providesSet[datalog.NewSymbol("?reason")] {
		t.Errorf("?reason should be provided (produced by all branches), got Provides: %v", syms.Provides)
	}
}

func TestNotClauseRequiresAllInnerVariables(t *testing.T) {
	// Verify NOT clauses require all variables from inner clauses
	t.Run("NOT requires variables from inner pattern", func(t *testing.T) {
		notClause := &query.NotClause{
			Clauses: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":user/archived")},
						query.Constant{Value: true},
					},
				},
			},
		}

		syms := extractNotClauseSymbols(notClause)

		requiresSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Requires {
			requiresSet[sym] = true
		}

		if !requiresSet[datalog.NewSymbol("?e")] {
			t.Errorf("NOT clause must require ?e from inner pattern, got Requires: %v", syms.Requires)
		}

		if len(syms.Provides) != 0 {
			t.Errorf("NOT clause should not provide any symbols, got Provides: %v", syms.Provides)
		}
	})

	t.Run("NOT requires variables from inner expression", func(t *testing.T) {
		notClause := &query.NotClause{
			Clauses: []query.Clause{
				&query.Expression{
					Function: &query.ArithmeticFunction{
						Op:    query.OpAdd,
						Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?count")},
						Right: query.ConstantTerm{Value: int64(10)},
					},
					Binding: datalog.NewSymbol("?result"),
				},
			},
		}

		syms := extractNotClauseSymbols(notClause)

		requiresSet := make(map[query.Symbol]bool)
		for _, sym := range syms.Requires {
			requiresSet[sym] = true
		}

		// Should require ?count (input to expression) and ?result (output of expression)
		if !requiresSet[datalog.NewSymbol("?count")] {
			t.Errorf("NOT clause must require ?count, got Requires: %v", syms.Requires)
		}
		if !requiresSet[datalog.NewSymbol("?result")] {
			t.Errorf("NOT clause must require ?result, got Requires: %v", syms.Requires)
		}
	})
}

func TestExtractExpressionSymbols(t *testing.T) {
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
					Op:    query.OpAdd,
					Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?a")},
					Right: query.VariableTerm{Symbol: datalog.NewSymbol("?b")},
				},
				Binding: datalog.NewSymbol("?sum"),
			},
			expectedProvides: []query.Symbol{datalog.NewSymbol("?sum")},
			expectedRequires: []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
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
