package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// MockMatcher implements PatternMatcher for testing
type MockMatcher struct {
	matchFunc func(*query.DataPattern, Relations) (Relation, error)
}

func (m *MockMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	if m.matchFunc != nil {
		return m.matchFunc(pattern, bindings)
	}
	// Default: return empty relation
	return NewMaterializedRelation(nil, nil), nil
}

// TestProductRelation tests the Product() operation
func TestProductRelation(t *testing.T) {
	t.Run("empty relations", func(t *testing.T) {
		rel := Relations(nil).Product()
		if rel == nil {
			t.Fatal("Product() should not return nil for empty input")
		}
		if !rel.IsEmpty() {
			t.Error("Product() of empty relations should be empty")
		}
	})

	t.Run("single relation passthrough", func(t *testing.T) {
		r1 := NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{int64(1)}, {int64(2)}},
		)
		product := Relations([]Relation{r1}).Product()
		if product.Size() != 2 {
			t.Errorf("Expected size 2, got %d", product.Size())
		}
	})

	t.Run("cartesian product", func(t *testing.T) {
		r1 := NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{int64(1)}, {int64(2)}},
		)
		r2 := NewMaterializedRelation(
			[]query.Symbol{"?y"},
			[]Tuple{{int64(10)}, {int64(20)}, {int64(30)}},
		)

		product := Relations([]Relation{r1, r2}).Product()

		// Should have 2 * 3 = 6 tuples
		if product.Size() != 6 {
			t.Errorf("Expected 6 tuples, got %d", product.Size())
		}

		// Check columns
		cols := product.Columns()
		if len(cols) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(cols))
		}
		if cols[0] != "?x" || cols[1] != "?y" {
			t.Errorf("Expected columns [?x ?y], got %v", cols)
		}

		// Verify all combinations exist
		expected := map[string]bool{
			"1,10": true, "1,20": true, "1,30": true,
			"2,10": true, "2,20": true, "2,30": true,
		}

		it := product.Iterator()
		defer it.Close()
		for it.Next() {
			tuple := it.Tuple()
			key := tuple[0].(int64)
			val := tuple[1].(int64)
			k := string(rune(key+'0')) + "," + string(rune(val/10+'0')) + string(rune(val%10+'0'))
			if !expected[k] {
				t.Errorf("Unexpected tuple: %v", tuple)
			}
			delete(expected, k)
		}

		if len(expected) > 0 {
			t.Errorf("Missing tuples: %v", expected)
		}
	})

	t.Run("three relation product", func(t *testing.T) {
		r1 := NewMaterializedRelation([]query.Symbol{"?x"}, []Tuple{{int64(1)}, {int64(2)}})
		r2 := NewMaterializedRelation([]query.Symbol{"?y"}, []Tuple{{int64(10)}, {int64(20)}})
		r3 := NewMaterializedRelation([]query.Symbol{"?z"}, []Tuple{{int64(100)}})

		product := Relations([]Relation{r1, r2, r3}).Product()

		// Should have 2 * 2 * 1 = 4 tuples
		if product.Size() != 4 {
			t.Errorf("Expected 4 tuples, got %d", product.Size())
		}

		// Check columns
		cols := product.Columns()
		if len(cols) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(cols))
		}
	})
}

// TestExecutePattern tests pattern execution
func TestExecutePattern(t *testing.T) {
	mockResult := NewMaterializedRelation(
		[]query.Symbol{"?e", "?v"},
		[]Tuple{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
		},
	)

	matcher := &MockMatcher{
		matchFunc: func(pattern *query.DataPattern, bindings Relations) (Relation, error) {
			return mockResult, nil
		},
	}

	executor := NewQueryExecutor(matcher, ExecutorOptions{})
	ctx := NewContext(nil)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: ":person/name"},
			query.Variable{Name: "?v"},
		},
	}

	result, err := executor.executePattern(ctx, pattern, nil)
	if err != nil {
		t.Fatalf("executePattern failed: %v", err)
	}

	if result.Size() != 2 {
		t.Errorf("Expected 2 tuples, got %d", result.Size())
	}
}

// TestExecuteExpression tests expression evaluation
func TestExecuteExpression(t *testing.T) {
	t.Run("single relation expression", func(t *testing.T) {
		executor := NewQueryExecutor(&MockMatcher{}, ExecutorOptions{})
		ctx := NewContext(nil)

		// Create a relation with ?x
		r := NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{int64(5)}, {int64(10)}},
		)

		// Expression: [(+ ?x 100) ?y]
		expr := &query.Expression{
			Function: &query.ArithmeticFunction{
				Op:    query.OpAdd,
				Left:  query.VariableTerm{Symbol: "?x"},
				Right: query.ConstantTerm{Value: int64(100)},
			},
			Binding: "?y",
		}

		groups, err := executor.executeExpression(ctx, expr, []Relation{r})
		if err != nil {
			t.Fatalf("executeExpression failed: %v", err)
		}

		if len(groups) != 1 {
			t.Fatalf("Expected 1 group, got %d", len(groups))
		}

		result := groups[0]
		if result.Size() != 2 {
			t.Errorf("Expected 2 tuples, got %d", result.Size())
		}

		// Check columns
		cols := result.Columns()
		if len(cols) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(cols))
		}
	})

	t.Run("multi-relation expression (Cartesian product)", func(t *testing.T) {
		executor := NewQueryExecutor(&MockMatcher{}, ExecutorOptions{})
		ctx := NewContext(nil)

		// Two disjoint relations
		r1 := NewMaterializedRelation([]query.Symbol{"?x"}, []Tuple{{int64(1)}, {int64(2)}})
		r2 := NewMaterializedRelation([]query.Symbol{"?y"}, []Tuple{{int64(10)}, {int64(20)}})

		// Expression: [(+ ?x ?y) ?z] - requires both ?x and ?y
		expr := &query.Expression{
			Function: &query.ArithmeticFunction{
				Op:    query.OpAdd,
				Left:  query.VariableTerm{Symbol: "?x"},
				Right: query.VariableTerm{Symbol: "?y"},
			},
			Binding: "?z",
		}

		groups, err := executor.executeExpression(ctx, expr, []Relation{r1, r2})
		if err != nil {
			t.Fatalf("executeExpression failed: %v", err)
		}

		if len(groups) != 1 {
			t.Fatalf("Expected 1 group, got %d", len(groups))
		}

		result := groups[0]
		// Should have 2 * 2 = 4 combinations
		if result.Size() != 4 {
			t.Errorf("Expected 4 tuples from Cartesian product, got %d", result.Size())
		}

		// Check columns: should have ?x, ?y, ?z
		cols := result.Columns()
		if len(cols) != 3 {
			t.Errorf("Expected 3 columns (?x, ?y, ?z), got %d: %v", len(cols), cols)
		}
	})
}

// TestExecutePredicate tests predicate filtering
func TestExecutePredicate(t *testing.T) {
	t.Run("single relation predicate", func(t *testing.T) {
		executor := NewQueryExecutor(&MockMatcher{}, ExecutorOptions{})
		ctx := NewContext(nil)

		// Create relation with ?x
		r := NewMaterializedRelation(
			[]query.Symbol{"?x"},
			[]Tuple{{int64(5)}, {int64(15)}, {int64(25)}},
		)

		// Predicate: [(< ?x 20)]
		pred := &query.Comparison{
			Op:    query.OpLT,
			Left:  query.VariableTerm{Symbol: "?x"},
			Right: query.ConstantTerm{Value: int64(20)},
		}

		groups, err := executor.executePredicate(ctx, pred, []Relation{r})
		if err != nil {
			t.Fatalf("executePredicate failed: %v", err)
		}

		if len(groups) != 1 {
			t.Fatalf("Expected 1 group, got %d", len(groups))
		}

		result := groups[0]
		// Should filter to values < 20: {5, 15}
		if result.Size() != 2 {
			t.Errorf("Expected 2 tuples after filtering, got %d", result.Size())
		}
	})
}

// TestExecuteAggregates tests aggregate handling
func TestExecuteAggregates(t *testing.T) {
	matcher := &MockMatcher{
		matchFunc: func(pattern *query.DataPattern, bindings Relations) (Relation, error) {
			// Return test data
			return NewMaterializedRelation(
				[]query.Symbol{"?person", "?value"},
				[]Tuple{
					{"Alice", int64(100)},
					{"Alice", int64(200)},
					{"Bob", int64(150)},
				},
			), nil
		},
	}

	executor := NewQueryExecutor(matcher, ExecutorOptions{})
	ctx := NewContext(nil)

	t.Run("grouped aggregation", func(t *testing.T) {
		// Query: [:find ?person (sum ?value)]
		q := &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: "?person"},
				query.FindAggregate{
					Function: "sum",
					Arg:      "?value",
				},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: "?person"},
						query.Constant{Value: ":person/value"},
						query.Variable{Name: "?value"},
					},
				},
			},
		}

		groups, err := executor.Execute(ctx, q, nil)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if len(groups) != 1 {
			t.Fatalf("Expected 1 result group, got %d", len(groups))
		}

		result := groups[0]
		// Should have 2 groups: Alice (sum=300), Bob (sum=150)
		if result.Size() != 2 {
			t.Errorf("Expected 2 aggregated tuples, got %d", result.Size())
		}
	})

	t.Run("error on multiple disjoint groups with aggregates", func(t *testing.T) {
		// This should error because we can't aggregate over disjoint relations
		q := &query.Query{
			Find: []query.FindElement{
				query.FindAggregate{
					Function: "sum",
					Arg:      "?value",
				},
			},
			Where: []query.Clause{
				&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: "?e1"},
					query.Constant{Value: ":person/name"},
					query.Variable{Name: "?n"},
				}},
			},
		}

		// Manually set up disjoint groups (this simulates the error case)
		r1 := NewMaterializedRelation([]query.Symbol{"?x"}, []Tuple{{int64(1)}})
		r2 := NewMaterializedRelation([]query.Symbol{"?y"}, []Tuple{{int64(2)}})

		// Try to aggregate with pre-existing disjoint groups
		_, err := executor.Execute(ctx, q, []Relation{r1, r2})
		if err == nil {
			t.Error("Expected error for aggregating over disjoint relations")
		}
	})
}

// TestEndToEndQueries tests complete query execution
func TestEndToEndQueries(t *testing.T) {
	matcher := &MockMatcher{
		matchFunc: func(pattern *query.DataPattern, bindings Relations) (Relation, error) {
			// Return different data based on the attribute
			attr := pattern.Elements[1]
			if c, ok := attr.(query.Constant); ok {
				if c.Value == ":person/age" {
					return NewMaterializedRelation(
						[]query.Symbol{"?person", "?age"},
						[]Tuple{
							{"Alice", int64(30)},
							{"Bob", int64(25)},
						},
					), nil
				}
			}
			return NewMaterializedRelation(nil, nil), nil
		},
	}

	executor := NewQueryExecutor(matcher, ExecutorOptions{})
	ctx := NewContext(nil)

	t.Run("pattern + predicate query", func(t *testing.T) {
		// Query: [:find ?person ?age :where [?person :person/age ?age] [(> ?age 26)]]
		q := &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: "?person"},
				query.FindVariable{Symbol: "?age"},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: "?person"},
						query.Constant{Value: ":person/age"},
						query.Variable{Name: "?age"},
					},
				},
				&query.Comparison{
					Op:    query.OpGT,
					Left:  query.VariableTerm{Symbol: "?age"},
					Right: query.ConstantTerm{Value: int64(26)},
				},
			},
		}

		groups, err := executor.Execute(ctx, q, nil)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if len(groups) != 1 {
			t.Fatalf("Expected 1 group, got %d", len(groups))
		}

		result := groups[0]
		// Should filter to ages > 26: only Alice (30)
		if result.Size() != 1 {
			t.Errorf("Expected 1 tuple after predicate, got %d", result.Size())
		}
	})
}
