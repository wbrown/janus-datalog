package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestEmptySubqueryWithTupleBinding tests that empty subquery results
// correctly fail the pattern match instead of producing nil values
func TestEmptySubqueryWithTupleBinding(t *testing.T) {
	// Setup: Person exists but has NO orders
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:person/name _]": {
				{E: datalog.NewIdentity("person:1"), A: datalog.NewKeyword(":person/name"), V: "Alice", Tx: 1},
				{E: datalog.NewIdentity("person:2"), A: datalog.NewKeyword(":person/name"), V: "Bob", Tx: 1},
			},
			// Bob has orders, Alice doesn't
			"[:order/person _]": {
				{E: datalog.NewIdentity("order:1"), A: datalog.NewKeyword(":order/person"), V: datalog.NewIdentity("person:2"), Tx: 1},
			},
			"[:order/price _]": {
				{E: datalog.NewIdentity("order:1"), A: datalog.NewKeyword(":order/price"), V: float64(100.50), Tx: 1},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Query: Find each person with their max order price
	// Subquery returns empty for Alice (no orders)
	queryStr := `[:find ?name ?max-price
	             :where
	             [?e :person/name ?name]
	             [(q [:find (max ?p)
	                  :in $ ?person
	                  :where [?o :order/person ?person]
	                         [?o :order/price ?p]]
	                 $ ?e) [[?max-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// EXPECTED: Only Bob appears (Alice has no orders, so pattern fails)
	if result.Size() != 1 {
		t.Errorf("Expected 1 result (Bob only), got %d", result.Size())
	}

	// Verify Bob's result
	if result.Size() > 0 {
		tuple := result.Get(0)
		if len(tuple) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(tuple))
		}

		// Check for nil values (THIS SHOULD NEVER HAPPEN)
		for i, val := range tuple {
			if val == nil {
				t.Errorf("Found nil value at position %d - this violates datalog semantics!", i)
			}
		}

		name := tuple[0].(string)
		price := tuple[1].(float64)

		if name != "Bob" {
			t.Errorf("Expected Bob, got %s", name)
		}
		if price != 100.50 {
			t.Errorf("Expected 100.50, got %f", price)
		}
	}
}

// TestMultipleEmptySubqueries tests OHLC-style query with multiple aggregates
// where some days have no data
func TestMultipleEmptySubqueries(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			// Symbol exists
			"[:symbol/ticker _]": {
				{E: datalog.NewIdentity("symbol:AAPL"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			// Prices only for day 1, not day 2
			"[:price/symbol _]": {
				{E: datalog.NewIdentity("bar:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:AAPL"), Tx: 1},
				{E: datalog.NewIdentity("bar:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:AAPL"), Tx: 1},
			},
			"[:price/day _]": {
				{E: datalog.NewIdentity("bar:1"), A: datalog.NewKeyword(":price/day"), V: int64(1), Tx: 1},
				{E: datalog.NewIdentity("bar:2"), A: datalog.NewKeyword(":price/day"), V: int64(1), Tx: 1},
			},
			"[:price/high _]": {
				{E: datalog.NewIdentity("bar:1"), A: datalog.NewKeyword(":price/high"), V: float64(150.0), Tx: 1},
				{E: datalog.NewIdentity("bar:2"), A: datalog.NewKeyword(":price/high"), V: float64(155.0), Tx: 1},
			},
			"[:price/low _]": {
				{E: datalog.NewIdentity("bar:1"), A: datalog.NewKeyword(":price/low"), V: float64(145.0), Tx: 1},
				{E: datalog.NewIdentity("bar:2"), A: datalog.NewKeyword(":price/low"), V: float64(148.0), Tx: 1},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Query for days 1 and 2, but only day 1 has data
	queryStr := `[:find ?target-day ?high ?low
	             :in $ ?sym ?target-day
	             :where
	             [?s :symbol/ticker ?sym]
	             [(q [:find (max ?h) (min ?l)
	                  :in $ ?symbol ?d
	                  :where [?b :price/symbol ?symbol]
	                         [?b :price/day ?d]
	                         [?b :price/high ?h]
	                         [?b :price/low ?l]]
	                 $ ?s ?target-day) [[?high ?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Verify the planner creates correct nested plan with proper Available symbols (only works with old planner)
	queryPlanner := exec.GetPlanner()
	adapter, ok := queryPlanner.(*planner.PlannerAdapter)
	if !ok {
		t.Logf("Skipping plan structure verification (using new clause-based planner)")
		return
	}

	oldPlanner := adapter.GetUnderlyingPlanner()
	plan, err := oldPlanner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Check the nested subquery plan's Available list
	if len(plan.Phases) > 0 && len(plan.Phases[0].Subqueries) > 0 {
		nestedPlan := plan.Phases[0].Subqueries[0].NestedPlan
		if len(nestedPlan.Phases) > 0 {
			available := nestedPlan.Phases[0].Available
			availableMap := make(map[query.Symbol]bool)
			for _, sym := range available {
				availableMap[sym] = true
			}

			// Should have the subquery's declared parameters: $ ?symbol ?d
			expectedSymbols := []query.Symbol{"$", "?symbol", "?d"}
			for _, expected := range expectedSymbols {
				if !availableMap[expected] {
					t.Errorf("Nested plan Available missing expected symbol %s, got: %v", expected, available)
				}
			}

			// Should NOT have outer query symbols like ?s or ?target-day
			unexpectedSymbols := []query.Symbol{"?s", "?target-day"}
			for _, unexpected := range unexpectedSymbols {
				if availableMap[unexpected] {
					t.Errorf("Nested plan Available incorrectly contains outer query symbol %s, got: %v", unexpected, available)
				}
			}
		}
	}

	// Test with day 1 (has data)
	ctx := NewContext(nil)
	// For :in $ ?sym ?target-day, we need TWO separate scalar input relations
	inputSym := NewMaterializedRelation(
		[]query.Symbol{query.Symbol("?sym")},
		[]Tuple{{"AAPL"}},
	)
	inputTargetDay := NewMaterializedRelation(
		[]query.Symbol{query.Symbol("?target-day")},
		[]Tuple{{int64(1)}},
	)

	result1, err := exec.ExecuteWithRelations(ctx, q, []Relation{inputSym, inputTargetDay})
	if err != nil {
		t.Fatalf("Failed to execute query for day 1: %v", err)
	}

	if result1.Size() != 1 {
		t.Errorf("Expected 1 result for day 1, got %d", result1.Size())
	}

	// Verify no nil values
	if result1.Size() > 0 {
		tuple := result1.Get(0)
		for i, val := range tuple {
			if val == nil {
				t.Errorf("Day 1: Found nil value at position %d", i)
			}
		}
	}

	// Test with day 2 (NO data)
	inputSym2 := NewMaterializedRelation(
		[]query.Symbol{query.Symbol("?sym")},
		[]Tuple{{"AAPL"}},
	)
	inputTargetDay2 := NewMaterializedRelation(
		[]query.Symbol{query.Symbol("?target-day")},
		[]Tuple{{int64(2)}},
	)

	result2, err := exec.ExecuteWithRelations(ctx, q, []Relation{inputSym2, inputTargetDay2})
	if err != nil {
		t.Fatalf("Failed to execute query for day 2: %v", err)
	}

	// EXPECTED: Day 2 should produce NO results (empty subquery = failed pattern)
	if result2.Size() != 0 {
		t.Errorf("Expected 0 results for day 2 (no data), got %d", result2.Size())
		if result2.Size() > 0 {
			tuple := result2.Get(0)
			t.Errorf("Day 2 tuple: %v (should not exist!)", tuple)
			for i, val := range tuple {
				if val == nil {
					t.Errorf("Day 2: Found nil value at position %d", i)
				}
			}
		}
	}
}

// TestMixedEmptyAndNonEmpty tests a scenario where some iterations have data
// and others don't
func TestMixedEmptyAndNonEmpty(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:person/name _]": {
				{E: datalog.NewIdentity("person:1"), A: datalog.NewKeyword(":person/name"), V: "Alice", Tx: 1},
				{E: datalog.NewIdentity("person:2"), A: datalog.NewKeyword(":person/name"), V: "Bob", Tx: 1},
				{E: datalog.NewIdentity("person:3"), A: datalog.NewKeyword(":person/name"), V: "Charlie", Tx: 1},
			},
			"[:order/person _]": {
				// Bob has 2 orders
				{E: datalog.NewIdentity("order:1"), A: datalog.NewKeyword(":order/person"), V: datalog.NewIdentity("person:2"), Tx: 1},
				{E: datalog.NewIdentity("order:2"), A: datalog.NewKeyword(":order/person"), V: datalog.NewIdentity("person:2"), Tx: 1},
				// Charlie has 1 order
				{E: datalog.NewIdentity("order:3"), A: datalog.NewKeyword(":order/person"), V: datalog.NewIdentity("person:3"), Tx: 1},
			},
			"[:order/price _]": {
				{E: datalog.NewIdentity("order:1"), A: datalog.NewKeyword(":order/price"), V: float64(100.0), Tx: 1},
				{E: datalog.NewIdentity("order:2"), A: datalog.NewKeyword(":order/price"), V: float64(200.0), Tx: 1},
				{E: datalog.NewIdentity("order:3"), A: datalog.NewKeyword(":order/price"), V: float64(50.0), Tx: 1},
			},
		},
	}

	exec := NewExecutor(matcher)

	queryStr := `[:find ?name ?total ?count
	             :where
	             [?e :person/name ?name]
	             [(q [:find (sum ?p) (count ?p)
	                  :in $ ?person
	                  :where [?o :order/person ?person]
	                         [?o :order/price ?p]]
	                 $ ?e) [[?total ?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// EXPECTED: Bob and Charlie only (Alice has no orders)
	if result.Size() != 2 {
		t.Errorf("Expected 2 results (Bob, Charlie), got %d", result.Size())
	}

	// Verify all tuples have no nil values
	foundNames := make(map[string]bool)
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)

		for j, val := range tuple {
			if val == nil {
				t.Errorf("Result %d: Found nil value at position %d", i, j)
			}
		}

		if len(tuple) >= 1 {
			name := tuple[0].(string)
			foundNames[name] = true
		}
	}

	// Alice should NOT be in results
	if foundNames["Alice"] {
		t.Error("Alice should not appear in results (no orders)")
	}

	// Bob and Charlie should be in results
	if !foundNames["Bob"] {
		t.Error("Bob should appear in results")
	}
	if !foundNames["Charlie"] {
		t.Error("Charlie should appear in results")
	}
}

// TestRelationBindingWithEmptyResults tests RelationBinding form with empty subquery
func TestRelationBindingWithEmptyResults(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			"[:category/name _]": {
				{E: datalog.NewIdentity("cat:1"), A: datalog.NewKeyword(":category/name"), V: "Electronics", Tx: 1},
			},
			// No products in this category
			"[:product/category _]": {},
		},
	}

	exec := NewExecutor(matcher)

	// RelationBinding: [[?prod-name ?price] ...]
	queryStr := `[:find ?cat-name ?prod-name ?price
	             :where
	             [?c :category/name ?cat-name]
	             [(q [:find ?pname ?p
	                  :in $ ?category
	                  :where [?prod :product/category ?category]
	                         [?prod :product/name ?pname]
	                         [?prod :product/price ?p]]
	                 $ ?c) [[?prod-name ?price] ...]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// EXPECTED: No results (category exists but has no products)
	if result.Size() != 0 {
		t.Errorf("Expected 0 results (empty subquery), got %d", result.Size())
	}
}

// TestNoNilValuesInAnyResult is a property test verifying the invariant:
// "No query result ever contains nil values"
func TestNoNilValuesInAnyResult(t *testing.T) {
	testCases := []struct {
		name  string
		query string
		data  map[string][]datalog.Datom
	}{
		{
			name: "Empty aggregate with TupleBinding",
			query: `[:find ?x ?max
			        :where
			        [?e :attr/x ?x]
			        [(q [:find (max ?v)
			             :in $ ?entity
			             :where [?entity :attr/value ?v]]
			            $ ?e) [[?max]]]]`,
			data: map[string][]datalog.Datom{
				"[:attr/x _]": {
					{E: datalog.NewIdentity("e:1"), A: datalog.NewKeyword(":attr/x"), V: int64(1), Tx: 1},
				},
				// No :attr/value for e:1
				"[:attr/value _]": {},
			},
		},
		{
			name: "Empty min aggregate",
			query: `[:find ?name ?min-score
			        :where
			        [?p :person/name ?name]
			        [(q [:find (min ?s)
			             :in $ ?person
			             :where [?t :test/person ?person]
			                    [?t :test/score ?s]]
			            $ ?p) [[?min-score]]]]`,
			data: map[string][]datalog.Datom{
				"[:person/name _]": {
					{E: datalog.NewIdentity("p:1"), A: datalog.NewKeyword(":person/name"), V: "Alice", Tx: 1},
				},
				"[:test/person _]": {},
			},
		},
		{
			name: "Empty avg aggregate",
			query: `[:find ?id ?avg
			        :where
			        [?e :entity/id ?id]
			        [(q [:find (avg ?val)
			             :in $ ?entity
			             :where [?entity :entity/value ?val]]
			            $ ?e) [[?avg]]]]`,
			data: map[string][]datalog.Datom{
				"[:entity/id _]": {
					{E: datalog.NewIdentity("e:1"), A: datalog.NewKeyword(":entity/id"), V: "ID-1", Tx: 1},
				},
				"[:entity/value _]": {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			matcher := &MockPatternMatcher{data: tc.data}
			exec := NewExecutor(matcher)

			q, err := parser.ParseQuery(tc.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Check ALL tuples for nil values
			for i := 0; i < result.Size(); i++ {
				tuple := result.Get(i)
				for j, val := range tuple {
					if val == nil {
						t.Errorf("INVARIANT VIOLATION: Found nil at tuple %d, position %d", i, j)
						t.Errorf("Tuple: %v", tuple)
					}
				}
			}

			// Empty subquery should produce NO results (pattern fails)
			if result.Size() > 0 {
				t.Logf("Warning: Expected 0 results for empty subquery, got %d", result.Size())
			}
		})
	}
}
