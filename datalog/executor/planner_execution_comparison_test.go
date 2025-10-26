package executor

import (
	"fmt"
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPlannerExecutionComparison rigorously compares execution results from both planners
func TestPlannerExecutionComparison(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")
	prod1 := datalog.NewIdentity("prod1")
	prod2 := datalog.NewIdentity("prod2")
	order1 := datalog.NewIdentity("order1")
	order2 := datalog.NewIdentity("order2")
	order3 := datalog.NewIdentity("order3")

	datoms := []datalog.Datom{
		// People
		{E: alice, A: datalog.NewKeyword(":person/name"), V: "Alice", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":person/age"), V: int64(30), Tx: 1},
		{E: bob, A: datalog.NewKeyword(":person/name"), V: "Bob", Tx: 1},
		{E: bob, A: datalog.NewKeyword(":person/age"), V: int64(25), Tx: 1},
		{E: charlie, A: datalog.NewKeyword(":person/name"), V: "Charlie", Tx: 1},
		{E: charlie, A: datalog.NewKeyword(":person/age"), V: int64(35), Tx: 1},

		// Products
		{E: prod1, A: datalog.NewKeyword(":product/name"), V: "Widget", Tx: 1},
		{E: prod1, A: datalog.NewKeyword(":product/price"), V: 10.0, Tx: 1},
		{E: prod2, A: datalog.NewKeyword(":product/name"), V: "Gadget", Tx: 1},
		{E: prod2, A: datalog.NewKeyword(":product/price"), V: 20.0, Tx: 1},

		// Orders
		{E: order1, A: datalog.NewKeyword(":order/customer"), V: alice, Tx: 1},
		{E: order1, A: datalog.NewKeyword(":order/product"), V: prod1, Tx: 1},
		{E: order2, A: datalog.NewKeyword(":order/customer"), V: alice, Tx: 1},
		{E: order2, A: datalog.NewKeyword(":order/product"), V: prod2, Tx: 1},
		{E: order3, A: datalog.NewKeyword(":order/customer"), V: bob, Tx: 1},
		{E: order3, A: datalog.NewKeyword(":order/product"), V: prod1, Tx: 1},
	}

	tests := []struct {
		name  string
		query string
	}{
		{
			name: "simple pattern",
			query: `[:find ?e ?name
			         :where [?e :person/name ?name]]`,
		},
		{
			name: "join on entity",
			query: `[:find ?name ?age
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]]`,
		},
		{
			name: "predicate filter",
			query: `[:find ?name ?age
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]
			         [(> ?age 25)]]`,
		},
		{
			name: "expression",
			query: `[:find ?name ?doubled
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]
			         [(* ?age 2) ?doubled]]`,
		},
		{
			name: "multi-phase join",
			query: `[:find ?customer-name ?product-name
			         :where
			         [?person :person/name ?customer-name]
			         [?order :order/customer ?person]
			         [?order :order/product ?product]
			         [?product :product/name ?product-name]]`,
		},
		{
			name: "join with predicate",
			query: `[:find ?customer-name ?product-name ?price
			         :where
			         [?person :person/name ?customer-name]
			         [?person :person/age ?age]
			         [(>= ?age 30)]
			         [?order :order/customer ?person]
			         [?order :order/product ?product]
			         [?product :product/name ?product-name]
			         [?product :product/price ?price]]`,
		},
		{
			name: "multiple predicates",
			query: `[:find ?name ?age
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]
			         [(> ?age 20)]
			         [(< ?age 35)]]`,
		},
		{
			name: "expression with predicate",
			query: `[:find ?name ?doubled
			         :where
			         [?e :person/name ?name]
			         [?e :person/age ?age]
			         [(* ?age 2) ?doubled]
			         [(> ?doubled 50)]]`,
		},
		{
			name: "aggregation count",
			query: `[:find (count ?e)
			         :where [?e :person/name ?name]]`,
		},
		{
			name: "aggregation max",
			query: `[:find (max ?age)
			         :where [?e :person/age ?age]]`,
		},
		{
			name: "aggregation min",
			query: `[:find (min ?age)
			         :where [?e :person/age ?age]]`,
		},
		{
			name: "aggregation sum",
			query: `[:find (sum ?price)
			         :where [?p :product/price ?price]]`,
		},
		{
			name: "grouped aggregation",
			query: `[:find ?name (count ?order)
			         :where
			         [?person :person/name ?name]
			         [?order :order/customer ?person]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			matcher := NewMemoryPatternMatcher(datoms)

			// Execute with old planner
			oldExec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: false,
			})
			oldResult, err := oldExec.Execute(q)
			if err != nil {
				t.Fatalf("old planner execution failed: %v", err)
			}

			// Execute with new planner
			newExec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: true,
			})
			newExec.SetUseQueryExecutor(true) // Required for new planner
			newResult, err := newExec.Execute(q)
			if err != nil {
				t.Fatalf("new planner execution failed: %v", err)
			}

			// Compare columns
			if !columnsEqual(oldResult.Columns(), newResult.Columns()) {
				t.Errorf("column mismatch:\n  old=%v\n  new=%v",
					oldResult.Columns(), newResult.Columns())
				return
			}

			// Compare tuples (sorted for deterministic comparison)
			oldTuples := collectAndSortTuples(oldResult)
			newTuples := collectAndSortTuples(newResult)

			if len(oldTuples) != len(newTuples) {
				t.Errorf("result size mismatch: old=%d, new=%d",
					len(oldTuples), len(newTuples))
				t.Logf("Old results:")
				dumpRelation(t, oldResult)
				t.Logf("New results:")
				dumpRelation(t, newResult)
				return
			}

			for i := range oldTuples {
				if !tuplesEqualComparison(oldTuples[i], newTuples[i]) {
					t.Errorf("tuple %d differs:\n  old=%v\n  new=%v",
						i, formatTuple(oldTuples[i]), formatTuple(newTuples[i]))
				}
			}

			t.Logf("✓ Both planners produced %d identical tuples", len(oldTuples))
		})
	}
}

// Helper functions

func collectAndSortTuples(rel Relation) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}

	// Sort for deterministic comparison
	sort.Slice(tuples, func(i, j int) bool {
		return compareTuples(tuples[i], tuples[j]) < 0
	})

	return tuples
}

func compareTuples(a, b Tuple) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		cmp := compareValues(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}

	return len(a) - len(b)
}

func compareValues(a, b interface{}) int {
	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

func tuplesEqualComparison(a, b Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !valuesEqualComparison(a[i], b[i]) {
			return false
		}
	}
	return true
}

func valuesEqualComparison(a, b interface{}) bool {
	// Handle nil
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle Identity
	if aId, ok := a.(datalog.Identity); ok {
		if bId, ok := b.(datalog.Identity); ok {
			return aId.L85() == bId.L85()
		}
		return false
	}

	// Simple comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func formatTuple(t Tuple) string {
	return fmt.Sprintf("%v", t)
}

func dumpRelation(t *testing.T, rel Relation) {
	it := rel.Iterator()
	count := 0
	for it.Next() {
		t.Logf("  [%d] %v", count, it.Tuple())
		count++
	}
}

func columnsEqual(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
