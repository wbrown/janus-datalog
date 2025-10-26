package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestSimpleDecorrelation tests decorrelation with a minimal example
func TestSimpleDecorrelation(t *testing.T) {
	// Create simple test data: products in categories with prices
	prodA := datalog.NewIdentity("prod-a")
	prodB := datalog.NewIdentity("prod-b")
	prodC := datalog.NewIdentity("prod-c")
	cat1 := datalog.NewIdentity("cat-1")
	cat2 := datalog.NewIdentity("cat-2")

	datoms := []datalog.Datom{
		// Category 1 products
		{E: prodA, A: datalog.NewKeyword(":product/category"), V: cat1, Tx: 1},
		{E: prodA, A: datalog.NewKeyword(":product/price"), V: 10.0, Tx: 1},
		{E: prodB, A: datalog.NewKeyword(":product/category"), V: cat1, Tx: 1},
		{E: prodB, A: datalog.NewKeyword(":product/price"), V: 20.0, Tx: 1},

		// Category 2 products
		{E: prodC, A: datalog.NewKeyword(":product/category"), V: cat2, Tx: 1},
		{E: prodC, A: datalog.NewKeyword(":product/price"), V: 30.0, Tx: 1},

		// Category names
		{E: cat1, A: datalog.NewKeyword(":category/name"), V: "Electronics", Tx: 1},
		{E: cat2, A: datalog.NewKeyword(":category/name"), V: "Books", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query: For each category, find max price and count of products
	queryStr := `[:find ?name ?max-price ?count
	             :where
	               [?c :category/name ?name]

	               ; SubQ1: Max price in category
	               [(q [:find (max ?p)
	                    :in $ ?cat
	                    :where [?prod :product/category ?cat]
	                           [?prod :product/price ?p]]
	                  $ ?c) [[?max-price]]]

	               ; SubQ2: Count products in category
	               [(q [:find (count ?prod)
	                    :in $ ?cat
	                    :where [?prod :product/category ?cat]]
	                  $ ?c) [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Execute WITHOUT decorrelation
	execNoDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	resultNoDecor, err := execNoDecor.Execute(q)
	if err != nil {
		t.Fatalf("Sequential execution failed: %v", err)
	}

	t.Logf("Sequential: %d results", resultNoDecor.Size())
	for i := 0; i < resultNoDecor.Size(); i++ {
		t.Logf("  %v", resultNoDecor.Get(i))
	}

	// Execute WITH decorrelation
	execWithDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	resultWithDecor, err := execWithDecor.Execute(q)
	if err != nil {
		t.Fatalf("Decorrelated execution failed: %v", err)
	}

	t.Logf("Decorrelated: %d results", resultWithDecor.Size())
	for i := 0; i < resultWithDecor.Size(); i++ {
		t.Logf("  %v", resultWithDecor.Get(i))
	}

	// Verify results match
	if resultNoDecor.Size() != resultWithDecor.Size() {
		t.Errorf("Size mismatch: sequential=%d, decorrelated=%d",
			resultNoDecor.Size(), resultWithDecor.Size())
	}

	// Should have 2 categories
	if resultNoDecor.Size() != 2 {
		t.Errorf("Expected 2 categories, got %d", resultNoDecor.Size())
	}

	// Verify decorrelation actually happened (only works with old planner)
	if adapter, ok := execWithDecor.planner.(*planner.PlannerAdapter); ok {
		oldPlanner := adapter.GetUnderlyingPlanner()
		testPlan, _ := oldPlanner.Plan(q)
		foundDecor := false
		totalDecorGroups := 0
		for i, phase := range testPlan.Phases {
			if len(phase.DecorrelatedSubqueries) > 0 {
				foundDecor = true
				t.Logf("Phase %d: Found %d decorrelated subquery groups", i, len(phase.DecorrelatedSubqueries))
				totalDecorGroups += len(phase.DecorrelatedSubqueries)
				for j, decor := range phase.DecorrelatedSubqueries {
					t.Logf("  Group %d: %d original subqueries, %d filter groups",
						j, len(decor.OriginalSubqueries), len(decor.FilterGroups))
				}
			}
			if len(phase.Subqueries) > 0 {
				t.Logf("Phase %d: %d subqueries", i, len(phase.Subqueries))
				for j, sq := range phase.Subqueries {
					t.Logf("  Subquery %d: decorrelated=%v", j, sq.Decorrelated)
				}
			}
		}

		// After decorrelation bug fix: Both subqueries are PURE aggregations
		// ([:find (max ?p)] and [:find (count ?prod)])
		// Pure aggregations should NOT be decorrelated because adding grouping keys breaks them.
		// Therefore we expect decorrelation to NOT be applied.
		if foundDecor {
			t.Error("Decorrelation was incorrectly applied to pure aggregations!")
		} else {
			t.Logf("SUCCESS: Pure aggregations correctly skipped decorrelation")
		}
	} else {
		t.Logf("Skipping decorrelation verification (using new clause-based planner)")
	}
}
