package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestStrStartsWithBug reproduces the bug where str/starts-with? predicate
// cannot find the ?timeStr variable bound by [(str ?time) ?timeStr]
//
// Bug report: ../gopher-street/repro/str_starts_with_bug.md
//
// Query:
// [:find (count ?p)
//
//	:where [?s :symbol/ticker "CRWV"]
//	       [?p :price/symbol ?s]
//	       [?p :price/time ?time]
//	       [(str ?time) ?timeStr]
//	       [(str/starts-with? ?timeStr "2025-10")]]
//
// Error: predicate [(str/starts-with? ?timeStr "2025-10")] requires symbols
//
//	not available in relation group: [?timeStr]
//
// Expected: ?timeStr should be recognized as bound by the str expression
func TestStrStartsWithBug(t *testing.T) {
	// Create test data
	crwvSymbol := datalog.NewIdentity("symbol-1")

	oct1, _ := time.Parse("2006-01-02", "2025-10-01")
	oct2, _ := time.Parse("2006-01-02", "2025-10-02")
	sep30, _ := time.Parse("2006-01-02", "2025-09-30")

	price1 := datalog.NewIdentity("price-1")
	price2 := datalog.NewIdentity("price-2")
	price3 := datalog.NewIdentity("price-3")

	datoms := []datalog.Datom{
		{E: crwvSymbol, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1},
		{E: price1, A: datalog.NewKeyword(":price/symbol"), V: crwvSymbol, Tx: 1},
		{E: price1, A: datalog.NewKeyword(":price/time"), V: oct1, Tx: 1},
		{E: price2, A: datalog.NewKeyword(":price/symbol"), V: crwvSymbol, Tx: 1},
		{E: price2, A: datalog.NewKeyword(":price/time"), V: oct2, Tx: 1},
		{E: price3, A: datalog.NewKeyword(":price/symbol"), V: crwvSymbol, Tx: 1},
		{E: price3, A: datalog.NewKeyword(":price/time"), V: sep30, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Parse the problematic query
	queryStr := `[:find (count ?p)
	 :where [?s :symbol/ticker "CRWV"]
	        [?p :price/symbol ?s]
	        [?p :price/time ?time]
	        [(str ?time) ?timeStr]
	        [(str/starts-with? ?timeStr "2025-10")]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Plan the query
	opts := planner.PlannerOptions{
		EnablePredicatePushdown: true,
		EnableFineGrainedPhases: true,
	}
	plnr := planner.NewPlanner(nil, opts)

	// Call PlanWithBindings directly to inspect intermediate state
	plan, err := plnr.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Debug: Print the plan to understand what's happening
	t.Logf("Query plan has %d phases", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Expressions: %d", len(phase.Expressions))
		for j, expr := range phase.Expressions {
			t.Logf("    Expr %d: %s -> %s", j, expr.Expression.String(), expr.Output)
		}
		t.Logf("  Predicates: %d", len(phase.Predicates))
		for j, pred := range phase.Predicates {
			t.Logf("    Pred %d: %s (requires: %v)", j, pred.Predicate.String(), pred.Predicate.RequiredSymbols())
		}
	}

	// Verify the plan structure
	// The expression [(str ?time) ?timeStr] should be in a phase
	foundStrExpression := false
	var strExprPhaseIdx int
	for i, phase := range plan.Phases {
		for _, expr := range phase.Expressions {
			if expr.Output == "?timeStr" {
				foundStrExpression = true
				strExprPhaseIdx = i
				t.Logf("Found str expression in phase %d providing ?timeStr", i)
				break
			}
		}
	}

	if !foundStrExpression {
		t.Fatalf("str expression not found in any phase")
	}

	// The predicate [(str/starts-with? ?timeStr ...)] should be in the same phase
	// or a later phase where ?timeStr is available
	foundStartsWithPredicate := false
	for i, phase := range plan.Phases {
		for _, pred := range phase.Predicates {
			reqSyms := pred.Predicate.RequiredSymbols()
			if len(reqSyms) > 0 && reqSyms[0] == query.Symbol("?timeStr") {
				foundStartsWithPredicate = true
				t.Logf("Found str/starts-with? predicate in phase %d", i)

				// Check if ?timeStr is available in this phase
				available := make(map[query.Symbol]bool)

				// Symbols from previous phases
				for j := 0; j < i; j++ {
					for _, sym := range plan.Phases[j].Provides {
						available[sym] = true
					}
				}

				// Symbols from current phase (including expressions evaluated before predicates)
				for _, sym := range phase.Provides {
					available[sym] = true
				}

				t.Logf("  Available symbols in phase %d: %v", i, available)

				if !available["?timeStr"] {
					t.Errorf("BUG REPRODUCED: ?timeStr not available in phase %d where str/starts-with? predicate is evaluated", i)
					t.Errorf("  str expression is in phase %d", strExprPhaseIdx)
					t.Errorf("  predicate is in phase %d", i)
					t.Errorf("  Phase %d provides: %v", i, phase.Provides)
				}
				break
			}
		}
	}

	if !foundStartsWithPredicate {
		t.Fatalf("str/starts-with? predicate not found in any phase")
	}

	// Execute the query - this is where the error actually occurs
	result, err := executor.Execute(q)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	// Verify results
	if result.IsEmpty() {
		t.Fatal("Expected non-empty result")
	}

	// Should have 1 row with count = 2 (oct1 and oct2 match "2025-10")
	iter := result.Iterator()
	defer iter.Close()

	if !iter.Next() {
		t.Fatal("Expected at least one result row")
	}

	tuple := iter.Tuple()
	if len(tuple) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(tuple))
	}

	count, ok := tuple[0].(int64)
	if !ok {
		t.Fatalf("Expected int64 count, got %T", tuple[0])
	}

	if count != 2 {
		t.Errorf("Expected count=2 (2 prices in October), got %d", count)
	}
}
