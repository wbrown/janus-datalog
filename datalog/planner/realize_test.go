package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestRealizeBasic verifies Realize() creates valid RealizedPlan
func TestRealizeBasic(t *testing.T) {
	queryStr := `[:find ?name
	             :where
	             [?e :person/name ?name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	// Verify structure
	if realized == nil {
		t.Fatal("Realize() returned nil")
	}

	if realized.Query != q {
		t.Error("RealizedPlan.Query should be original query")
	}

	if len(realized.Phases) != len(plan.Phases) {
		t.Errorf("Expected %d realized phases, got %d", len(plan.Phases), len(realized.Phases))
	}

	// Verify first phase
	if len(realized.Phases) > 0 {
		phase := realized.Phases[0]

		// Should have a Query
		if phase.Query == nil {
			t.Fatal("RealizedPhase.Query is nil")
		}

		// Query should have Where clauses
		if len(phase.Query.Where) == 0 {
			t.Error("RealizedPhase.Query.Where is empty")
		}

		// Should have Find clause
		if len(phase.Query.Find) == 0 {
			t.Error("RealizedPhase.Query.Find is empty")
		}
	}
}

// TestRealizeClauseOrdering verifies clauses are in correct execution order
func TestRealizeClauseOrdering(t *testing.T) {
	queryStr := `[:find ?name ?doubled
	             :where
	             [?e :person/name ?name]
	             [?e :person/age ?age]
	             [(* ?age 2) ?doubled]
	             [(> ?age 18)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	phase := realized.Phases[0]

	// Count clause types in order
	var patterns, expressions, predicates int
	var lastType string

	for _, clause := range phase.Query.Where {
		switch clause.(type) {
		case *query.DataPattern:
			patterns++
			if lastType == "expression" || lastType == "predicate" {
				t.Error("Found pattern after expression or predicate - wrong order!")
			}
			lastType = "pattern"
		case *query.Expression:
			expressions++
			if lastType == "predicate" {
				t.Error("Found expression after predicate - wrong order!")
			}
			lastType = "expression"
		case query.Predicate:
			predicates++
			lastType = "predicate"
		}
	}

	// Verify we found the expected clause types
	if patterns != 2 {
		t.Errorf("Expected 2 patterns, got %d", patterns)
	}
	if expressions != 1 {
		t.Errorf("Expected 1 expression, got %d", expressions)
	}
	if predicates != 1 {
		t.Errorf("Expected 1 predicate, got %d", predicates)
	}
}

// TestRealizeSymbolTracking verifies symbol tracking is preserved
func TestRealizeSymbolTracking(t *testing.T) {
	queryStr := `[:find ?name
	             :where
	             [?e :person/name ?name]
	             [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	phase := realized.Phases[0]
	originalPhase := plan.Phases[0]

	// Verify symbol tracking is preserved
	if len(phase.Available) != len(originalPhase.Available) {
		t.Errorf("Available symbols mismatch: expected %v, got %v",
			originalPhase.Available, phase.Available)
	}

	if len(phase.Provides) != len(originalPhase.Provides) {
		t.Errorf("Provides symbols mismatch: expected %v, got %v",
			originalPhase.Provides, phase.Provides)
	}

	if len(phase.Keep) != len(originalPhase.Keep) {
		t.Errorf("Keep symbols mismatch: expected %v, got %v",
			originalPhase.Keep, phase.Keep)
	}
}

// TestRealizeFindClause verifies Find clause is built correctly
// - Last phase: Find matches Keep (what we output)
// - Intermediate phases: Find matches Provides (compute everything)
func TestRealizeFindClause(t *testing.T) {
	queryStr := `[:find ?name ?age
	             :where
	             [?e :person/name ?name]
	             [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	// For a single-phase query, it's the last phase, so Find should match Keep
	phase := realized.Phases[0]

	// Find clause should match Keep (what we're outputting)
	if len(phase.Query.Find) != len(phase.Keep) {
		t.Errorf("Find clause length %d doesn't match Keep length %d",
			len(phase.Query.Find), len(phase.Keep))
	}

	// Build a set of Keep symbols for comparison (order may differ)
	keepSet := make(map[query.Symbol]bool)
	for _, sym := range phase.Keep {
		keepSet[sym] = true
	}

	// Each Find element should be a FindVariable in Keep
	for i, elem := range phase.Query.Find {
		fv, ok := elem.(query.FindVariable)
		if !ok {
			t.Errorf("Find element %d is not FindVariable", i)
			continue
		}

		if !keepSet[fv.Symbol] {
			t.Errorf("Find variable %s not in Keep %v", fv.Symbol, phase.Keep)
		}
	}
}

// TestRealizeInClause verifies In clause is built from Available
func TestRealizeInClause(t *testing.T) {
	// This test would need a multi-phase query to see Available symbols
	// For now, just verify empty Available means no RelationInput

	queryStr := `[:find ?name
	             :where
	             [?e :person/name ?name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	phase := realized.Phases[0]

	// First phase typically has no Available symbols
	if len(phase.Available) == 0 {
		// In clause should be empty or just have DatabaseInput
		hasRelationInput := false
		for _, inp := range phase.Query.In {
			if _, ok := inp.(query.RelationInput); ok {
				hasRelationInput = true
				break
			}
		}
		if hasRelationInput {
			t.Error("Found RelationInput in In clause when Available is empty")
		}
	}
}

// TestRealizeMetadata verifies metadata is preserved
func TestRealizeMetadata(t *testing.T) {
	queryStr := `[:find ?name
	             :where
	             [?e :person/name ?name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Add some metadata to a phase
	if len(plan.Phases) > 0 {
		plan.Phases[0].Metadata = map[string]interface{}{
			"test_key": "test_value",
		}
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	phase := realized.Phases[0]

	// Metadata should be preserved
	if phase.Metadata == nil {
		t.Fatal("Metadata is nil")
	}

	if val, ok := phase.Metadata["test_key"]; !ok {
		t.Error("Metadata key 'test_key' not found")
	} else if val != "test_value" {
		t.Errorf("Metadata value mismatch: expected 'test_value', got %v", val)
	}
}

// TestRealizeWithSubquery verifies subqueries are included in Where clause
func TestRealizeWithSubquery(t *testing.T) {
	queryStr := `[:find ?name ?max
	             :where
	             [?p :person/name ?name]
	             [(q [:find (max ?age)
	                  :in $ ?person
	                  :where
	                  [?person :person/age ?age]]
	               $ ?p) [[?max]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnableConditionalAggregateRewriting: false, // Don't rewrite for this test
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	// Find the phase with the subquery
	foundSubquery := false
	for _, phase := range realized.Phases {
		for _, clause := range phase.Query.Where {
			if _, ok := clause.(*query.SubqueryPattern); ok {
				foundSubquery = true
				break
			}
		}
	}

	if !foundSubquery {
		t.Error("Subquery not found in realized Where clauses")
	}
}

// TestRealizeStringOutput validates String() method produces readable output
func TestRealizeStringOutput(t *testing.T) {
	queryStr := `[:find ?name ?doubled
	             :where
	             [?e :person/name ?name]
	             [?e :person/age ?age]
	             [(* ?age 2) ?doubled]
	             [(> ?age 18)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	// Get string representation
	output := realized.String()

	// Validate it contains expected content
	if output == "" {
		t.Fatal("String() returned empty string")
	}

	// Should contain "Realized Query Plan"
	if !containsString(output, "Realized Query Plan") {
		t.Error("Output doesn't contain 'Realized Query Plan'")
	}

	// Should contain phase information
	if !containsString(output, "Phase 1") {
		t.Error("Output doesn't contain 'Phase 1'")
	}

	// Should contain Datalog query syntax
	if !containsString(output, ":find") {
		t.Error("Output doesn't contain ':find'")
	}

	if !containsString(output, ":where") {
		t.Error("Output doesn't contain ':where'")
	}

	// Should contain symbols
	if !containsString(output, "Provides") {
		t.Error("Output doesn't contain 'Provides'")
	}

	// Print it for visual inspection during test run
	t.Logf("\n%s", output)
}

// TestRealizePhaseStringOutput validates individual phase String() output
func TestRealizePhaseStringOutput(t *testing.T) {
	queryStr := `[:find ?name
	             :where
	             [?e :person/name ?name]
	             [?e :person/age ?age]
	             [(> ?age 18)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan
	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	// Get string representation of first phase
	output := realized.Phases[0].String()

	// Validate content
	if output == "" {
		t.Fatal("Phase String() returned empty string")
	}

	// Should contain Query formatting
	if !containsString(output, "Query:") {
		t.Error("Output doesn't contain 'Query:'")
	}

	// Should contain symbol tracking
	if !containsString(output, "Provides") {
		t.Error("Output doesn't contain 'Provides'")
	}

	// Print it for visual inspection
	t.Logf("\n%s", output)
}

// TestRealizeMultiPhase verifies multi-phase query realization
func TestRealizeMultiPhase(t *testing.T) {
	// Query that creates multiple phases due to symbol dependencies
	queryStr := `[:find ?name ?value
	             :where
	             [?e :event/person ?p]
	             [?e :event/value ?value]
	             [?p :person/name ?name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	realized := plan.Realize()

	// Should have at least 1 phase
	if len(realized.Phases) == 0 {
		t.Fatal("Expected at least one phase")
	}

	t.Logf("\n%s", realized.String())

	// Verify last phase uses Keep for :find
	lastPhase := realized.Phases[len(realized.Phases)-1]
	if len(lastPhase.Query.Find) != len(lastPhase.Keep) {
		t.Errorf("Last phase :find length %d doesn't match Keep length %d",
			len(lastPhase.Query.Find), len(lastPhase.Keep))
	}

	// Verify :find symbols match Keep
	keepSet := make(map[query.Symbol]bool)
	for _, sym := range lastPhase.Keep {
		keepSet[sym] = true
	}
	for _, elem := range lastPhase.Query.Find {
		if fv, ok := elem.(query.FindVariable); ok {
			if !keepSet[fv.Symbol] {
				t.Errorf("Last phase :find symbol %s not in Keep %v", fv.Symbol, lastPhase.Keep)
			}
		}
	}

	// If there are multiple phases, verify intermediate phases have :in clause
	if len(realized.Phases) > 1 {
		for i := 1; i < len(realized.Phases); i++ {
			phase := realized.Phases[i]
			if len(phase.Query.In) == 0 {
				t.Errorf("Phase %d should have :in clause", i+1)
			}
			if len(phase.Available) == 0 {
				t.Errorf("Phase %d should have Available symbols", i+1)
			}
		}
	}
}

// TestRealizeAggregation verifies aggregation queries
func TestRealizeAggregation(t *testing.T) {
	queryStr := `[:find ?name (max ?age)
	             :where
	             [?e :person/name ?name]
	             [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	t.Logf("\n%s", realized.String())

	// Original query should show the aggregate
	origQueryStr := realized.Query.String()
	if !containsString(origQueryStr, "max") {
		t.Error("Original query should contain 'max' aggregate")
	}

	// Verify phases have proper structure
	for i, phase := range realized.Phases {
		if phase.Query == nil {
			t.Errorf("Phase %d has nil Query", i+1)
		}
		if len(phase.Query.Find) == 0 {
			t.Errorf("Phase %d has empty :find clause", i+1)
		}
	}
}

// TestRealizeSubquery verifies subquery handling
func TestRealizeSubquery(t *testing.T) {
	queryStr := `[:find ?name ?max
	             :where
	             [?p :person/name ?name]
	             [(q [:find (max ?age)
	                  :in $ ?person
	                  :where
	                  [?person :person/age ?age]]
	               $ ?p) [[?max]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnableConditionalAggregateRewriting: false, // Don't rewrite for this test
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	t.Logf("\n%s", realized.String())

	// Find the phase with the subquery
	foundSubquery := false
	for i, phase := range realized.Phases {
		phaseStr := phase.Query.String()
		if containsString(phaseStr, "[(q") {
			foundSubquery = true
			t.Logf("Phase %d contains subquery", i+1)
		}
	}

	if !foundSubquery {
		t.Error("Subquery not found in any realized phase")
	}
}

// TestRealizeSymbolOrderPreservation verifies findVars order is preserved
func TestRealizeSymbolOrderPreservation(t *testing.T) {
	queryStr := `[:find ?z ?y ?x
	             :where
	             [?e :foo/x ?x]
	             [?e :foo/y ?y]
	             [?e :foo/z ?z]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	realized := plan.Realize()

	if len(realized.Phases) == 0 {
		t.Fatal("No phases in realized plan")
	}

	lastPhase := realized.Phases[len(realized.Phases)-1]

	// Verify Keep order matches original :find order
	expectedOrder := []query.Symbol{"?z", "?y", "?x"}

	// Keep should have the symbols in the right order
	for i, sym := range expectedOrder {
		if i >= len(lastPhase.Keep) {
			t.Errorf("Keep too short: expected %v, got %v", expectedOrder, lastPhase.Keep)
			break
		}
		if lastPhase.Keep[i] != sym {
			t.Errorf("Keep[%d]: expected %s, got %s", i, sym, lastPhase.Keep[i])
		}
	}

	// Phase :find should match Keep order
	for i, elem := range lastPhase.Query.Find {
		if fv, ok := elem.(query.FindVariable); ok {
			if i >= len(lastPhase.Keep) {
				break
			}
			if fv.Symbol != lastPhase.Keep[i] {
				t.Errorf("Find[%d]: expected %s, got %s", i, lastPhase.Keep[i], fv.Symbol)
			}
		}
	}

	t.Logf("\n%s", realized.String())
}

// TestRealizeSymbolPassThrough tests the critical pass-through behavior:
// - Phase 1: Generates ?sym-a and ?e1 (join key for Phase 2)
// - Phase 2: Joins via ?e1, generates ?e2 (join key for Phase 3), but doesn't use ?sym-a in patterns
// - Phase 3: Uses ?e2 to join, needs ?sym-a (from Phase 1) and ?sym-c (generated) for expression
// Therefore: Phase 2 must Keep ?sym-a (pass-through) even though it only appears in Available, not Provides
func TestRealizeSymbolPassThrough(t *testing.T) {
	queryStr := `[:find ?result
	             :where
	             [?e1 :entity/value ?sym-a]
	             [?e2 :entity/value ?sym-b]
	             [?e1 :entity/link ?e2]
	             [?e3 :entity/value ?sym-c]
	             [?e2 :entity/link ?e3]
	             [(str ?sym-a ?sym-c) ?result]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	stats := &Statistics{
		AttributeCardinality: make(map[string]int),
		EntityCount:          1000,
	}
	planner := NewPlanner(stats, PlannerOptions{})

	plan, err := planner.Plan(q)
	require.NoError(t, err)

	realized := plan.Realize()

	t.Logf("\n%s", realized.String())

	// Test the semantic property: Keep ⊆ (Available ∪ Provides)
	// This verifies that Keep can contain:
	// 1. Symbols from Provides (generated by this phase)
	// 2. Symbols from Available (pass-through from earlier phases)
	for i, phase := range realized.Phases {
		availableSet := make(map[query.Symbol]bool)
		for _, sym := range phase.Available {
			availableSet[sym] = true
		}

		providesSet := make(map[query.Symbol]bool)
		for _, sym := range phase.Provides {
			providesSet[sym] = true
		}

		// Every symbol in Keep must be in Available OR Provides
		for _, sym := range phase.Keep {
			if !availableSet[sym] && !providesSet[sym] {
				t.Errorf("Phase %d Keep contains %s which is neither Available nor Provides", i+1, sym)
			}
		}

		// Document which symbols are pass-through vs generated
		var passThrough []query.Symbol
		var generated []query.Symbol
		for _, sym := range phase.Keep {
			if availableSet[sym] && !providesSet[sym] {
				passThrough = append(passThrough, sym)
			} else if providesSet[sym] {
				generated = append(generated, sym)
			}
		}

		t.Logf("Phase %d:", i+1)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)
		t.Logf("  Keep (pass-through): %v", passThrough)
		t.Logf("  Keep (generated): %v", generated)
	}
}

// TestPlannerKeepsExpressionInputs tests that the planner correctly adds symbols
// to Keep when they're needed by expressions in future phases.
// This is a regression test for a bug where expression inputs were not considered.
func TestPlannerKeepsExpressionInputs(t *testing.T) {
	queryStr := `[:find ?result
	             :where
	             [?p1 :person/name ?name]
	             [?p1 :person/friend ?p2]
	             [?p2 :person/age ?age]
	             [(str ?name ?age) ?result]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	require.NoError(t, err)

	// This query should create 2 phases:
	// Phase 1: [?p1 :person/name ?name], [?p1 :person/friend ?p2]
	// Phase 2: [?p2 :person/age ?age], [(str ?name ?age) ?result]
	//
	// The critical test: Phase 1 must Keep ?name because Phase 2's expression uses it

	require.GreaterOrEqual(t, len(plan.Phases), 2, "Should have at least 2 phases")

	if len(plan.Phases) >= 2 {
		phase1 := plan.Phases[0]

		// Phase 1 should provide ?name
		require.Contains(t, phase1.Provides, query.Symbol("?name"))

		// CRITICAL: Phase 1 must Keep ?name for Phase 2's expression
		require.Contains(t, phase1.Keep, query.Symbol("?name"),
			"Phase 1 must keep ?name for Phase 2's expression input")

		t.Logf("Phase 1 Provides: %v", phase1.Provides)
		t.Logf("Phase 1 Keep: %v", phase1.Keep)
		t.Logf("Phase 2 has %d expressions", len(plan.Phases[1].Expressions))
	}
}

// Helper function to check if string contains substring
func containsString(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
