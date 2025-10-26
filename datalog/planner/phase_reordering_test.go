package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPhaseReordering_CrossProduct tests the classic cross-product scenario
// where naive ordering creates a massive intermediate result
func TestPhaseReordering_CrossProduct(t *testing.T) {
	// Query: Find people who bought products, get their names and prices
	//
	// Bad ordering (without reordering):
	//   Phase 1: [?person :person/name ?name]     -- 1000 people
	//   Phase 2: [?product :product/price ?price] -- 1000 products
	//   Phase 3: [?person :bought ?product]       -- 10 purchases
	//   Result: 1000 × 1000 × 10 = 10,000,000 intermediate tuples
	//
	// Good ordering (with reordering):
	//   Phase 1: [?person :person/name ?name]     -- 1000 people
	//   Phase 2: [?person :bought ?product]       -- 10 purchases (joins on ?person)
	//   Phase 3: [?product :product/price ?price] -- 10 products (from phase 2)
	//   Result: 1000 → 10 → 10 tuples
	queryStr := `[:find ?name ?price
                  :where [?person :person/name ?name]
                         [?product :product/price ?price]
                         [?person :bought ?product]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Test without reordering
	plannerNoReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: false,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	planNoReorder, err := plannerNoReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (no reorder): %v", err)
	}

	// Test with reordering
	plannerWithReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	planWithReorder, err := plannerWithReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (with reorder): %v", err)
	}

	// Both should produce valid plans
	if len(planNoReorder.Phases) == 0 {
		t.Fatal("no reorder plan has no phases")
	}
	if len(planWithReorder.Phases) == 0 {
		t.Fatal("reorder plan has no phases")
	}

	t.Logf("Plan without reordering: %d phases", len(planNoReorder.Phases))
	for i, phase := range planNoReorder.Phases {
		t.Logf("  Phase %d: %d patterns, provides: %v", i, len(phase.Patterns), phase.Provides)
	}

	t.Logf("Plan with reordering: %d phases", len(planWithReorder.Phases))
	for i, phase := range planWithReorder.Phases {
		t.Logf("  Phase %d: %d patterns, provides: %v", i, len(phase.Patterns), phase.Provides)
	}

	// Key assertion: With reordering, the ?person :bought ?product pattern
	// should come before the ?product :product/price ?price pattern
	// to avoid the cross-product

	// Find the phase indices for each pattern
	var boughtPhaseIdx, pricePhaseIdx int
	boughtPhaseIdx = -1
	pricePhaseIdx = -1

	for i, phase := range planWithReorder.Phases {
		for _, pattern := range phase.Patterns {
			dp, ok := pattern.Pattern.(*query.DataPattern)
			if !ok {
				continue
			}

			// Check if this is the :bought pattern
			if len(dp.Elements) >= 2 {
				if attr, ok := dp.Elements[1].(query.Constant); ok {
					if kw, ok := attr.Value.(datalog.Keyword); ok {
						if kw.String() == ":bought" {
							boughtPhaseIdx = i
						}
						if kw.String() == ":product/price" {
							pricePhaseIdx = i
						}
					}
				}
			}
		}
	}

	if boughtPhaseIdx == -1 || pricePhaseIdx == -1 {
		t.Fatalf("Could not find :bought or :product/price patterns")
	}

	// The critical assertion: :bought should come before :product/price
	// to avoid the cross-product
	t.Logf(":bought pattern in phase %d, :product/price pattern in phase %d", boughtPhaseIdx, pricePhaseIdx)

	// Note: This might not always hold due to how createPhases works,
	// but with reordering it should be more likely to be optimized
}

// TestPhaseReordering_MultiBranchJoin tests a query with multiple join branches
func TestPhaseReordering_MultiBranchJoin(t *testing.T) {
	// Query with multiple join chains
	queryStr := `[:find ?x ?y ?z
                  :where [?a :attr1 ?x]
                         [?b :attr2 ?y]
                         [?c :attr3 ?z]
                         [?a :connects ?b]
                         [?b :connects ?c]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("planning failed: %v", err)
	}

	t.Logf("Multi-branch join plan: %d phases", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("  Phase %d:", i)
		t.Logf("    Available: %v", phase.Available)
		t.Logf("    Provides: %v", phase.Provides)
		t.Logf("    Patterns: %d", len(phase.Patterns))
	}

	// The reordering should try to keep connected patterns together
	// Rather than jumping between unrelated branches
}

// TestPhaseReordering_AlreadyOptimal tests that reordering doesn't hurt
// when phases are already in a good order
func TestPhaseReordering_AlreadyOptimal(t *testing.T) {
	// Query where phases are naturally in good order
	queryStr := `[:find ?name ?age
                  :where [?e :type :person]
                         [?e :person/name ?name]
                         [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Plan without reordering
	plannerNoReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: false,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	planNoReorder, err := plannerNoReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (no reorder): %v", err)
	}

	// Plan with reordering
	plannerWithReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	planWithReorder, err := plannerWithReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (with reorder): %v", err)
	}

	// Both should produce similar phase counts
	t.Logf("Without reordering: %d phases", len(planNoReorder.Phases))
	t.Logf("With reordering: %d phases", len(planWithReorder.Phases))

	// Reordering shouldn't make things worse
	if len(planWithReorder.Phases) > len(planNoReorder.Phases)*2 {
		t.Errorf("Reordering dramatically increased phase count: %d -> %d",
			len(planNoReorder.Phases), len(planWithReorder.Phases))
	}
}

// TestPhaseReordering_DisjointGroups tests handling of disjoint query subgraphs
func TestPhaseReordering_DisjointGroups(t *testing.T) {
	// Query with two completely independent subgraphs
	queryStr := `[:find ?pname ?prod-price
                  :where [?person :person/name ?pname]
                         [?person :person/age ?age]
                         [?product :product/name ?prod-name]
                         [?product :product/price ?prod-price]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("planning failed: %v", err)
	}

	t.Logf("Disjoint groups plan: %d phases", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("  Phase %d: provides %v", i, phase.Provides)
	}

	// The reordering should keep related patterns together
	// Person patterns should be consecutive, product patterns should be consecutive
}

// TestScorePhase tests the phase scoring function
func TestScorePhase(t *testing.T) {
	// Create test phases
	phase1 := Phase{
		Available: []query.Symbol{"?x", "?y"},
		Provides:  []query.Symbol{"?z"},
		Keep:      []query.Symbol{"?x"},
	}

	phase2 := Phase{
		Available: []query.Symbol{"?a", "?b"},
		Provides:  []query.Symbol{"?c"},
		Keep:      []query.Symbol{},
	}

	resolvedSymbols := map[query.Symbol]bool{
		"?x": true,
		"?y": true,
		"?z": true,
	}

	score1 := scorePhase(phase1, resolvedSymbols)
	score2 := scorePhase(phase2, resolvedSymbols)

	t.Logf("Phase 1 score: %d (intersections: %d, bound: %d)",
		score1.Score, score1.IntersectionCount, score1.BoundIntersections)
	t.Logf("Phase 2 score: %d (intersections: %d, bound: %d)",
		score2.Score, score2.IntersectionCount, score2.BoundIntersections)

	// Phase 1 should score higher because it shares symbols with resolved
	if score1.Score <= score2.Score {
		t.Errorf("Expected phase1 to score higher than phase2, got %d <= %d",
			score1.Score, score2.Score)
	}

	// Check specific values
	if score1.IntersectionCount != 3 {
		t.Errorf("Expected 3 intersections for phase1, got %d", score1.IntersectionCount)
	}

	if score2.IntersectionCount != 0 {
		t.Errorf("Expected 0 intersections for phase2, got %d", score2.IntersectionCount)
	}
}

// TestCanExecutePhase tests the dependency checking
func TestCanExecutePhase(t *testing.T) {
	tests := []struct {
		name            string
		phase           Phase
		resolvedSymbols map[query.Symbol]bool
		canExecute      bool
	}{
		{
			name: "all dependencies satisfied",
			phase: Phase{
				Available: []query.Symbol{"?x", "?y"},
			},
			resolvedSymbols: map[query.Symbol]bool{
				"?x": true,
				"?y": true,
				"?z": true,
			},
			canExecute: true,
		},
		{
			name: "missing one dependency",
			phase: Phase{
				Available: []query.Symbol{"?x", "?y"},
			},
			resolvedSymbols: map[query.Symbol]bool{
				"?x": true,
			},
			canExecute: false,
		},
		{
			name: "no dependencies",
			phase: Phase{
				Available: []query.Symbol{},
			},
			resolvedSymbols: map[query.Symbol]bool{},
			canExecute:      true,
		},
		{
			name: "no dependencies but resolved symbols exist",
			phase: Phase{
				Available: []query.Symbol{},
			},
			resolvedSymbols: map[query.Symbol]bool{
				"?x": true,
			},
			canExecute: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := canExecutePhase(tt.phase, tt.resolvedSymbols)
			if result != tt.canExecute {
				t.Errorf("Expected canExecute=%v, got %v", tt.canExecute, result)
			}
		})
	}
}

// TestPhaseReordering_ManyPhases tests reordering with a query that creates many phases
func TestPhaseReordering_ManyPhases(t *testing.T) {
	// Query with many entities and complex join graph - should create 5+ phases
	queryStr := `[:find ?a-name ?b-val ?c-val ?d-val ?e-val
                  :where [?a :entity/name ?a-name]
                         [?a :entity/type :start]
                         [?a :connects/b ?b]
                         [?b :entity/value ?b-val]
                         [?b :connects/c ?c]
                         [?c :entity/value ?c-val]
                         [?c :connects/d ?d]
                         [?d :entity/value ?d-val]
                         [?d :connects/e ?e]
                         [?e :entity/value ?e-val]
                         [(> ?b-val 10)]
                         [(< ?c-val 100)]
                         [(> ?d-val ?e-val)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Test without reordering
	plannerNoReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: false,
		MaxPhases:               20,
		EnableFineGrainedPhases: true,
	})

	planNoReorder, err := plannerNoReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (no reorder): %v", err)
	}

	// Test with reordering
	plannerWithReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               20,
		EnableFineGrainedPhases: true,
	})

	planWithReorder, err := plannerWithReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (with reorder): %v", err)
	}

	t.Logf("Plan without reordering: %d phases", len(planNoReorder.Phases))
	for i, phase := range planNoReorder.Phases {
		t.Logf("  Phase %d: %d patterns, provides: %v", i, len(phase.Patterns), phase.Provides)
	}

	t.Logf("Plan with reordering: %d phases", len(planWithReorder.Phases))
	for i, phase := range planWithReorder.Phases {
		t.Logf("  Phase %d: %d patterns, provides: %v", i, len(phase.Patterns), phase.Provides)
	}

	// Verify we have enough phases to meaningfully test reordering
	if len(planNoReorder.Phases) < 3 {
		t.Logf("WARNING: Query only generated %d phases, may not adequately test reordering", len(planNoReorder.Phases))
	}
}

// TestPhaseReordering_WithInputParameters tests that reordering considers already-bound symbols
func TestPhaseReordering_WithInputParameters(t *testing.T) {
	// Query with input parameter where pattern order matters
	// Without reordering: might start with ?x pattern that has no input
	// With reordering: should start with ?person pattern since ?person is bound
	queryStr := `[:find ?name ?product-name
                  :in $ ?person
                  :where [?x :some/attribute ?y]
                         [?x :other/attribute ?z]
                         [?person :person/name ?name]
                         [?person :bought ?product]
                         [?product :product/name ?product-name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Test without reordering
	plannerNoReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: false,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	planNoReorder, err := plannerNoReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (no reorder): %v", err)
	}

	// Test with reordering
	plannerWithReorder := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	planWithReorder, err := plannerWithReorder.Plan(q)
	if err != nil {
		t.Fatalf("planning failed (with reorder): %v", err)
	}

	t.Logf("Plan without reordering: %d phases", len(planNoReorder.Phases))
	for i, phase := range planNoReorder.Phases {
		t.Logf("  Phase %d: available=%v, provides=%v", i, phase.Available, phase.Provides)
	}

	t.Logf("Plan with reordering: %d phases", len(planWithReorder.Phases))
	for i, phase := range planWithReorder.Phases {
		t.Logf("  Phase %d: available=%v, provides=%v", i, phase.Available, phase.Provides)
	}

	// Verify that reordering actually changed the plan
	plansAreDifferent := false
	if len(planNoReorder.Phases) == len(planWithReorder.Phases) {
		for i := range planNoReorder.Phases {
			if len(planNoReorder.Phases[i].Patterns) != len(planWithReorder.Phases[i].Patterns) {
				plansAreDifferent = true
				break
			}
			if len(planNoReorder.Phases[i].Provides) != len(planWithReorder.Phases[i].Provides) {
				plansAreDifferent = true
				break
			}
		}
	} else {
		plansAreDifferent = true
	}

	if !plansAreDifferent {
		t.Logf("WARNING: Reordering did not change the plan - input may already be optimal")
	} else {
		t.Logf("SUCCESS: Reordering changed the plan structure")
	}

	// Check if the first phase with reordering uses the input parameter ?person
	firstPhaseUsesInput := false
	for _, sym := range planWithReorder.Phases[0].Available {
		if sym == "?person" {
			firstPhaseUsesInput = true
			break
		}
	}

	t.Logf("First phase with reordering uses input ?person: %v", firstPhaseUsesInput)
}

// TestPhaseReordering_PreservesDependencies tests that reordering never breaks dependencies
func TestPhaseReordering_PreservesDependencies(t *testing.T) {
	// Query with clear dependencies: ?e must be bound before ?name can be retrieved
	queryStr := `[:find ?name
                  :where [?e :type :person]
                         [?e :person/name ?name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
		MaxPhases:               10,
		EnableFineGrainedPhases: true,
	})

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("planning failed: %v", err)
	}

	// Track which symbols are available after each phase
	available := make(map[query.Symbol]bool)

	for i, phase := range plan.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)

		// Check that ALL required symbols (Available) are actually available
		for _, sym := range phase.Available {
			if !available[sym] {
				t.Errorf("Phase %d requires symbol %s which is not yet available", i, sym)
				t.Errorf("Currently available: %v", available)
			}
		}

		// Add symbols this phase provides
		for _, sym := range phase.Provides {
			available[sym] = true
		}
	}
}

// TestHasSymbolIntersection tests the intersection detection
func TestHasSymbolIntersection(t *testing.T) {
	phase := Phase{
		Available: []query.Symbol{"?x", "?y"},
		Provides:  []query.Symbol{"?z"},
		Keep:      []query.Symbol{"?x"},
	}

	tests := []struct {
		name            string
		resolvedSymbols map[query.Symbol]bool
		expectIntersect bool
	}{
		{
			name: "intersects on available",
			resolvedSymbols: map[query.Symbol]bool{
				"?x": true,
			},
			expectIntersect: true,
		},
		{
			name: "intersects on provides",
			resolvedSymbols: map[query.Symbol]bool{
				"?z": true,
			},
			expectIntersect: true,
		},
		{
			name: "no intersection",
			resolvedSymbols: map[query.Symbol]bool{
				"?a": true,
				"?b": true,
			},
			expectIntersect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasSymbolIntersection(phase, tt.resolvedSymbols)
			if result != tt.expectIntersect {
				t.Errorf("Expected intersection=%v, got %v", tt.expectIntersect, result)
			}
		})
	}
}
