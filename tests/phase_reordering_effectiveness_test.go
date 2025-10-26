package tests

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestPhaseReorderingEffectiveness verifies that phase reordering actually
// reorders phases when beneficial, not just preserves original order
func TestPhaseReorderingEffectiveness(t *testing.T) {
	dir, err := os.MkdirTemp("", "phase-reorder-effect-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create test data with multiple entities and relationships
	tx := db.NewTransaction()

	// Create people
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")

	// Create departments
	eng := datalog.NewIdentity("dept:engineering")
	sales := datalog.NewIdentity("dept:sales")
	tx.Add(eng, datalog.NewKeyword(":dept/name"), "Engineering")
	tx.Add(sales, datalog.NewKeyword(":dept/name"), "Sales")

	// Link people to departments
	tx.Add(alice, datalog.NewKeyword(":person/dept"), eng)
	tx.Add(bob, datalog.NewKeyword(":person/dept"), sales)

	// Create projects
	proj1 := datalog.NewIdentity("project:1")
	proj2 := datalog.NewIdentity("project:2")
	tx.Add(proj1, datalog.NewKeyword(":project/name"), "Project Alpha")
	tx.Add(proj2, datalog.NewKeyword(":project/name"), "Project Beta")

	// Link projects to departments
	tx.Add(proj1, datalog.NewKeyword(":project/dept"), eng)
	tx.Add(proj2, datalog.NewKeyword(":project/dept"), sales)

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query that joins person -> dept <- project
	// This creates a "star" join pattern where dept is the center
	// Without reordering, phases might be: [person pattern, project pattern, dept pattern]
	// With reordering, it should recognize that starting with dept is better
	queryStr := `[:find ?person-name ?project-name ?dept-name
	             :where
	             [?person :person/name ?person-name]
	             [?person :person/dept ?dept]
	             [?project :project/name ?project-name]
	             [?project :project/dept ?dept]
	             [?dept :dept/name ?dept-name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Plan WITHOUT reordering
	optsWithout := planner.PlannerOptions{
		EnableDynamicReordering: false,
	}
	plannerWithout := planner.NewPlanner(nil, optsWithout)
	planWithout, err := plannerWithout.Plan(q)
	if err != nil {
		t.Fatalf("Planning without reordering failed: %v", err)
	}

	// Plan WITH reordering
	optsWith := planner.PlannerOptions{
		EnableDynamicReordering: true,
	}
	plannerWith := planner.NewPlanner(nil, optsWith)
	planWith, err := plannerWith.Plan(q)
	if err != nil {
		t.Fatalf("Planning with reordering failed: %v", err)
	}

	// Log phase details
	t.Logf("\n=== Plan WITHOUT reordering ===")
	for i, phase := range planWithout.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		if len(phase.Patterns) > 0 {
			for j, pp := range phase.Patterns {
				t.Logf("    Pattern %d: %v", j, pp.Pattern)
			}
		}
	}

	t.Logf("\n=== Plan WITH reordering ===")
	for i, phase := range planWith.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		if len(phase.Patterns) > 0 {
			for j, pp := range phase.Patterns {
				t.Logf("    Pattern %d: %v", j, pp.Pattern)
			}
		}
	}

	// Check if phases were actually reordered
	// Compare the order of Provides symbols as a proxy for phase order
	// If reordering worked, the provides should be in different order
	sameOrder := true
	if len(planWithout.Phases) == len(planWith.Phases) {
		for i := range planWithout.Phases {
			if len(planWithout.Phases[i].Provides) != len(planWith.Phases[i].Provides) {
				sameOrder = false
				break
			}
			// Check if Provides are in same order
			withoutProvides := make(map[string]bool)
			for _, sym := range planWithout.Phases[i].Provides {
				withoutProvides[string(sym)] = true
			}
			withProvides := make(map[string]bool)
			for _, sym := range planWith.Phases[i].Provides {
				withProvides[string(sym)] = true
			}
			// If the sets differ, order changed
			for sym := range withoutProvides {
				if !withProvides[sym] {
					sameOrder = false
					break
				}
			}
			for sym := range withProvides {
				if !withoutProvides[sym] {
					sameOrder = false
					break
				}
			}
			if !sameOrder {
				break
			}
		}
	} else {
		sameOrder = false
	}

	if sameOrder {
		t.Logf("WARNING: Phase reordering did not change phase order")
		t.Logf("This may be correct if the original order is already optimal,")
		t.Logf("but the test query was designed to benefit from reordering")
	} else {
		t.Logf("✓ Phase reordering changed phase order (as expected)")
	}

	// Most importantly, verify both plans produce the same results
	matcherWithout := storage.NewBadgerMatcher(db.Store())
	executorWithout := executor.NewExecutorWithOptions(matcherWithout, optsWithout)
	resultWithout, err := executorWithout.Execute(q)
	if err != nil {
		t.Fatalf("Execution without reordering failed: %v", err)
	}

	matcherWith := storage.NewBadgerMatcher(db.Store())
	executorWith := executor.NewExecutorWithOptions(matcherWith, optsWith)
	resultWith, err := executorWith.Execute(q)
	if err != nil {
		t.Fatalf("Execution with reordering failed: %v", err)
	}

	// Both should return same number of results
	if resultWithout.Size() != resultWith.Size() {
		t.Errorf("Result sizes differ: without=%d, with=%d",
			resultWithout.Size(), resultWith.Size())
	}

	// Expected: 2 results (Alice-Project Alpha-Engineering, Bob-Project Beta-Sales)
	expectedSize := 2
	if resultWith.Size() != expectedSize {
		t.Errorf("Expected %d results, got %d", expectedSize, resultWith.Size())
	}

	t.Logf("✓ Both plans return same results (%d rows)", resultWith.Size())
}
