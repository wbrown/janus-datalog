package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestMultiPositionDebug provides detailed debugging for multi-position binding issues
func TestMultiPositionDebug(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup: 2 entities with different codes
	tx := db.NewTransaction()
	entity1 := datalog.NewIdentity("entity:1")
	entity2 := datalog.NewIdentity("entity:2")

	tx.Add(entity1, datalog.NewKeyword(":attr/code"), "A")
	tx.Add(entity2, datalog.NewKeyword(":attr/code"), "B")

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create pattern: [?e :attr/code ?code]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":attr/code")},
			query.Variable{Name: "?code"},
		},
	}
	t.Logf("Pattern: %s", pattern.String())

	// Create binding relation: entity1 with code="A"
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{"?e", "?code"},
		[]executor.Tuple{
			{entity1, "A"},
		},
	)
	t.Logf("Binding columns: %v", bindingRel.Columns())

	it := bindingRel.Iterator()
	for it.Next() {
		t.Logf("Binding tuple: %v", it.Tuple())
	}
	it.Close()

	// Test the pattern extractor
	extractor := query.NewPatternExtractor(pattern, bindingRel.Columns())
	bindingTuple := executor.Tuple{entity1, "A"}
	values := extractor.Extract(bindingTuple)
	t.Logf("Extracted values:")
	t.Logf("  E: %v (%T)", values.E, values.E)
	t.Logf("  A: %v (%T)", values.A, values.A)
	t.Logf("  V: %v (%T)", values.V, values.V)
	t.Logf("  T: %v (%T)", values.T, values.T)

	// Now call Match
	t.Logf("Calling Match...")
	matcher := NewBadgerMatcher(db.Store())
	result, err := matcher.Match(pattern, executor.Relations{bindingRel})
	if err != nil {
		t.Fatalf("Match error: %v", err)
	}

	// Iterate results
	t.Logf("Results:")
	resIt := result.Iterator()
	count := 0
	for resIt.Next() {
		tuple := resIt.Tuple()
		t.Logf("  %v", tuple)
		count++
	}
	resIt.Close()
	t.Logf("Total: %d results", count)

	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}
