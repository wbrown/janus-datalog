package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// Test 3: OrFallbackRelation with unsafe outer relation
// =============================================================================

func TestOrFallbackIteratorCopiesFromUnsafeOuter(t *testing.T) {
	// Create datoms for the OR branches to match against
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/priority"), V: int64(1)},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":item/priority"), V: int64(2)},
		{E: datalog.NewIdentity("e3"), A: datalog.NewKeyword(":item/priority"), V: int64(3)},
	}

	matcher := factsDatabase(t, datoms)
	queryExec := NewQueryExecutor(matcher, nil, ExecutorOptions{})
	ctx := NewContext()

	// The outer relation over [?e, ?name] reuses its workspace, as a
	// storage scan does.
	entity, priority := datalog.NewSymbol("?e"), datalog.NewSymbol("?p")
	outerSymbols := []query.Symbol{entity, datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"},
	}
	outerRel := NewStreamingRelation(outerSymbols, NewWorkspaceReusingIterator(len(outerSymbols), outerData))

	// The form over the entity with one branch: [?e :item/priority ?p]
	// This branch will match entities with priority
	branches := [][]query.Clause{
		{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: entity},
					query.Constant{Value: datalog.NewKeyword(":item/priority")},
					query.Variable{Name: priority},
				},
			},
		},
	}

	// Create OrFallbackRelation directly
	orFallbackRel, err := NewOrFallbackRelation(queryExec, ctx, []query.Symbol{entity}, []query.Symbol{priority}, branches, outerRel, ExecutorOptions{}, true)
	if err != nil {
		t.Fatalf("NewOrFallbackRelation: %v", err)
	}

	t.Logf("OrFallbackRelation RequiresCopy: %v", orFallbackRel.RequiresCopy())
	t.Logf("Outer relation RequiresCopy: %v", outerRel.RequiresCopy())

	// Iterate and store tuple references WITHOUT copying
	var storedTuples []Tuple
	it := orFallbackRel.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	t.Logf("Got %d tuples", len(storedTuples))
	for i, tuple := range storedTuples {
		t.Logf("  tuple %d: %v", i, tuple)
	}

	// Should have 3 results (all entities have priority)
	if len(storedTuples) != 3 {
		t.Fatalf("expected 3 tuples, got %d", len(storedTuples))
	}

	// CRITICAL: Verify stored tuples have correct values
	// If OrFallbackIterator doesn't copy when outer.RequiresCopy() = true,
	// all stored tuples will have corrupted values
	names := make(map[string]bool)
	for i, tuple := range storedTuples {
		if len(tuple) < 2 {
			t.Errorf("tuple %d has wrong length: %d", i, len(tuple))
			continue
		}
		if name, ok := tuple[1].(string); ok {
			names[name] = true
		} else {
			t.Errorf("tuple %d[1] has wrong type: %T", i, tuple[1])
		}
	}

	// Should have 3 unique names
	if len(names) != 3 {
		t.Errorf("expected 3 unique names, got %d (possible tuple corruption)", len(names))
		t.Logf("names: %v", names)
	}

	// Additional check: verify tuples are independent (not sharing memory)
	if len(storedTuples) >= 2 {
		// Modify first stored tuple
		storedTuples[0][1] = "MODIFIED"
		// Second tuple should be unaffected
		if storedTuples[1][1] == "MODIFIED" {
			t.Error("tuples share memory - modification to tuple 0 affected tuple 1")
		}
	}
}

// =============================================================================
// Test 5: OrFallbackRelation with multiple branch results per outer tuple
// (Integration test - may pass due to join materialization)
// =============================================================================

func TestOrFallbackIteratorMultipleBranchResultsIntegration(t *testing.T) {
	// Create datoms - each entity has MULTIPLE tags
	// This ensures the branch returns multiple tuples per outer tuple
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/tag"), V: "tag1"},
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/tag"), V: "tag2"},
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/tag"), V: "tag3"},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":item/tag"), V: "tag4"},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":item/tag"), V: "tag5"},
	}

	matcher := factsDatabase(t, datoms)
	options := ExecutorOptions{Handler: func(e annotations.Event) {
		t.Logf("[ANNOTATION] %s: %v", e.Name, e.Data)
	}}
	queryExec := NewQueryExecutor(matcher, nil, options)
	ctx := NewContext()

	// The outer relation over 2 entities reuses its workspace.
	entity, tag := datalog.NewSymbol("?e"), datalog.NewSymbol("?tag")
	outerSymbols := []query.Symbol{entity}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1")},
		{datalog.NewIdentity("e2")},
	}
	outerRel := NewStreamingRelation(outerSymbols, NewWorkspaceReusingIterator(len(outerSymbols), outerData))

	// The form over the entity with one branch: [?e :item/tag ?tag]
	// This will return MULTIPLE tuples per outer tuple
	branches := [][]query.Clause{
		{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: entity},
					query.Constant{Value: datalog.NewKeyword(":item/tag")},
					query.Variable{Name: tag},
				},
			},
		},
	}

	// Create OrFallbackRelation directly
	orFallbackRel, err := NewOrFallbackRelation(queryExec, ctx, []query.Symbol{entity}, []query.Symbol{tag}, branches, outerRel, options, true)
	if err != nil {
		t.Fatalf("NewOrFallbackRelation: %v", err)
	}

	// Iterate and store tuple references WITHOUT copying
	var storedTuples []Tuple
	it := orFallbackRel.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	t.Logf("Got %d tuples", len(storedTuples))
	for i, tuple := range storedTuples {
		t.Logf("  tuple %d: %v", i, tuple)
	}

	// Should have 5 results (3 tags for e1 + 2 tags for e2)
	if len(storedTuples) != 5 {
		t.Fatalf("expected 5 tuples, got %d", len(storedTuples))
	}

	// CRITICAL: Verify stored tuples have correct, unique values
	// If workspace is reused without copying, we'll see duplicates or wrong values
	tags := make(map[string]bool)
	for i, tuple := range storedTuples {
		if len(tuple) < 2 {
			t.Errorf("tuple %d has wrong length: %d", i, len(tuple))
			continue
		}
		if tag, ok := tuple[1].(string); ok {
			tags[tag] = true
		} else {
			t.Errorf("tuple %d[1] has wrong type: %T", i, tuple[1])
		}
	}

	// Should have 5 unique tags
	if len(tags) != 5 {
		t.Errorf("expected 5 unique tags, got %d (possible tuple corruption)", len(tags))
		t.Logf("tags: %v", tags)
	}

	// Check for specific tags
	expectedTags := []string{"tag1", "tag2", "tag3", "tag4", "tag5"}
	for _, tag := range expectedTags {
		if !tags[tag] {
			t.Errorf("missing tag: %s", tag)
		}
	}
}

// =============================================================================
// Test 6: OrFallbackRelation with fallback branch
// =============================================================================

func TestOrFallbackIteratorWithFallbackBranch(t *testing.T) {
	// Create datoms - only e1 and e2 have priority
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/priority"), V: int64(1)},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":item/priority"), V: int64(2)},
	}

	matcher := factsDatabase(t, datoms)
	queryExec := NewQueryExecutor(matcher, nil, ExecutorOptions{})
	ctx := NewContext()

	// The outer relation over 3 items reuses its workspace.
	entity, priority := datalog.NewSymbol("?e"), datalog.NewSymbol("?p")
	outerSymbols := []query.Symbol{entity, datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"}, // No priority - will use fallback
	}
	outerRel := NewStreamingRelation(outerSymbols, NewWorkspaceReusingIterator(len(outerSymbols), outerData))

	// The form over the entity with a fallback: [?e :item/priority ?p] or [(ground 0) ?p]
	branches := [][]query.Clause{
		{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: entity},
					query.Constant{Value: datalog.NewKeyword(":item/priority")},
					query.Variable{Name: priority},
				},
			},
		},
		{
			&query.Expression{
				Function: &query.GroundFunction{Value: int64(0)},
				Binding:  priority,
			},
		},
	}

	// Create OrFallbackRelation directly
	orFallbackRel, err := NewOrFallbackRelation(queryExec, ctx, []query.Symbol{entity}, []query.Symbol{priority}, branches, outerRel, ExecutorOptions{}, true)
	if err != nil {
		t.Fatalf("NewOrFallbackRelation: %v", err)
	}

	// Iterate and store tuple references WITHOUT copying
	var storedTuples []Tuple
	it := orFallbackRel.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	t.Logf("Got %d tuples", len(storedTuples))
	for i, tuple := range storedTuples {
		t.Logf("  tuple %d: %v", i, tuple)
	}

	// Should have 3 results
	if len(storedTuples) != 3 {
		t.Fatalf("expected 3 tuples, got %d", len(storedTuples))
	}

	// Verify results - collect name -> priority
	priorities := make(map[string]int64)
	for i, tuple := range storedTuples {
		if len(tuple) < 3 {
			t.Errorf("tuple %d has wrong length: %d", i, len(tuple))
			continue
		}
		name, ok := tuple[1].(string)
		if !ok {
			t.Errorf("tuple %d name has wrong type: %T", i, tuple[1])
			continue
		}
		var p int64
		switch v := tuple[2].(type) {
		case int64:
			p = v
		case int:
			p = int64(v)
		default:
			t.Errorf("tuple %d priority has wrong type: %T", i, tuple[2])
			continue
		}
		priorities[name] = p
	}

	// Verify we got unique items
	if len(priorities) != 3 {
		t.Errorf("expected 3 unique items, got %d (possible tuple corruption)", len(priorities))
	}

	// Items 1, 2 should have priorities 1, 2
	// Item 3 should have priority 0 (fallback)
	expected := map[string]int64{
		"item1": 1,
		"item2": 2,
		"item3": 0,
	}

	for name, expectedP := range expected {
		if p, ok := priorities[name]; !ok {
			t.Errorf("missing item: %s", name)
		} else if p != expectedP {
			t.Errorf("wrong priority for %s: got %d, want %d", name, p, expectedP)
		}
	}

	// Additional check: verify tuples are independent (not sharing memory)
	if len(storedTuples) >= 2 {
		// Modify first stored tuple
		storedTuples[0][1] = "MODIFIED"
		// Second tuple should be unaffected
		if storedTuples[1][1] == "MODIFIED" {
			t.Error("tuples share memory - modification to tuple 0 affected tuple 1")
		}
	}
}
