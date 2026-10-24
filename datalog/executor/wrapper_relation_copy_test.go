package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// workspaceReusingIterator yields every tuple in one workspace slice,
// overwritten on each Next(), as a storage iterator does with
// BuildTupleInternedInto. A StreamingRelation over it is the relation whose
// RequiresCopy() is true, the shape every storage scan takes.
type workspaceReusingIterator struct {
	data      [][]interface{}
	workspace Tuple
	pos       int
	err       error
}

func newWorkspaceReusingIterator(width int, data [][]interface{}) *workspaceReusingIterator {
	return &workspaceReusingIterator{data: data, workspace: make(Tuple, width), pos: -1}
}

func (it *workspaceReusingIterator) Next() bool {
	it.pos++
	if it.pos >= len(it.data) {
		return false
	}
	copy(it.workspace, it.data[it.pos])
	return true
}

func (it *workspaceReusingIterator) Tuple() Tuple {
	return it.workspace
}

func (it *workspaceReusingIterator) Close() error {
	return nil
}

func (it *workspaceReusingIterator) Error() error { return it.err }

// =============================================================================
// Test 1: the iterator's positive control. A reference held across pulls
// without a copy reads the workspace's last value, so a reader that keeps a
// tuple past the next pull must copy it.
// =============================================================================

func TestWorkspaceReusingIteratorYieldsOneWorkspace(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	unsafe := NewStreamingRelation(symbols, newWorkspaceReusingIterator(len(symbols), [][]interface{}{
		{1},
		{2},
		{3},
	}))
	if !unsafe.RequiresCopy() {
		t.Fatal("a streaming relation over a workspace-reusing iterator requires a copy")
	}

	var refs []Tuple
	it := unsafe.Iterator()
	for it.Next() {
		refs = append(refs, it.Tuple())
	}
	it.Close()

	for i, ref := range refs {
		if ref[0] != 3 {
			t.Errorf("ref %d: expected 3 (last value due to workspace reuse), got %v", i, ref[0])
		}
	}

	if len(refs) >= 2 && &refs[0][0] != &refs[1][0] {
		t.Error("expected all refs to point to same workspace memory")
	}
}

// =============================================================================
// Test 2: Verify MaterializedRelation doesn't corrupt (control test)
// =============================================================================

func TestMaterializedRelationDoesNotCorrupt(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	safe := NewMaterializedRelation(symbols, []Tuple{
		{1},
		{2},
		{3},
	})

	// Store tuple references WITHOUT copying
	var refs []Tuple
	it := safe.Iterator()
	for it.Next() {
		refs = append(refs, it.Tuple()) // No copy!
	}
	it.Close()

	// Each ref should have the correct value
	for i, ref := range refs {
		expected := i + 1
		if ref[0] != expected {
			t.Errorf("ref %d: expected %d, got %v", i, expected, ref[0])
		}
	}
}

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

	matcher := NewIndexedMemoryMatcher(datoms)
	queryExec := newQueryExecutor(matcher, nil, ExecutorOptions{})
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
	outerRel := NewStreamingRelation(outerSymbols, newWorkspaceReusingIterator(len(outerSymbols), outerData))

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
// Test 4: a branch result that reuses its workspace is copied when retained
//
// collectBranch is the boundary at which a branch's result is kept for the
// branch's index, so a tuple from a source whose RequiresCopy() is true is
// copied there, once.
// =============================================================================

func TestCollectBranchCopiesFromAnUnsafeBranchResult(t *testing.T) {
	branchSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	unsafeBranchData := [][]interface{}{
		{1, "a"},
		{1, "b"},
		{1, "c"},
		{2, "d"},
		{2, "e"},
	}
	unsafeBranch := NewStreamingRelation(branchSymbols, newWorkspaceReusingIterator(len(branchSymbols), unsafeBranchData))

	storedTuples, err := collectBranch(unsafeBranch)
	if err != nil {
		t.Fatalf("collectBranch: %v", err)
	}
	if len(storedTuples) != 5 {
		t.Fatalf("expected 5 tuples, got %d", len(storedTuples))
	}

	// Without the copy every kept tuple is the workspace, holding the last
	// value.
	values := make(map[string]bool)
	for _, tuple := range storedTuples {
		if len(tuple) >= 2 {
			if y, ok := tuple[1].(string); ok {
				values[y] = true
			}
		}
	}
	if len(values) != 5 {
		t.Errorf("expected 5 unique values, got %d (TUPLE CORRUPTION DETECTED)", len(values))
		t.Logf("values: %v", values)
	}
	if &storedTuples[0][0] == &storedTuples[1][0] {
		t.Error("kept tuples share memory")
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

	matcher := NewIndexedMemoryMatcher(datoms)
	options := ExecutorOptions{Handler: func(e annotations.Event) {
		t.Logf("[ANNOTATION] %s: %v", e.Name, e.Data)
	}}
	queryExec := newQueryExecutor(matcher, nil, options)
	ctx := NewContext()

	// The outer relation over 2 entities reuses its workspace.
	entity, tag := datalog.NewSymbol("?e"), datalog.NewSymbol("?tag")
	outerSymbols := []query.Symbol{entity}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1")},
		{datalog.NewIdentity("e2")},
	}
	outerRel := NewStreamingRelation(outerSymbols, newWorkspaceReusingIterator(len(outerSymbols), outerData))

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

	matcher := NewIndexedMemoryMatcher(datoms)
	queryExec := newQueryExecutor(matcher, nil, ExecutorOptions{})
	ctx := NewContext()

	// The outer relation over 3 items reuses its workspace.
	entity, priority := datalog.NewSymbol("?e"), datalog.NewSymbol("?p")
	outerSymbols := []query.Symbol{entity, datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"}, // No priority - will use fallback
	}
	outerRel := NewStreamingRelation(outerSymbols, newWorkspaceReusingIterator(len(outerSymbols), outerData))

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
