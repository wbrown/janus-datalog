package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// Mock Relations for Testing RequiresCopy() Behavior
// =============================================================================

// mockUnsafeRelation simulates a streaming relation that reuses workspace memory.
// Its iterator overwrites a shared workspace slice on each Next() call,
// just like storage iterators do with BuildTupleInternedInto.
type mockUnsafeRelation struct {
	symbols    []query.Symbol
	data       [][]interface{} // Source data (will be copied into workspace)
	options    ExecutorOptions
	properties RelationProperties
}

func newMockUnsafeRelation(symbols []query.Symbol, data [][]interface{}) *mockUnsafeRelation {
	return &mockUnsafeRelation{
		symbols: symbols,
		data:    data,
	}
}

func (r *mockUnsafeRelation) Symbols() []query.Symbol                                { return r.symbols }
func (r *mockUnsafeRelation) Properties() RelationProperties                         { return r.properties }
func (r *mockUnsafeRelation) Size() int                                              { return len(r.data) }
func (r *mockUnsafeRelation) Get(i int) Tuple                                        { return nil }
func (r *mockUnsafeRelation) String() string                                         { return "mockUnsafeRelation" }
func (r *mockUnsafeRelation) Table() string                                          { return "" }
func (r *mockUnsafeRelation) ProjectFromPattern(*query.DataPattern) Relation         { return nil }
func (r *mockUnsafeRelation) Sorted() ([]Tuple, error)                               { return nil, nil }
func (r *mockUnsafeRelation) Project([]query.Symbol) (Relation, error)               { return nil, nil }
func (r *mockUnsafeRelation) Materialize() Relation                                  { return r }
func (r *mockUnsafeRelation) Sort([]query.OrderByClause) Relation                    { return nil }
func (r *mockUnsafeRelation) FilterWithPredicate(query.Predicate) Relation           { return nil }
func (r *mockUnsafeRelation) EvaluateFunction(query.Function, query.Symbol) Relation { return nil }
func (r *mockUnsafeRelation) Select(func(Tuple) bool) Relation                       { return nil }
func (r *mockUnsafeRelation) Join(Relation) Relation                                 { return nil }
func (r *mockUnsafeRelation) HashJoin(Relation, []query.Symbol) Relation             { return nil }
func (r *mockUnsafeRelation) SemiJoin(Relation, []query.Symbol) Relation             { return nil }
func (r *mockUnsafeRelation) AntiJoin(Relation, []query.Symbol) Relation             { return nil }
func (r *mockUnsafeRelation) Aggregate([]query.FindElement) Relation                 { return nil }
func (r *mockUnsafeRelation) Options() ExecutorOptions                               { return r.options }

// RequiresCopy returns true - this relation reuses workspace memory
func (r *mockUnsafeRelation) RequiresCopy() bool { return true }

func (r *mockUnsafeRelation) Iterator() Iterator {
	return &mockUnsafeIterator{
		data:      r.data,
		workspace: make(Tuple, len(r.symbols)), // Shared workspace
		pos:       -1,
	}
}

// mockUnsafeIterator reuses a workspace slice for each tuple
type mockUnsafeIterator struct {
	data      [][]interface{}
	workspace Tuple // Reused on each Next()
	pos       int
	err       error
}

func (it *mockUnsafeIterator) Next() bool {
	it.pos++
	if it.pos >= len(it.data) {
		return false
	}
	// Overwrite workspace with current tuple's data
	copy(it.workspace, it.data[it.pos])
	return true
}

func (it *mockUnsafeIterator) Tuple() Tuple {
	return it.workspace // Returns the SAME slice every time
}

func (it *mockUnsafeIterator) Close() error {
	return nil
}

func (it *mockUnsafeIterator) Error() error { return it.err }

// =============================================================================
// Test 1: Verify mockUnsafeRelation actually corrupts without copying
// This is a sanity check that our mock is working correctly
// =============================================================================

func TestMockUnsafeRelationActuallyCorrupts(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x")}
	unsafe := newMockUnsafeRelation(symbols, [][]interface{}{
		{1},
		{2},
		{3},
	})

	// Store tuple references WITHOUT copying
	var refs []Tuple
	it := unsafe.Iterator()
	for it.Next() {
		refs = append(refs, it.Tuple()) // No copy!
	}
	it.Close()

	// All refs should point to the same workspace
	// So they should all have the LAST value
	for i, ref := range refs {
		if ref[0] != 3 {
			t.Errorf("ref %d: expected 3 (last value due to workspace reuse), got %v", i, ref[0])
		}
	}

	// Also verify all refs are the same slice
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

	// Create an unsafe outer relation with [?e, ?name]
	// This simulates what BadgerDB returns - workspace reuse
	outerSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"},
	}
	outerRel := newMockUnsafeRelation(outerSymbols, outerData)

	// Create OR clause: [?e :item/priority ?p]
	// This branch will match entities with priority
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":item/priority")},
						query.Variable{Name: datalog.NewSymbol("?p")},
					},
				},
			},
		},
	}

	// Create OrFallbackRelation directly
	orFallbackRel := NewOrFallbackRelation(queryExec, ctx, orClause.Branches, outerRel, ExecutorOptions{}, true)

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
// Test 4: OrFallbackIterator with unsafe branch result (direct test)
//
// This tests the OrFallbackIterator directly by wrapping an unsafe relation
// as the branch result. This bypasses the execution pipeline that masks the bug.
// =============================================================================

func TestOrFallbackIteratorWithUnsafeBranchResult(t *testing.T) {
	// Create an unsafe relation that will be returned by the "branch"
	// This simulates what would happen if a branch returned a StreamingRelation
	// with workspace reuse
	branchSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	unsafeBranchData := [][]interface{}{
		{1, "a"},
		{1, "b"},
		{1, "c"},
		{2, "d"},
		{2, "e"},
	}
	unsafeBranch := newMockUnsafeRelation(branchSymbols, unsafeBranchData)

	// Test projectedIterator - this is what OrFallbackIterator wraps the branch with
	// The fix is in projectedIterator.Tuple() which should copy when source.RequiresCopy() = true
	branchIt := unsafeBranch.Iterator()
	projIt := &projectedIterator{
		inner:          branchIt,
		branchRelation: unsafeBranch, // This has RequiresCopy() = true
		// Same symbols, no projection needed — identity plan
		plan: newProjectionPlan(branchSymbols, branchSymbols, nil),
	}

	var storedTuples []Tuple
	for projIt.Next() {
		// projectedIterator.Tuple() should copy because branchRelation.RequiresCopy() = true
		tuple := projIt.Tuple()
		storedTuples = append(storedTuples, tuple)
	}
	projIt.Close()

	t.Logf("Got %d tuples", len(storedTuples))
	for i, tuple := range storedTuples {
		t.Logf("  tuple %d: %v", i, tuple)
	}

	// Should have 5 tuples
	if len(storedTuples) != 5 {
		t.Fatalf("expected 5 tuples, got %d", len(storedTuples))
	}

	// CRITICAL: If projectedIterator doesn't copy from unsafe source, all stored tuples
	// will have the LAST value
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

	// Create an outer relation with 2 entities
	outerSymbols := []query.Symbol{datalog.NewSymbol("?e")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1")},
		{datalog.NewIdentity("e2")},
	}
	outerRel := newMockUnsafeRelation(outerSymbols, outerData)

	// Create OR clause: [?e :item/tag ?tag]
	// This will return MULTIPLE tuples per outer tuple
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":item/tag")},
						query.Variable{Name: datalog.NewSymbol("?tag")},
					},
				},
			},
		},
	}

	// Create OrFallbackRelation directly
	orFallbackRel := NewOrFallbackRelation(queryExec, ctx, orClause.Branches, outerRel, options, true)

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

	// Create an unsafe outer relation with 3 items
	outerSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"}, // No priority - will use fallback
	}
	outerRel := newMockUnsafeRelation(outerSymbols, outerData)

	// Create OR clause with fallback: [?e :item/priority ?p] or [(ground 0) ?p]
	orClause := &query.OrClause{
		Branches: [][]query.Clause{
			{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":item/priority")},
						query.Variable{Name: datalog.NewSymbol("?p")},
					},
				},
			},
			{
				&query.Expression{
					Function: &query.GroundFunction{Value: int64(0)},
					Binding:  datalog.NewSymbol("?p"),
				},
			},
		},
	}

	// Create OrFallbackRelation directly
	orFallbackRel := NewOrFallbackRelation(queryExec, ctx, orClause.Branches, outerRel, ExecutorOptions{}, true)

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
