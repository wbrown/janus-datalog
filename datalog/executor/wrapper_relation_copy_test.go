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
	columns []query.Symbol
	data    [][]interface{} // Source data (will be copied into workspace)
	options ExecutorOptions
}

func newMockUnsafeRelation(columns []query.Symbol, data [][]interface{}) *mockUnsafeRelation {
	return &mockUnsafeRelation{
		columns: columns,
		data:    data,
	}
}

func (r *mockUnsafeRelation) Columns() []query.Symbol                                { return r.columns }
func (r *mockUnsafeRelation) Symbols() []query.Symbol                                { return r.columns }
func (r *mockUnsafeRelation) Size() int                                              { return len(r.data) }
func (r *mockUnsafeRelation) IsEmpty() bool                                          { return len(r.data) == 0 }
func (r *mockUnsafeRelation) Get(i int) Tuple                                        { return nil }
func (r *mockUnsafeRelation) String() string                                         { return "mockUnsafeRelation" }
func (r *mockUnsafeRelation) Table() string                                          { return "" }
func (r *mockUnsafeRelation) ProjectFromPattern(*query.DataPattern) Relation         { return nil }
func (r *mockUnsafeRelation) Sorted() []Tuple                                        { return nil }
func (r *mockUnsafeRelation) Project([]query.Symbol) (Relation, error)               { return nil, nil }
func (r *mockUnsafeRelation) Materialize() Relation                                  { return r }
func (r *mockUnsafeRelation) Sort([]query.OrderByClause) Relation                    { return nil }
func (r *mockUnsafeRelation) Filter(Filter) Relation                                 { return nil }
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
		workspace: make(Tuple, len(r.columns)), // Shared workspace
		pos:       -1,
	}
}

// mockUnsafeIterator reuses a workspace slice for each tuple
type mockUnsafeIterator struct {
	data      [][]interface{}
	workspace Tuple // Reused on each Next()
	pos       int
}

func (it *mockUnsafeIterator) Next() bool {
	it.pos++
	if it.pos >= len(it.data) {
		return false
	}
	// Overwrite workspace with current row's data
	copy(it.workspace, it.data[it.pos])
	return true
}

func (it *mockUnsafeIterator) Tuple() Tuple {
	return it.workspace // Returns the SAME slice every time
}

func (it *mockUnsafeIterator) Close() error {
	return nil
}

// =============================================================================
// Test 1: UnionIterator with RequiresCopy source
// =============================================================================

func TestUnionIteratorCopiesFromUnsafeSource(t *testing.T) {
	// Create an unsafe relation that reuses workspace
	cols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	unsafeRel := newMockUnsafeRelation(cols, [][]interface{}{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	})

	// Create channel and send the relation
	ch := make(chan relationItem, 1)
	ch <- relationItem{relation: unsafeRel}
	close(ch)

	// Create UnionRelation
	union := NewUnionRelation(ch, cols, ExecutorOptions{})

	// Iterate and store tuple references
	var storedTuples []Tuple
	it := union.Iterator()
	for it.Next() {
		// Store the tuple WITHOUT copying - this is what downstream code does
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	// Verify we got 3 tuples
	if len(storedTuples) != 3 {
		t.Fatalf("expected 3 tuples, got %d", len(storedTuples))
	}

	// CRITICAL: Verify stored tuples have correct values
	// If UnionIterator doesn't copy when source.RequiresCopy() = true,
	// all stored tuples will have the SAME values (the last one)
	expected := [][]interface{}{
		{1, "a"},
		{2, "b"},
		{3, "c"},
	}

	for i, tuple := range storedTuples {
		if tuple[0] != expected[i][0] || tuple[1] != expected[i][1] {
			t.Errorf("tuple %d corrupted: got %v, want %v", i, tuple, expected[i])
		}
	}

	// Additional check: verify tuples are independent (not sharing memory)
	if len(storedTuples) >= 2 {
		// Modify first stored tuple
		storedTuples[0][0] = 999
		// Second tuple should be unaffected
		if storedTuples[1][0] == 999 {
			t.Error("tuples share memory - modification to tuple 0 affected tuple 1")
		}
	}
}

// =============================================================================
// Test 2: UnionIterator with safe source (no unnecessary copies)
// =============================================================================

func TestUnionIteratorPassthroughFromSafeSource(t *testing.T) {
	// Create a safe MaterializedRelation (RequiresCopy() = false)
	cols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	tuples := []Tuple{
		{1, "a"},
		{2, "b"},
	}
	safeRel := NewMaterializedRelation(cols, tuples)

	// Create channel and send the relation
	ch := make(chan relationItem, 1)
	ch <- relationItem{relation: safeRel}
	close(ch)

	// Create UnionRelation
	union := NewUnionRelation(ch, cols, ExecutorOptions{})

	// Iterate and check that tuples pass through correctly
	var results []Tuple
	it := union.Iterator()
	for it.Next() {
		results = append(results, it.Tuple())
	}
	it.Close()

	if len(results) != 2 {
		t.Fatalf("expected 2 tuples, got %d", len(results))
	}

	// Verify values are correct
	if results[0][0] != 1 || results[0][1] != "a" {
		t.Errorf("tuple 0 incorrect: got %v", results[0])
	}
	if results[1][0] != 2 || results[1][1] != "b" {
		t.Errorf("tuple 1 incorrect: got %v", results[1])
	}

	// For safe sources, tuples COULD share memory with original (optimization)
	// but correctness just requires the values are right
}

// =============================================================================
// Test 3: UnionIterator with mixed sources
// =============================================================================

func TestUnionIteratorMixedSources(t *testing.T) {
	cols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// Safe source
	safeRel := NewMaterializedRelation(cols, []Tuple{
		{1, "safe1"},
		{2, "safe2"},
	})

	// Unsafe source
	unsafeRel := newMockUnsafeRelation(cols, [][]interface{}{
		{3, "unsafe1"},
		{4, "unsafe2"},
	})

	// Create channel with both relations
	ch := make(chan relationItem, 2)
	ch <- relationItem{relation: safeRel}
	ch <- relationItem{relation: unsafeRel}
	close(ch)

	// Create UnionRelation
	union := NewUnionRelation(ch, cols, ExecutorOptions{})

	// Iterate and store all tuples
	var storedTuples []Tuple
	it := union.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	if len(storedTuples) != 4 {
		t.Fatalf("expected 4 tuples, got %d", len(storedTuples))
	}

	// Build a map of x values to y values for verification
	results := make(map[interface{}]interface{})
	for _, tuple := range storedTuples {
		results[tuple[0]] = tuple[1]
	}

	// Verify all values are correct
	expected := map[interface{}]interface{}{
		1: "safe1",
		2: "safe2",
		3: "unsafe1",
		4: "unsafe2",
	}

	for k, v := range expected {
		if results[k] != v {
			t.Errorf("for key %v: got %v, want %v", k, results[k], v)
		}
	}
}

// =============================================================================
// Test 4: PrependedIterator with unsafe rest relation
// =============================================================================

func TestPrependedIteratorCopiesFromUnsafeRest(t *testing.T) {
	cols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// Create unsafe relation for the "rest"
	unsafeRel := newMockUnsafeRelation(cols, [][]interface{}{
		{2, "rest1"},
		{3, "rest2"},
		{4, "rest3"},
	})

	// Create PrependedRelation with a safe first tuple and unsafe rest
	// Use the new API that accepts Relation for proper RequiresCopy handling
	firstTuple := Tuple{1, "first"}
	prepended := NewPrependedRelationFromRelation(cols, firstTuple, unsafeRel, ExecutorOptions{})

	// Iterate and store all tuples
	var storedTuples []Tuple
	it := prepended.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	if len(storedTuples) != 4 {
		t.Fatalf("expected 4 tuples, got %d", len(storedTuples))
	}

	// Verify first tuple (always safe - it's a separate value)
	if storedTuples[0][0] != 1 || storedTuples[0][1] != "first" {
		t.Errorf("first tuple incorrect: got %v", storedTuples[0])
	}

	// CRITICAL: Verify rest tuples have correct values
	// If PrependedIterator doesn't copy from unsafe rest, they'll be corrupted
	expected := [][]interface{}{
		{1, "first"},
		{2, "rest1"},
		{3, "rest2"},
		{4, "rest3"},
	}

	for i, tuple := range storedTuples {
		if tuple[0] != expected[i][0] || tuple[1] != expected[i][1] {
			t.Errorf("tuple %d corrupted: got %v, want %v", i, tuple, expected[i])
		}
	}
}

// =============================================================================
// Test 5: PrependedIterator with safe rest relation
// =============================================================================

func TestPrependedIteratorPassthroughFromSafeRest(t *testing.T) {
	cols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// Create safe relation for the "rest"
	safeRel := NewMaterializedRelation(cols, []Tuple{
		{2, "rest1"},
		{3, "rest2"},
	})

	// Create PrependedRelation using new API
	firstTuple := Tuple{1, "first"}
	prepended := NewPrependedRelationFromRelation(cols, firstTuple, safeRel, ExecutorOptions{})

	// Iterate and store all tuples
	var storedTuples []Tuple
	it := prepended.Iterator()
	for it.Next() {
		storedTuples = append(storedTuples, it.Tuple())
	}
	it.Close()

	if len(storedTuples) != 3 {
		t.Fatalf("expected 3 tuples, got %d", len(storedTuples))
	}

	// Verify all values are correct
	expected := [][]interface{}{
		{1, "first"},
		{2, "rest1"},
		{3, "rest2"},
	}

	for i, tuple := range storedTuples {
		if tuple[0] != expected[i][0] || tuple[1] != expected[i][1] {
			t.Errorf("tuple %d incorrect: got %v, want %v", i, tuple, expected[i])
		}
	}
}

// =============================================================================
// Test 6: Verify mockUnsafeRelation actually corrupts without copying
// This is a sanity check that our mock is working correctly
// =============================================================================

func TestMockUnsafeRelationActuallyCorrupts(t *testing.T) {
	cols := []query.Symbol{datalog.NewSymbol("?x")}
	unsafe := newMockUnsafeRelation(cols, [][]interface{}{
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
// Test 7: Verify MaterializedRelation doesn't corrupt (control test)
// =============================================================================

func TestMaterializedRelationDoesNotCorrupt(t *testing.T) {
	cols := []query.Symbol{datalog.NewSymbol("?x")}
	safe := NewMaterializedRelation(cols, []Tuple{
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
// Test 8: OrFallbackRelation with unsafe outer relation
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
	ctx := NewContext(nil)

	// Create an unsafe outer relation with [?e, ?name]
	// This simulates what BadgerDB returns - workspace reuse
	outerCols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"},
	}
	outerRel := newMockUnsafeRelation(outerCols, outerData)

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
	orFallbackRel := NewOrFallbackRelation(queryExec, ctx, orClause, outerRel, ExecutorOptions{})

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
// Test 9: OrFallbackIterator with unsafe branch result (direct test)
//
// This tests the OrFallbackIterator directly by wrapping an unsafe relation
// as the branch result. This bypasses the execution pipeline that masks the bug.
// =============================================================================

func TestOrFallbackIteratorWithUnsafeBranchResult(t *testing.T) {
	// Create an unsafe relation that will be returned by the "branch"
	// This simulates what would happen if a branch returned a StreamingRelation
	// with workspace reuse
	branchCols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	unsafeBranchData := [][]interface{}{
		{1, "a"},
		{1, "b"},
		{1, "c"},
		{2, "d"},
		{2, "e"},
	}
	unsafeBranch := newMockUnsafeRelation(branchCols, unsafeBranchData)

	// Test projectedIterator - this is what OrFallbackIterator wraps the branch with
	// The fix is in projectedIterator.Tuple() which should copy when source.RequiresCopy() = true
	branchIt := unsafeBranch.Iterator()
	projIt := &projectedIterator{
		inner:          branchIt,
		branchRelation: unsafeBranch, // This has RequiresCopy() = true
		branchCols:     branchCols,
		outputCols:     branchCols, // Same columns, no projection needed
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
// Test 10: OrFallbackRelation with multiple branch results per outer tuple
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
	queryExec := newQueryExecutor(matcher, nil, ExecutorOptions{})
	ctx := NewContext(func(e annotations.Event) {
		t.Logf("[ANNOTATION] %s: %v", e.Name, e.Data)
	})

	// Create an outer relation with 2 entities
	outerCols := []query.Symbol{datalog.NewSymbol("?e")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1")},
		{datalog.NewIdentity("e2")},
	}
	outerRel := newMockUnsafeRelation(outerCols, outerData)

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
	orFallbackRel := NewOrFallbackRelation(queryExec, ctx, orClause, outerRel, ExecutorOptions{})

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
// Test 10: OrFallbackRelation with fallback branch
// =============================================================================

func TestOrFallbackIteratorWithFallbackBranch(t *testing.T) {
	// Create datoms - only e1 and e2 have priority
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: datalog.NewKeyword(":item/priority"), V: int64(1)},
		{E: datalog.NewIdentity("e2"), A: datalog.NewKeyword(":item/priority"), V: int64(2)},
	}

	matcher := NewIndexedMemoryMatcher(datoms)
	queryExec := newQueryExecutor(matcher, nil, ExecutorOptions{})
	ctx := NewContext(nil)

	// Create an unsafe outer relation with 3 items
	outerCols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?name")}
	outerData := [][]interface{}{
		{datalog.NewIdentity("e1"), "item1"},
		{datalog.NewIdentity("e2"), "item2"},
		{datalog.NewIdentity("e3"), "item3"}, // No priority - will use fallback
	}
	outerRel := newMockUnsafeRelation(outerCols, outerData)

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
	orFallbackRel := NewOrFallbackRelation(queryExec, ctx, orClause, outerRel, ExecutorOptions{})

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
