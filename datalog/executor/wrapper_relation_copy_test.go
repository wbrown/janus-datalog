package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
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

// RequiresCopy is true: Tuple hands back the one workspace the next pull
// overwrites.
func (it *workspaceReusingIterator) RequiresCopy() bool { return true }

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
