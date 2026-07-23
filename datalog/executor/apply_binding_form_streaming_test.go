// Tests for applyBindingForm that exercise the streaming path.
//
// Prior to the streaming rewrite, applyBindingForm called result.Size()
// and result.Get(i) directly. Those APIs only work on materialized
// relations — on a StreamingRelation, Size() returns -1 regardless of
// the actual tuple count, which broke two invariants:
//
//  1. Empty streaming subquery result → datalog semantics say "pattern
//     fails to match, return empty relation", but the code returned an
//     error because Size() == 0 was false (it was -1).
//  2. Non-1 streaming subquery result with TupleBinding/ScalarBinding →
//     the error message reported "got -1" instead of the real count.
//
// The rewrite iterates the input relation directly, enforcing
// cardinality (TupleBinding/ScalarBinding) or wrapping the iterator in
// a streaming output (RelationBinding) so end-to-end streaming is
// preserved through the subquery → union boundary.
package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// streamingRelOf wraps tuples as a StreamingRelation with the given
// symbols. Size() returns -1 — the whole point of these tests is that
// applyBindingForm must not rely on it.
func streamingRelOf(t *testing.T, symbols []query.Symbol, tuples []Tuple) Relation {
	t.Helper()
	iter := &sliceIterator{tuples: tuples, pos: -1}
	rel := NewStreamingRelation(symbols, iter)
	require.Equal(t, -1, rel.Size(),
		"streaming relation precondition: Size() must be -1 for these tests to be meaningful")
	return rel
}

func sym(s string) query.Symbol { return datalog.NewSymbol(s) }

// --- TupleBinding ---

// Empty streaming result → empty relation (datalog pattern-fails-to-match),
// not an error. Input-value bindings carried through with no rows.
func TestApplyBindingForm_TupleBinding_StreamingEmpty(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.TupleBinding{Variables: []query.Symbol{sym("?age")}}

	result := streamingRelOf(t, []query.Symbol{sym("?age")}, nil)

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, []query.Symbol{sym("?e"), sym("?age")}, out.Symbols())

	var rows []Tuple
	it := out.Iterator()
	for it.Next() {
		rows = append(rows, append(Tuple{}, it.Tuple()...))
	}
	require.NoError(t, it.Error())
	it.Close()
	assert.Empty(t, rows, "empty streaming subquery must produce zero output rows")
}

// Exactly-1 streaming result → 1 output row: input values + subquery tuple.
func TestApplyBindingForm_TupleBinding_StreamingSingle(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.TupleBinding{Variables: []query.Symbol{sym("?age")}}

	result := streamingRelOf(t, []query.Symbol{sym("?age")}, []Tuple{{int64(42)}})

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{sym("?e"), sym("?age")}, out.Symbols())

	var rows []Tuple
	it := out.Iterator()
	for it.Next() {
		rows = append(rows, append(Tuple{}, it.Tuple()...))
	}
	require.NoError(t, it.Error())
	it.Close()
	require.Len(t, rows, 1)
	assert.Equal(t, Tuple{"e1", int64(42)}, rows[0])
}

// More than 1 streaming result → error mentioning "expects 1 result".
// Error count must be accurate (not -1), but we only require "at least 2"
// because the implementation may short-circuit after the second tuple.
func TestApplyBindingForm_TupleBinding_StreamingMultiple(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.TupleBinding{Variables: []query.Symbol{sym("?age")}}

	result := streamingRelOf(t, []query.Symbol{sym("?age")},
		[]Tuple{{int64(42)}, {int64(43)}, {int64(44)}})

	_, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tuple binding expects exactly 1 result")
	assert.NotContains(t, err.Error(), "got -1",
		"error must report a real count, not -1 from an unmaterialized relation")
}

// --- ScalarBinding ---

func TestApplyBindingForm_ScalarBinding_StreamingEmpty(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.ScalarBinding{Variable: sym("?age")}

	result := streamingRelOf(t, []query.Symbol{sym("?age")}, nil)

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{sym("?e"), sym("?age")}, out.Symbols())

	it := out.Iterator()
	hasAny := it.Next()
	require.NoError(t, it.Error())
	it.Close()
	assert.False(t, hasAny, "empty streaming scalar binding must produce zero rows")
}

func TestApplyBindingForm_ScalarBinding_StreamingSingle(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.ScalarBinding{Variable: sym("?age")}

	result := streamingRelOf(t, []query.Symbol{sym("?age")}, []Tuple{{int64(42)}})

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)

	var rows []Tuple
	it := out.Iterator()
	for it.Next() {
		rows = append(rows, append(Tuple{}, it.Tuple()...))
	}
	require.NoError(t, it.Error())
	it.Close()
	require.Len(t, rows, 1)
	assert.Equal(t, Tuple{"e1", int64(42)}, rows[0])
}

func TestApplyBindingForm_ScalarBinding_StreamingMultiple(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.ScalarBinding{Variable: sym("?age")}

	result := streamingRelOf(t, []query.Symbol{sym("?age")},
		[]Tuple{{int64(42)}, {int64(43)}})

	_, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scalar binding expects exactly 1 result")
	assert.NotContains(t, err.Error(), "got -1")
}

// --- RelationBinding ---

func TestApplyBindingForm_RelationBinding_StreamingEmpty(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.RelationBinding{Variables: []query.Symbol{sym("?t"), sym("?v")}}

	result := streamingRelOf(t, []query.Symbol{sym("?t"), sym("?v")}, nil)

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{sym("?e"), sym("?t"), sym("?v")}, out.Symbols())

	it := out.Iterator()
	hasAny := it.Next()
	require.NoError(t, it.Error())
	it.Close()
	assert.False(t, hasAny)
}

// N-tuple streaming RelationBinding → N output rows, each prefixed with
// input values. Exercises the per-tuple transform.
func TestApplyBindingForm_RelationBinding_StreamingMany(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.RelationBinding{Variables: []query.Symbol{sym("?t"), sym("?v")}}

	result := streamingRelOf(t, []query.Symbol{sym("?t"), sym("?v")}, []Tuple{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	})

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)
	assert.Equal(t, []query.Symbol{sym("?e"), sym("?t"), sym("?v")}, out.Symbols())

	var rows []Tuple
	it := out.Iterator()
	for it.Next() {
		rows = append(rows, append(Tuple{}, it.Tuple()...))
	}
	require.NoError(t, it.Error())
	it.Close()
	assert.ElementsMatch(t, []Tuple{
		{"e1", int64(1), "a"},
		{"e1", int64(2), "b"},
		{"e1", int64(3), "c"},
	}, rows)
}

// RelationBinding must preserve streaming end-to-end: the output
// relation's Size() should be -1 (streaming, unknown) — materializing
// at this boundary defeats the whole streaming pipeline.
func TestApplyBindingForm_RelationBinding_OutputIsStreaming(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.RelationBinding{Variables: []query.Symbol{sym("?t"), sym("?v")}}

	result := streamingRelOf(t, []query.Symbol{sym("?t"), sym("?v")}, []Tuple{
		{int64(1), "a"},
		{int64(2), "b"},
	})

	out, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.NoError(t, err)
	assert.Equal(t, -1, out.Size(),
		"RelationBinding over a streaming input must return a streaming relation (Size() == -1)")
}

// Verify ScalarBinding rejects multi-symbol input tuples even in the
// streaming path. This is a schema check, not a cardinality check.
func TestApplyBindingForm_ScalarBinding_RejectsMultiSymbolTuple(t *testing.T) {
	inputSyms := []query.Symbol{sym("?e")}
	inputValues := Tuple{"e1"}
	binding := query.ScalarBinding{Variable: sym("?v")}

	result := streamingRelOf(t, []query.Symbol{sym("?a"), sym("?b")},
		[]Tuple{{int64(1), int64(2)}})

	_, err := applyBindingForm(result, binding, inputSyms, inputValues)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scalar binding expects 1 symbol")
	assert.Contains(t, err.Error(), "got 2")
}
