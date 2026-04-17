// Regression test for EXTERNAL_REVIEW_2026_04.md item 5:
// MaterializeResult false-positive panic on legitimate equal tuples.

package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestMaterializeResult_EqualTuplesDoNotPanic verifies that a relation
// containing multiple interface-equal tuples materializes cleanly.
//
// The current implementation contains a debug assertion (intended to
// catch a historical iterator-workspace-reuse bug) that panics if the
// first and last tuples are `==`-equal on every element. With interned
// Identity and Keyword pointers, legitimate queries can produce such
// results — e.g., two entities with the same interned name value.
//
// Pre-fix: this test panics with "BUG DETECTED in MaterializeResult".
func TestMaterializeResult_EqualTuplesDoNotPanic(t *testing.T) {
	// Two distinct entities that happen to share an interned value.
	// Under interning, tuple[0] for each is pointer-equal across tuples
	// if we project to [?name], which triggers the false-positive panic.
	nameKw := datalog.NewKeyword(":user/name")

	// Use the no-dedup variant so multiple interface-equal tuples
	// survive as separate entries. In production, such sequences can
	// arise when the iterator chain hasn't yet dedupped (e.g.,
	// streaming relations mid-pipeline, or results from joins with
	// one-side pre-expansion). The debug check fires regardless of
	// whether the upstream was "supposed to" dedup.
	symbols := []query.Symbol{datalog.NewSymbol("?attr")}
	tuples := []Tuple{
		{nameKw},
		{nameKw},
		{nameKw},
	}
	rel := NewMaterializedRelationNoDedupe(symbols, tuples)

	// Should not panic.
	assert.NotPanics(t, func() {
		_ = MaterializeResult(rel, symbols)
	}, "MaterializeResult must not panic on legitimate equal-tuple results")
}
