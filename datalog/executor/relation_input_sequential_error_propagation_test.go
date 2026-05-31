// Reproductions for docs/bugs/BUG_SEQUENTIAL_RELATION_INPUT_DROPS_ITERATOR_ERROR.md
//
// The sequential RelationInput path drives the iteration relation directly:
//
//	it := iterationRelation.Iterator()
//	defer it.Close()
//	for it.Next() { ... }
//
// Per the Iterator contract, Error() must be checked after Next() returns false,
// and Close()'s error must not be discarded. The parallel path already does
// both; the sequential path is the counterpart that this file pins. These are
// the sequential analogues of TestRelationInputParallel_PropagatesDeferredIteratorError,
// but they fail the *input* iteration relation (the actual sequential loss
// site) rather than a per-tuple result.

package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// peopleInputSymbols are the [[?n ?y] ...] relation-input symbols for
// peopleQueryNoAgg.
func peopleInputSymbols() []query.Symbol {
	return []query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y")}
}

// TestRelationInputSequential_PropagatesDeferredInputIteratorError targets the
// loss site unique to the sequential path: the loop driving the RelationInput
// iteration relation. If that input relation's iterator yields a prefix and then
// reports a deferred error (the way a streaming/storage-backed input aborts
// mid-scan), the sequential path must surface it rather than return the prefix's
// per-tuple results as a clean success.
func TestRelationInputSequential_PropagatesDeferredInputIteratorError(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	// Iteration relation for [[?n ?y] ...]: yields one tuple, then defers
	// errInjectedIterator via Iterator().Error().
	base := NewMaterializedRelation(peopleInputSymbols(), []Tuple{
		{"Alice", int64(2020)},
		{"Bob", int64(2020)},
	})
	inputRel := failingRelation{Relation: base, failAfter: 1}

	exec := NewExecutor(matcher, nil)
	exec.DisableParallelSubqueries()

	_, err = exec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
	require.Error(t, err,
		"deferred input-iterator error must propagate, not be laundered into clean partial results")
	require.True(t, errors.Is(err, errInjectedIterator),
		"error must unwrap to errInjectedIterator; got %v", err)
}

// TestRelationInputSequential_PropagatesInputCloseError pins the second signal:
// the iteration relation iterates cleanly to exhaustion but its Close() reports
// an error. The sequential path defers Close() and discards its return; that
// error must surface rather than be dropped.
func TestRelationInputSequential_PropagatesInputCloseError(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	closeErr := errors.New("injected input Close failure")

	tuples := []Tuple{
		{"Alice", int64(2020)},
		{"Bob", int64(2020)},
	}
	base := NewMaterializedRelation(peopleInputSymbols(), tuples)
	// failAfter past the tuple count => no deferred iteration error; Close()
	// returns closeErr.
	inputRel := failingRelation{Relation: base, failAfter: len(tuples) + 1, closeErr: closeErr}

	exec := NewExecutor(matcher, nil)
	exec.DisableParallelSubqueries()

	_, err = exec.ExecuteWithRelations(NewContext(nil), q, []Relation{inputRel})
	require.Error(t, err,
		"input Close() error must propagate, not be discarded by a deferred Close()")
	require.True(t, errors.Is(err, closeErr),
		"error must unwrap to the injected Close error; got %v", err)
}
