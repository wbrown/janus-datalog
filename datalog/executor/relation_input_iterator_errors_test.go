// Reproductions for BUG_SEQUENTIAL_RELATION_INPUT_DROPS_ITERATOR_ERROR.md
//
// BindQueryInputs (executor_utils.go) reads a relation input's tuples into
// the bound group through the relation's iterator, takes Error() after Next()
// returns false, and takes Close()'s error when the iteration's is nil; each
// is the query's error. These tests fail the *input* relation, where
// TestRelationInput_PropagatesDeferredIteratorError fails a pattern's result.

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

// TestRelationInput_PropagatesDeferredInputIteratorError: an input relation
// whose iterator yields a prefix and then reports a deferred error (the way a
// streaming/storage-backed input aborts mid-scan) makes the query fail with
// that error rather than answer over the prefix as a clean success.
func TestRelationInput_PropagatesDeferredInputIteratorError(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	// Input relation for [[?n ?y] ...]: yields one tuple, then defers
	// errInjectedIterator via Iterator().Error().
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// failingRelation wraps a single-use relation, so it must be
			// rebuilt fresh per mode rather than shared across modes.
			base := NewMaterializedRelation(peopleInputSymbols(), []Tuple{
				{"Alice", int64(2020)},
				{"Bob", int64(2020)},
			})
			inputRel := failingRelation{Relation: base, failAfter: 1}

			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			_, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.Error(t, err,
				"deferred input-iterator error must propagate, not be laundered into clean partial results")
			require.True(t, errors.Is(err, errInjectedIterator),
				"error must unwrap to errInjectedIterator; got %v", err)
		})
	}
}

// TestRelationInput_PropagatesInputCloseError pins the second signal: the
// input relation iterates cleanly to exhaustion but its Close() reports an
// error, and that error must surface rather than be dropped.
func TestRelationInput_PropagatesInputCloseError(t *testing.T) {
	matcher := NewMemoryPatternMatcher(buildPeopleDatoms())
	q, err := parser.ParseQuery(peopleQueryNoAgg)
	require.NoError(t, err)

	closeErr := errors.New("injected input Close failure")

	tuples := []Tuple{
		{"Alice", int64(2020)},
		{"Bob", int64(2020)},
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// failingRelation wraps a single-use relation, so it must be
			// rebuilt fresh per mode rather than shared across modes.
			base := NewMaterializedRelation(peopleInputSymbols(), tuples)
			// failAfter past the tuple count => no deferred iteration error; Close()
			// returns closeErr.
			inputRel := failingRelation{Relation: base, failAfter: len(tuples) + 1, closeErr: closeErr}

			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

			_, err := exec.ExecuteWithRelations(NewContext(), q, []Relation{inputRel})
			require.Error(t, err,
				"input Close() error must propagate, not be discarded by a deferred Close()")
			require.True(t, errors.Is(err, closeErr),
				"error must unwrap to the injected Close error; got %v", err)
		})
	}
}
