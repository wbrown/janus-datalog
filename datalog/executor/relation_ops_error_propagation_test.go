package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

var errInjectedEval = errors.New("injected eval failure")

// erroringFunction is a query.Function that always fails. Used by the
// expression-evaluation eval-error regression tests.
type erroringFunction struct{}

func (erroringFunction) String() string                  { return "errorFn" }
func (erroringFunction) RequiredSymbols() []query.Symbol { return nil }
func (erroringFunction) Eval(map[query.Symbol]interface{}) (interface{}, error) {
	return nil, errInjectedEval
}
func (erroringFunction) ReturnType() string { return "any" }

// Reproductions for docs/bugs/BUG_RELATION_TRANSFORMS_DROP_ITERATOR_ERRORS.md
//
// filterWithPredicateAndLookup, evaluateExpressionWithLookup, and
// projectToSymbols in relation_ops.go consume their source iterator via a
// hand-rolled `for iter.Next()` loop and return a fresh MaterializedRelation
// without checking iter.Error() or capturing the Close() error. Same class as
// the collectTuplesInto sites fixed in BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES
// and the SemiJoin/AntiJoin sites fixed in BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS.
// These three slipped past both sweeps because they are manual loops, not
// collectTuplesInto calls (the static guard) and not join helpers.
//
// The failing-iterator helpers (failingIterator, newFailingStream, driveErr,
// errInjectedIterator) live in iterator_error_boundary_test.go.

// alwaysTrue is a Predicate that always passes. `[(!= 1 2)]` evaluates true
// for every binding; using a real query.Predicate (rather than a test stub)
// avoids re-implementing the Clause interface's package-private clause()
// marker outside the query package.
func alwaysTrue() query.Predicate {
	return &query.Comparison{
		Op:    query.OpNE,
		Left:  query.ConstantTerm{Value: int64(1)},
		Right: query.ConstantTerm{Value: int64(2)},
	}
}

// TestFilterWithPredicateAndLookup_PropagatesIteratorError: a deferred
// iterator failure must not be laundered into a clean filtered relation.
func TestFilterWithPredicateAndLookup_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	rel := filterWithPredicateAndLookup(src, alwaysTrue(), nil, nil)
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestFilterWithPredicateAndLookup_PropagatesAfterPartialResults: same, but
// after the iterator has yielded some tuples and then failed. The pre-fix
// function returns a clean MaterializedRelation containing only the tuples
// that survived the predicate before the failure — a truncated success.
func TestFilterWithPredicateAndLookup_PropagatesAfterPartialResults(t *testing.T) {
	src := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	rel := filterWithPredicateAndLookup(src, alwaysTrue(), nil, nil)
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestEvaluateExpressionWithLookup_PropagatesIteratorError: same shape for
// the expression-evaluation transform. expr.Function never runs because the
// loop body never runs; only the pre-loop Binding inspection executes.
func TestEvaluateExpressionWithLookup_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	expr := &query.Expression{Binding: datalog.NewSymbol("?y")}
	rel := evaluateExpressionWithLookup(src, expr, nil, nil)
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestProjectToSymbols_PropagatesIteratorError: projection must not launder a
// failed source into a clean projected result.
func TestProjectToSymbols_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	rel := projectToSymbols(src, testSymbols(), ExecutorOptions{})
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestProjectToSymbols_PropagatesAfterPartialResults: same, but after partial
// projection succeeds and the iterator fails partway through.
func TestProjectToSymbols_PropagatesAfterPartialResults(t *testing.T) {
	src := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	rel := projectToSymbols(src, testSymbols(), ExecutorOptions{})
	require.ErrorIs(t, driveErr(rel), errInjectedIterator)
}

// TestFilterWithPredicateAndLookup_PropagatesEvalError: a predicate that fails
// to evaluate (here: references a variable that isn't in the source's bindings,
// so Comparison.Eval returns "cannot resolve...") must surface the error, not
// silently `continue` past the tuple. Fail-fast: first eval error stops the
// loop, surviving tuples are not reported as a clean filtered result.
func TestFilterWithPredicateAndLookup_PropagatesEvalError(t *testing.T) {
	src := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	// ?missing isn't a column in `src`, so every tuple's bindings lack it
	// and Comparison.Eval returns ("cannot resolve left term ?missing").
	pred := &query.Comparison{
		Op:    query.OpEQ,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?missing")},
		Right: query.ConstantTerm{Value: int64(1)},
	}
	rel := filterWithPredicateAndLookup(src, pred, nil, nil)
	err := driveErr(rel)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resolve")
}

// TestEvaluateExpressionWithLookup_PropagatesEvalError: an expression whose
// Function returns an error must surface it. The previous behavior was a bare
// `continue` past the failing tuple — the result looked like the function had
// "no answer" rather than failing.
func TestEvaluateExpressionWithLookup_PropagatesEvalError(t *testing.T) {
	src := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	expr := &query.Expression{
		Function: erroringFunction{},
		Binding:  datalog.NewSymbol("?y"),
	}
	rel := evaluateExpressionWithLookup(src, expr, nil, nil)
	require.ErrorIs(t, driveErr(rel), errInjectedEval)
}

// TestMaterializedRelation_EvaluateFunction_PropagatesEvalError: same shape as
// the relation_ops fix, but for the MaterializedRelation method that
// duplicated the swallowing pattern (relation.go EvaluateFunction).
func TestMaterializedRelation_EvaluateFunction_PropagatesEvalError(t *testing.T) {
	src := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	rel := src.EvaluateFunction(erroringFunction{}, datalog.NewSymbol("?y"))
	require.ErrorIs(t, driveErr(rel), errInjectedEval)
}

// TestFunctionEvaluatorIterator_PropagatesEvalError: streaming counterpart of
// the above (iterator_composition.go FunctionEvaluatorIterator). Eval failures
// now stop iteration and surface via Error(), instead of silently skipping.
func TestFunctionEvaluatorIterator_PropagatesEvalError(t *testing.T) {
	src := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}}).Iterator()
	it := NewFunctionEvaluatorIterator(src, testSymbols(), erroringFunction{}, datalog.NewSymbol("?y"))
	for it.Next() {
	}
	require.ErrorIs(t, it.Error(), errInjectedEval)
	_ = it.Close()
}

func TestUniqueCombinationExtractionPropagatesIteratorAndCloseErrors(t *testing.T) {
	x := datalog.NewSymbol("?x")
	closeErr := errors.New("combination close failure")
	failing := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})

	_, err := getUniqueCombinations(failing, []query.Symbol{x})
	require.ErrorIs(t, err, errInjectedIterator)
	_, err = getUniqueInputCombinations(failing, []query.Symbol{x})
	require.ErrorIs(t, err, errInjectedIterator)

	closeFailing := failingRelation{
		Relation:  NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}}),
		failAfter: 100,
		closeErr:  closeErr,
	}
	_, err = getUniqueCombinations(closeFailing, []query.Symbol{x})
	require.ErrorIs(t, err, closeErr)
	_, err = getUniqueInputCombinations(closeFailing, []query.Symbol{x})
	require.ErrorIs(t, err, closeErr)
}
