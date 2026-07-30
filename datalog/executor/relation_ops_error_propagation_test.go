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

// Reproductions for BUG_RELATION_TRANSFORMS_DROP_ITERATOR_ERRORS.md
//
// filterWithPredicateAndLookup, evaluateExpressionWithLookup, and
// projectToSymbols in relation_ops.go consume their source iterator via a
// hand-rolled `for iter.Next()` loop and return a fresh MaterializedRelation
// without checking iter.Error() or capturing the Close() error. Same class as
// the collectTuplesInto sites fixed in BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES
// and the SemiJoin/AntiJoin sites fixed in BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS.
// These three are manual loops, not collectTuplesInto calls (the static
// guard) and not join helpers.
//
// The failing-iterator helpers (failingIterator, newFailingStream, driveErr,
// errInjectedIterator) live in iterator_error_boundary_test.go.

// alwaysTrue is a Predicate that always passes. `[(!= 1 2)]` evaluates true
// for every binding; using a real query.Predicate (rather than a test stub)
// avoids re-implementing the Clause interface's package-private clause()
// marker outside the query package.
func alwaysTrue() query.Predicate {
	return &query.Comparison{
		Op:    datalog.SymNE,
		Left:  query.ConstantTerm{Value: int64(1)},
		Right: query.ConstantTerm{Value: int64(2)},
	}
}

// TestThetaJoinPair_PropagatesPredicateEvalError: a predicate eval failure
// during a theta join must surface, not silently drop the pair — the same
// fail-fast contract filterWithPredicateAndLookup honors.
func TestThetaJoinPair_PropagatesPredicateEvalError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	left := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}})
	right := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(2)}})
	pred := &query.FunctionPredicate{
		Fn:   "no-such-predicate",
		Args: []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}},
	}
	_, err := thetaJoinWithPredicate([]Relation{left, right}, pred, nil, nil, ExecutorOptions{})
	require.Error(t, err)
}

// TestThetaJoinPair_PropagatesOuterIteratorError: a failed outer scan must
// not be presented as a completed join.
func TestThetaJoinPair_PropagatesOuterIteratorError(t *testing.T) {
	y := datalog.NewSymbol("?y")
	outer := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)})
	inner := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(9)}})
	_, err := thetaJoinPair(outer, inner, nil, nil, nil, ExecutorOptions{})
	require.ErrorIs(t, err, errInjectedIterator)
}

// TestThetaJoinPair_PropagatesInnerIteratorError: same for the buffered
// inner relation.
func TestThetaJoinPair_PropagatesInnerIteratorError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	outer := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}})
	inner := newFailingStream(1, Tuple{int64(9)}, Tuple{int64(8)})
	_, err := thetaJoinPair(outer, inner, nil, nil, nil, ExecutorOptions{})
	require.ErrorIs(t, err, errInjectedIterator)
}

// TestFilterWithPredicateAndLookup_PropagatesIteratorError: an iterator
// failure must not be laundered into a clean filtered relation. Eager
// producers return errors in-band.
func TestFilterWithPredicateAndLookup_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	_, err := filterWithPredicateAndLookup(src, alwaysTrue(), nil, nil)
	require.ErrorIs(t, err, errInjectedIterator)
}

// TestFilterWithPredicateAndLookup_PropagatesAfterPartialResults: same, but
// after the iterator has yielded some tuples and then failed. The pre-fix
// function returned a clean MaterializedRelation containing only the tuples
// that survived the predicate before the failure — a truncated success.
func TestFilterWithPredicateAndLookup_PropagatesAfterPartialResults(t *testing.T) {
	src := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	_, err := filterWithPredicateAndLookup(src, alwaysTrue(), nil, nil)
	require.ErrorIs(t, err, errInjectedIterator)
}

// TestEvaluateExpressionWithLookup_PropagatesIteratorError: same shape for
// the expression-evaluation transform. expr.Function never runs because the
// loop body never runs; only the pre-loop Binding inspection executes.
func TestEvaluateExpressionWithLookup_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	expr := &query.Expression{Binding: datalog.NewSymbol("?y")}
	_, err := evaluateExpressionWithLookup(src, expr, nil, nil)
	require.ErrorIs(t, err, errInjectedIterator)
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
	// ?missing isn't a symbol in `src`, so every tuple's bindings lack it
	// and Comparison.Eval returns ("cannot resolve left term ?missing").
	pred := &query.Comparison{
		Op:    datalog.SymEQ,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?missing")},
		Right: query.ConstantTerm{Value: int64(1)},
	}
	_, err := filterWithPredicateAndLookup(src, pred, nil, nil)
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
	_, err := evaluateExpressionWithLookup(src, expr, nil, nil)
	require.ErrorIs(t, err, errInjectedEval)
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

	// Unique-combination extraction is filterSourceSymbols + Relation.Project
	// consumed through the projection's iterator (executeSubquery drains
	// comboIter, then checks Error() and its deferred Close()). The
	// extraction is streaming, so an injected failure surfaces as the
	// projection's deferred error, not a synchronous return — pinned through
	// the same drain-then-check shape.
	// failingRelation is invisible to MaterializedRelation.Project (which
	// reads tuples directly); the streaming sources below present the failing
	// iterator to the path Project actually consumes.
	dataSymbols := filterSourceSymbols([]query.Symbol{x})
	combos, err := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}).Project(dataSymbols)
	require.NoError(t, err)
	require.ErrorIs(t, driveErr(combos), errInjectedIterator)

	closeFailing := failingRelation{
		Relation:  NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(1)}}),
		failAfter: 100,
		closeErr:  closeErr,
	}
	_, err = getUniqueCombinations(closeFailing, []query.Symbol{x})
	require.ErrorIs(t, err, closeErr)

	closeFailingStream := NewStreamingRelation(testSymbols(), &failingIterator{
		inner:     NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}}).Iterator(),
		failAfter: 100,
		closeErr:  closeErr,
	})
	combos, err = closeFailingStream.Project(dataSymbols)
	require.NoError(t, err)
	closeIter := combos.Iterator()
	for closeIter.Next() {
	}
	require.NoError(t, closeIter.Error())
	require.ErrorIs(t, closeIter.Close(), closeErr)
}

// TestMaterializedRelation_FilterWithPredicate_PropagatesEvalError: the
// materialized filter method silently dropped tuples whose predicate failed
// to evaluate (`err == nil && passes`) — a truncated success. The first eval
// error must surface as the result's deferred error.
func TestMaterializedRelation_FilterWithPredicate_PropagatesEvalError(t *testing.T) {
	src := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}})
	pred := &query.Comparison{
		Op:    datalog.SymEQ,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?missing")},
		Right: query.ConstantTerm{Value: int64(1)},
	}
	rel := src.FilterWithPredicate(pred)
	err := driveErr(rel)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resolve")
}

// TestMaterializedRelation_FilterWithPredicate_CarriesSourceError: filtering
// an errored relation must not launder the source error into a clean result.
func TestMaterializedRelation_FilterWithPredicate_CarriesSourceError(t *testing.T) {
	srcErr := errors.New("source relation failure")
	src := NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}})
	src.err = srcErr
	rel := src.FilterWithPredicate(alwaysTrue())
	require.ErrorIs(t, driveErr(rel), srcErr)
}

// TestMatchTreatsErroredEmptyBindingAsError: an errored relation that
// materialized empty (the genuine route: a stream fails, Sort materializes
// zero tuples plus the taint) is not an empty binding. Match's emptiness
// fallback must surface the error instead of discarding the binding and
// scanning unbound — the laundering half of
// BUG_MISSING_ON_LOOKUPLESS_MATCHER_SILENTLY_EMPTY.md.
func TestMatchTreatsErroredEmptyBindingAsError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	valAttr := datalog.NewKeyword(":item/val")
	matcher := NewMemoryPatternMatcher([]datalog.Datom{
		{E: datalog.NewIdentity("item:1"), A: valAttr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	})

	erroredEmpty := newFailingStream(0, Tuple{int64(1)}).Sort(nil)
	require.Equal(t, 0, erroredEmpty.Size(), "fixture: materialized empty")
	require.ErrorIs(t, EmptyRelationError(erroredEmpty), errInjectedIterator, "fixture: carries the taint")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: x},
		query.Constant{Value: valAttr},
		query.Blank{},
	}}
	_, err := matcher.Match(query.PatternQuery(pattern), Relations{erroredEmpty})
	require.ErrorIs(t, err, errInjectedIterator)
}

// TestHashJoinSurfacesErroredEmptySide: joining against an errored-empty
// relation must not present the empty join as a clean result.
func TestHashJoinSurfacesErroredEmptySide(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	left := NewMaterializedRelation([]query.Symbol{x, y}, []Tuple{{int64(1), int64(2)}})
	erroredEmpty := newFailingStream(0, Tuple{int64(1)}).Sort(nil)

	joined := HashJoinWithOptions(left, erroredEmpty, []query.Symbol{x}, ExecutorOptions{})
	require.ErrorIs(t, driveErr(joined), errInjectedIterator)
}
