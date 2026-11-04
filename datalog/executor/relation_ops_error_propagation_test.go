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
	joined, err := thetaJoinWithPredicate([]Relation{left, right}, pred, nil, nil, ExecutorOptions{})
	if err == nil {
		err = driveErr(joined)
	}
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
// the expression-evaluation transform, which evaluates as it is pulled, so
// the failure surfaces when its result is driven. expr.Function never runs
// because the source fails before yielding a tuple.
func TestEvaluateExpressionWithLookup_PropagatesIteratorError(t *testing.T) {
	src := newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}, Tuple{int64(3)})
	expr := &query.Expression{Function: erroringFunction{}, Binding: datalog.NewSymbol("?y")}
	rel := evaluateExpressionWithLookup(src, expr, nil, nil)
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
// Function returns an error must surface it when its result is driven. The
// previous behavior was a bare `continue` past the failing tuple — the result
// looked like the function had "no answer" rather than failing.
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
	it := NewFunctionEvaluatorIterator(src, testSymbols(), &query.Expression{Function: erroringFunction{}, Binding: datalog.NewSymbol("?y")}, nil, nil)
	for it.Next() {
	}
	require.ErrorIs(t, it.Error(), errInjectedEval)
	require.NoError(t, it.Close())
}

func TestUniqueCombinationExtractionPropagatesIteratorAndCloseErrors(t *testing.T) {
	x := datalog.NewSymbol("?x")
	closeErr := errors.New("combination close failure")
	failing := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})

	_, err := getUniqueCombinations(failing, []query.Symbol{x})
	require.ErrorIs(t, err, errInjectedIterator)

	// A streaming relation's projection streams, so an injected failure
	// surfaces as the projection's deferred error, not a synchronous return,
	// and is read through the drain-then-check shape. failingRelation is
	// invisible to MaterializedRelation.Project (which reads tuples
	// directly); the streaming sources below present the failing iterator to
	// the path Project actually consumes.
	combos, err := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}).Project([]query.Symbol{x})
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
	combos, err = closeFailingStream.Project([]query.Symbol{x})
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

// A build side's failure is the join's failure only where the probe side
// holds a tuple: beside an empty probe side the join answers no tuple and no
// failure, the build failing at its first tuple, leaving its index empty, or
// past it, and beside a probe side with a tuple it answers the failure
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, the failure contract). Of two
// streams the left is the build side.
func TestHashJoinRaisesABuildFailureOnlyWhereTheProbeSideHoldsATuple(t *testing.T) {
	x := []query.Symbol{datalog.NewSymbol("?x")}
	for _, b := range []struct {
		name  string
		build func() Relation
	}{
		{"a build failing at its first tuple", func() Relation { return newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}) }},
		{"a build failing past its first tuple", func() Relation { return newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}) }},
	} {
		t.Run(b.name+" beside an empty probe side", func(t *testing.T) {
			tuples, err := CollectTuples(HashJoin(b.build(), streamOf("?x"), x), nil)
			require.NoError(t, err, "no assignment holds the empty probe side, so no failure is raised")
			require.Empty(t, tuples)
		})
		t.Run(b.name+" beside a probe side with a tuple", func(t *testing.T) {
			_, err := CollectTuples(HashJoin(b.build(), streamOf("?x", Tuple{int64(1)}), x), nil)
			require.ErrorIs(t, err, errInjectedIterator)
		})
	}
}

// Every join strategy answers a side's failure only where the other side
// holds a tuple or a failure: beside an empty side the join answers no tuple
// and no failure, and beside a side with a tuple it answers the failure,
// whether the failing side is the left or the right and whether it fails at
// its first tuple or past it (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md,
// the failure contract). The hash join and the symmetric hash join, which
// EnableSymmetricHashJoin selects over two streams, answer each case alike.
func TestEveryJoinStrategyAnswersASideFailureOnlyWhereTheOtherSideHoldsATuple(t *testing.T) {
	x := []query.Symbol{datalog.NewSymbol("?x")}
	failing := []struct {
		name string
		side func() Relation
	}{
		{"failing at its first tuple", func() Relation { return newFailingStream(0, Tuple{int64(1)}, Tuple{int64(2)}) }},
		{"failing past its first tuple", func() Relation { return newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}) }},
	}
	strategies := []struct {
		name string
		opts ExecutorOptions
	}{
		{"hash join", ExecutorOptions{}},
		{"symmetric hash join", ExecutorOptions{EnableSymmetricHashJoin: true}},
	}
	for _, strategy := range strategies {
		for _, f := range failing {
			t.Run(strategy.name+"/the left side "+f.name+" beside an empty right side", func(t *testing.T) {
				tuples, err := CollectTuples(HashJoinWithOptions(f.side(), streamOf("?x"), x, strategy.opts), nil)
				require.NoError(t, err, "no assignment holds the empty side, so no failure is raised")
				require.Empty(t, tuples)
			})
			t.Run(strategy.name+"/the right side "+f.name+" beside an empty left side", func(t *testing.T) {
				tuples, err := CollectTuples(HashJoinWithOptions(streamOf("?x"), f.side(), x, strategy.opts), nil)
				require.NoError(t, err, "no assignment holds the empty side, so no failure is raised")
				require.Empty(t, tuples)
			})
			t.Run(strategy.name+"/the left side "+f.name+" beside a right side with a tuple", func(t *testing.T) {
				_, err := CollectTuples(HashJoinWithOptions(f.side(), streamOf("?x", Tuple{int64(1)}), x, strategy.opts), nil)
				require.ErrorIs(t, err, errInjectedIterator)
			})
			t.Run(strategy.name+"/the right side "+f.name+" beside a left side with a tuple", func(t *testing.T) {
				_, err := CollectTuples(HashJoinWithOptions(streamOf("?x", Tuple{int64(1)}), f.side(), x, strategy.opts), nil)
				require.ErrorIs(t, err, errInjectedIterator)
			})
		}
		t.Run(strategy.name+"/both sides failing", func(t *testing.T) {
			_, err := CollectTuples(HashJoinWithOptions(newFailingStream(0, Tuple{int64(1)}), newFailingStream(0, Tuple{int64(1)}), x, strategy.opts), nil)
			require.ErrorIs(t, err, errInjectedIterator)
		})
		t.Run(strategy.name+"/both sides holding tuples", func(t *testing.T) {
			tuples, err := CollectTuples(HashJoinWithOptions(streamOf("?x", Tuple{int64(1)}, Tuple{int64(2)}), streamOf("?x", Tuple{int64(2)}), x, strategy.opts), nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, tuples)
		})
	}
}

// A join of two sides answers no failure where a side ended with no tuple and
// no failure, and otherwise the first side's failure, else the second's.
func TestJoinFailureIsTheFirstSidesFailureUnlessASideEndedEmpty(t *testing.T) {
	first, second := errors.New("the first side's failure"), errors.New("the second side's failure")
	cases := []struct {
		name          string
		first, second sideRead
		want          error
	}{
		{"both sides yielded and neither failed", sideRead{yielded: true}, sideRead{yielded: true}, nil},
		{"the first side ended empty beside a failing second side", sideRead{}, sideRead{err: second}, nil},
		{"the second side ended empty beside a failing first side", sideRead{yielded: true, err: first}, sideRead{}, nil},
		{"the first side failed in place of a tuple beside a second side that yielded", sideRead{err: first}, sideRead{yielded: true}, first},
		{"the second side failed beside a first side that yielded", sideRead{yielded: true}, sideRead{yielded: true, err: second}, second},
		{"both sides failed", sideRead{err: first}, sideRead{err: second}, first},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := joinFailure(c.first, c.second)
			if c.want == nil {
				require.NoError(t, got)
				return
			}
			require.ErrorIs(t, got, c.want)
		})
	}
}
