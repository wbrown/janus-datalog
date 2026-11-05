package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A filter answers the source tuple its own failure arose on, a value at
// every position, and resumes past it, so Next reads on to the source's other
// tuples. A failure of its source is not one it answers
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, the failure contract).
func TestAFilterAnswersTheTupleItsFailureAroseOnAndResumesPastIt(t *testing.T) {
	RegisterCustomFunction("test/fails-on-x?", func(args []interface{}) (interface{}, error) {
		if args[0] == "x" {
			return nil, errInjectedEval
		}
		return true, nil
	})
	a := datalog.NewSymbol("?a")
	pred := &query.FunctionPredicate{Fn: "test/fails-on-x?", Args: []query.PatternElement{query.Variable{Name: a}}}
	source := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {"x"}, {int64(2)}})

	var filter Iterator = NewPredicateFilterIterator(source.Iterator(), source.Symbols(), pred, nil, nil)
	require.True(t, filter.Next())
	require.Equal(t, Tuple{int64(1)}, filter.Tuple())
	require.False(t, filter.Next(), "the predicate fails on the second tuple")
	require.ErrorContains(t, filter.Error(), errInjectedEval.Error())

	failing, ok := filter.(failedTupleIterator)
	require.True(t, ok, "a filter answers the tuple its own failure arose on")
	tuple, unknown, ok := failing.failedTuple()
	require.True(t, ok)
	require.Equal(t, Tuple{"x"}, tuple)
	require.Empty(t, unknown, "a filter's tuple is its source's, a value at every position")

	failing.resume()
	require.NoError(t, filter.Error())
	require.True(t, filter.Next(), "Next reads on past the failed tuple")
	require.Equal(t, Tuple{int64(2)}, filter.Tuple())
	require.False(t, filter.Next())
	require.NoError(t, filter.Error())

	var overFailingSource Iterator = NewPredicateFilterIterator(newFailingStream(0, Tuple{int64(1)}).Iterator(), testSymbols(), alwaysTrue(), nil, nil)
	require.False(t, overFailingSource.Next())
	require.ErrorIs(t, overFailingSource.Error(), errInjectedIterator)
	sourceFailing, ok := overFailingSource.(failedTupleIterator)
	require.True(t, ok)
	_, _, ok = sourceFailing.failedTuple()
	require.False(t, ok, "a failure of the source is not the filter's")
}

// An evaluator answers the source tuple its own failure arose on, over its
// symbols, with no value at the binding symbol that extends the source, and
// resumes past it, so Next reads on to the source's other tuples. A failure
// of its source is not one it answers.
func TestAnEvaluatorAnswersTheTupleItsFailureAroseOnAndResumesPastIt(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	source := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {"x"}, {int64(2)}})

	var evaluator Iterator = NewFunctionEvaluatorIterator(source.Iterator(), source.Symbols(), plus, nil, nil)
	require.True(t, evaluator.Next())
	require.Equal(t, Tuple{int64(1), int64(2)}, evaluator.Tuple())
	require.False(t, evaluator.Next(), "the expression fails on the second tuple")
	require.ErrorContains(t, evaluator.Error(), "is not a number")

	failing, ok := evaluator.(failedTupleIterator)
	require.True(t, ok, "an evaluator answers the tuple its own failure arose on")
	tuple, unknown, ok := failing.failedTuple()
	require.True(t, ok)
	require.Equal(t, Tuple{"x", nil}, tuple)
	require.Equal(t, []int{1}, unknown, "the binding symbol extending the source holds no value")

	failing.resume()
	require.NoError(t, evaluator.Error())
	require.True(t, evaluator.Next(), "Next reads on past the failed tuple")
	require.Equal(t, Tuple{int64(2), int64(3)}, evaluator.Tuple())
	require.False(t, evaluator.Next())
	require.NoError(t, evaluator.Error())

	var overFailingSource Iterator = NewFunctionEvaluatorIterator(newFailingStream(0, Tuple{int64(1)}).Iterator(), testSymbols(), plus, nil, nil)
	require.False(t, overFailingSource.Next())
	require.ErrorIs(t, overFailingSource.Error(), errInjectedIterator)
	sourceFailing, ok := overFailingSource.(failedTupleIterator)
	require.True(t, ok)
	_, _, ok = sourceFailing.failedTuple()
	require.False(t, ok, "a failure of the source is not the evaluator's")
}

// A stream's iterator, its source wrapped for counting, answers the failed
// tuple of the evaluator it streams and resumes it.
func TestAStreamsIteratorAnswersItsSourcesFailedTupleAndResumesIt(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	source := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{"x"}, {int64(2)}})
	evaluator := NewFunctionEvaluatorIterator(source.Iterator(), source.Symbols(), plus, nil, nil)

	it := NewStreamingRelation(evaluator.Symbols(), evaluator).Iterator()
	require.False(t, it.Next(), "the expression fails on the first tuple")
	failing, ok := it.(failedTupleIterator)
	require.True(t, ok, "a stream's iterator answers its source's failed tuple")
	tuple, unknown, ok := failing.failedTuple()
	require.True(t, ok)
	require.Equal(t, Tuple{"x", nil}, tuple)
	require.Equal(t, []int{1}, unknown)

	failing.resume()
	require.True(t, it.Next(), "Next reads on past the failed tuple")
	require.Equal(t, Tuple{int64(2), int64(3)}, it.Tuple())
	require.False(t, it.Next())
	require.NoError(t, it.Error())
}

// A subject tuple whose evaluation failed enters the window, marked with its
// failure, when the form reads none of the positions it holds no value at,
// and the reader reads on past it. A failed tuple without a value at a
// position the form reads ends the reader with the failure, as the subject's
// failure does.
func TestWindowReader_CarriesAFailedTupleTheFormReadsOnlyWhereItHoldsValues(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	subject := func() Relation {
		source := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {"x"}, {int64(2)}})
		return evaluateExpressionWithLookup(source, plus, nil, nil)
	}
	opts := ExecutorOptions{ScopeWindowCap: 8}

	t.Run("the form reads the source's value alone", func(t *testing.T) {
		s := subject()
		w := newWindowReader(s.Iterator(), "subquery", s.RequiresCopy(), []int{0}, []int{0}, opts)
		require.True(t, w.next())
		require.Equal(t, []Tuple{{int64(1), int64(2)}}, w.window)
		require.True(t, w.next(), "the reader reads on past the failed tuple")
		require.Equal(t, []Tuple{{"x", nil}, {int64(2), int64(3)}}, w.window)
		require.ErrorContains(t, w.failureAt(0), "is not a number")
		require.NoError(t, w.failureAt(1))
		require.False(t, w.next())
		require.NoError(t, w.err)
	})
	t.Run("the form reads the failing binding's value", func(t *testing.T) {
		s := subject()
		w := newWindowReader(s.Iterator(), "subquery", s.RequiresCopy(), []int{1}, []int{1}, opts)
		require.True(t, w.next())
		require.False(t, w.next())
		require.ErrorContains(t, w.err, "is not a number")
	})
}

// A union and a fallback over a subject whose evaluation failed at one tuple
// decide the failure at that tuple's key: where no branch holds at the key the
// tuple is dropped and the form answers the subject's other tuples a branch
// holds at, and where a branch holds at it the form answers the failure
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, the failure contract).
func TestABranchFormDecidesItsSubjectsFailedTupleAtItsKey(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	subject := func() Relation {
		source := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {"x"}, {int64(2)}})
		return evaluateExpressionWithLookup(source, plus, nil, nil)
	}
	holdingAt := func(value interface{}) [][]query.Clause {
		return [][]query.Clause{{&query.Comparison{Op: datalog.SymEQ, Left: query.VariableTerm{Symbol: a}, Right: query.ConstantTerm{Value: value}}}}
	}
	exec := newQueryExecutor(&MockMatcher{}, nil, ExecutorOptions{})
	for _, form := range []struct {
		name         string
		shortCircuit bool
	}{{"the union", false}, {"the fallback", true}} {
		t.Run(form.name+" with no branch holding at the failed tuple's key", func(t *testing.T) {
			rel, err := NewOrFallbackRelation(exec, NewContext(), []query.Symbol{a}, nil, holdingAt(int64(2)), subject(), ExecutorOptions{}, form.shortCircuit)
			require.NoError(t, err)
			tuples, err := CollectTuples(rel, nil)
			require.NoError(t, err, "no branch holds at the failed tuple's key, so its failure is not raised")
			require.Equal(t, [][]interface{}{{int64(2), int64(3)}}, tuples)
		})
		t.Run(form.name+" with a branch holding at the failed tuple's key", func(t *testing.T) {
			rel, err := NewOrFallbackRelation(exec, NewContext(), []query.Symbol{a}, nil, holdingAt("x"), subject(), ExecutorOptions{}, form.shortCircuit)
			require.NoError(t, err)
			_, err = CollectTuples(rel, nil)
			require.ErrorContains(t, err, "is not a number")
		})
	}
}
