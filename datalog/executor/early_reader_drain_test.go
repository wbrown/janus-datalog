package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A reader that stops early reads a relation whose reading evaluates a
// function to its end, so a failure past the tuples it keeps is its answer:
// a limit over an expression's result, and the existence verdict over one. A
// relation whose reading evaluates no function keeps the early stop: the
// verdict pulls one of its tuples.
func TestAnEarlyReaderReadsAnEvaluatingRelationToItsEnd(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	// The failing value comes last, past the one tuple an early reader keeps.
	evaluated := func() Relation {
		values := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {int64(2)}, {"x"}})
		return evaluateExpressionWithLookup(values, plus, nil, nil)
	}
	unit := NewUnitRelation(ExecutorOptions{})

	t.Run("a limit over an expression's result", func(t *testing.T) {
		_, err := CollectTuples(NewLimitRelation(evaluated(), 1), nil)
		require.ErrorContains(t, err, "is not a number")
	})

	t.Run("the existence verdict over an expression's result", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{evaluated()}, nil, unit)
		require.ErrorContains(t, err, "is not a number")
	})

	t.Run("the existence verdict over a relation evaluating no function", func(t *testing.T) {
		calls := 0
		tuples := make([]Tuple, 100)
		for i := range tuples {
			tuples[i] = Tuple{int64(i)}
		}
		stream := NewStreamingRelation([]query.Symbol{a}, &countingSliceIterator{tuples: tuples, nextCalls: &calls})
		groups, err := existenceVerdict([]Relation{stream}, nil, unit)
		require.NoError(t, err)
		require.Equal(t, []Relation{unit}, groups, "a group with a tuple holds the verdict")
		require.Equal(t, 1, calls, "the verdict pulls one tuple of a relation evaluating no function")
	})
}

// The existence verdict raises a group's failure only when every other group
// has a tuple, a held group included: one the verdict's caller reads after
// it. A failing group evaluates as it is read or holds a failure its producer
// read: a union over a failing branch, holding the tuples before the failure
// or none, and an expression's result failing at its first tuple. Each fails
// the verdict beside a group with a tuple and fails nothing beside an empty
// group, on either side of it, whether the other group is the verdict's own or
// held.
func TestTheExistenceVerdictRaisesAFailureOnlyWhereEveryGroupHolds(t *testing.T) {
	a, b, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	evaluated := func(values ...Tuple) Relation {
		return evaluateExpressionWithLookup(NewMaterializedRelation([]query.Symbol{a}, values), plus, nil, nil)
	}
	union := func(values ...Tuple) Relation {
		return unionRelations([]Relation{evaluated(values...)}, []query.Symbol{a, b}, ExecutorOptions{})
	}
	holding := func() Relation { return NewMaterializedRelation([]query.Symbol{c}, []Tuple{{int64(7)}}) }
	empty := func() Relation { return NewMaterializedRelation([]query.Symbol{c}, nil) }
	unit := NewUnitRelation(ExecutorOptions{})

	failing := []struct {
		name  string
		group func() Relation
	}{
		{"a union holding the tuples before its branch's failure", func() Relation { return union(Tuple{int64(1)}, Tuple{"x"}) }},
		{"a union whose branch fails at its first tuple", func() Relation { return union(Tuple{"x"}, Tuple{int64(1)}) }},
		{"an expression's result failing at its first tuple", func() Relation { return evaluated(Tuple{"x"}, Tuple{int64(1)}) }},
	}
	for _, f := range failing {
		t.Run(f.name, func(t *testing.T) {
			_, err := existenceVerdict([]Relation{f.group(), holding()}, nil, unit)
			require.ErrorContains(t, err, "is not a number", "the failing group before a group with a tuple")
			_, err = existenceVerdict([]Relation{holding(), f.group()}, nil, unit)
			require.ErrorContains(t, err, "is not a number", "the failing group after a group with a tuple")

			groups, err := existenceVerdict([]Relation{f.group(), empty()}, nil, unit)
			require.NoError(t, err, "the failing group before an empty group")
			require.Empty(t, groups)
			groups, err = existenceVerdict([]Relation{empty(), f.group()}, nil, unit)
			require.NoError(t, err, "the failing group after an empty group")
			require.Empty(t, groups)

			groups, err = existenceVerdict([]Relation{f.group()}, []Relation{empty()}, unit)
			require.NoError(t, err, "the failing group beside an empty held group")
			require.Empty(t, groups)
			_, err = existenceVerdict([]Relation{f.group()}, []Relation{holding()}, unit)
			require.ErrorContains(t, err, "is not a number", "the failing group beside a held group with a tuple")
		})
	}
}

// The existence verdict reads no held group when it has no failure to raise,
// so its caller reads each held group whole: the verdict pulls no tuple of a
// held stream.
func TestTheExistenceVerdictLeavesAHeldGroupUnreadWithoutAFailure(t *testing.T) {
	a, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?c")
	calls := 0
	held := NewStreamingRelation([]query.Symbol{a}, &countingSliceIterator{tuples: []Tuple{{int64(1)}}, nextCalls: &calls})
	unit := NewUnitRelation(ExecutorOptions{})
	groups, err := existenceVerdict([]Relation{NewMaterializedRelation([]query.Symbol{c}, []Tuple{{int64(7)}})}, []Relation{held}, unit)
	require.NoError(t, err)
	require.Equal(t, []Relation{unit}, groups, "a group with a tuple holds the verdict")
	require.Equal(t, 0, calls, "the verdict pulls no tuple of a held group when it has no failure")
}

// The existence verdict reads the failure a relation keeps behind the size it
// answers, as it reads a held relation's: an aggregate whose fold of an
// expression's result failed, and a stream whose replay cache completed with
// its source's failure, each holding the tuple before the failure. Each fails
// the verdict beside a group with a tuple and fails nothing beside an empty
// group, on either side of it.
func TestTheExistenceVerdictReadsAFailureARelationKeepsBehindItsSize(t *testing.T) {
	a, b, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	holding := func() Relation { return NewMaterializedRelation([]query.Symbol{c}, []Tuple{{int64(7)}}) }
	empty := func() Relation { return NewMaterializedRelation([]query.Symbol{c}, nil) }
	unit := NewUnitRelation(ExecutorOptions{})

	failing := []struct {
		name    string
		failure string
		group   func(t *testing.T) Relation
	}{
		{"an aggregate whose fold of an expression's result failed", "is not a number", func(t *testing.T) Relation {
			values := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {"x"}})
			aggregate := NewStreamingAggregateRelation(evaluateExpressionWithLookup(values, plus, nil, nil),
				[]query.FindElement{query.FindAggregate{Function: datalog.SymCount, Arg: b}})
			require.Equal(t, 1, aggregate.Size(), "the fold holds the group before the failure")
			return aggregate
		}},
		{"a stream whose replay cache completed with its source's failure", errInjectedIterator.Error(), func(t *testing.T) Relation {
			replayable := newFailingStream(1, Tuple{int64(1)}, Tuple{int64(2)}).Materialize()
			require.ErrorIs(t, driveErr(replayable), errInjectedIterator, "the first read fills the cache up to the failure")
			require.Equal(t, 1, replayable.Size(), "the cache holds the tuple before the failure")
			return replayable
		}},
	}
	for _, f := range failing {
		t.Run(f.name, func(t *testing.T) {
			_, err := existenceVerdict([]Relation{f.group(t), holding()}, nil, unit)
			require.ErrorContains(t, err, f.failure, "the failing group before a group with a tuple")
			_, err = existenceVerdict([]Relation{holding(), f.group(t)}, nil, unit)
			require.ErrorContains(t, err, f.failure, "the failing group after a group with a tuple")

			groups, err := existenceVerdict([]Relation{f.group(t), empty()}, nil, unit)
			require.NoError(t, err, "the failing group before an empty group")
			require.Empty(t, groups)
			groups, err = existenceVerdict([]Relation{empty(), f.group(t)}, nil, unit)
			require.NoError(t, err, "the failing group after an empty group")
			require.Empty(t, groups)
		})
	}
}

// A relation's EvaluateFunction result evaluates the function as it is
// pulled, on a streaming relation and on a lazy sequence alike, so a limit
// over it answers a failure on a tuple past the one it keeps. A function
// reading no variable answers alike on every tuple, so a limit over its
// result keeps the early stop.
func TestALimitOverEvaluateFunctionAnswersAFailurePastItsTuples(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	plus := &query.ArithmeticFunction{
		Op:   datalog.SymAdd,
		Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
	}
	sources := []struct {
		name   string
		source func(Iterator) Relation
	}{
		{"a streaming relation", func(it Iterator) Relation { return NewStreamingRelation([]query.Symbol{a}, it) }},
		{"a lazy sequence", func(it Iterator) Relation {
			return NewLazySeqRelation(NewTupleSeq(it, false), []query.Symbol{a})
		}},
	}
	for _, s := range sources {
		t.Run(s.name, func(t *testing.T) {
			// The failing value comes last, past the one tuple the limit keeps.
			failing := &sliceIterator{tuples: []Tuple{{int64(1)}, {int64(2)}, {"x"}}, pos: -1}
			_, err := CollectTuples(NewLimitRelation(s.source(failing).EvaluateFunction(plus, b), 1), nil)
			require.ErrorContains(t, err, "is not a number")

			calls := 0
			tuples := make([]Tuple, 100)
			for i := range tuples {
				tuples[i] = Tuple{int64(i)}
			}
			counted := &countingSliceIterator{tuples: tuples, nextCalls: &calls}
			kept, err := CollectTuples(NewLimitRelation(s.source(counted).EvaluateFunction(query.GroundFunction{Value: int64(0)}, b), 1), nil)
			require.NoError(t, err)
			require.Len(t, kept, 1)
			require.Equal(t, 1, calls, "a limit over a function reading no variable pulls the tuple it keeps")
		})
	}
}

// A relation's FilterWithPredicate result evaluates the predicate as it is
// pulled. A predicate that can fail on a value, a transaction range over a
// value naming no transaction, makes a limit over the result answer a
// failure past the tuple it keeps; a comparison, which fails on no value,
// keeps the limit's early stop. Both hold on a streaming relation and on a
// lazy sequence.
func TestALimitOverFilterWithPredicateReadsAsThePredicateCanFail(t *testing.T) {
	tx := datalog.NewSymbol("?tx")
	txRange := &query.TxRangePredicate{TxVar: tx, Low: 0, High: 10}
	less := &query.Comparison{Op: datalog.SymLT, Left: query.VariableTerm{Symbol: tx}, Right: query.ConstantTerm{Value: int64(1000)}}
	sources := []struct {
		name   string
		source func(Iterator) Relation
	}{
		{"a streaming relation", func(it Iterator) Relation { return NewStreamingRelation([]query.Symbol{tx}, it) }},
		{"a lazy sequence", func(it Iterator) Relation {
			return NewLazySeqRelation(NewTupleSeq(it, false), []query.Symbol{tx})
		}},
	}
	for _, s := range sources {
		t.Run(s.name, func(t *testing.T) {
			// The value naming no transaction comes last, past the one tuple
			// the limit keeps.
			failing := &sliceIterator{tuples: []Tuple{
				{datalog.ElementID{Lamport: 1, ReplicaID: 1}}, {datalog.ElementID{Lamport: 2, ReplicaID: 1}}, {"x"},
			}, pos: -1}
			_, err := CollectTuples(NewLimitRelation(s.source(failing).FilterWithPredicate(txRange), 1), nil)
			require.ErrorContains(t, err, "cannot extract Lamport")

			calls := 0
			tuples := make([]Tuple, 100)
			for i := range tuples {
				tuples[i] = Tuple{int64(i)}
			}
			counted := &countingSliceIterator{tuples: tuples, nextCalls: &calls}
			kept, err := CollectTuples(NewLimitRelation(s.source(counted).FilterWithPredicate(less), 1), nil)
			require.NoError(t, err)
			require.Len(t, kept, 1)
			require.Equal(t, 1, calls, "a limit over a comparison's filter pulls the tuple it keeps")
		})
	}
}

// A product reads its members as it is pulled: the first as a stream, and
// every other through the cache its first pass fills. So a limit over a
// product, or over a projection of one, answers a failure a member raises
// past the tuple the limit keeps, whichever member evaluates. A limit over a
// product of members evaluating nothing keeps the early stop.
func TestALimitOverAProductAnswersAMembersFailurePastItsTuples(t *testing.T) {
	a, b, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")
	plus := &query.Expression{
		Function: &query.ArithmeticFunction{
			Op:   datalog.SymAdd,
			Args: []query.Term{query.VariableTerm{Symbol: a}, query.ConstantTerm{Value: int64(1)}},
		},
		Binding: b,
	}
	// The failing value comes last, past the one tuple the limit keeps.
	evaluated := func() Relation {
		values := NewMaterializedRelation([]query.Symbol{a}, []Tuple{{int64(1)}, {int64(2)}, {"x"}})
		return evaluateExpressionWithLookup(values, plus, nil, nil)
	}
	holding := func() Relation { return NewMaterializedRelation([]query.Symbol{c}, []Tuple{{int64(7)}}) }

	t.Run("the evaluating member first", func(t *testing.T) {
		_, err := CollectTuples(NewLimitRelation(Relations{evaluated(), holding()}.Product(), 1), nil)
		require.ErrorContains(t, err, "is not a number")
	})

	t.Run("the evaluating member second", func(t *testing.T) {
		_, err := CollectTuples(NewLimitRelation(Relations{holding(), evaluated()}.Product(), 1), nil)
		require.ErrorContains(t, err, "is not a number")
	})

	t.Run("a projection of the product", func(t *testing.T) {
		projected, err := Relations{evaluated(), holding()}.Product().Project([]query.Symbol{b, c})
		require.NoError(t, err)
		_, err = CollectTuples(NewLimitRelation(projected, 1), nil)
		require.ErrorContains(t, err, "is not a number")
	})

	t.Run("members evaluating nothing", func(t *testing.T) {
		calls := 0
		tuples := make([]Tuple, 100)
		for i := range tuples {
			tuples[i] = Tuple{int64(i)}
		}
		stream := NewStreamingRelation([]query.Symbol{a}, &countingSliceIterator{tuples: tuples, nextCalls: &calls})
		kept, err := CollectTuples(NewLimitRelation(Relations{stream, holding()}.Product(), 1), nil)
		require.NoError(t, err)
		require.Len(t, kept, 1)
		require.Equal(t, 1, calls, "a limit over a product of members evaluating nothing pulls the tuple it keeps")
	})
}
