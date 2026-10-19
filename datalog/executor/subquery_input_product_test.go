package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A subquery whose inputs span two groups reads them as their product: the
// one result group carries every combination beside its sum, in the place of
// both groups.
func TestCorrelatedSubqueryStreamsInputProductIntoCombinationExtraction(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	sum := datalog.NewSymbol("?sum")
	ctx := NewContext()
	options := ExecutorOptions{}
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, options)
	left := NewStreamingRelationWithOptions(
		[]query.Symbol{x},
		NewMaterializedRelation(
			[]query.Symbol{x},
			[]Tuple{{int64(1)}, {int64(2)}},
		).Iterator(),
		options,
	)
	right := NewStreamingRelationWithOptions(
		[]query.Symbol{y},
		NewMaterializedRelation(
			[]query.Symbol{y},
			[]Tuple{{int64(10)}, {int64(20)}},
		).Iterator(),
		options,
	)

	groups, err := exec.executeSubquery(
		ctx,
		arithmeticSubqueryPattern(x, y, sum),
		[]Relation{left, right},
	)
	require.NoError(t, err)
	require.Len(t, groups, 1, "the result stands in the place of both groups")
	tuples, err := CollectTuples(groups[0], nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{int64(1), int64(10), int64(11)},
		{int64(1), int64(20), int64(21)},
		{int64(2), int64(10), int64(12)},
		{int64(2), int64(20), int64(22)},
	}, tuples)
}

// A failure of a subject group's iterator is the result's, surfacing when
// the result is driven.
func TestCorrelatedSubqueryInputProductPropagatesDeferredError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	sum := datalog.NewSymbol("?sum")
	leftBase := NewMaterializedRelation(
		[]query.Symbol{x},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	left := NewStreamingRelation(
		[]query.Symbol{x},
		&failingIterator{inner: leftBase.Iterator(), failAfter: 1},
	)
	right := NewMaterializedRelation(
		[]query.Symbol{y},
		[]Tuple{{int64(10)}, {int64(20)}},
	)
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})

	groups, err := exec.executeSubquery(
		NewContext(),
		arithmeticSubqueryPattern(x, y, sum),
		[]Relation{left, right},
	)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	_, err = CollectTuples(groups[0], nil)
	require.ErrorIs(t, err, errInjectedIterator)
}

func arithmeticSubqueryPattern(
	x query.Symbol,
	y query.Symbol,
	sum query.Symbol,
) *query.SubqueryPattern {
	return &query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{query.FindVariable{Symbol: sum}},
			In: []query.InputSpec{
				query.DatabaseInput{Name: datalog.SymDollar},
				query.ScalarInput{Symbol: x},
				query.ScalarInput{Symbol: y},
			},
			Where: []query.Clause{&query.Expression{
				Function: query.ArithmeticFunction{
					Op: datalog.SymAdd,
					Args: []query.Term{
						query.VariableTerm{Symbol: x},
						query.VariableTerm{Symbol: y},
					},
				},
				Binding: sum,
			}},
		},
		Inputs: []query.PatternElement{
			query.Constant{Value: datalog.SymDollar},
			query.Variable{Name: x},
			query.Variable{Name: y},
		},
		Binding: query.RelationBinding{Variables: []query.Symbol{sum}},
	}
}
