package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestCorrelatedSubqueryStreamsInputProductIntoCombinationExtraction(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	sum := datalog.NewSymbol("?sum")
	var combinationEvent annotations.Event
	handler := func(event annotations.Event) {
		if event.Name == "subquery/input-combinations" {
			combinationEvent = event
		}
	}
	ctx := NewContext()
	options := ExecutorOptions{Handler: handler}
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

	result, err := exec.executeSubquery(
		ctx,
		arithmeticSubqueryPattern(x, y, sum),
		[]Relation{left, right},
	)
	require.NoError(t, err)
	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{int64(1), int64(10), int64(11)},
		{int64(1), int64(20), int64(21)},
		{int64(2), int64(10), int64(12)},
		{int64(2), int64(20), int64(22)},
	}, tuples)
	require.Equal(t, 2, combinationEvent.Data["relation_groups"])
	require.Equal(t, true, combinationEvent.Data["product"])
	require.Equal(t, false, combinationEvent.Data["eager_materialized"])
	require.Equal(t, 4, combinationEvent.Data["combination_count"])
}

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

	_, err := exec.executeSubquery(
		NewContext(),
		arithmeticSubqueryPattern(x, y, sum),
		[]Relation{left, right},
	)
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
