package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBindQueryInputsAcceptsUnknownSizeStreams(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")

	t.Run("scalar", func(t *testing.T) {
		q := &query.Query{In: []query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar},
			query.ScalarInput{Symbol: x},
		}}
		input := NewStreamingRelation(
			[]query.Symbol{x},
			NewMaterializedRelation(
				[]query.Symbol{x},
				[]Tuple{{int64(7)}},
			).Iterator(),
		)
		bound := BindQueryInputs(q, []Relation{input})
		tuples, err := CollectTuples(bound, nil)
		require.NoError(t, err)
		require.Equal(t, []query.Symbol{x}, bound.Symbols())
		require.Equal(t, [][]interface{}{{int64(7)}}, tuples)
	})

	t.Run("tuple", func(t *testing.T) {
		q := &query.Query{In: []query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar},
			query.TupleInput{Symbols: []query.Symbol{x, y}},
		}}
		input := NewStreamingRelation(
			[]query.Symbol{x, y},
			NewMaterializedRelation(
				[]query.Symbol{x, y},
				[]Tuple{{int64(7), int64(8)}},
			).Iterator(),
		)
		bound := BindQueryInputs(q, []Relation{input})
		tuples, err := CollectTuples(bound, nil)
		require.NoError(t, err)
		require.Equal(t, []query.Symbol{x, y}, bound.Symbols())
		require.Equal(t, [][]interface{}{{int64(7), int64(8)}}, tuples)
	})

	t.Run("relation", func(t *testing.T) {
		q := &query.Query{In: []query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar},
			query.RelationInput{Symbols: []query.Symbol{x, y}},
		}}
		input := NewStreamingRelation(
			[]query.Symbol{x, y},
			NewMaterializedRelation(
				[]query.Symbol{x, y},
				[]Tuple{{int64(1), int64(2)}, {int64(3), int64(4)}},
			).Iterator(),
		)
		bound := BindQueryInputs(q, []Relation{input})
		tuples, err := CollectTuples(bound, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{
			{int64(1), int64(2)},
			{int64(3), int64(4)},
		}, tuples)
	})

	t.Run("collection", func(t *testing.T) {
		q := &query.Query{In: []query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar},
			query.CollectionInput{Symbol: x},
		}}
		input := NewStreamingRelation(
			[]query.Symbol{x},
			NewMaterializedRelation(
				[]query.Symbol{x},
				[]Tuple{{int64(1)}, {int64(2)}},
			).Iterator(),
		)
		var bound Relation
		require.NotPanics(t, func() {
			bound = BindQueryInputs(q, []Relation{input})
		})
		tuples, err := CollectTuples(bound, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, tuples)
	})
}

func TestBindQueryInputsPropagatesStreamingInputError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	q := &query.Query{In: []query.InputSpec{
		query.DatabaseInput{Name: datalog.SymDollar},
		query.ScalarInput{Symbol: x},
	}}
	base := NewMaterializedRelation(
		[]query.Symbol{x},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	input := NewStreamingRelation(
		[]query.Symbol{x},
		&failingIterator{inner: base.Iterator(), failAfter: 1},
	)

	bound := BindQueryInputs(q, []Relation{input})
	require.ErrorIs(t, driveErr(bound), errInjectedIterator)
}
