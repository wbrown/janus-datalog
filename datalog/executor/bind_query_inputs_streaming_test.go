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
		bound, err := BindQueryInputs(q, []Relation{input})
		require.NoError(t, err)
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
		bound, err := BindQueryInputs(q, []Relation{input})
		require.NoError(t, err)
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
		bound, err := BindQueryInputs(q, []Relation{input})
		require.NoError(t, err)
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
		var err error
		require.NotPanics(t, func() {
			bound, err = BindQueryInputs(q, []Relation{input})
		})
		require.NoError(t, err)
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

	_, err := BindQueryInputs(q, []Relation{input})
	require.ErrorIs(t, err, errInjectedIterator)
}

// The relation BindQueryInputs answers is held: it reports its size and is
// read twice, since executeRealizedPlan projects the environment out of it
// and then projects it again (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md,
// L4). Two relation inputs sharing a symbol are joined, and the join's arm,
// whichever the inputs' options select, never reaches the binder's caller.
func TestBindQueryInputsAnswersAHeldRelation(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	z := datalog.NewSymbol("?z")
	q := &query.Query{In: []query.InputSpec{
		query.DatabaseInput{Name: datalog.SymDollar},
		query.RelationInput{Symbols: []query.Symbol{x, y}},
		query.RelationInput{Symbols: []query.Symbol{x, z}},
	}}
	cases := []struct {
		name string
		opts ExecutorOptions
	}{
		{"with streaming joins off", ExecutorOptions{}},
		{"with streaming joins on", ExecutorOptions{EnableStreamingJoins: true}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bound, err := BindQueryInputs(q, []Relation{
				NewMaterializedRelationWithOptions([]query.Symbol{x, y},
					[]Tuple{{int64(1), int64(10)}, {int64(2), int64(20)}}, c.opts),
				NewMaterializedRelationWithOptions([]query.Symbol{x, z},
					[]Tuple{{int64(1), int64(100)}, {int64(3), int64(300)}}, c.opts),
			})
			require.NoError(t, err)
			require.Equal(t, []query.Symbol{x, y, z}, bound.Symbols())
			require.Equal(t, 1, bound.Size(), "the bound group reports its size")
			for read := 1; read <= 2; read++ {
				tuples, err := CollectTuples(bound, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1), int64(10), int64(100)}}, tuples,
					"read %d of the bound group", read)
			}
		})
	}
}
