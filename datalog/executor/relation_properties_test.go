package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestRelationPropertiesAreStableAtInterfaceBoundary(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	properties := RelationProperties{
		Ordering: []query.OrderByClause{
			{Variable: a, Direction: query.OrderAsc},
			{Variable: b, Direction: query.OrderDesc},
		},
		Keys: [][]query.Symbol{{a}, {a, b}},
	}
	rel := NewMaterializedRelationWithProperties(
		[]query.Symbol{a, b},
		[]Tuple{{int64(1), int64(2)}},
		ExecutorOptions{},
		properties,
	)

	require.Equal(t, properties, rel.Properties())
	require.Equal(t, rel.Properties(), rel.Properties())
}

func TestRelationPropertyPropagation(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	properties := RelationProperties{
		Ordering: []query.OrderByClause{
			{Variable: a, Direction: query.OrderAsc},
			{Variable: b, Direction: query.OrderDesc},
		},
		Keys: [][]query.Symbol{{a}, {a, b}},
	}
	rel := NewMaterializedRelationWithProperties(
		[]query.Symbol{a, b},
		[]Tuple{{int64(1), int64(2)}, {int64(2), int64(1)}},
		ExecutorOptions{},
		properties,
	)

	filtered := rel.Filter(NewSimpleFilter(func(Tuple) bool { return true }))
	require.Equal(t, properties, filtered.Properties(), "filter must preserve properties")

	projectedA, err := rel.Project([]query.Symbol{a})
	require.NoError(t, err)
	require.Equal(t, RelationProperties{
		Ordering: []query.OrderByClause{{Variable: a, Direction: query.OrderAsc}},
		Keys:     [][]query.Symbol{{a}},
	}, projectedA.Properties())

	projectedB, err := rel.Project([]query.Symbol{b})
	require.NoError(t, err)
	require.Equal(t, RelationProperties{}, projectedB.Properties(),
		"dropping the leading order symbol and every key must clear properties")

	sorted := rel.Sort([]query.OrderByClause{{Variable: b, Direction: query.OrderDesc}})
	require.Equal(t, RelationProperties{
		Ordering: []query.OrderByClause{{Variable: b, Direction: query.OrderDesc}},
		Keys:     [][]query.Symbol{{a}, {a, b}},
	}, sorted.Properties())

	right := NewMaterializedRelation(
		[]query.Symbol{a, c},
		[]Tuple{{int64(1), "x"}, {int64(2), "y"}},
	)
	joined := rel.Join(right)
	require.Equal(t, RelationProperties{}, joined.Properties(),
		"join properties must be cleared until a derivation rule proves them")
}

func TestRelationPropertiesWhenAddingSymbols(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	fresh := datalog.NewSymbol("?fresh")
	properties := RelationProperties{
		Ordering: []query.OrderByClause{{Variable: a, Direction: query.OrderAsc}},
		Keys:     [][]query.Symbol{{a}, {a, b}},
	}

	require.Equal(t, properties, properties.addSymbol(fresh),
		"adding a fresh output symbol must preserve existing guarantees")
	require.Equal(t, RelationProperties{}, properties.addSymbol(a),
		"replacing a property-bearing symbol must clear affected guarantees")
}

func TestStreamingRelationPropertyPropagation(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	fresh := datalog.NewSymbol("?fresh")
	symbols := []query.Symbol{a, b}
	tuples := []Tuple{{int64(1), int64(2)}, {int64(2), int64(1)}}
	properties := RelationProperties{
		Ordering: []query.OrderByClause{{Variable: a, Direction: query.OrderAsc}},
		Keys:     [][]query.Symbol{{a}},
	}
	open := func() *StreamingRelation {
		base := NewMaterializedRelationNoDedupe(symbols, tuples)
		return NewStreamingRelationWithProperties(
			symbols,
			base.Iterator(),
			ExecutorOptions{EnableIteratorComposition: true},
			properties,
		)
	}

	require.Equal(t, properties,
		open().Filter(NewSimpleFilter(func(Tuple) bool { return true })).Properties())

	projected, err := open().Project([]query.Symbol{a})
	require.NoError(t, err)
	require.Equal(t, properties, projected.Properties())

	evaluated := open().EvaluateFunction(
		&query.ArithmeticFunction{
			Op:    query.OpAdd,
			Left:  query.VariableTerm{Symbol: a},
			Right: query.VariableTerm{Symbol: b},
		},
		fresh,
	)
	require.Equal(t, properties, evaluated.Properties())

	require.Equal(t, properties, NewLimitRelation(open(), 1).Properties())
	require.Equal(t, properties, open().Materialize().Properties())
}

func TestRelationPropertiesSatisfyOrderingPrefixes(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	properties := RelationProperties{Ordering: []query.OrderByClause{
		{Variable: a, Direction: query.OrderAsc},
		{Variable: b, Direction: query.OrderDesc},
	}}

	require.True(t, properties.satisfiesOrdering([]query.OrderByClause{
		{Variable: a, Direction: query.OrderAsc},
	}))
	require.True(t, properties.satisfiesOrdering(properties.Ordering))
	require.False(t, properties.satisfiesOrdering([]query.OrderByClause{
		{Variable: a, Direction: query.OrderDesc},
	}))
	require.False(t, properties.satisfiesOrdering([]query.OrderByClause{
		{Variable: b, Direction: query.OrderDesc},
	}))
}

func TestStreamingProjectionSkipsDedupWhenKeyIsRetained(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	symbols := []query.Symbol{a, b}
	tuples := []Tuple{{int64(1), "same"}, {int64(2), "same"}}
	open := func(properties RelationProperties) *StreamingRelation {
		base := NewMaterializedRelationNoDedupe(symbols, tuples)
		return NewStreamingRelationWithProperties(
			symbols,
			base.Iterator(),
			ExecutorOptions{EnableIteratorComposition: true},
			properties,
		)
	}

	keyed, err := open(RelationProperties{Keys: [][]query.Symbol{{a}}}).Project([]query.Symbol{a})
	require.NoError(t, err)
	_, keyedDedups := keyed.(*StreamingRelation).iterator.(*DedupIterator)
	require.False(t, keyedDedups, "projection retaining a candidate key is injective")

	unkeyed, err := open(RelationProperties{}).Project([]query.Symbol{b})
	require.NoError(t, err)
	_, unkeyedDedups := unkeyed.(*StreamingRelation).iterator.(*DedupIterator)
	require.True(t, unkeyedDedups, "projection without a retained key must deduplicate")
}
