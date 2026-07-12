package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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
			Op: query.OpAdd,
			Args: []query.Term{
				query.VariableTerm{Symbol: a},
				query.VariableTerm{Symbol: b},
			},
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

func TestHashJoinPreservesLeftKeyWhenRightJoinSymbolsAreKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	opts := ExecutorOptions{
		EnableStreamingJoins:      true,
		EnableIteratorComposition: true,
	}

	leftBase := NewMaterializedRelationNoDedupe(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}, {int64(2), "left-2"}},
	)
	left := NewStreamingRelationWithProperties(
		leftBase.Symbols(),
		leftBase.Iterator(),
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)
	right := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, rightValue},
		[]Tuple{{int64(1), "right-1"}, {int64(2), "right-2"}},
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)

	joined := HashJoinWithOptions(left, right, []query.Symbol{id}, opts)
	require.Equal(t, RelationProperties{Keys: [][]query.Symbol{{id}}}, joined.Properties())
	joinIterator := joined.(*StreamingRelation).iterator.(*hashJoinIterator)
	require.True(t, joinIterator.buildKeysUnique)
	buildValue, found := joinIterator.hashTable.Get(NewTupleKey(Tuple{int64(1)}, []int{0}))
	require.True(t, found)
	require.IsType(t, Tuple{}, buildValue,
		"a unique build key should store one tuple directly rather than []Tuple")
	require.Nil(t, joinIterator.seen,
		"a proven result key makes internal full-tuple join deduplication redundant")

	projected, err := joined.Project([]query.Symbol{id, leftValue})
	require.NoError(t, err)
	_, deduplicates := projected.(*StreamingRelation).iterator.(*DedupIterator)
	require.False(t, deduplicates,
		"a join against a unique right side cannot duplicate keyed left rows")
}

func TestHashJoinDoesNotPreserveLeftKeyWhenRightJoinSymbolsAreNotKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	opts := ExecutorOptions{
		EnableStreamingJoins:      true,
		EnableIteratorComposition: true,
	}

	leftBase := NewMaterializedRelationNoDedupe(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}},
	)
	left := NewStreamingRelationWithProperties(
		leftBase.Symbols(),
		leftBase.Iterator(),
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)
	right := NewMaterializedRelationNoDedupeWithOptions(
		[]query.Symbol{id, rightValue},
		[]Tuple{{int64(1), "right-1"}, {int64(1), "right-2"}},
		opts,
	)

	joined := HashJoinWithOptions(left, right, []query.Symbol{id}, opts)
	require.Empty(t, joined.Properties().Keys)
	joinIterator := joined.(*StreamingRelation).iterator.(*hashJoinIterator)
	require.False(t, joinIterator.buildKeysUnique)
	buildValue, found := joinIterator.hashTable.Get(NewTupleKey(Tuple{int64(1)}, []int{0}))
	require.True(t, found)
	require.Len(t, buildValue.([]Tuple), 2,
		"a non-unique build key must retain every fanout tuple")
	require.NotNil(t, joinIterator.seen,
		"an unkeyed join must retain internal full-tuple deduplication")

	projected, err := joined.Project([]query.Symbol{id, leftValue})
	require.NoError(t, err)
	_, deduplicates := projected.(*StreamingRelation).iterator.(*DedupIterator)
	require.True(t, deduplicates,
		"a non-unique right side can duplicate left rows and projection must restore set semantics")

	rows, err := CollectTuples(projected, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), "left-1"}}, rows)
}

func TestHashJoinPreservesRightKeyWhenLeftJoinSymbolsAreKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	opts := ExecutorOptions{
		EnableStreamingJoins:      true,
		EnableIteratorComposition: true,
	}

	left := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}, {int64(2), "left-2"}},
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)
	rightBase := NewMaterializedRelationNoDedupe(
		[]query.Symbol{id, rightValue},
		[]Tuple{{int64(1), "right-1"}, {int64(2), "right-2"}},
	)
	right := NewStreamingRelationWithProperties(
		rightBase.Symbols(),
		rightBase.Iterator(),
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id, rightValue}}},
	)

	joined := HashJoinWithOptions(left, right, []query.Symbol{id}, opts)
	require.Equal(t,
		RelationProperties{Keys: [][]query.Symbol{{id, rightValue}}},
		joined.Properties(),
	)

	projected, err := joined.Project([]query.Symbol{id, rightValue})
	require.NoError(t, err)
	_, deduplicates := projected.(*StreamingRelation).iterator.(*DedupIterator)
	require.False(t, deduplicates,
		"a join against a unique left side cannot duplicate keyed right rows")
}

func TestMaterializedAndSymmetricHashJoinsPreserveCandidateKeys(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	leftSymbols := []query.Symbol{id, leftValue}
	rightSymbols := []query.Symbol{id, rightValue}
	leftTuples := []Tuple{{int64(1), "left-1"}, {int64(2), "left-2"}}
	rightTuples := []Tuple{{int64(1), "right-1"}, {int64(2), "right-2"}}
	properties := RelationProperties{Keys: [][]query.Symbol{{id}}}

	materializedLeft := NewMaterializedRelationWithProperties(
		leftSymbols, leftTuples, ExecutorOptions{}, properties)
	materializedRight := NewMaterializedRelationWithProperties(
		rightSymbols, rightTuples, ExecutorOptions{}, properties)
	materializedJoin := HashJoinWithOptions(
		materializedLeft, materializedRight, []query.Symbol{id}, ExecutorOptions{})
	require.Equal(t, properties, materializedJoin.Properties())

	opts := ExecutorOptions{
		EnableStreamingJoins:      true,
		EnableSymmetricHashJoin:   true,
		DefaultHashTableSize:      16,
		EnableTrueStreaming:       true,
		EnableIteratorComposition: true,
	}
	streamingLeftBase := NewMaterializedRelationNoDedupe(leftSymbols, leftTuples)
	streamingRightBase := NewMaterializedRelationNoDedupe(rightSymbols, rightTuples)
	streamingLeft := NewStreamingRelationWithProperties(
		leftSymbols, streamingLeftBase.Iterator(), opts, properties)
	streamingRight := NewStreamingRelationWithProperties(
		rightSymbols, streamingRightBase.Iterator(), opts, properties)
	symmetricJoin := SymmetricHashJoinWithOptions(
		streamingLeft, streamingRight, []query.Symbol{id}, opts)
	require.Equal(t, properties, symmetricJoin.Properties())
	require.Nil(t, symmetricJoin.(*StreamingRelation).iterator.(*symmetricHashJoinIterator).seen,
		"a proven symmetric-join result key makes internal deduplication redundant")
}

func TestSemiAndAntiJoinsPreserveLeftProperties(t *testing.T) {
	id := datalog.NewSymbol("?id")
	value := datalog.NewSymbol("?value")
	properties := RelationProperties{
		Ordering: []query.OrderByClause{{Variable: id, Direction: query.OrderAsc}},
		Keys:     [][]query.Symbol{{id}},
	}
	left := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, value},
		[]Tuple{
			{int64(1), "one"},
			{int64(2), "two"},
			{int64(3), "three"},
		},
		ExecutorOptions{},
		properties,
	)
	right := NewMaterializedRelation(
		[]query.Symbol{id},
		[]Tuple{{int64(1)}, {int64(3)}},
	)

	semi := SemiJoin(left, right, []query.Symbol{id})
	require.Equal(t, properties, semi.Properties())
	semiRows, err := CollectTuples(semi, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), "one"}, {int64(3), "three"}}, semiRows)

	anti := AntiJoin(left, right, []query.Symbol{id})
	require.Equal(t, properties, anti.Properties())
	antiRows, err := CollectTuples(anti, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), "two"}}, antiRows)
}

func TestSemiAndAntiJoinsDeduplicateUnkeyedLeftInput(t *testing.T) {
	id := datalog.NewSymbol("?id")
	left := NewMaterializedRelationNoDedupe(
		[]query.Symbol{id},
		[]Tuple{{int64(1)}, {int64(1)}, {int64(2)}, {int64(2)}},
	)
	right := NewMaterializedRelation(
		[]query.Symbol{id},
		[]Tuple{{int64(1)}},
	)

	require.Equal(t, 1, SemiJoin(left, right, []query.Symbol{id}).Size())
	require.Equal(t, 1, AntiJoin(left, right, []query.Symbol{id}).Size())
}

func TestExpandingExpressionDoesNotPreserveOuterKey(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	vector := datalog.NewSymbol("?vector")
	index := datalog.NewSymbol("?index")
	value := datalog.NewSymbol("?value")
	source := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, vector},
		[]Tuple{{int64(1), []interface{}{"same", "same", "other"}}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	expanded := evaluateExpressionWithLookup(source, &query.Expression{
		Function: query.EnumerateFunction{
			VecTerm: query.VariableTerm{Symbol: vector},
		},
		Binding: query.TupleBinding{Variables: []query.Symbol{index, value}},
	}, nil, nil)

	require.False(t, containsSymbolSet(expanded.Properties().Keys, []query.Symbol{entity}),
		"one entity expands to multiple rows, so the outer key is no longer unique")
	require.True(t, containsSymbolSet(
		expanded.Properties().Keys,
		[]query.Symbol{entity, index, value},
	))

	projected, err := expanded.Project([]query.Symbol{entity, value})
	require.NoError(t, err)
	rows, err := CollectTuples(projected, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{int64(1), "same"},
		{int64(1), "other"},
	}, rows)
}

func TestSemiAndAntiJoinsCopyWorkspaceReusingLeftInput(t *testing.T) {
	id := datalog.NewSymbol("?id")
	value := datalog.NewSymbol("?value")
	data := [][]interface{}{
		{int64(1), "one"},
		{int64(2), "two"},
		{int64(3), "three"},
	}
	testCases := []struct {
		name     string
		rightIDs []Tuple
		run      func(Relation, Relation, []query.Symbol) Relation
		want     [][]interface{}
	}{
		{
			name:     "semi",
			rightIDs: []Tuple{{int64(1)}, {int64(3)}},
			run:      SemiJoin,
			want:     [][]interface{}{{int64(1), "one"}, {int64(3), "three"}},
		},
		{
			name:     "anti",
			rightIDs: []Tuple{{int64(2)}},
			run:      AntiJoin,
			want:     [][]interface{}{{int64(1), "one"}, {int64(3), "three"}},
		},
	}

	for _, testCase := range testCases {
		for _, keyed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/keyed_%t", testCase.name, keyed), func(t *testing.T) {
				tuples := make([]Tuple, len(data))
				for i := range data {
					tuples[i] = Tuple(data[i])
				}
				left := newReusingWorkspaceStream([]query.Symbol{id, value}, tuples)
				if keyed {
					left.properties = RelationProperties{Keys: [][]query.Symbol{{id}}}
				}
				right := NewMaterializedRelation([]query.Symbol{id}, testCase.rightIDs)
				result := testCase.run(left, right, []query.Symbol{id})
				rows, err := CollectTuples(result, nil)
				require.NoError(t, err)
				require.Equal(t, testCase.want, rows)
			})
		}
	}
}

func TestStreamingRelationCacheEmitsStructuredAnnotation(t *testing.T) {
	var events []annotations.Event
	collector := annotations.NewCollector(func(event annotations.Event) {
		events = append(events, event)
	})
	symbol := datalog.NewSymbol("?value")
	base := NewMaterializedRelationNoDedupe(
		[]query.Symbol{symbol},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	stream := NewStreamingRelationWithOptions(
		base.Symbols(),
		base.Iterator(),
		ExecutorOptions{Collector: collector},
	)

	stream.Materialize()
	it := stream.Iterator()
	for it.Next() {
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())

	found := false
	for _, event := range events {
		if event.Name == annotations.RelationCacheEnabled {
			found = true
			require.Equal(t, 1, event.Data["symbol_count"])
			break
		}
	}
	require.True(t, found, "relation cache annotation was not emitted")
}
