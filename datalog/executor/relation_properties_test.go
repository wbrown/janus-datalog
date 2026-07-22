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
			{Variable: a, Descending: false},
			{Variable: b, Descending: true},
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

func TestRelationPropertiesRenameSymbols(t *testing.T) {
	innerA := datalog.NewSymbol("?inner-a")
	innerB := datalog.NewSymbol("?inner-b")
	outerA := datalog.NewSymbol("?outer-a")
	outerB := datalog.NewSymbol("?outer-b")
	properties := RelationProperties{
		Ordering: []query.OrderByClause{
			{Variable: innerA, Descending: false},
			{Variable: innerB, Descending: true},
		},
		Keys: [][]query.Symbol{{innerA}, {innerA, innerB}},
	}

	renamed := properties.renameSymbols(
		[]query.Symbol{innerA, innerB},
		[]query.Symbol{outerA, outerB},
	)
	require.Equal(t,
		RelationProperties{
			Ordering: []query.OrderByClause{
				{Variable: outerA, Descending: false},
				{Variable: outerB, Descending: true},
			},
			Keys: [][]query.Symbol{{outerA}, {outerA, outerB}},
		},
		renamed,
	)
	require.Equal(t, innerA, properties.Ordering[0].Variable,
		"renaming must not mutate the source properties")
	require.Empty(t, properties.renameSymbols(
		[]query.Symbol{innerA},
		[]query.Symbol{outerA, outerB},
	))
}

func TestNewMaterializedRelationFromSetPreservesProofs(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	properties := RelationProperties{Keys: [][]query.Symbol{{entity}}}
	tuples := []Tuple{{int64(1), "one"}, {int64(2), "two"}}

	relation := newMaterializedRelationFromSet(
		[]query.Symbol{entity, name},
		tuples,
		ExecutorOptions{EnableTrueStreaming: true},
		properties,
	)
	require.Equal(t, tuples, relation.tuples)
	require.Equal(t, properties, relation.Properties())
	require.Equal(t, true, relation.Options().EnableTrueStreaming)
}

func TestRelationPropertyPropagation(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	properties := RelationProperties{
		Ordering: []query.OrderByClause{
			{Variable: a, Descending: false},
			{Variable: b, Descending: true},
		},
		Keys: [][]query.Symbol{{a}, {a, b}},
	}
	rel := NewMaterializedRelationWithProperties(
		[]query.Symbol{a, b},
		[]Tuple{{int64(1), int64(2)}, {int64(2), int64(1)}},
		ExecutorOptions{},
		properties,
	)

	filtered := rel.FilterWithPredicate(&query.Comparison{
		Op:    datalog.SymGTE,
		Left:  query.VariableTerm{Symbol: a},
		Right: query.ConstantTerm{Value: int64(0)},
	})
	require.Equal(t, properties, filtered.Properties(), "filter must preserve properties")

	projectedA, err := rel.Project([]query.Symbol{a})
	require.NoError(t, err)
	require.Equal(t, RelationProperties{
		Ordering: []query.OrderByClause{{Variable: a, Descending: false}},
		Keys:     [][]query.Symbol{{a}},
	}, projectedA.Properties())

	projectedB, err := rel.Project([]query.Symbol{b})
	require.NoError(t, err)
	require.Equal(t, RelationProperties{}, projectedB.Properties(),
		"dropping the leading order symbol and every key must clear properties")

	sorted := rel.Sort([]query.OrderByClause{{Variable: b, Descending: true}})
	require.Equal(t, RelationProperties{
		Ordering: []query.OrderByClause{{Variable: b, Descending: true}},
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
		Ordering: []query.OrderByClause{{Variable: a, Descending: false}},
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
		Ordering: []query.OrderByClause{{Variable: a, Descending: false}},
		Keys:     [][]query.Symbol{{a}},
	}
	open := func() *StreamingRelation {
		base := NewMaterializedRelationFromSet(symbols, tuples, ExecutorOptions{})
		return NewStreamingRelationWithProperties(
			symbols,
			base.Iterator(),
			ExecutorOptions{},
			properties,
		)
	}

	require.Equal(t, properties,
		open().FilterWithPredicate(&query.Comparison{
			Op:    datalog.SymGTE,
			Left:  query.VariableTerm{Symbol: a},
			Right: query.ConstantTerm{Value: int64(0)},
		}).Properties())

	projected, err := open().Project([]query.Symbol{a})
	require.NoError(t, err)
	require.Equal(t, properties, projected.Properties())

	evaluated := open().EvaluateFunction(
		&query.ArithmeticFunction{
			Op: datalog.SymAdd,
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
		{Variable: a, Descending: false},
		{Variable: b, Descending: true},
	}}

	require.True(t, properties.satisfiesOrdering([]query.OrderByClause{
		{Variable: a, Descending: false},
	}))
	require.True(t, properties.satisfiesOrdering(properties.Ordering))
	require.False(t, properties.satisfiesOrdering([]query.OrderByClause{
		{Variable: a, Descending: true},
	}))
	require.False(t, properties.satisfiesOrdering([]query.OrderByClause{
		{Variable: b, Descending: true},
	}))
}

func TestStreamingProjectionSkipsDedupWhenKeyIsRetained(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	symbols := []query.Symbol{a, b}
	tuples := []Tuple{{int64(1), "same"}, {int64(2), "same"}}
	open := func(properties RelationProperties) *StreamingRelation {
		base := NewMaterializedRelationFromSet(symbols, tuples, ExecutorOptions{})
		return NewStreamingRelationWithProperties(
			symbols,
			base.Iterator(),
			ExecutorOptions{},
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

func TestProjectionPreservesSet(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	source := []query.Symbol{a, b}
	keyed := RelationProperties{Keys: [][]query.Symbol{{a}}}

	require.True(t, projectionPreservesSet(source, []query.Symbol{a}, keyed),
		"a retained candidate key keeps distinct tuples distinct")
	require.True(t, projectionPreservesSet(source, []query.Symbol{a, b}, RelationProperties{}),
		"the identity projection is injective")
	require.True(t, projectionPreservesSet(source, []query.Symbol{b, a}, RelationProperties{}),
		"a reordering permutation is injective")
	require.False(t, projectionPreservesSet(source, []query.Symbol{a}, RelationProperties{}),
		"a reducing projection without a retained key can collapse distinct tuples")
	require.False(t, projectionPreservesSet(source, []query.Symbol{a, a}, RelationProperties{}),
		"a repeated target reads one source position twice — equal arity is not injectivity")
}

func TestStreamingProjectionSkipsDedupOnPermutation(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	symbols := []query.Symbol{a, b}
	base := NewMaterializedRelationFromSet(symbols,
		[]Tuple{{int64(1), "x"}, {int64(2), "y"}}, ExecutorOptions{})
	stream := NewStreamingRelationWithOptions(symbols, base.Iterator(), ExecutorOptions{})

	reordered, err := stream.Project([]query.Symbol{b, a})
	require.NoError(t, err)
	_, dedups := reordered.(*StreamingRelation).iterator.(*DedupIterator)
	require.False(t, dedups,
		"a permutation of the full symbol set is injective on tuples — no dedup pass")
	rows, err := CollectTuples(reordered, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{"x", int64(1)}, {"y", int64(2)}}, rows)
}

func TestHashJoinPreservesLeftKeyWhenRightJoinSymbolsAreKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	opts := ExecutorOptions{
		EnableStreamingJoins: true,
	}

	leftBase := NewMaterializedRelationFromSet(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}, {int64(2), "left-2"}},
		ExecutorOptions{},
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
	require.True(t, joinIterator.buildIndex.keysUnique(),
		"the candidate-key proof must hold in the grouped build rows")
	require.Len(t, joinIterator.buildIndex.probe(Tuple{int64(1)}), 1,
		"a unique build key groups exactly one row")
	require.Nil(t, joinIterator.seen,
		"a proven result key makes internal full-tuple join deduplication redundant")

	projected, err := joined.Project([]query.Symbol{id, leftValue})
	require.NoError(t, err)
	_, deduplicates := projected.(*StreamingRelation).iterator.(*DedupIterator)
	require.False(t, deduplicates,
		"a join against a unique right side cannot duplicate keyed left rows")
}

func TestHashJoinPanicsWhenCandidateKeyClaimIsViolated(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	opts := ExecutorOptions{}

	left := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}, {int64(2), "left-2"}, {int64(3), "left-3"}},
		opts,
		RelationProperties{},
	)
	// The build side claims ?id as a candidate key but carries duplicate ?id
	// rows — a false proof the grouped build must detect, not trust.
	right := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, rightValue},
		[]Tuple{{int64(1), "right-1"}, {int64(1), "right-2"}},
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)

	require.Panics(t, func() {
		HashJoinWithOptions(left, right, []query.Symbol{id}, opts)
	}, "a violated candidate-key guarantee must fail loudly, not join quietly")
}

func TestHashJoinDoesNotPreserveLeftKeyWhenRightJoinSymbolsAreNotKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	leftValue := datalog.NewSymbol("?left")
	rightValue := datalog.NewSymbol("?right")
	opts := ExecutorOptions{
		EnableStreamingJoins: true,
	}

	leftBase := NewMaterializedRelationFromSet(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}},
		ExecutorOptions{},
	)
	left := NewStreamingRelationWithProperties(
		leftBase.Symbols(),
		leftBase.Iterator(),
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)
	right := NewMaterializedRelationFromSet(
		[]query.Symbol{id, rightValue},
		[]Tuple{{int64(1), "right-1"}, {int64(1), "right-2"}},
		opts,
	)

	joined := HashJoinWithOptions(left, right, []query.Symbol{id}, opts)
	require.Empty(t, joined.Properties().Keys)
	joinIterator := joined.(*StreamingRelation).iterator.(*hashJoinIterator)
	require.False(t, joinIterator.buildIndex.keysUnique(),
		"a fanout key must be visible in the grouped build rows")
	require.Len(t, joinIterator.buildIndex.probe(Tuple{int64(1)}), 2,
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
		EnableStreamingJoins: true,
	}

	left := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, leftValue},
		[]Tuple{{int64(1), "left-1"}, {int64(2), "left-2"}},
		opts,
		RelationProperties{Keys: [][]query.Symbol{{id}}},
	)
	rightBase := NewMaterializedRelationFromSet(
		[]query.Symbol{id, rightValue},
		[]Tuple{{int64(1), "right-1"}, {int64(2), "right-2"}},
		ExecutorOptions{},
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
		EnableStreamingJoins:    true,
		EnableSymmetricHashJoin: true,
		DefaultHashTableSize:    16,
		EnableTrueStreaming:     true,
	}
	streamingLeftBase := NewMaterializedRelationFromSet(leftSymbols, leftTuples, ExecutorOptions{})
	streamingRightBase := NewMaterializedRelationFromSet(rightSymbols, rightTuples, ExecutorOptions{})
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
		Ordering: []query.OrderByClause{{Variable: id, Descending: false}},
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
	// Deliberately constructs a duplicate-carrying relation — an invariant
	// violation no constructor admits — to pin that semi/anti-joins
	// deduplicate unkeyed input rather than trusting it.
	left := &MaterializedRelation{
		symbols: []query.Symbol{id},
		tuples:  []Tuple{{int64(1)}, {int64(1)}, {int64(2)}, {int64(2)}},
	}
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
	expanded, err := evaluateExpressionWithLookup(source, &query.Expression{
		Function: query.EnumerateFunction{
			VecTerm: query.VariableTerm{Symbol: vector},
		},
		Binding: query.TupleBinding{Variables: []query.Symbol{index, value}},
	}, nil, nil)
	require.NoError(t, err)

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
	base := NewMaterializedRelationFromSet(
		[]query.Symbol{symbol},
		[]Tuple{{int64(1)}, {int64(2)}},
		ExecutorOptions{},
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
