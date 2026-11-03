package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A fallback whose branches each answer at most one tuple per subject tuple,
// a pure count subquery and a ground, keeps the subject's key and ordering
// and deduplicates nothing.
func TestOrDefaultSingletonBranchesPreserveOuterProperties(t *testing.T) {
	var events []annotations.Event
	options := ExecutorOptions{Handler: func(event annotations.Event) {
		events = append(events, event)
	}}
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	value := datalog.NewSymbol("?value")
	task := datalog.NewSymbol("?task")
	ordering := []query.OrderByClause{{Variable: entity, Descending: false}}
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, name},
		[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
		ExecutorOptions{},
		RelationProperties{
			Ordering: ordering,
			Keys:     [][]query.Symbol{{entity}},
		},
	)
	owner := datalog.NewSymbol("?owner")
	branches := [][]query.Clause{
		{
			&query.SubqueryPattern{
				Query: &query.Query{
					Find:  []query.FindElement{query.FindAggregate{Function: datalog.SymCount, Arg: task}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
					Where: []query.Clause{branchPattern(task, owner, datalog.NewKeyword(":task/owner"))},
				},
				Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
				Binding: query.RelationBinding{Variables: []query.Symbol{value}},
			},
		},
		{
			&query.Expression{
				Function: query.GroundFunction{Value: int64(0)},
				Binding:  value,
			},
		},
	}

	rel, err := NewOrFallbackRelation(
		newQueryExecutor(nil, nil, options),
		NewContext(),
		nil,
		[]query.Symbol{value},
		branches,
		outer,
		options,
		true,
	)
	require.NoError(t, err)

	require.Equal(t, ordering, rel.Properties().Ordering)
	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	// The executor plans the branch frames at construction, under no plan
	// cache, and reports each; the properties are derived once.
	var derived []annotations.Event
	for _, event := range events {
		switch event.Name {
		case annotations.OrPropertiesDerived:
			derived = append(derived, event)
		case annotations.PlannerPlanMade:
		default:
			require.Failf(t, "an event of another name reached the handler", "%s", event.Name)
		}
	}
	require.Len(t, derived, 1)
	require.Equal(t, true, derived[0].Data["branches_at_most_one"])
	require.Equal(t, false, derived[0].Data["deduplicate"])
}

func TestOrDefaultDecorrelatedAggregatePreservesOuterGroupKey(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	count := datalog.NewSymbol("?count")
	task := datalog.NewSymbol("?task")
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	decorrelated := &query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: entity},
				query.FindAggregate{Function: datalog.SymCount, Arg: task},
			},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{branchPattern(task, entity, datalog.NewKeyword(":task/owner"))},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, count}},
	}
	rel, err := NewOrFallbackRelation(
		newQueryExecutor(nil, nil, ExecutorOptions{}),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{count},
		[][]query.Clause{
			{decorrelated},
			{&query.Expression{
				Function: query.GroundFunction{Value: int64(0)},
				Binding:  count,
			}},
		},
		outer,
		ExecutorOptions{},
		true,
	)
	require.NoError(t, err)

	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
}

func TestOrDefaultRelationBindingWithFreshGroupDoesNotPreserveOuterKey(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	key := datalog.NewSymbol("?key")
	updatedAt := datalog.NewSymbol("?updatedAt")
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	multiTuple := &query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: entity},
				query.FindVariable{Symbol: key},
				query.FindVariable{Symbol: updatedAt},
			},
			In: []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{
				branchPattern(entity, key, datalog.NewKeyword(":item/key")),
				branchPattern(entity, updatedAt, datalog.NewKeyword(":item/updated")),
			},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, key, updatedAt}},
	}
	rel, err := NewOrFallbackRelation(
		newQueryExecutor(nil, nil, ExecutorOptions{}),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{key, updatedAt},
		[][]query.Clause{
			{multiTuple},
			{&query.Expression{
				Function: query.GroundFunction{Value: []interface{}{":none", ":none"}},
				Binding:  query.TupleBinding{Variables: []query.Symbol{key, updatedAt}},
			}},
		},
		outer,
		ExecutorOptions{},
		true,
	)
	require.NoError(t, err)

	require.False(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.True(t, containsSymbolSet(
		rel.Properties().Keys,
		[]query.Symbol{entity, key, updatedAt},
	))
}

func TestOrDefaultMultiTupleBranchesDeriveCompositeKey(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	tag := datalog.NewSymbol("?tag")
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, name},
		[]Tuple{{int64(1), "one"}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	pattern := func(attribute string) query.Clause {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: datalog.NewKeyword(attribute)},
			query.Variable{Name: tag},
		}}
	}

	rel, err := NewOrFallbackRelation(
		newQueryExecutor(nil, nil, ExecutorOptions{}),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{tag},
		[][]query.Clause{{pattern(":item/tag")}, {pattern(":item/category")}},
		outer,
		ExecutorOptions{},
		true,
	)
	require.NoError(t, err)

	require.False(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity, tag}))
	require.True(t, containsSymbolSet(rel.Properties().Keys, rel.Symbols()))
}

func TestCorrelatedUnionDeduplicatesAcrossBranches(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	ground := func() query.Clause {
		return &query.Expression{
			Function: query.GroundFunction{Value: int64(7)},
			Binding:  value,
		}
	}
	rel, err := NewOrFallbackRelation(
		newQueryExecutor(nil, nil, ExecutorOptions{}),
		NewContext(),
		nil,
		[]query.Symbol{value},
		[][]query.Clause{{ground()}, {ground()}},
		outer,
		ExecutorOptions{},
		false,
	)
	require.NoError(t, err)

	require.Equal(t, []Tuple{{int64(1), int64(7)}}, collectTuples(rel))
	require.False(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity, value}))
}

func TestDeduplicatingUnionAdvertisesOnlyFullOutputKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	payload := datalog.NewSymbol("?payload")
	properties := RelationProperties{Keys: [][]query.Symbol{{id}}}
	left := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, payload},
		[]Tuple{{int64(1), "left"}},
		ExecutorOptions{},
		properties,
	)
	right := NewMaterializedRelationWithProperties(
		[]query.Symbol{id, payload},
		[]Tuple{{int64(1), "right"}},
		ExecutorOptions{},
		properties,
	)

	result := unionRelations(
		[]Relation{left, right},
		[]query.Symbol{id, payload},
		ExecutorOptions{},
	)

	require.False(t, containsSymbolSet(result.Properties().Keys, []query.Symbol{id}))
	require.True(t, containsSymbolSet(result.Properties().Keys, []query.Symbol{id, payload}))
	require.Equal(t, 2, result.Size())
}

func TestOrFallbackMaterializationPreservesProperties(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	rel, err := NewOrFallbackRelation(
		newQueryExecutor(nil, nil, ExecutorOptions{}),
		NewContext(),
		nil,
		[]query.Symbol{value},
		[][]query.Clause{{
			&query.Expression{
				Function: query.GroundFunction{Value: int64(0)},
				Binding:  value,
			},
		}},
		outer,
		ExecutorOptions{},
		true,
	)
	require.NoError(t, err)

	materialized := rel.Materialize()
	require.Equal(t, rel.Properties(), materialized.Properties())
	require.Equal(t, 2, materialized.Size())
}

func TestNestedOrDefaultAndEmptyOuterSetSemantics(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	ground := func(v int64) query.Clause {
		return &query.Expression{Function: query.GroundFunction{Value: v}, Binding: value}
	}
	nested := &query.OrDefaultJoinClause{
		OutputVars: []query.Symbol{value},
		Branches: [][]query.Clause{
			{ground(1)},
			{ground(2)},
		},
	}
	queryExecutor := newQueryExecutor(nil, nil, ExecutorOptions{})

	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	relation, err := NewOrFallbackRelation(
		queryExecutor,
		NewContext(),
		nil,
		[]query.Symbol{value},
		[][]query.Clause{{nested}, {ground(3)}},
		outer,
		ExecutorOptions{},
		true,
	)
	require.NoError(t, err)
	tuples, err := collectTypedTuples(relation)
	require.NoError(t, err)
	require.Equal(t, []Tuple{{int64(1), int64(1)}}, tuples)
	assertRelationPropertiesHold(t, relation, tuples, 0)

	emptyOuter := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		nil,
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	empty, err := NewOrFallbackRelation(
		queryExecutor,
		NewContext(),
		nil,
		[]query.Symbol{value},
		[][]query.Clause{{nested}},
		emptyOuter,
		ExecutorOptions{},
		true,
	)
	require.NoError(t, err)
	emptyTuples, err := collectTypedTuples(empty)
	require.NoError(t, err)
	require.Empty(t, emptyTuples)
	assertRelationPropertiesHold(t, empty, emptyTuples, 1)
}

// The carried-output filter compares a vector value by its elements: a
// branch tuple carrying an equal vector agrees with the subject tuple.
func TestAgreeingComparesVectorValues(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	vector := datalog.NewSymbol("?vector")
	id := datalog.NewIdentity("vector-filter")
	value := []interface{}{int64(1), "two"}
	it := &matchEmit{carried: []carriedOutput{{symbol: vector, outerPos: 1}}}

	require.NotPanics(t, func() {
		kept := it.agreeing(
			[]Tuple{{id, value}},
			[]query.Symbol{entity, vector},
			Tuple{id, []interface{}{int64(1), "two"}},
		)
		require.Len(t, kept, 1)
	})
}

func tupleSequencesEqualPairwise(left, right []Tuple) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !tuplesEqualPairwise(left[i], right[i], nil) {
			return false
		}
	}
	return true
}

func containsTuplePairwise(tuples []Tuple, candidate Tuple) bool {
	for _, tuple := range tuples {
		if tuplesEqualPairwise(tuple, candidate, nil) {
			return true
		}
	}
	return false
}

func tuplesEqualPairwise(left, right Tuple, indices []int) bool {
	if len(indices) == 0 {
		if len(left) != len(right) {
			return false
		}
		indices = make([]int, len(left))
		for i := range indices {
			indices[i] = i
		}
	}
	for _, index := range indices {
		if index >= len(left) || index >= len(right) ||
			!datalog.ValuesEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func collectTypedTuples(relation Relation) ([]Tuple, error) {
	raw, err := CollectTuples(relation, nil)
	if err != nil {
		return nil, err
	}
	result := make([]Tuple, len(raw))
	for i, tuple := range raw {
		result[i] = Tuple(tuple)
	}
	return result, nil
}

func assertRelationPropertiesHold(
	t *testing.T,
	relation Relation,
	tuples []Tuple,
	caseIndex int,
) {
	t.Helper()
	symbolPositions := make(map[query.Symbol]int, len(relation.Symbols()))
	for i, symbol := range relation.Symbols() {
		symbolPositions[symbol] = i
	}
	for _, key := range relation.Properties().Keys {
		indices := make([]int, len(key))
		for i, symbol := range key {
			position, ok := symbolPositions[symbol]
			require.True(t, ok, "case %d: key symbol %s missing", caseIndex, symbol)
			indices[i] = position
		}
		for i := range tuples {
			for j := i + 1; j < len(tuples); j++ {
				require.False(t, tuplesEqualPairwise(tuples[i], tuples[j], indices),
					"case %d: candidate key %v is not unique for tuples %v and %v",
					caseIndex, key, tuples[i], tuples[j])
			}
		}
	}
	if len(relation.Properties().Ordering) > 0 {
		indices, err := orderBySymbolIndices(relation.Symbols(), relation.Properties().Ordering)
		require.NoError(t, err, "case %d", caseIndex)
		for i := 1; i < len(tuples); i++ {
			require.LessOrEqual(t,
				compareTuplesByOrder(
					tuples[i-1],
					tuples[i],
					relation.Properties().Ordering,
					indices,
				),
				0,
				"case %d: declared ordering does not hold",
				caseIndex,
			)
		}
	}
}

func BenchmarkOrFallbackPropertyPropagation(b *testing.B) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")
	value := datalog.NewSymbol("?value")
	branches := [][]query.Clause{{
		&query.Expression{
			Function: query.GroundFunction{Value: int64(1)},
			Binding:  value,
		},
	}}
	queryExecutor := newQueryExecutor(
		nil,
		nil,
		ExecutorOptions{},
	)

	for _, count := range []int{10_000, 100_000} {
		tuples := make([]Tuple, count)
		for i := range tuples {
			tuples[i] = Tuple{int64(i), fmt.Sprintf("payload-%d", i)}
		}
		for _, proven := range []bool{false, true} {
			name := "unproven"
			properties := RelationProperties{}
			if proven {
				name = "proven"
				properties.Keys = [][]query.Symbol{{entity}}
			}
			outer := NewMaterializedRelationWithProperties(
				[]query.Symbol{entity, payload},
				tuples,
				ExecutorOptions{},
				properties,
			)
			b.Run(fmt.Sprintf("%s/tuples_%d", name, count), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rel, err := NewOrFallbackRelation(
						queryExecutor,
						NewContext(),
						nil,
						[]query.Symbol{value},
						branches,
						outer,
						ExecutorOptions{},
						true,
					)
					if err != nil {
						b.Fatal(err)
					}
					projected, err := rel.Project([]query.Symbol{entity, value})
					if err != nil {
						b.Fatal(err)
					}
					if projected.Size() != count {
						b.Fatalf("projected size = %d, want %d", projected.Size(), count)
					}
				}
			})
		}
	}
}
