package executor

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestOrDefaultSingletonBranchesPreserveOuterProperties(t *testing.T) {
	var events []annotations.Event
	ctx := NewContext(func(event annotations.Event) {
		events = append(events, event)
	})
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	value := datalog.NewSymbol("?value")
	ordering := []query.OrderByClause{{Variable: entity, Direction: query.OrderAsc}}
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, name},
		[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
		ExecutorOptions{},
		RelationProperties{
			Ordering: ordering,
			Keys:     [][]query.Symbol{{entity}},
		},
	)
	branches := [][]query.Clause{
		{
			&query.SubqueryPattern{
				Query:   &query.Query{},
				Binding: query.TupleBinding{Variables: []query.Symbol{value}},
			},
		},
		{
			&query.Expression{
				Function: query.GroundFunction{Value: int64(0)},
				Binding:  value,
			},
		},
	}

	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		ctx,
		branches,
		outer,
		ExecutorOptions{},
		true,
	)

	require.Equal(t, ordering, rel.Properties().Ordering)
	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.Len(t, events, 1)
	require.Equal(t, annotations.OrPropertiesDerived, events[0].Name)
	require.Equal(t, true, events[0].Data["branches_at_most_one"])
	require.Equal(t, false, events[0].Data["deduplicate"])
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
		Query: &query.Query{Find: []query.FindElement{
			query.FindVariable{Symbol: entity},
			query.FindAggregate{Function: "count", Arg: task},
		}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, count}},
	}
	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
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
	multirow := &query.SubqueryPattern{
		Query: &query.Query{Find: []query.FindElement{
			query.FindVariable{Symbol: entity},
			query.FindVariable{Symbol: key},
			query.FindVariable{Symbol: updatedAt},
		}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, key, updatedAt}},
	}
	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
		[][]query.Clause{
			{multirow},
			{&query.Expression{
				Function: query.GroundFunction{Value: []interface{}{":none", ":none"}},
				Binding:  query.TupleBinding{Variables: []query.Symbol{key, updatedAt}},
			}},
		},
		outer,
		ExecutorOptions{},
		true,
	)

	require.False(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.True(t, containsSymbolSet(
		rel.Properties().Keys,
		[]query.Symbol{entity, key, updatedAt},
	))
}

func TestOrDefaultMultirowBranchesDeriveCompositeKey(t *testing.T) {
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

	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
		[][]query.Clause{{pattern(":item/tag")}, {pattern(":item/category")}},
		outer,
		ExecutorOptions{},
		true,
	)

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
	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
		[][]query.Clause{{ground()}, {ground()}},
		outer,
		ExecutorOptions{},
		false,
	)

	require.Equal(t, []Tuple{{int64(1), int64(7)}}, collectTuples(rel))
	require.False(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity, value}))
}

func TestOrBranchOverwriteInvalidatesOuterProperty(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	ordering := []query.OrderByClause{{Variable: entity, Direction: query.OrderAsc}}
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, name},
		[]Tuple{{int64(1), "one"}},
		ExecutorOptions{},
		RelationProperties{
			Ordering: ordering,
			Keys:     [][]query.Symbol{{entity}},
		},
	)
	overwrite := func(value int64) query.Clause {
		return &query.Expression{
			Function: query.GroundFunction{Value: value},
			Binding:  entity,
		}
	}
	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
		[][]query.Clause{{overwrite(10)}, {overwrite(20)}},
		outer,
		ExecutorOptions{},
		true,
	)

	require.Empty(t, rel.Properties().Ordering)
	require.False(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
	require.True(t, containsSymbolSet(rel.Properties().Keys, rel.Symbols()))
}

func TestOrBranchOverwriteRetainsUnaffectedOrderingPrefix(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, name},
		[]Tuple{{int64(1), "one"}},
		ExecutorOptions{},
		RelationProperties{
			Ordering: []query.OrderByClause{
				{Variable: entity, Direction: query.OrderAsc},
				{Variable: name, Direction: query.OrderAsc},
			},
			Keys: [][]query.Symbol{{entity}},
		},
	)
	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
		[][]query.Clause{{
			&query.Expression{
				Function: query.GroundFunction{Value: "replacement"},
				Binding:  name,
			},
		}},
		outer,
		ExecutorOptions{},
		true,
	)

	require.Equal(t,
		[]query.OrderByClause{{Variable: entity, Direction: query.OrderAsc}},
		rel.Properties().Ordering,
	)
	require.True(t, containsSymbolSet(rel.Properties().Keys, []query.Symbol{entity}))
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

func TestUnionRelationAndMaterializationPreserveFullOutputKey(t *testing.T) {
	id := datalog.NewSymbol("?id")
	payload := datalog.NewSymbol("?payload")
	symbols := []query.Symbol{id, payload}
	source := make(chan relationItem, 2)
	source <- relationItem{relation: NewMaterializedRelation(symbols, []Tuple{{int64(1), "same"}})}
	source <- relationItem{relation: NewMaterializedRelation(symbols, []Tuple{{int64(1), "same"}})}
	close(source)

	union := NewUnionRelation(source, symbols, ExecutorOptions{})
	require.True(t, containsSymbolSet(union.Properties().Keys, symbols))

	materialized := union.Materialize()
	require.Equal(t, union.Properties(), materialized.Properties())
	require.Equal(t, 1, materialized.Size())
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
	rel := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
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

	materialized := rel.Materialize()
	require.Equal(t, rel.Properties(), materialized.Properties())
	require.Equal(t, 2, materialized.Size())
}

func TestFilterBranchToOuterTupleComparesVectorValues(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	vector := datalog.NewSymbol("?vector")
	id := datalog.NewIdentity("vector-filter")
	value := []interface{}{int64(1), "two"}
	branch := NewMaterializedRelation(
		[]query.Symbol{entity, vector},
		[]Tuple{{id, value}},
	)

	require.NotPanics(t, func() {
		result := filterBranchToOuterTuple(
			branch,
			Tuple{id, []interface{}{int64(1), "two"}},
			[]query.Symbol{entity, vector},
		)
		require.Equal(t, 1, result.Size())
	})
}

func TestOrPropertyClassifierRandomized(t *testing.T) {
	const cases = 500
	random := rand.New(rand.NewSource(0x5eed))
	entity := datalog.NewSymbol("?entity")
	vector := datalog.NewSymbol("?vector")
	index := datalog.NewSymbol("?index")
	value := datalog.NewSymbol("?value")
	tagAttribute := datalog.NewKeyword(":item/tag")
	outerSymbols := []query.Symbol{entity, vector}

	for caseIndex := 0; caseIndex < cases; caseIndex++ {
		rowCount := 1 + random.Intn(8)
		outerTuples := make([]Tuple, rowCount)
		var datoms []datalog.Datom
		for rowIndex := range outerTuples {
			id := datalog.NewIdentity(fmt.Sprintf("random-or:%d:%d", caseIndex, rowIndex))
			vectorLength := 1 + random.Intn(4)
			vectorValue := make([]interface{}, vectorLength)
			for i := range vectorValue {
				vectorValue[i] = int64(random.Intn(3))
			}
			outerTuples[rowIndex] = Tuple{id, vectorValue}

			tagCount := random.Intn(4)
			for tagIndex := 0; tagIndex < tagCount; tagIndex++ {
				datoms = append(datoms, datalog.Datom{
					E:  id,
					A:  tagAttribute,
					V:  fmt.Sprintf("tag-%d", tagIndex),
					Tx: datalog.ElementID{Lamport: uint64(tagIndex + 1), ReplicaID: 1},
				})
			}
		}
		ordering := []query.OrderByClause{{Variable: entity, Direction: query.OrderAsc}}
		sort.Slice(outerTuples, func(i, j int) bool {
			return compareTuplesByOrder(outerTuples[i], outerTuples[j], ordering, []int{0}) < 0
		})
		outer := NewMaterializedRelationWithProperties(
			outerSymbols,
			outerTuples,
			ExecutorOptions{},
			RelationProperties{
				Ordering: ordering,
				Keys:     [][]query.Symbol{{entity}},
			},
		)
		queryExecutor := newQueryExecutor(
			NewMemoryPatternMatcher(datoms),
			nil,
			ExecutorOptions{},
		)

		branchCount := 1 + random.Intn(4)
		branches := make([][]query.Clause, branchCount)
		for branchIndex := range branches {
			switch random.Intn(3) {
			case 0:
				branches[branchIndex] = []query.Clause{&query.Expression{
					Function: query.GroundFunction{Value: int64(random.Intn(3))},
					Binding:  value,
				}}
			case 1:
				branches[branchIndex] = []query.Clause{&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: entity},
						query.Constant{Value: tagAttribute},
						query.Variable{Name: value},
					},
				}}
			case 2:
				branches[branchIndex] = []query.Clause{&query.Expression{
					Function: query.EnumerateFunction{
						VecTerm: query.VariableTerm{Symbol: vector},
					},
					Binding: query.TupleBinding{Variables: []query.Symbol{index, value}},
				}}
			}
		}
		shortCircuit := random.Intn(2) == 0
		relation := NewOrFallbackRelation(
			queryExecutor,
			NewContext(nil),
			branches,
			outer,
			ExecutorOptions{},
			shortCircuit,
		)

		actual, err := collectTypedTuples(relation)
		require.NoError(t, err, "case %d", caseIndex)
		expected, err := evaluateOrReference(
			queryExecutor,
			branches,
			outerSymbols,
			outerTuples,
			relation.Symbols(),
			shortCircuit,
		)
		require.NoError(t, err, "case %d", caseIndex)
		require.Equal(t, expected, actual, "case %d", caseIndex)
		assertRelationPropertiesHold(t, relation, actual, caseIndex)
	}
}

func evaluateOrReference(
	queryExecutor *DefaultQueryExecutor,
	branches [][]query.Clause,
	outerSymbols []query.Symbol,
	outerTuples []Tuple,
	outputSymbols []query.Symbol,
	shortCircuit bool,
) ([]Tuple, error) {
	seen := NewTupleKeyMap()
	result := make([]Tuple, 0)
	for _, outerTuple := range outerTuples {
		input := NewMaterializedRelationNoDedupe(
			outerSymbols,
			[]Tuple{copyTuple(outerTuple)},
		)
		for _, branch := range branches {
			branchResult, err := queryExecutor.executeInnerClauses(
				NewContext(nil),
				branch,
				input,
			)
			if err != nil {
				return nil, err
			}
			if branchResult == nil {
				continue
			}
			branchTuples, err := collectTypedTuples(branchResult)
			if err != nil {
				return nil, err
			}
			for _, branchTuple := range branchTuples {
				projected := projectTupleWithFallback(
					branchTuple,
					branchResult.Symbols(),
					outputSymbols,
					outerTuple,
					outerSymbols,
				)
				if !seen.PutIfAbsent(NewTupleKeyFull(projected), struct{}{}) {
					result = append(result, projected)
				}
			}
			if shortCircuit && len(branchTuples) > 0 {
				break
			}
		}
	}
	return result, nil
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
		seen := NewTupleKeyMap()
		for _, tuple := range tuples {
			existed := seen.PutIfAbsent(NewTupleKey(tuple, indices), struct{}{})
			require.False(t, existed, "case %d: candidate key %v is not unique", caseIndex, key)
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
		NewMemoryPatternMatcher(nil),
		nil,
		ExecutorOptions{},
	)

	for _, rows := range []int{10_000, 100_000} {
		tuples := make([]Tuple, rows)
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
			b.Run(fmt.Sprintf("%s/rows_%d", name, rows), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rel := NewOrFallbackRelation(
						queryExecutor,
						NewContext(nil),
						branches,
						outer,
						ExecutorOptions{},
						true,
					)
					projected, err := rel.Project([]query.Symbol{entity, value})
					if err != nil {
						b.Fatal(err)
					}
					if projected.Size() != rows {
						b.Fatalf("projected size = %d, want %d", projected.Size(), rows)
					}
				}
			})
		}
	}
}
