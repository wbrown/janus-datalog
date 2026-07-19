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
			query.FindAggregate{Function: datalog.SymCount, Arg: task},
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
	ordering := []query.OrderByClause{{Variable: entity, Descending: false}}
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
				{Variable: entity, Descending: false},
				{Variable: name, Descending: false},
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
		[]query.OrderByClause{{Variable: entity, Descending: false}},
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

func TestNestedOrDefaultAndEmptyOuterSetSemantics(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	ground := func(v int64) query.Clause {
		return &query.Expression{Function: query.GroundFunction{Value: v}, Binding: value}
	}
	nested := &query.OrDefaultClause{Branches: [][]query.Clause{
		{ground(1)},
		{ground(2)},
	}}
	queryExecutor := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})

	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	relation := NewOrFallbackRelation(
		queryExecutor,
		NewContext(nil),
		[][]query.Clause{{nested}, {ground(3)}},
		outer,
		ExecutorOptions{},
		true,
	)
	rows, err := collectTypedTuples(relation)
	require.NoError(t, err)
	require.Equal(t, []Tuple{{int64(1), int64(1)}}, rows)
	assertRelationPropertiesHold(t, relation, rows, 0)

	emptyOuter := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity},
		nil,
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	empty := NewOrFallbackRelation(
		queryExecutor,
		NewContext(nil),
		[][]query.Clause{{nested}},
		emptyOuter,
		ExecutorOptions{},
		true,
	)
	emptyRows, err := collectTypedTuples(empty)
	require.NoError(t, err)
	require.Empty(t, emptyRows)
	assertRelationPropertiesHold(t, empty, emptyRows, 1)
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
	for _, seed := range []int64{0x5eed, 0x5eee, 0x5eef, 0x5ef0, 0x5ef1, 0x5ef2, 0x5ef3, 0x5ef4} {
		t.Run(fmt.Sprintf("seed_%x", seed), func(t *testing.T) {
			runOrPropertyDifferential(t, seed)
		})
	}
}

func runOrPropertyDifferential(t *testing.T, seed int64) {
	const cases = 500
	random := rand.New(rand.NewSource(seed))
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
		ordering := []query.OrderByClause{{Variable: entity, Descending: false}}
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
		expected := evaluateOrReference(
			branches,
			outerSymbols,
			outerTuples,
			relation.Symbols(),
			shortCircuit,
			datoms,
		)
		require.True(t, tupleSequencesEqualPairwise(expected, actual),
			"case %d: expected %v, got %v", caseIndex, expected, actual)
		assertRelationPropertiesHold(t, relation, actual, caseIndex)
	}
}

func evaluateOrReference(
	branches [][]query.Clause,
	outerSymbols []query.Symbol,
	outerTuples []Tuple,
	outputSymbols []query.Symbol,
	shortCircuit bool,
	datoms []datalog.Datom,
) []Tuple {
	result := make([]Tuple, 0)
	for _, outerTuple := range outerTuples {
		for _, branch := range branches {
			branchSymbols, branchTuples := evaluateGeneratedOrBranch(
				branch,
				outerSymbols,
				outerTuple,
				datoms,
			)
			for _, branchTuple := range branchTuples {
				projected := projectGeneratedTuple(branchTuple, branchSymbols, outputSymbols)
				if !containsTuplePairwise(result, projected) {
					result = append(result, projected)
				}
			}
			if shortCircuit && len(branchTuples) > 0 {
				break
			}
		}
	}
	return result
}

func evaluateGeneratedOrBranch(
	branch []query.Clause,
	outerSymbols []query.Symbol,
	outerTuple Tuple,
	datoms []datalog.Datom,
) ([]query.Symbol, []Tuple) {
	if len(branch) != 1 {
		panic("generated OR branch must contain exactly one clause")
	}
	switch clause := branch[0].(type) {
	case *query.Expression:
		switch function := clause.Function.(type) {
		case query.GroundFunction:
			binding := clause.Binding.(query.Symbol)
			symbols := append(append([]query.Symbol(nil), outerSymbols...), binding)
			tuple := append(copyTuple(outerTuple), function.Value)
			return symbols, []Tuple{tuple}
		case query.EnumerateFunction:
			binding := clause.Binding.(query.TupleBinding)
			symbols := append(append([]query.Symbol(nil), outerSymbols...), binding.Variables...)
			vectorValue := outerTuple[1].([]interface{})
			tuples := make([]Tuple, len(vectorValue))
			for i, value := range vectorValue {
				tuples[i] = append(copyTuple(outerTuple), int64(i), value)
			}
			return symbols, tuples
		default:
			panic(fmt.Sprintf("unsupported generated expression %T", function))
		}
	case *query.DataPattern:
		entity := outerTuple[0].(datalog.Identity)
		attribute := clause.GetA().(query.Constant).Value.(datalog.Keyword)
		valueSymbol := clause.GetV().(query.Variable).Name
		symbols := append(append([]query.Symbol(nil), outerSymbols...), valueSymbol)
		var tuples []Tuple
		for _, datom := range datoms {
			if datom.E.Equal(entity) && datom.A.Equal(attribute) {
				tuples = append(tuples, append(copyTuple(outerTuple), datom.V))
			}
		}
		return symbols, tuples
	default:
		panic(fmt.Sprintf("unsupported generated OR clause %T", clause))
	}
}

func projectGeneratedTuple(
	tuple Tuple,
	sourceSymbols []query.Symbol,
	outputSymbols []query.Symbol,
) Tuple {
	positions := make(map[query.Symbol]int, len(sourceSymbols))
	for i, symbol := range sourceSymbols {
		positions[symbol] = i
	}
	result := make(Tuple, len(outputSymbols))
	for i, symbol := range outputSymbols {
		result[i] = tuple[positions[symbol]]
	}
	return result
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
