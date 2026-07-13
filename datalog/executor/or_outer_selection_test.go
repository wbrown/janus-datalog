package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestFindOuterRelationBySymbolsLeavesSingleStreamLazy(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	stream := NewStreamingRelation(
		[]query.Symbol{entity},
		base.Iterator(),
	)
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})

	selected, consumed := exec.findOuterRelationBySymbols(
		map[query.Symbol]bool{entity: true},
		Relations{stream},
	)
	require.Same(t, stream, selected)
	require.Equal(t, []int{0}, consumed)
	require.False(t, stream.shouldCache,
		"single outer selection must not request eager materialization")
}

func TestFindOuterRelationCombinesMultipleStreamsReusably(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	leftBase := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{int64(2)}})
	rightBase := NewMaterializedRelation([]query.Symbol{y}, []Tuple{{int64(3)}})
	left := NewStreamingRelation([]query.Symbol{x}, leftBase.Iterator())
	right := NewStreamingRelation([]query.Symbol{y}, rightBase.Iterator())
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})

	selected, consumed := exec.findOuterRelation(
		[]query.Symbol{x, y},
		Relations{left, right},
	)
	require.Equal(t, []int{0, 1}, consumed)
	rows, err := CollectTuples(selected, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(3)}}, rows)
}

func TestOrFallbackAnnotatesDeferredOuterMaterialization(t *testing.T) {
	entitySymbol := datalog.NewSymbol("?entity")
	valueSymbol := datalog.NewSymbol("?value")
	entity := datalog.NewIdentity("outer-materialization")
	attr := datalog.NewKeyword(":item/value")
	var materializations int
	ctx := NewContext(func(event annotations.Event) {
		if event.Name == "or-fallback/outer.materialized" {
			materializations++
			require.Equal(t, "join-key-narrowing", event.Data["reason"])
		}
	})
	options := ExecutorOptions{Collector: ctx.Collector()}
	exec := newQueryExecutor(
		NewMemoryPatternMatcher([]datalog.Datom{{E: entity, A: attr, V: "present"}}),
		nil,
		options,
	)
	outerBase := NewMaterializedRelation(
		[]query.Symbol{entitySymbol},
		[]Tuple{{entity}},
	)
	outer := NewStreamingRelation(
		[]query.Symbol{entitySymbol},
		outerBase.Iterator(),
	)
	relation := NewOrFallbackRelation(
		exec,
		ctx,
		[][]query.Clause{{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entitySymbol},
			query.Constant{Value: attr},
			query.Variable{Name: valueSymbol},
		}}}},
		outer,
		options,
		true,
	)
	relation.joinSyms = []query.Symbol{entitySymbol}

	rows, err := CollectTuples(relation, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{entity, "present"}}, rows)
	require.Equal(t, 1, materializations)
}

func TestOrFallbackSinglePassBranchKeepsOuterStreaming(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	var materializations int
	ctx := NewContext(func(event annotations.Event) {
		if event.Name == "or-fallback/outer.materialized" {
			materializations++
		}
	})
	options := ExecutorOptions{Collector: ctx.Collector()}
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	outer := NewStreamingRelation([]query.Symbol{entity}, base.Iterator())
	relation := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, options),
		ctx,
		[][]query.Clause{{&query.Expression{
			Function: &query.GroundFunction{Value: "default"},
			Binding:  value,
		}}},
		outer,
		options,
		true,
	)

	rows, err := CollectTuples(relation, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{int64(1), "default"},
		{int64(2), "default"},
	}, rows)
	require.Zero(t, materializations)
}

func TestOuterJoinKeysProducesSetWithJoinKey(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	value := datalog.NewSymbol("?value")
	e1 := datalog.NewIdentity("join-key-e1")
	e2 := datalog.NewIdentity("join-key-e2")
	outer := NewMaterializedRelation(
		[]query.Symbol{entity, name},
		[]Tuple{
			{e1, "first"},
			{e1, "second"},
			{e2, "third"},
		},
	)
	relation := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(nil),
		[][]query.Clause{{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: datalog.NewKeyword(":item/value")},
			query.Variable{Name: value},
		}}}},
		outer,
		ExecutorOptions{},
		true,
	)
	relation.joinSyms = []query.Symbol{entity}
	iterator := relation.Iterator().(*OrFallbackIterator)

	keys := iterator.outerJoinKeys()
	rows, err := CollectTuples(keys, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{{e1}, {e2}}, rows)
	require.Equal(t,
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
		keys.Properties(),
	)
}

func TestFilterBranchToOuterTuplePreservesIteratorAndCloseErrors(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	branch := NewMaterializedRelation(
		[]query.Symbol{entity, value},
		[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
	)

	iterationFailure := filterBranchToOuterTuple(
		failingRelation{Relation: branch, failAfter: 1},
		Tuple{int64(1)},
		[]query.Symbol{entity},
	)
	require.ErrorIs(t, driveErr(iterationFailure), errInjectedIterator)

	closeFailure := errors.New("filter branch close failure")
	closeResult := filterBranchToOuterTuple(
		failingRelation{Relation: branch, failAfter: 100, closeErr: closeFailure},
		Tuple{int64(1)},
		[]query.Symbol{entity},
	)
	require.ErrorIs(t, driveErr(closeResult), closeFailure)
}

func TestOuterJoinKeysPreservesOuterIteratorAndCloseErrors(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)

	testCases := []struct {
		name     string
		outer    Relation
		expected error
	}{
		{
			name:     "iteration",
			outer:    failingRelation{Relation: base, failAfter: 1},
			expected: errInjectedIterator,
		},
		{
			name:     "close",
			outer:    failingRelation{Relation: base, failAfter: 100, closeErr: errors.New("outer join keys close failure")},
			expected: errors.New("outer join keys close failure"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			relation := NewOrFallbackRelation(
				newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
				NewContext(nil),
				[][]query.Clause{{&query.Expression{
					Function: &query.GroundFunction{Value: "fallback"},
					Binding:  value,
				}}},
				testCase.outer,
				ExecutorOptions{},
				true,
			)
			relation.joinSyms = []query.Symbol{entity}
			iterator := relation.Iterator().(*OrFallbackIterator)

			require.Nil(t, iterator.outerJoinKeys())
			require.EqualError(t, iterator.err, testCase.expected.Error())
		})
	}
}

func TestBuildBranchFromEACachePreservesOuterIteratorAndCloseErrors(t *testing.T) {
	entitySymbol := datalog.NewSymbol("?entity")
	valueSymbol := datalog.NewSymbol("?value")
	entity := datalog.NewIdentity("ea-cache-outer-error")
	attr := datalog.NewKeyword(":item/value")
	base := NewMaterializedRelation(
		[]query.Symbol{entitySymbol},
		[]Tuple{{entity}},
	)
	branch := []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entitySymbol},
		query.Constant{Value: attr},
		query.Variable{Name: valueSymbol},
	}}}
	matcher := &bundleLookupMatcher{values: map[bundleLookupKey]interface{}{
		{entity: entity, attr: attr}: "present",
	}}

	testCases := []struct {
		name     string
		outer    Relation
		expected error
	}{
		{
			name:     "iteration",
			outer:    failingRelation{Relation: base, failAfter: 0},
			expected: errInjectedIterator,
		},
		{
			name:     "close",
			outer:    failingRelation{Relation: base, failAfter: 100, closeErr: errors.New("EA cache outer close failure")},
			expected: errors.New("EA cache outer close failure"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			relation := NewOrFallbackRelation(
				newQueryExecutor(matcher, nil, ExecutorOptions{}),
				NewContext(nil),
				[][]query.Clause{branch},
				testCase.outer,
				ExecutorOptions{},
				true,
			)
			iterator := relation.Iterator().(*OrFallbackIterator)

			require.Nil(t, iterator.buildBranchFromEACache(branch))
			require.EqualError(t, iterator.err, testCase.expected.Error())
		})
	}
}
