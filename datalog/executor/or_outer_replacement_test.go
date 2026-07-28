package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestOrDefaultJoinReplacesConsumedOuterWithoutRedundantJoin(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	name := datalog.NewSymbol("?name")
	value := datalog.NewSymbol("?value")
	unrelated := datalog.NewSymbol("?unrelated")
	var replacementEvents int
	var joinEvents int
	ctx := NewContext(func(event annotations.Event) {
		switch event.Name {
		case "or/outer-replaced":
			replacementEvents++
			require.Equal(t, 1, event.Data["consumed_groups"])
			require.Equal(t, 2, event.Data["remaining_groups"])
		case annotations.JoinStrategy:
			joinEvents++
		}
	})
	options := ExecutorOptions{Collector: ctx.Collector()}
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, options)
	outer := NewMaterializedRelationWithProperties(
		[]query.Symbol{entity, name},
		[]Tuple{{int64(1), "one"}, {int64(2), "two"}},
		options,
		RelationProperties{Keys: [][]query.Symbol{{entity}}},
	)
	other := NewMaterializedRelationWithOptions(
		[]query.Symbol{unrelated},
		[]Tuple{{"kept"}},
		options,
	)
	q := &query.Query{Where: []query.Clause{&query.OrDefaultJoinClause{
		RequiredVars: []query.Symbol{entity},
		OutputVars:   []query.Symbol{value},
		Branches: [][]query.Clause{
			{&query.Expression{
				Function: &query.GroundFunction{Value: "selected"},
				Binding:  value,
			}},
			{&query.Expression{
				Function: &query.GroundFunction{Value: "default"},
				Binding:  value,
			}},
		},
	}}}

	groups, err := exec.Execute(ctx, q, []Relation{outer, other})
	require.NoError(t, err)
	require.Len(t, groups, 2)

	var expanded Relation
	for _, group := range groups {
		if query.ContainsSymbol(group.Symbols(), value) {
			expanded = group
		}
	}
	require.NotNil(t, expanded)
	tuples, err := CollectTuples(expanded, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]interface{}{
		{int64(1), "one", "selected"},
		{int64(2), "two", "selected"},
	}, tuples)
	require.True(t, containsSymbolSet(expanded.Properties().Keys, []query.Symbol{entity}))
	require.Equal(t, 1, replacementEvents)
	require.Zero(t, joinEvents)
}

func TestOrDefaultReplacesEveryConsumedOuterGroup(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	sum := datalog.NewSymbol("?sum")
	var consumed int
	ctx := NewContext(func(event annotations.Event) {
		if event.Name == "or/outer-replaced" {
			consumed, _ = event.Data["consumed_groups"].(int)
		}
	})
	options := ExecutorOptions{Collector: ctx.Collector()}
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, options)
	left := NewMaterializedRelationWithOptions(
		[]query.Symbol{x},
		[]Tuple{{int64(2)}},
		options,
	)
	right := NewMaterializedRelationWithOptions(
		[]query.Symbol{y},
		[]Tuple{{int64(3)}},
		options,
	)
	q := &query.Query{Where: []query.Clause{&query.OrDefaultClause{
		Branches: [][]query.Clause{
			{&query.Expression{
				Function: query.ArithmeticFunction{
					Op: datalog.SymAdd,
					Args: []query.Term{
						query.VariableTerm{Symbol: x},
						query.VariableTerm{Symbol: y},
					},
				},
				Binding: sum,
			}},
			{&query.Expression{
				Function: &query.GroundFunction{Value: int64(0)},
				Binding:  sum,
			}},
		},
	}}}

	groups, err := exec.Execute(ctx, q, []Relation{left, right})
	require.NoError(t, err)
	require.Len(t, groups, 1)
	tuples, err := CollectTuples(groups[0], nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(3), int64(5)}}, tuples)
	require.Equal(t, 2, consumed)
}

func TestOrOuterReplacementPreservesDeferredOuterError(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	options := ExecutorOptions{}
	base := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{int64(1)}, {int64(2)}},
	)
	outer := NewStreamingRelation(
		[]query.Symbol{entity},
		&failingIterator{inner: base.Iterator(), failAfter: 1},
	)
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, options)
	// Two branches to satisfy the declared-interface contract (the parser
	// never accepted fewer); branch 1 always matches, so branch 2 is inert
	// and the test still exercises deferred-error propagation through the
	// outer replacement.
	q := &query.Query{Where: []query.Clause{&query.OrDefaultJoinClause{
		RequiredVars: []query.Symbol{entity},
		OutputVars:   []query.Symbol{value},
		Branches: [][]query.Clause{
			{&query.Expression{
				Function: &query.GroundFunction{Value: "selected"},
				Binding:  value,
			}},
			{&query.Expression{
				Function: &query.GroundFunction{Value: "default"},
				Binding:  value,
			}},
		},
	}}}

	groups, err := exec.Execute(NewContext(nil), q, []Relation{outer})
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.ErrorIs(t, driveErr(groups[0]), errInjectedIterator)
}

func TestOrFallbackDoesNotTreatBranchErrorAsNoMatch(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	failingAttr := datalog.NewKeyword(":item/failing")
	outer := NewMaterializedRelation(
		[]query.Symbol{entity},
		[]Tuple{{datalog.NewIdentity("branch-error")}},
	)
	matcher := &failingScanMatcher{
		failAttr: failingAttr,
		dataRel: NewMaterializedRelation(
			[]query.Symbol{entity, value},
			nil,
		),
	}
	exec := newQueryExecutor(matcher, nil, ExecutorOptions{})
	branches := [][]query.Clause{
		{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity},
			query.Constant{Value: failingAttr},
			query.Variable{Name: value},
		}}},
		{&query.Expression{
			Function: &query.GroundFunction{Value: "default"},
			Binding:  value,
		}},
	}
	relation := NewOrFallbackRelation(
		exec,
		NewContext(nil),
		branches,
		outer,
		ExecutorOptions{},
		true,
	)
	relation.joinSyms = []query.Symbol{entity}

	require.ErrorIs(t, driveErr(relation), errInjectedIterator,
		"a failed preferred branch must not fall through to the default branch")
}
