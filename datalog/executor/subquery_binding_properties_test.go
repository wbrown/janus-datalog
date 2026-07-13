package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestRelationBindingRenamesPropertiesPositionally(t *testing.T) {
	innerGroup := datalog.NewSymbol("?inner-group")
	innerCount := datalog.NewSymbol("(count ?item)")
	outerGroup := datalog.NewSymbol("?group")
	outerCount := datalog.NewSymbol("?count")
	innerProperties := RelationProperties{
		Ordering: []query.OrderByClause{{
			Variable:  innerGroup,
			Direction: query.OrderAsc,
		}},
		Keys: [][]query.Symbol{{innerGroup}},
	}
	inner := NewMaterializedRelationWithProperties(
		[]query.Symbol{innerGroup, innerCount},
		[]Tuple{{"a", int64(2)}, {"b", int64(1)}},
		ExecutorOptions{},
		innerProperties,
	)

	bound, err := applyBindingForm(
		inner,
		query.RelationBinding{Variables: []query.Symbol{outerGroup, outerCount}},
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t,
		RelationProperties{
			Ordering: []query.OrderByClause{{
				Variable:  outerGroup,
				Direction: query.OrderAsc,
			}},
			Keys: [][]query.Symbol{{outerGroup}},
		},
		bound.Properties(),
	)
	require.Equal(t, innerProperties, inner.Properties(),
		"binding must not mutate the inner relation properties")
}

func TestRelationBindingPropertiesEnableUniqueJoinBuild(t *testing.T) {
	innerGroup := datalog.NewSymbol("?inner-group")
	innerCount := datalog.NewSymbol("(count ?item)")
	group := datalog.NewSymbol("?group")
	count := datalog.NewSymbol("?count")
	name := datalog.NewSymbol("?name")
	inner := NewMaterializedRelationWithProperties(
		[]query.Symbol{innerGroup, innerCount},
		[]Tuple{{"a", int64(2)}, {"b", int64(1)}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{innerGroup}}},
	)
	bound, err := applyBindingForm(
		inner,
		query.RelationBinding{Variables: []query.Symbol{group, count}},
		nil,
		nil,
	)
	require.NoError(t, err)
	var boundTuples []Tuple
	require.NoError(t, collectTuplesInto(&boundTuples, bound))
	bound = NewMaterializedRelationWithProperties(
		bound.Symbols(),
		boundTuples,
		bound.Options(),
		bound.Properties(),
	)

	var strategy annotations.Event
	options := ExecutorOptions{
		Collector: annotations.NewCollector(func(event annotations.Event) {
			if event.Name == annotations.JoinStrategy {
				strategy = event
			}
		}),
	}
	left := NewMaterializedRelationWithOptions(
		[]query.Symbol{group, name},
		[]Tuple{{"a", "A1"}, {"a", "A2"}, {"b", "B1"}},
		options,
	)
	result := HashJoinWithOptions(left, bound, []query.Symbol{group}, options)
	rows, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, "right", strategy.Data["build_side"])
	require.Equal(t, true, strategy.Data["build_key_unique"])
}

func TestRelationBindingWithConstantPrefixRetainsRemappedKey(t *testing.T) {
	innerGroup := datalog.NewSymbol("?inner-group")
	outerGroup := datalog.NewSymbol("?group")
	tenant := datalog.NewSymbol("?tenant")
	source := datalog.SymDollar
	inner := NewMaterializedRelationWithProperties(
		[]query.Symbol{innerGroup},
		[]Tuple{{"a"}, {"b"}},
		ExecutorOptions{},
		RelationProperties{Keys: [][]query.Symbol{{innerGroup}}},
	)

	bound, err := applyBindingForm(
		inner,
		query.RelationBinding{Variables: []query.Symbol{outerGroup}},
		map[query.Symbol]interface{}{tenant: "tenant-1"},
		[]query.Symbol{source, tenant},
	)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{tenant, outerGroup}, bound.Symbols())
	require.Equal(t,
		RelationProperties{Keys: [][]query.Symbol{{outerGroup}}},
		bound.Properties(),
	)
}
