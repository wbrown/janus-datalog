package executor

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestTopNRelationMatchesSortThenLimit(t *testing.T) {
	score := datalog.NewSymbol("?score")
	name := datalog.NewSymbol("?name")
	symbols := []query.Symbol{score, name}
	tuples := []Tuple{
		{int64(30), "c"},
		{int64(10), "a"},
		{int64(50), "e"},
		{int64(20), "b"},
		{int64(40), "d"},
	}

	orders := []struct {
		name    string
		orderBy []query.OrderByClause
	}{
		{
			name: "ascending",
			orderBy: []query.OrderByClause{{
				Variable: score, Direction: query.OrderAsc,
			}},
		},
		{
			name: "descending",
			orderBy: []query.OrderByClause{{
				Variable: score, Direction: query.OrderDesc,
			}},
		},
		{
			name: "multiple keys",
			orderBy: []query.OrderByClause{
				{Variable: score, Direction: query.OrderDesc},
				{Variable: name, Direction: query.OrderAsc},
			},
		},
	}

	for _, order := range orders {
		for _, limit := range []int{0, 1, 3, len(tuples), len(tuples) + 5} {
			t.Run(order.name+"/limit_"+strconv.Itoa(limit), func(t *testing.T) {
				wantRel := NewLimitRelation(
					NewMaterializedRelationNoDedupe(symbols, tuples).Sort(order.orderBy),
					limit,
				)
				want, err := CollectTuples(wantRel, nil)
				require.NoError(t, err)

				gotRel := TopNRelation(
					NewMaterializedRelationNoDedupe(symbols, tuples),
					order.orderBy,
					limit,
				)
				got, err := CollectTuples(gotRel, nil)
				require.NoError(t, err)
				require.Equal(t, want, got)
			})
		}
	}
}

func TestTopNRelationCopiesStreamingWorkspace(t *testing.T) {
	score := datalog.NewSymbol("?score")
	symbols := []query.Symbol{score}
	tuples := []Tuple{
		{int64(3)},
		{int64(9)},
		{int64(1)},
		{int64(7)},
		{int64(8)},
	}
	orderBy := []query.OrderByClause{{
		Variable: score, Direction: query.OrderDesc,
	}}

	result := TopNRelation(newReusingWorkspaceStream(symbols, tuples), orderBy, 3)
	got, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(9)}, {int64(8)}, {int64(7)}}, got)
}

func TestTopNRelationUsesSecondarySortKeys(t *testing.T) {
	score := datalog.NewSymbol("?score")
	name := datalog.NewSymbol("?name")
	symbols := []query.Symbol{score, name}
	orderBy := []query.OrderByClause{
		{Variable: score, Direction: query.OrderDesc},
		{Variable: name, Direction: query.OrderAsc},
	}
	tuples := []Tuple{
		{int64(10), "b"},
		{int64(9), "z"},
		{int64(10), "a"},
	}

	result := TopNRelation(NewMaterializedRelationNoDedupe(symbols, tuples), orderBy, 2)
	got, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(10), "a"}, {int64(10), "b"}}, got)
}

func TestTopNRelationPropagatesDeferredIteratorError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	orderBy := []query.OrderByClause{{
		Variable: x, Direction: query.OrderDesc,
	}}

	result := TopNRelation(
		newFailingStream(2, Tuple{int64(3)}, Tuple{int64(1)}, Tuple{int64(2)}),
		orderBy,
		1,
	)
	require.ErrorIs(t, driveErr(result), errInjectedIterator)
}

func TestOrderedLimitWithNonProjectedKeyRetainsFullSortSemantics(t *testing.T) {
	nameAttr := datalog.NewKeyword(":person/name")
	scoreAttr := datalog.NewKeyword(":person/score")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: nameAttr, V: "A", Tx: tx},
		{E: datalog.NewIdentity("p1"), A: scoreAttr, V: int64(100), Tx: tx},
		{E: datalog.NewIdentity("p2"), A: nameAttr, V: "A", Tx: tx},
		{E: datalog.NewIdentity("p2"), A: scoreAttr, V: int64(90), Tx: tx},
		{E: datalog.NewIdentity("p3"), A: nameAttr, V: "B", Tx: tx},
		{E: datalog.NewIdentity("p3"), A: scoreAttr, V: int64(80), Tx: tx},
	}

	q, err := parser.ParseQuery(`[:find ?name
	                              :where [?e :person/name ?name]
	                                     [?e :person/score ?score]
	                              :order-by [[?score :desc]]
	                              :limit 2]`)
	require.NoError(t, err)

	exec := NewExecutor(NewMemoryPatternMatcher(datoms), nil)
	result, err := exec.Execute(q)
	require.NoError(t, err)
	got, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"A"}, {"B"}}, got)
}

func TestOrderedLimitAfterAggregationUsesGlobalTopN(t *testing.T) {
	city := datalog.NewKeyword(":person/city")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: city, V: "BOS", Tx: tx},
		{E: datalog.NewIdentity("p2"), A: city, V: "LA", Tx: tx},
		{E: datalog.NewIdentity("p3"), A: city, V: "NYC", Tx: tx},
		{E: datalog.NewIdentity("p4"), A: city, V: "SF", Tx: tx},
	}

	q, err := parser.ParseQuery(`[:find ?city (count ?p)
	                              :where [?p :person/city ?city]
	                              :order-by [[?city :asc]]
	                              :limit 2]`)
	require.NoError(t, err)

	exec := NewExecutor(NewMemoryPatternMatcher(datoms), nil)
	result, err := exec.Execute(q)
	require.NoError(t, err)
	got, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"BOS", int64(1)}, {"LA", int64(1)}}, got)
}
