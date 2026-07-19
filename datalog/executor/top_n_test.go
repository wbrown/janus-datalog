package executor

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
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
				Variable: score, Descending: false,
			}},
		},
		{
			name: "descending",
			orderBy: []query.OrderByClause{{
				Variable: score, Descending: true,
			}},
		},
		{
			name: "multiple keys",
			orderBy: []query.OrderByClause{
				{Variable: score, Descending: true},
				{Variable: name, Descending: false},
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
		Variable: score, Descending: true,
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
		{Variable: score, Descending: true},
		{Variable: name, Descending: false},
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
		Variable: x, Descending: true,
	}}

	result := TopNRelation(
		newFailingStream(2, Tuple{int64(3)}, Tuple{int64(1)}, Tuple{int64(2)}),
		orderBy,
		1,
	)
	require.ErrorIs(t, driveErr(result), errInjectedIterator)
}

func TestTopNRelationRandomizedDifferential(t *testing.T) {
	for _, seed := range []int64{0x70f, 0x710, 0x711, 0x712, 0x713, 0x714, 0x715, 0x716} {
		t.Run(fmt.Sprintf("seed_%x", seed), func(t *testing.T) {
			runTopNDifferential(t, seed)
		})
	}
}

func runTopNDifferential(t *testing.T, seed int64) {
	random := rand.New(rand.NewSource(seed))
	score := datalog.NewSymbol("?score")
	name := datalog.NewSymbol("?name")
	symbols := []query.Symbol{score, name}
	for caseIndex := 0; caseIndex < 500; caseIndex++ {
		count := random.Intn(40)
		tuples := make([]Tuple, count)
		for i := range tuples {
			tuples[i] = Tuple{
				int64(random.Intn(12) - 6),
				fmt.Sprintf("case-%03d-row-%03d", caseIndex, i),
			}
		}
		orderBy := []query.OrderByClause{
			{Variable: score, Descending: false},
			{Variable: name, Descending: false},
		}
		if random.Intn(2) == 0 {
			orderBy[0].Descending = true
		}
		if random.Intn(2) == 0 {
			orderBy[1].Descending = true
		}
		limit := random.Intn(count + 3)
		expected := nativeTopNReference(tuples, orderBy, limit)
		actual, err := collectTypedTuples(TopNRelation(
			NewMaterializedRelationNoDedupe(symbols, tuples),
			orderBy,
			limit,
		))
		require.NoError(t, err)
		require.True(t, tupleSequencesEqualPairwise(expected, actual),
			"case %d limit %d: expected %v, got %v", caseIndex, limit, expected, actual)
	}
}

func nativeTopNReference(
	tuples []Tuple,
	orderBy []query.OrderByClause,
	limit int,
) []Tuple {
	result := make([]Tuple, len(tuples))
	for i, tuple := range tuples {
		result[i] = copyTuple(tuple)
	}
	sort.Slice(result, func(i, j int) bool {
		leftScore := result[i][0].(int64)
		rightScore := result[j][0].(int64)
		if leftScore != rightScore {
			if orderBy[0].Descending {
				return leftScore > rightScore
			}
			return leftScore < rightScore
		}
		leftName := result[i][1].(string)
		rightName := result[j][1].(string)
		if orderBy[1].Descending {
			return leftName > rightName
		}
		return leftName < rightName
	})
	if limit < len(result) {
		result = result[:limit]
	}
	return result
}

func TestTopNRelationCompleteTiesRemainValid(t *testing.T) {
	score := datalog.NewSymbol("?score")
	payload := datalog.NewSymbol("?payload")
	var tuples []Tuple
	for i := 0; i < 20; i++ {
		tuples = append(tuples, Tuple{int64(1), fmt.Sprintf("payload-%d", i)})
	}
	result := TopNRelation(
		NewMaterializedRelationNoDedupe([]query.Symbol{score, payload}, tuples),
		[]query.OrderByClause{{Variable: score, Descending: false}},
		5,
	)
	rows, err := collectTypedTuples(result)
	require.NoError(t, err)
	require.Len(t, rows, 5)
	for _, row := range rows {
		require.Equal(t, int64(1), row[0])
	}
}

func TestTopNRelationZeroAndMalformedOrderDoNotOpenSource(t *testing.T) {
	x := datalog.NewSymbol("?x")
	source := &iteratorCountingRelation{
		Relation: newFailingRelation(0, Tuple{int64(1)}),
	}
	zero := TopNRelation(
		source,
		[]query.OrderByClause{{Variable: x, Descending: false}},
		0,
	)
	rows, err := collectTypedTuples(zero)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Zero(t, source.iteratorCalls)

	malformed := TopNRelation(
		source,
		[]query.OrderByClause{{Variable: datalog.NewSymbol("?missing"), Descending: false}},
		1,
	)
	require.Error(t, driveErr(malformed))
	require.Zero(t, source.iteratorCalls)
}

func TestTopNRelationPropagatesCloseError(t *testing.T) {
	x := datalog.NewSymbol("?x")
	closeErr := errors.New("top-n close failure")
	source := failingRelation{
		Relation: NewMaterializedRelation(
			[]query.Symbol{x},
			[]Tuple{{int64(2)}, {int64(1)}},
		),
		failAfter: 100,
		closeErr:  closeErr,
	}
	result := TopNRelation(
		source,
		[]query.OrderByClause{{Variable: x, Descending: false}},
		1,
	)
	require.ErrorIs(t, driveErr(result), closeErr)
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
