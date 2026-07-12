package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestReadyPredicateScheduledBeforeUnrelatedScan(t *testing.T) {
	q, err := parser.ParseQuery(
		`[:find ?e ?payload
		  :where
		  [?e :item/score ?score]
		  [?e :item/payload ?payload]
		  [(> ?score 9900)]]`,
	)
	require.NoError(t, err)

	phases, err := createPhasesGreedy(
		q.Where,
		[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?payload")},
		map[query.Symbol]bool{},
	)
	require.NoError(t, err)
	require.Len(t, phases, 1)
	require.Len(t, phases[0].Clauses, 3)

	firstPattern, ok := phases[0].Clauses[0].(*query.DataPattern)
	require.True(t, ok)
	require.Equal(t, datalog.NewKeyword(":item/score"), firstPattern.GetA().(query.Constant).Value)
	require.IsType(t, &query.Comparison{}, phases[0].Clauses[1],
		"the predicate is ready after ?score is bound and must filter before another scan")
	secondPattern, ok := phases[0].Clauses[2].(*query.DataPattern)
	require.True(t, ok)
	require.Equal(t, datalog.NewKeyword(":item/payload"), secondPattern.GetA().(query.Constant).Value)
}

func TestPredicateSpanningTwoScansWaitsForBothInputs(t *testing.T) {
	q, err := parser.ParseQuery(
		`[:find ?e
		  :where
		  [?e :item/min ?min]
		  [?e :item/max ?max]
		  [(< ?min ?max)]]`,
	)
	require.NoError(t, err)

	phases, err := createPhasesGreedy(
		q.Where,
		[]query.Symbol{datalog.NewSymbol("?e")},
		map[query.Symbol]bool{},
	)
	require.NoError(t, err)
	require.Len(t, phases, 1)
	require.Len(t, phases[0].Clauses, 3)
	require.IsType(t, &query.DataPattern{}, phases[0].Clauses[0])
	require.IsType(t, &query.DataPattern{}, phases[0].Clauses[1])
	require.IsType(t, &query.Comparison{}, phases[0].Clauses[2],
		"predicate must remain after the scans that provide both required symbols")
}
