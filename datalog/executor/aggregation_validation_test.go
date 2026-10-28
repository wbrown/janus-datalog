package executor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A find over a symbol the relation lacks is an error at the aggregation
// boundary, naming the symbol, whether the symbol stands alone, beside a
// symbol the relation carries, or beside an aggregate, through
// ExecuteAggregations and the relation's method alike. A find over symbols
// the relation carries projects onto them.
func TestAggregate_AFindSymbolTheRelationLacksIsAnError(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	absent := datalog.NewSymbol("?absent")
	rel := NewMaterializedRelation(
		[]query.Symbol{group, value},
		[]Tuple{{"a", int64(1)}, {"b", int64(3)}},
	)
	entries := []struct {
		name      string
		aggregate func(find []query.FindElement) Relation
	}{
		{"ExecuteAggregations", func(find []query.FindElement) Relation { return ExecuteAggregations(rel, find) }},
		{"Relation.Aggregate", func(find []query.FindElement) Relation { return rel.Aggregate(find) }},
	}
	for _, e := range entries {
		t.Run(e.name, func(t *testing.T) {
			tuples, err := CollectTuples(e.aggregate([]query.FindElement{query.FindVariable{Symbol: group}}), nil)
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{"a"}, {"b"}}, tuples)
			for _, find := range [][]query.FindElement{
				{query.FindVariable{Symbol: absent}},
				{query.FindVariable{Symbol: group}, query.FindVariable{Symbol: absent}},
				{query.FindVariable{Symbol: absent}, query.FindAggregate{Function: datalog.SymSum, Arg: value}},
			} {
				_, err := CollectTuples(e.aggregate(find), nil)
				require.ErrorContains(t, err, absent.String(), "a find over a symbol the relation lacks is an error naming the symbol")
			}
		})
	}
}

// Aggregate argument and predicate symbols must be present in the source
// relation, exactly as group-by symbols already must be. Before this
// validation the three aggregation paths diverged silently on an absent
// argument: single aggregation collected nothing, batch grouped aggregation
// read tuple position 0, and streaming aggregation skipped via -1.
func TestAggregationRejectsAbsentSymbols(t *testing.T) {
	group := datalog.NewSymbol("?group")
	value := datalog.NewSymbol("?value")
	absent := datalog.NewSymbol("?absent")
	rel := func() Relation {
		return NewMaterializedRelation(
			[]query.Symbol{group, value},
			[]Tuple{
				{"a", int64(1)},
				{"a", int64(2)},
				{"b", int64(3)},
			},
		)
	}

	assertError := func(t *testing.T, result Relation, wantSubstring string) {
		t.Helper()
		_, err := CollectTuples(result, nil)
		if err == nil {
			t.Fatal("expected an error, got none")
		}
		if !strings.Contains(err.Error(), wantSubstring) {
			t.Errorf("error should contain %q; got: %v", wantSubstring, err)
		}
	}

	t.Run("single aggregation, absent argument", func(t *testing.T) {
		result := ExecuteAggregations(rel(), []query.FindElement{
			query.FindAggregate{Function: datalog.SymSum, Arg: absent},
		})
		assertError(t, result, "?absent")
	})

	t.Run("grouped aggregation, absent argument", func(t *testing.T) {
		result := ExecuteAggregations(rel(), []query.FindElement{
			query.FindVariable{Symbol: group},
			query.FindAggregate{Function: datalog.SymSum, Arg: absent},
		})
		assertError(t, result, "?absent")
	})

	t.Run("conditional aggregate, absent predicate", func(t *testing.T) {
		result := ExecuteAggregations(rel(), []query.FindElement{
			query.FindVariable{Symbol: group},
			query.FindAggregate{Function: datalog.SymSum, Arg: value, Predicate: absent},
		})
		assertError(t, result, "?absent")
	})
}
