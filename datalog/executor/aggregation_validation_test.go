package executor

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
