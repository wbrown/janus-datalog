package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A join event carries each side's symbols and the result's, the slices the
// relations hold, so a reader renders the join from them.
func TestReportJoin_TheEventCarriesEachSidesAndTheResultsSymbols(t *testing.T) {
	var event *annotations.Event
	handler := func(e annotations.Event) {
		if e.Name == annotations.JoinHash {
			event = &e
		}
	}
	joinSymbol := datalog.NewSymbol("?id")
	left := NewMaterializedRelation(
		[]query.Symbol{joinSymbol, datalog.NewSymbol("?left")},
		[]Tuple{{int64(1), "a"}, {int64(2), "b"}},
	)
	right := NewMaterializedRelation(
		[]query.Symbol{joinSymbol, datalog.NewSymbol("?right")},
		[]Tuple{{int64(1), "x"}, {int64(3), "z"}},
	)
	result := reportJoin(handler, left, right, func() Relation {
		return HashJoinWithOptions(left, right, []query.Symbol{joinSymbol}, ExecutorOptions{})
	})
	require.Len(t, collectTuples(result), 1)

	require.NotNil(t, event, "the join is reported")
	require.Equal(t, left.Symbols(), event.Data["left.symbols"], "the left side's symbols")
	require.Equal(t, right.Symbols(), event.Data["right.symbols"], "the right side's symbols")
	require.Equal(t, result.Symbols(), event.Data["result.symbols"], "the result's symbols")
}
