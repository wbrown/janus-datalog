package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A find clause consumes, from the relation groups, a variable, an
// aggregate's argument and a pull's entity variable, in the find's order.
func TestConsumedFindSymbols(t *testing.T) {
	e, n, c := datalog.NewSymbol("?e"), datalog.NewSymbol("?n"), datalog.NewSymbol("?c")
	find := []query.FindElement{
		query.FindVariable{Symbol: e},
		query.FindAggregate{Function: datalog.SymCount, Arg: n},
		query.FindPull{Variable: c},
	}
	got := consumedFindSymbols(find)
	want := []query.Symbol{e, n, c}
	if len(got) != len(want) {
		t.Fatalf("consumedFindSymbols = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("consumedFindSymbols[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
