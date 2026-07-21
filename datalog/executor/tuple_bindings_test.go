package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBindTuplePopulates(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	constant := datalog.NewSymbol("?c")

	// A prelude value survives; a prelude value shadowed by a tuple symbol is
	// overwritten — the population order every call site relies on.
	bindings := map[query.Symbol]interface{}{
		constant: "prelude",
		x:        "stale",
	}
	bindTuple(bindings, []query.Symbol{x, y}, Tuple{int64(1), int64(2)})

	if got := bindings[x]; got != int64(1) {
		t.Errorf("bindings[?x] = %v, want 1", got)
	}
	if got := bindings[y]; got != int64(2) {
		t.Errorf("bindings[?y] = %v, want 2", got)
	}
	if got := bindings[constant]; got != "prelude" {
		t.Errorf("bindings[?c] = %v, want the prelude value", got)
	}
}

// A tuple narrower than its symbol list is a broken Relation invariant —
// every tuple carries exactly its relation's symbols. Binding must fail
// loudly, never silently bind a partial row.
func TestBindTupleShortTuplePanics(t *testing.T) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")

	defer func() {
		if recover() == nil {
			t.Error("bindTuple must panic on a tuple narrower than its symbols")
		}
	}()
	bindTuple(map[query.Symbol]interface{}{}, []query.Symbol{x, y}, Tuple{int64(1)})
}
