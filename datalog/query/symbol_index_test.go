package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestSymbolIndex(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	absent := datalog.NewSymbol("?absent")
	symbols := []Symbol{a, b, c}

	if got := SymbolIndex(symbols, a); got != 0 {
		t.Errorf("SymbolIndex(?a) = %d, want 0", got)
	}
	if got := SymbolIndex(symbols, c); got != 2 {
		t.Errorf("SymbolIndex(?c) = %d, want 2", got)
	}
	if got := SymbolIndex(symbols, absent); got != -1 {
		t.Errorf("SymbolIndex(?absent) = %d, want -1", got)
	}
	if got := SymbolIndex(nil, a); got != -1 {
		t.Errorf("SymbolIndex on nil slice = %d, want -1", got)
	}
}

func TestContainsSymbol(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	symbols := []Symbol{a}

	if !ContainsSymbol(symbols, a) {
		t.Error("ContainsSymbol must find a present symbol")
	}
	if ContainsSymbol(symbols, b) {
		t.Error("ContainsSymbol must not find an absent symbol")
	}
	if ContainsSymbol(nil, a) {
		t.Error("ContainsSymbol on nil slice must be false")
	}
}

func TestSymbolIndexTable(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	absent := datalog.NewSymbol("?absent")
	symbols := []Symbol{a, b, c}

	table := SymbolIndexTable(symbols, []Symbol{c, absent, a})
	want := []int{2, -1, 0}
	if len(table) != len(want) {
		t.Fatalf("table length = %d, want %d", len(table), len(want))
	}
	for i := range want {
		if table[i] != want[i] {
			t.Errorf("table[%d] = %d, want %d", i, table[i], want[i])
		}
	}

	if got := SymbolIndexTable(symbols, nil); len(got) != 0 {
		t.Errorf("empty targets must produce an empty table, got %v", got)
	}
}
