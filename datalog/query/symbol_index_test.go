package query

import (
	"strings"
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

// SymbolIndexTable gives each wanted symbol's position in the symbols, and
// a wanted symbol the symbols lack is an error naming it and the symbols,
// never a position the caller may read past: an attribute is addressed by
// its symbol, and a relation lacking one it must carry is an error, never
// a misread (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7).
func TestSymbolIndexTable(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	absent := datalog.NewSymbol("?absent")
	symbols := []Symbol{a, b, c}

	table, err := SymbolIndexTable(symbols, []Symbol{c, a})
	if err != nil {
		t.Fatalf("every wanted symbol is present, got error %v", err)
	}
	want := []int{2, 0}
	if len(table) != len(want) {
		t.Fatalf("table length = %d, want %d", len(table), len(want))
	}
	for i := range want {
		if table[i] != want[i] {
			t.Errorf("table[%d] = %d, want %d", i, table[i], want[i])
		}
	}

	_, err = SymbolIndexTable(symbols, []Symbol{c, absent, a})
	if err == nil {
		t.Fatal("a wanted symbol the symbols lack is an error")
	}
	for _, named := range []string{"?absent", "?a", "?b", "?c"} {
		if !strings.Contains(err.Error(), named) {
			t.Errorf("the error names the absent symbol and the symbols searched; %q lacks %s", err.Error(), named)
		}
	}

	got, err := SymbolIndexTable(symbols, nil)
	if err != nil {
		t.Fatalf("no wanted symbol is no error, got %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty targets must produce an empty table, got %v", got)
	}
}
