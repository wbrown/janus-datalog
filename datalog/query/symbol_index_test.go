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

// SymbolPositions gives each distinct wanted symbol every position carrying
// it. A symbol written twice among the wanted symbols or among the symbols
// names one variable at those positions, so a binding form naming a symbol
// twice unifies (docs/proposals/RELATION_INPUT_PROOF.md §7). A wanted symbol
// the symbols lack is an error naming it and the symbols.
func TestSymbolPositions(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	cases := []struct {
		name      string
		symbols   []Symbol
		targets   []Symbol
		distinct  []Symbol
		positions [][]int
	}{
		{"a permuted subset", []Symbol{a, b, c}, []Symbol{c, a}, []Symbol{c, a}, [][]int{{2}, {0}}},
		{"a wanted symbol written twice", []Symbol{a, b}, []Symbol{a, a}, []Symbol{a}, [][]int{{0}}},
		{"a symbol carried twice", []Symbol{a, a, b}, []Symbol{a, b}, []Symbol{a, b}, [][]int{{0, 1}, {2}}},
		{"carried twice and wanted twice", []Symbol{a, a}, []Symbol{a, a}, []Symbol{a}, [][]int{{0, 1}}},
		{"no wanted symbol", []Symbol{a, b}, nil, nil, nil},
	}
	for _, tc := range cases {
		distinct, positions, err := SymbolPositions(tc.symbols, tc.targets)
		if err != nil {
			t.Fatalf("%s: every wanted symbol is present, got error %v", tc.name, err)
		}
		if len(distinct) != len(tc.distinct) || len(positions) != len(tc.distinct) {
			t.Fatalf("%s: distinct = %v at %v, want %v at %v", tc.name, distinct, positions, tc.distinct, tc.positions)
		}
		for i := range tc.distinct {
			if distinct[i] != tc.distinct[i] {
				t.Errorf("%s: distinct[%d] = %s, want %s", tc.name, i, distinct[i], tc.distinct[i])
			}
			if len(positions[i]) != len(tc.positions[i]) {
				t.Errorf("%s: positions of %s = %v, want %v", tc.name, tc.distinct[i], positions[i], tc.positions[i])
				continue
			}
			for j := range tc.positions[i] {
				if positions[i][j] != tc.positions[i][j] {
					t.Errorf("%s: positions of %s = %v, want %v", tc.name, tc.distinct[i], positions[i], tc.positions[i])
				}
			}
		}
	}

	_, _, err := SymbolPositions([]Symbol{a, b, c}, []Symbol{c, datalog.NewSymbol("?absent"), a})
	if err == nil {
		t.Fatal("a wanted symbol the symbols lack is an error")
	}
	for _, named := range []string{"?absent", "?a", "?b", "?c"} {
		if !strings.Contains(err.Error(), named) {
			t.Errorf("the error names the absent symbol and the symbols searched; %q lacks %s", err.Error(), named)
		}
	}
}

// SymbolGroups gives a symbol list's distinct symbols in first-occurrence
// order and, for each, every position carrying it, so a symbol written at
// several positions is one variable at all of them. A nil entry, the mark
// of a position that carries no symbol, is in no group.
func TestSymbolGroups(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")
	c := datalog.NewSymbol("?c")
	cases := []struct {
		name     string
		symbols  []Symbol
		distinct []Symbol
		groups   [][]int
	}{
		{"no repeat", []Symbol{a, b, c}, []Symbol{a, b, c}, [][]int{{0}, {1}, {2}}},
		{"a symbol written twice", []Symbol{a, a, b}, []Symbol{a, b}, [][]int{{0, 1}, {2}}},
		{"a repeat across a nil entry", []Symbol{a, nil, b, a}, []Symbol{a, b}, [][]int{{0, 3}, {2}}},
		{"no symbol", nil, nil, nil},
	}
	for _, tc := range cases {
		distinct, groups := SymbolGroups(tc.symbols)
		if len(distinct) != len(tc.distinct) || len(groups) != len(tc.distinct) {
			t.Fatalf("%s: distinct = %v at %v, want %v at %v", tc.name, distinct, groups, tc.distinct, tc.groups)
		}
		for i := range tc.distinct {
			if distinct[i] != tc.distinct[i] {
				t.Errorf("%s: distinct[%d] = %s, want %s", tc.name, i, distinct[i], tc.distinct[i])
			}
			if len(groups[i]) != len(tc.groups[i]) {
				t.Errorf("%s: positions of %s = %v, want %v", tc.name, tc.distinct[i], groups[i], tc.groups[i])
				continue
			}
			for j := range tc.groups[i] {
				if groups[i][j] != tc.groups[i][j] {
					t.Errorf("%s: positions of %s = %v, want %v", tc.name, tc.distinct[i], groups[i], tc.groups[i])
				}
			}
		}
	}
}

// AgreesAt holds of a tuple when its values at every position of each group
// are equal, the condition under which the tuple binds a symbol written at
// those positions; a group of one position and no group at all hold of
// every tuple.
func TestAgreesAt(t *testing.T) {
	cases := []struct {
		name   string
		tuple  Tuple
		groups [][]int
		want   bool
	}{
		{"agreeing", Tuple{int64(1), int64(1), int64(2)}, [][]int{{0, 1}, {2}}, true},
		{"disagreeing", Tuple{int64(1), int64(2), int64(2)}, [][]int{{0, 1}, {2}}, false},
		{"groups of one", Tuple{int64(1), int64(2)}, [][]int{{0}, {1}}, true},
		{"no group", Tuple{int64(1)}, nil, true},
	}
	for _, tc := range cases {
		if got := AgreesAt(tc.tuple, tc.groups); got != tc.want {
			t.Errorf("%s: AgreesAt(%v, %v) = %t, want %t", tc.name, tc.tuple, tc.groups, got, tc.want)
		}
	}
}
