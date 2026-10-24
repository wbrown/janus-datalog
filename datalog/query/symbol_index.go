package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// SymbolIndex returns the position of target within symbols, or -1 when
// absent. Symbols are interned pointers, so each comparison is pointer
// equality.
func SymbolIndex(symbols []Symbol, target Symbol) int {
	for i, sym := range symbols {
		if sym == target {
			return i
		}
	}
	return -1
}

// ContainsSymbol reports whether target occurs in symbols.
func ContainsSymbol(symbols []Symbol, target Symbol) bool {
	return SymbolIndex(symbols, target) >= 0
}

// SymbolIndexTable returns, for each target in order, its position within
// symbols. A target the symbols lack is an error naming it and the symbols:
// an attribute is addressed by its symbol, and no caller reads past one the
// relation does not carry. Hoist this out of per-tuple loops: one pass over
// the symbol list per target at setup replaces a rescan per tuple.
func SymbolIndexTable(symbols, targets []Symbol) ([]int, error) {
	table := make([]int, len(targets))
	for i, target := range targets {
		pos := SymbolIndex(symbols, target)
		if pos < 0 {
			return nil, fmt.Errorf("symbol %s not found among the symbols %v", target, symbols)
		}
		table[i] = pos
	}
	return table, nil
}

// SymbolGroups returns the distinct symbols of symbols in first-occurrence
// order and, for each, every position carrying it. A symbol written at
// several positions names one variable at all of them: a tuple binds it
// when AgreesAt holds of its group, and carries the first position's value.
// A nil entry carries no symbol and is in no group.
func SymbolGroups(symbols []Symbol) ([]Symbol, [][]int) {
	var distinct []Symbol
	var groups [][]int
	for pos, sym := range symbols {
		if sym == nil {
			continue
		}
		if at := SymbolIndex(distinct, sym); at >= 0 {
			groups[at] = append(groups[at], pos)
			continue
		}
		distinct = append(distinct, sym)
		groups = append(groups, []int{pos})
	}
	return distinct, groups
}

// SymbolPositions returns the distinct targets in first-occurrence order
// and, for each, every position within symbols carrying it, the group
// SymbolGroups gives the target. A target the symbols lack is an error
// naming it and the symbols. A target written twice names one variable.
func SymbolPositions(symbols, targets []Symbol) ([]Symbol, [][]int, error) {
	carried, groups := SymbolGroups(symbols)
	var distinct []Symbol
	var positions [][]int
	for _, target := range targets {
		if ContainsSymbol(distinct, target) {
			continue
		}
		at := SymbolIndex(carried, target)
		if at < 0 {
			return nil, nil, fmt.Errorf("symbol %s not found among the symbols %v", target, symbols)
		}
		distinct = append(distinct, target)
		positions = append(positions, groups[at])
	}
	return distinct, positions, nil
}

// AgreesAt reports whether the tuple's values at every position of each
// group are equal, the condition under which a tuple binds a symbol written
// at those positions. A group of one position holds of every tuple.
func AgreesAt(tuple Tuple, groups [][]int) bool {
	for _, group := range groups {
		first := tuple[group[0]]
		for _, pos := range group[1:] {
			if !datalog.ValuesEqual(first, tuple[pos]) {
				return false
			}
		}
	}
	return true
}
