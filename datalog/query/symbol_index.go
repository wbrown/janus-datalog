package query

import "fmt"

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
