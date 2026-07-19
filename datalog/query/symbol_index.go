package query

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
// symbols (-1 when absent). Hoist this out of per-tuple loops: one pass over
// the symbol list per target at setup replaces a rescan per tuple.
func SymbolIndexTable(symbols, targets []Symbol) []int {
	table := make([]int, len(targets))
	for i, target := range targets {
		table[i] = SymbolIndex(symbols, target)
	}
	return table
}
