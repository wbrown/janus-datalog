package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// RelationProperties are guarantees made by a Relation about its produced
// tuples. Query requirements remain in Datalog; these properties report which
// requirements the physical relation already satisfies.
//
// Ordering is the guaranteed tuple order. Keys are candidate symbol sets whose
// values uniquely identify a tuple. Callers must treat returned properties as
// immutable, matching the Relation contract itself. Constructors copy external
// inputs; internal propagation creates new property values.
type RelationProperties struct {
	Ordering []query.OrderByClause
	Keys     [][]query.Symbol
}

func (p RelationProperties) clone() RelationProperties {
	result := RelationProperties{}
	if len(p.Ordering) > 0 {
		result.Ordering = append([]query.OrderByClause(nil), p.Ordering...)
	}
	if len(p.Keys) > 0 {
		result.Keys = make([][]query.Symbol, len(p.Keys))
		for i, key := range p.Keys {
			result.Keys[i] = append([]query.Symbol(nil), key...)
		}
	}
	return result
}

func (p RelationProperties) project(symbols []query.Symbol) RelationProperties {
	retained := make(map[query.Symbol]bool, len(symbols))
	for _, symbol := range symbols {
		retained[symbol] = true
	}

	result := RelationProperties{}
	for _, clause := range p.Ordering {
		if !retained[clause.Variable] {
			break
		}
		result.Ordering = append(result.Ordering, clause)
	}
	for _, key := range p.Keys {
		keep := true
		for _, symbol := range key {
			if !retained[symbol] {
				keep = false
				break
			}
		}
		if keep {
			result.Keys = append(result.Keys, append([]query.Symbol(nil), key...))
		}
	}
	return result
}

func (p RelationProperties) addSymbol(symbol query.Symbol) RelationProperties {
	result := p.clone()
	for _, clause := range result.Ordering {
		if clause.Variable == symbol {
			result.Ordering = nil
			break
		}
	}
	var keys [][]query.Symbol
	for _, key := range result.Keys {
		overwritten := false
		for _, keySymbol := range key {
			if keySymbol == symbol {
				overwritten = true
				break
			}
		}
		if !overwritten {
			keys = append(keys, key)
		}
	}
	result.Keys = keys
	return result
}

func (p RelationProperties) withoutReboundSymbols(symbols []query.Symbol) RelationProperties {
	rebound := make(map[query.Symbol]bool, len(symbols))
	for _, symbol := range symbols {
		rebound[symbol] = true
	}

	result := RelationProperties{}
	for _, clause := range p.Ordering {
		if rebound[clause.Variable] {
			break
		}
		result.Ordering = append(result.Ordering, clause)
	}
	for _, key := range p.Keys {
		valid := true
		for _, symbol := range key {
			if rebound[symbol] {
				valid = false
				break
			}
		}
		if valid {
			result.Keys = append(result.Keys, append([]query.Symbol(nil), key...))
		}
	}
	return result
}

func (p RelationProperties) satisfiesOrdering(required []query.OrderByClause) bool {
	if len(required) > len(p.Ordering) {
		return false
	}
	for i, clause := range required {
		if p.Ordering[i] != clause {
			return false
		}
	}
	return true
}

func deduplicatedProperties(symbols []query.Symbol) RelationProperties {
	if len(symbols) == 0 {
		return RelationProperties{}
	}
	return RelationProperties{
		Keys: [][]query.Symbol{append([]query.Symbol(nil), symbols...)},
	}
}

func orProperties(
	outer RelationProperties,
	producedSymbols []query.Symbol,
	overwrittenSymbols []query.Symbol,
	outputSymbols []query.Symbol,
	shortCircuit bool,
	branchesEmitAtMostOne bool,
) (RelationProperties, bool) {
	preserved := outer.withoutReboundSymbols(overwrittenSymbols)
	unaffectedOuterKeys := preserved.Keys
	preserved.Keys = nil

	result := preserved
	result.Keys = appendDistinctKeys(result.Keys, deduplicatedProperties(outputSymbols).Keys)

	if shortCircuit && branchesEmitAtMostOne {
		result.Keys = appendDistinctKeys(result.Keys, unaffectedOuterKeys)
		return result, len(unaffectedOuterKeys) == 0
	}

	outputSet := make(map[query.Symbol]bool, len(outputSymbols))
	for _, symbol := range outputSymbols {
		outputSet[symbol] = true
	}
	for _, outerKey := range unaffectedOuterKeys {
		composite := append([]query.Symbol(nil), outerKey...)
		for _, symbol := range producedSymbols {
			if outputSet[symbol] && !symbolInSlice(composite, symbol) {
				composite = append(composite, symbol)
			}
		}
		result.Keys = appendDistinctKeys(result.Keys, [][]query.Symbol{composite})
	}

	return result, true
}

func symbolInSlice(symbols []query.Symbol, candidate query.Symbol) bool {
	for _, symbol := range symbols {
		if symbol == candidate {
			return true
		}
	}
	return false
}

func joinProperties(
	left RelationProperties,
	right RelationProperties,
	joinSymbols []query.Symbol,
) RelationProperties {
	joinSet := make(map[query.Symbol]bool, len(joinSymbols))
	for _, symbol := range joinSymbols {
		joinSet[symbol] = true
	}

	result := RelationProperties{}
	if hasKeyWithin(right.Keys, joinSet) {
		result.Keys = appendDistinctKeys(result.Keys, left.Keys)
	}
	if hasKeyWithin(left.Keys, joinSet) {
		result.Keys = appendDistinctKeys(result.Keys, right.Keys)
	}
	return result
}

func hasKeyWithin(keys [][]query.Symbol, symbols map[query.Symbol]bool) bool {
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		contained := true
		for _, symbol := range key {
			if !symbols[symbol] {
				contained = false
				break
			}
		}
		if contained {
			return true
		}
	}
	return false
}

func appendDistinctKeys(existing, additions [][]query.Symbol) [][]query.Symbol {
	for _, addition := range additions {
		if len(addition) == 0 || containsSymbolSet(existing, addition) {
			continue
		}
		existing = append(existing, append([]query.Symbol(nil), addition...))
	}
	return existing
}

func containsSymbolSet(keys [][]query.Symbol, candidate []query.Symbol) bool {
	for _, key := range keys {
		if len(key) != len(candidate) {
			continue
		}
		matches := true
		for _, symbol := range candidate {
			found := false
			for _, keySymbol := range key {
				if keySymbol == symbol {
					found = true
					break
				}
			}
			if !found {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}
