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

func (p RelationProperties) renameSymbols(from, to []query.Symbol) RelationProperties {
	if len(from) != len(to) {
		return RelationProperties{}
	}
	rename := func(symbol query.Symbol) (query.Symbol, bool) {
		if i := query.SymbolIndex(from, symbol); i >= 0 {
			return to[i], true
		}
		return nil, false
	}

	result := RelationProperties{}
	for _, clause := range p.Ordering {
		symbol, ok := rename(clause.Variable)
		if !ok {
			break
		}
		result.Ordering = append(result.Ordering, query.OrderByClause{
			Variable:   symbol,
			Descending: clause.Descending,
		})
	}
	for _, key := range p.Keys {
		renamed := make([]query.Symbol, len(key))
		valid := true
		for i, symbol := range key {
			replacement, ok := rename(symbol)
			if !ok {
				valid = false
				break
			}
			renamed[i] = replacement
		}
		if valid {
			result.Keys = append(result.Keys, renamed)
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

// projectionPreservesSet reports whether projecting a set relation with the
// given source symbols onto targets is structurally guaranteed to remain a
// set, making a dedup pass unnecessary. Callers have already validated that
// every target symbol is present in the source. Two cases prove it:
//
//  1. The projected properties retain a candidate key of the source: distinct
//     source tuples keep distinct key values.
//  2. The projection is a permutation: targets are pairwise distinct and equal
//     in count to the source symbols, so (with presence validated and source
//     symbols distinct by the Relation invariant) the targets cover every
//     source position — injective on tuples.
//
// Every other projection can map distinct source tuples to the same output
// tuple and must restore set semantics with a dedup pass. Arity equality alone
// is not enough: a repeated target ([?x ?x] over [?x ?y]) passes presence
// validation at equal arity yet reads one source position twice, which is not
// injective — hence the pairwise-distinct clause.
func projectionPreservesSet(source, targets []query.Symbol, projected RelationProperties) bool {
	if len(projected.Keys) > 0 {
		return true
	}
	if len(targets) != len(source) {
		return false
	}
	for i, symbol := range targets {
		if query.ContainsSymbol(targets[:i], symbol) {
			return false
		}
	}
	return true
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
		if !query.ContainsSymbol(key, symbol) {
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
			if outputSet[symbol] && !query.ContainsSymbol(composite, symbol) {
				composite = append(composite, symbol)
			}
		}
		result.Keys = appendDistinctKeys(result.Keys, [][]query.Symbol{composite})
	}

	return result, true
}

func expansionProperties(
	outer RelationProperties,
	bindingSymbols []query.Symbol,
	outputSymbols []query.Symbol,
) RelationProperties {
	result, _ := orProperties(
		outer,
		bindingSymbols,
		bindingSymbols,
		outputSymbols,
		false,
		false,
	)
	return result
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
			if !query.ContainsSymbol(key, symbol) {
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
