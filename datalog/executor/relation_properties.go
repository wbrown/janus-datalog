package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// RelationProperties are guarantees made by a Relation about its produced
// tuples. Query requirements remain in Datalog; these properties report which
// requirements the physical relation already satisfies.
//
// Ordering is the guaranteed tuple order. Keys are candidate symbol sets whose
// values uniquely identify a tuple. Identities are the symbols whose every
// value is an identity, as a data pattern's entity position binds them.
// Evaluates reports that reading the relation evaluates a function on its
// tuples as they are pulled that can fail on a value (canFailOnAValue): an
// expression's function or a database-function predicate's over a variable,
// a predicate function or a transaction range in a filter, or a form's body
// run per window that holds one. A failure of that evaluation surfaces only
// when a reader reaches the tuple it fails on, so a reader that stops before
// the relation's end reads a relation that evaluates to its end
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T11). Callers must treat
// returned properties as immutable, matching the Relation contract itself.
// Constructors copy external inputs; internal propagation creates new
// property values.
type RelationProperties struct {
	Ordering   []query.OrderByClause
	Keys       [][]query.Symbol
	Identities []query.Symbol
	Evaluates  bool
}

// AnswerIdentities is the identity symbols of a data pattern's answer: its
// entity variable, since every datom's entity is an identity and a binding at
// the entity position is one (the storage matcher's
// typedPositionBindingCheck). A pattern whose entity is a constant names none.
func AnswerIdentities(pattern *query.DataPattern) []query.Symbol {
	if entity, ok := pattern.GetE().(query.Variable); ok {
		return []query.Symbol{entity.Name}
	}
	return nil
}

func (p RelationProperties) clone() RelationProperties {
	result := RelationProperties{Evaluates: p.Evaluates}
	if len(p.Ordering) > 0 {
		result.Ordering = append([]query.OrderByClause(nil), p.Ordering...)
	}
	if len(p.Keys) > 0 {
		result.Keys = make([][]query.Symbol, len(p.Keys))
		for i, key := range p.Keys {
			result.Keys[i] = append([]query.Symbol(nil), key...)
		}
	}
	if len(p.Identities) > 0 {
		result.Identities = append([]query.Symbol(nil), p.Identities...)
	}
	return result
}

// renameSymbols is the properties under a substitution: each ordering and
// key symbol the substitution names becomes the symbol standing for it, and
// every other keeps its name, as it does in the renamed relation.
func (p RelationProperties) renameSymbols(subst query.Substitution) RelationProperties {
	rename := func(symbol query.Symbol) query.Symbol {
		if to, ok := subst.Of(symbol); ok {
			return to
		}
		return symbol
	}

	result := RelationProperties{Evaluates: p.Evaluates}
	for _, clause := range p.Ordering {
		result.Ordering = append(result.Ordering, query.OrderByClause{
			Variable:   rename(clause.Variable),
			Descending: clause.Descending,
		})
	}
	for _, key := range p.Keys {
		renamed := make([]query.Symbol, len(key))
		for i, symbol := range key {
			renamed[i] = rename(symbol)
		}
		result.Keys = append(result.Keys, renamed)
	}
	for _, symbol := range p.Identities {
		result.Identities = append(result.Identities, rename(symbol))
	}
	return result
}

func (p RelationProperties) project(symbols []query.Symbol) RelationProperties {
	retained := make(map[query.Symbol]bool, len(symbols))
	for _, symbol := range symbols {
		retained[symbol] = true
	}

	result := RelationProperties{Evaluates: p.Evaluates}
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
	for _, symbol := range p.Identities {
		if retained[symbol] {
			result.Identities = append(result.Identities, symbol)
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
	var identities []query.Symbol
	for _, identity := range result.Identities {
		if identity != symbol {
			identities = append(identities, identity)
		}
	}
	result.Identities = identities
	return result
}

func (p RelationProperties) withoutReboundSymbols(symbols []query.Symbol) RelationProperties {
	rebound := make(map[query.Symbol]bool, len(symbols))
	for _, symbol := range symbols {
		rebound[symbol] = true
	}

	result := RelationProperties{Evaluates: p.Evaluates}
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
	for _, symbol := range p.Identities {
		if !rebound[symbol] {
			result.Identities = append(result.Identities, symbol)
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

// orProperties derives a form's result properties from the subject's and
// the header: the subject's ordering survives every form; its keys survive
// a form that answers at most one tuple per subject tuple, answersAtMostOne,
// which the form's policy decides, a fallback whose every branch answers at
// most one, a one-body form whose body does, and a union never, since two
// branches can answer one subject tuple; and otherwise the result's key is
// the composite of each subject key and the produced outputs. The result
// evaluates as the subject does.
func orProperties(
	outer RelationProperties,
	producedSymbols []query.Symbol,
	overwrittenSymbols []query.Symbol,
	outputSymbols []query.Symbol,
	answersAtMostOne bool,
) RelationProperties {
	preserved := outer.withoutReboundSymbols(overwrittenSymbols)
	unaffectedOuterKeys := preserved.Keys
	preserved.Keys = nil

	result := preserved
	result.Keys = appendDistinctKeys(result.Keys, deduplicatedProperties(outputSymbols).Keys)

	if answersAtMostOne {
		result.Keys = appendDistinctKeys(result.Keys, unaffectedOuterKeys)
		return result
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

	return result
}

func expansionProperties(
	outer RelationProperties,
	bindingSymbols []query.Symbol,
	outputSymbols []query.Symbol,
) RelationProperties {
	return orProperties(
		outer,
		bindingSymbols,
		bindingSymbols,
		outputSymbols,
		false,
	)
}

// joinProperties is a join result's keys, derived from its sides'. The left
// keys stand when a right key lies within the join symbols, since each left
// tuple then meets at most one right tuple, and the right keys stand when a
// left key does. Each left key with each right key composes a key, since
// result tuples that agree on both are one left tuple and one right tuple
// (docs/proposals/JOIN_OUTPUT_IS_A_SET_PROOF.md, T3). Reading the result
// reads both sides, so it evaluates when either side does. A join keeps each
// side's values, and a shared symbol's are equal on both sides, so a symbol
// holding identities on either side holds them in the result.
func joinProperties(
	left RelationProperties,
	right RelationProperties,
	joinSymbols []query.Symbol,
) RelationProperties {
	joinSet := make(map[query.Symbol]bool, len(joinSymbols))
	for _, symbol := range joinSymbols {
		joinSet[symbol] = true
	}

	result := RelationProperties{Evaluates: left.Evaluates || right.Evaluates}
	result.Identities = append(result.Identities, left.Identities...)
	for _, symbol := range right.Identities {
		if !query.ContainsSymbol(result.Identities, symbol) {
			result.Identities = append(result.Identities, symbol)
		}
	}
	if hasKeyWithin(right.Keys, joinSet) {
		result.Keys = appendDistinctKeys(result.Keys, left.Keys)
	}
	if hasKeyWithin(left.Keys, joinSet) {
		result.Keys = appendDistinctKeys(result.Keys, right.Keys)
	}
	for _, leftKey := range left.Keys {
		for _, rightKey := range right.Keys {
			composite := append([]query.Symbol(nil), leftKey...)
			for _, symbol := range rightKey {
				if !query.ContainsSymbol(composite, symbol) {
					composite = append(composite, symbol)
				}
			}
			result.Keys = appendDistinctKeys(result.Keys, [][]query.Symbol{composite})
		}
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
