package query

import "fmt"

// ClauseScope is a clause's interface to its enclosing scope. Provides are
// the variables the clause can bind there; Correlates are its free variables
// that unify with the enclosing scope when bound there. Variables scoped
// inside a clause — explicit-join bodies, subquery inner queries, or/
// or-default branch locals — appear in neither. This is the canonical
// scoping definition; the planner's scheduling, the executor's anti-join
// keys, and the algebra bridge's inference all derive from it.
//
// Whether a Correlates variable actually unifies is a property of the
// enclosing query: those the query can bind must be bound before the clause
// runs; the rest are existential (Datomic's unification rule). Resolving
// that is the scheduler's job — ScopeOf reports the interface only.
type ClauseScope struct {
	Provides   []Symbol
	Correlates []Symbol
}

// ScopeOf returns the scope interface of a clause. The clause taxonomy is
// closed; a form without a case here panics rather than silently exposing
// nothing.
func ScopeOf(clause Clause) ClauseScope {
	switch c := clause.(type) {
	case *DataPattern:
		return ClauseScope{Provides: c.Symbols()}

	case *Expression:
		var provides []Symbol
		switch b := c.Binding.(type) {
		case Symbol:
			if b != nil {
				provides = []Symbol{b}
			}
		case TupleBinding:
			provides = b.Variables
		}
		return ClauseScope{Provides: provides, Correlates: c.Function.RequiredSymbols()}

	case *SubqueryPattern:
		var correlates []Symbol
		for _, input := range c.Inputs {
			if v, ok := input.(Variable); ok && !v.Name.IsSource() {
				correlates = append(correlates, v.Name)
			}
		}
		return ClauseScope{Provides: c.Binding.BoundVariables(), Correlates: correlates}

	case *NotClause:
		// A plain NOT unifies on every free variable of its body that the
		// enclosing query binds; the rest are existential.
		return ClauseScope{Correlates: FreeVariables(c.Clauses)}

	case *NotJoinClause:
		// The header is the complete interface by language contract.
		return ClauseScope{Correlates: c.JoinVars}

	case *OrClause:
		// Union semantics: only variables every branch binds are provided.
		provides, externals := branchInterfaces(c.Branches, intersectSymbolSets)
		return ClauseScope{Provides: provides, Correlates: externals}

	case *OrJoinClause:
		return headerScope(c.JoinVars, c.Branches, intersectSymbolSets)

	case *OrDefaultClause:
		// Fallback semantics: any single branch may execute, so any branch's
		// bindings may appear. Correlation additionally rides the entity
		// variable of each branch's first pattern (per-tuple evaluation).
		provides, externals := branchInterfaces(c.Branches, unionSymbolSets)
		correlates := externals
		for _, branch := range c.Branches {
			for _, clause := range branch {
				if pattern, ok := clause.(*DataPattern); ok {
					if len(pattern.Elements) > 0 {
						if v, ok := pattern.Elements[0].(Variable); ok && !ContainsSymbol(correlates, v.Name) {
							correlates = append(correlates, v.Name)
						}
					}
					break
				}
			}
		}
		return ClauseScope{Provides: provides, Correlates: correlates}

	case *OrDefaultJoinClause:
		return headerScope(c.JoinVars, c.Branches, unionSymbolSets)

	case Predicate:
		// Every predicate form: consumes, never binds.
		return ClauseScope{Correlates: c.RequiredSymbols()}

	default:
		panic(fmt.Sprintf("BUG: unknown clause type %T in ScopeOf", clause))
	}
}

// FreeVariables returns the variables a clause list exposes to its enclosing
// scope — every Provides and Correlates across the clauses, deduplicated in
// first-appearance order. Variables scoped inside the clauses do not appear.
func FreeVariables(clauses []Clause) []Symbol {
	var free []Symbol
	for _, clause := range clauses {
		scope := ScopeOf(clause)
		for _, sym := range scope.Provides {
			if !ContainsSymbol(free, sym) {
				free = append(free, sym)
			}
		}
		for _, sym := range scope.Correlates {
			if !ContainsSymbol(free, sym) {
				free = append(free, sym)
			}
		}
	}
	return free
}

// branchInterfaces computes a branch set's combined interface: provides
// combined across branches by combine (intersection for union semantics,
// union for fallback semantics), and externals — every branch's Correlates
// not self-provided within that branch, plus source symbols filtered out.
func branchInterfaces(
	branches [][]Clause,
	combine func([][]Symbol) []Symbol,
) (provides, externals []Symbol) {
	if len(branches) == 0 {
		return nil, nil
	}

	branchProvides := make([][]Symbol, len(branches))
	for i, branch := range branches {
		var provided, correlated []Symbol
		for _, clause := range branch {
			scope := ScopeOf(clause)
			for _, sym := range scope.Provides {
				if !ContainsSymbol(provided, sym) {
					provided = append(provided, sym)
				}
			}
			for _, sym := range scope.Correlates {
				if !ContainsSymbol(correlated, sym) {
					correlated = append(correlated, sym)
				}
			}
		}
		branchProvides[i] = provided
		for _, sym := range correlated {
			if !ContainsSymbol(provided, sym) && !sym.IsSource() && !ContainsSymbol(externals, sym) {
				externals = append(externals, sym)
			}
		}
	}

	return combine(branchProvides), externals
}

// headerScope computes the scope of an explicit-join form: Provides is the
// declared header restricted to what the branch combination produces —
// branch locals never escape — and header variables the branches cannot
// produce correlate with the enclosing scope, alongside branch externals.
func headerScope(
	joinVars []Symbol,
	branches [][]Clause,
	combine func([][]Symbol) []Symbol,
) ClauseScope {
	produced, externals := branchInterfaces(branches, combine)

	var provides []Symbol
	correlates := externals
	for _, jv := range joinVars {
		if ContainsSymbol(produced, jv) {
			provides = append(provides, jv)
		} else if !ContainsSymbol(correlates, jv) {
			correlates = append(correlates, jv)
		}
	}
	return ClauseScope{Provides: provides, Correlates: correlates}
}

// intersectSymbolSets returns the symbols present in every set, in the first
// set's order.
func intersectSymbolSets(sets [][]Symbol) []Symbol {
	if len(sets) == 0 {
		return nil
	}
	var result []Symbol
	for _, sym := range sets[0] {
		inAll := true
		for _, other := range sets[1:] {
			if !ContainsSymbol(other, sym) {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, sym)
		}
	}
	return result
}

// unionSymbolSets returns the symbols present in any set, deduplicated in
// first-appearance order.
func unionSymbolSets(sets [][]Symbol) []Symbol {
	var result []Symbol
	for _, set := range sets {
		for _, sym := range set {
			if !ContainsSymbol(result, sym) {
				result = append(result, sym)
			}
		}
	}
	return result
}
