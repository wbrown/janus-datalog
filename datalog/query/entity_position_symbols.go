package query

// EntityPositionSymbols returns the set of symbols that occupy the entity
// position of a data pattern anywhere in the query — directly, inside
// not/or containers, or transitively as an argument to a subquery whose
// corresponding :in parameter occupies an entity position in the nested
// query.
//
// The entity position is inhabited only by Identity, so input values bound
// to these symbols must be Identities. Validating that at the input boundary
// turns a query defect into a loud error before execution; interior data
// flow (values joined from other positions) is never validated — a
// non-Identity there is the equality join's ordinary typed non-match.
func EntityPositionSymbols(q *Query) map[Symbol]bool {
	result := make(map[Symbol]bool)
	entityPositionSymbolsInClauses(q.Where, result)
	return result
}

func entityPositionSymbolsInClauses(clauses []Clause, result map[Symbol]bool) {
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *DataPattern:
			if v, ok := c.GetE().(Variable); ok {
				result[v.Name] = true
			}
		case *NotClause:
			entityPositionSymbolsInClauses(c.Clauses, result)
		case *NotJoinClause:
			entityPositionSymbolsInClauses(c.Clauses, result)
		case *OrClause:
			for _, branch := range c.Branches {
				entityPositionSymbolsInClauses(branch, result)
			}
		case *OrJoinClause:
			for _, branch := range c.Branches {
				entityPositionSymbolsInClauses(branch, result)
			}
		case *OrDefaultClause:
			for _, branch := range c.Branches {
				entityPositionSymbolsInClauses(branch, result)
			}
		case *OrDefaultJoinClause:
			for _, branch := range c.Branches {
				entityPositionSymbolsInClauses(branch, result)
			}
		case *SubqueryPattern:
			entityPositionSymbolsFromSubquery(*c, result)
		}
	}
}

// entityPositionSymbolsFromSubquery maps the nested query's entity-position
// :in parameters back onto the outer argument symbols: an outer symbol passed
// into an inner entity-position parameter is itself entity-bound.
func entityPositionSymbolsFromSubquery(p SubqueryPattern, result map[Symbol]bool) {
	if p.Query == nil {
		return
	}
	inner := EntityPositionSymbols(p.Query)
	if len(inner) == 0 {
		return
	}

	// The argument list may or may not carry the database markers alongside
	// the value parameters; detect which alignment the arguments use. Any
	// other shape is malformed and maps nothing — the analysis is
	// conservative, and an unmapped symbol degrades to the interior typed
	// non-match rather than the boundary error.
	dbCount := 0
	for _, spec := range p.Query.In {
		if _, ok := spec.(DatabaseInput); ok {
			dbCount++
		}
	}
	includesDBArgs := len(p.Inputs) == len(p.Query.In)
	if !includesDBArgs && len(p.Inputs) != len(p.Query.In)-dbCount {
		return
	}

	argIdx := 0
	for _, spec := range p.Query.In {
		if _, ok := spec.(DatabaseInput); ok {
			if includesDBArgs {
				argIdx++
			}
			continue
		}
		if argIdx >= len(p.Inputs) {
			return
		}
		arg := p.Inputs[argIdx]
		argIdx++

		var innerSym Symbol
		switch s := spec.(type) {
		case ScalarInput:
			innerSym = s.Symbol
		case CollectionInput:
			innerSym = s.Symbol
		default:
			// Tuple/relation parameters receive a single outer value that is
			// itself a tuple or relation, never an entity.
			continue
		}
		if !inner[innerSym] {
			continue
		}
		if v, ok := arg.(Variable); ok {
			result[v.Name] = true
		}
	}
}
