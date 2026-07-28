package query

// Sort-key semantics for :order-by.
//
// The result of the :where body is sorted before the final projection to
// :find (π_find ∘ τ_keys over the satisfying-assignment relation), so a sort
// key may be any variable bound by :where — projected or not. Scalar and
// tuple :in inputs are per-execution constants: every tuple carries the same
// value, so a sort clause keyed on one is a well-defined identity and is
// dropped at the execution boundary. See the Design Decision section of
// docs/bugs/resolved/BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md for the
// full reasoning.

// ConstantInputSymbols returns the :in symbols that are constant for an
// entire query execution: scalar inputs and tuple-input components.
func ConstantInputSymbols(specs []InputSpec) map[Symbol]bool {
	constants := make(map[Symbol]bool)
	for _, spec := range specs {
		switch s := spec.(type) {
		case ScalarInput:
			constants[s.Symbol] = true
		case TupleInput:
			for _, sym := range s.Symbols {
				constants[sym] = true
			}
		}
	}
	return constants
}

// IteratedInputSymbols returns the :in symbols whose values vary across the
// result: collection inputs and relation-input symbols.
func IteratedInputSymbols(specs []InputSpec) map[Symbol]bool {
	iterated := make(map[Symbol]bool)
	for _, spec := range specs {
		switch s := spec.(type) {
		case CollectionInput:
			iterated[s.Symbol] = true
		case RelationInput:
			for _, sym := range s.Symbols {
				iterated[sym] = true
			}
		}
	}
	return iterated
}

// EffectiveOrderBy returns the :order-by clauses that can actually order
// tuples, dropping clauses keyed on constant inputs (identity sorts).
func EffectiveOrderBy(q *Query) []OrderByClause {
	if len(q.OrderBy) == 0 {
		return nil
	}
	constants := ConstantInputSymbols(q.In)
	if len(constants) == 0 {
		return q.OrderBy
	}
	effective := make([]OrderByClause, 0, len(q.OrderBy))
	for _, clause := range q.OrderBy {
		if !constants[clause.Variable] {
			effective = append(effective, clause)
		}
	}
	return effective
}

// RetainedSortSymbols returns the :order-by variables that must survive the
// final projection so the executor can sort the assembled result before
// stripping it back to the declared :find shape: every effective sort key
// that is not already a :find symbol. Aggregate queries retain nothing —
// their valid sort keys are group keys, which are :find variables by
// definition, and appending variables to an aggregate find clause would
// change its grouping.
func RetainedSortSymbols(q *Query) []Symbol {
	if len(q.OrderBy) == 0 {
		return nil
	}
	for _, elem := range q.Find {
		if elem.IsAggregate() {
			return nil
		}
	}
	inFind := make(map[Symbol]bool)
	for _, elem := range q.Find {
		switch e := elem.(type) {
		case FindVariable:
			inFind[e.Symbol] = true
		case FindPull:
			inFind[e.Variable] = true
		}
	}
	seen := make(map[Symbol]bool)
	var retained []Symbol
	for _, clause := range EffectiveOrderBy(q) {
		v := clause.Variable
		if inFind[v] || seen[v] {
			continue
		}
		seen[v] = true
		retained = append(retained, v)
	}
	return retained
}
