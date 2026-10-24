package query

import "fmt"

// ValidateQuery runs every static check a query's text admits, the checks
// that depend on the text alone: a non-empty :find and :where, the scalar
// find's one element and its :limit of at most one, ValidateInputShapes on
// :in, ValidateStaticClauseShapes on :where, ValidateRulesInput on both, and
// the :order-by rule. It reaches every inner query through
// SubqueryPattern.Validate. ParseQuery, qb.Build and Prepare each run it and
// run no check of their own, so one text is accepted or refused alike at
// every boundary with one message
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 5 and §6).
func ValidateQuery(q *Query) error {
	if len(q.Find) == 0 {
		return fmt.Errorf("query must have at least one find variable")
	}
	if len(q.Where) == 0 {
		return fmt.Errorf("query must have at least one where pattern")
	}
	if q.ScalarReturn && len(q.Find) != 1 {
		return fmt.Errorf("scalar find spec (.) requires exactly one find element, got %d", len(q.Find))
	}
	if q.ScalarReturn && q.Limit != nil && *q.Limit > 1 {
		return fmt.Errorf("scalar find spec (.) is incompatible with :limit %d (limit must be 0 or 1)", *q.Limit)
	}
	if err := ValidateInputShapes(q.In); err != nil {
		return err
	}
	if err := ValidateStaticClauseShapes(q.Where); err != nil {
		return err
	}
	if err := ValidateRulesInput(q.In, q.Where); err != nil {
		return err
	}
	return validateOrderBy(q)
}

// validateOrderBy is the sort-key rule: the result is sorted before the final
// projection, so a sort key is a variable the :where binds, read through binds
// with no rule set, a :find variable, a scalar or tuple :in constant, an
// identity sort dropped at execution, or a collection or relation :in symbol,
// which its input binds and which orders the product of the result with the
// input's group (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §3). Under an
// aggregating find only a :find variable orders, since aggregation collapses
// the tuples. A variable bound nowhere is refused.
func validateOrderBy(q *Query) error {
	if len(q.OrderBy) == 0 {
		return nil
	}
	constants := ConstantInputSymbols(q.In)
	multiValued := MultiValuedInputSymbols(q.In)
	findVars := make(map[Symbol]bool)
	hasAggregates := false
	for _, elem := range q.Find {
		switch e := elem.(type) {
		case FindVariable:
			findVars[e.Symbol] = true
		case FindPull:
			findVars[e.Variable] = true
		case FindAggregate:
			hasAggregates = true
		}
	}
	for _, clause := range q.OrderBy {
		v := clause.Variable
		if constants[v] {
			continue
		}
		if hasAggregates {
			if !findVars[v] {
				return fmt.Errorf("order-by variable %s is not a group key: aggregation collapses tuples, so only :find variables can order an aggregate query", v)
			}
			continue
		}
		if findVars[v] || multiValued[v] || otherClauseBinds(q.Where, -1, v, nil) {
			continue
		}
		return fmt.Errorf("order-by variable %s is not bound in the query", v)
	}
	return nil
}
