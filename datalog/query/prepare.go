package query

import "fmt"

// Prepare takes a parsed query and the rule set bound to % and returns the
// query ready to plan. It composes the four static steps, each a function of
// the text and the rule set: ValidateInputShapes on q.In,
// ValidateStaticClauseShapes on q.Where, InferHeaders, and the check that a
// rule set was given when the query invokes a predicate and that every
// invocation names a predicate it defines at its arity. It runs once per
// execution, before planning, and its result is not held on the query.
func Prepare(q *Query, rules *RuleSet) (*Query, error) {
	if err := ValidateInputShapes(q.In); err != nil {
		return nil, err
	}
	if err := ValidateStaticClauseShapes(q.Where); err != nil {
		return nil, err
	}
	prepared, err := InferHeaders(q, rules)
	if err != nil {
		return nil, err
	}
	if invoked := Invocations(prepared.Where); len(invoked) > 0 {
		if rules == nil {
			return nil, fmt.Errorf("rule invocation %s: no rule set was given for %%", invoked[0])
		}
		for _, inv := range invoked {
			if err := rules.CheckInvocation(inv); err != nil {
				return nil, err
			}
		}
	}
	return prepared, nil
}
