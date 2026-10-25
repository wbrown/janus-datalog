package query

import "fmt"

// Prepare takes a parsed query and the rule set bound to % and returns the
// query ready to plan. It composes the five static steps, each a function of
// the text and the rule set: ValidateQuery, which reaches every inner query;
// the check that no variable of the text lies in a namespace the engine mints
// into, run before the inference and the unification write the engine's own
// there; InferHeaders; UnifyPatterns; and the check that a rule set was given
// when the query invokes a predicate and that every invocation names a
// predicate it defines at its arity. It runs once per execution, before
// planning, and its result is not held on the query. ParseQuery and qb.Build
// run ValidateQuery alone and accept a variable in an engine namespace, since
// the prepared text carries the engine's own and every phase the planner
// emits parses back through ParseQuery.
func Prepare(q *Query, rules *RuleSet) (*Query, error) {
	if err := ValidateQuery(q); err != nil {
		return nil, err
	}
	if v := engineVariable(q, rules); v != nil {
		return nil, &EngineNamespaceError{Variable: v}
	}
	prepared, err := InferHeaders(q, rules)
	if err != nil {
		return nil, err
	}
	prepared = UnifyPatterns(prepared)
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
