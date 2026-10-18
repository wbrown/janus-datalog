package query

// Bindings is what a caller holds when it asks for a plan, beyond the query:
// the symbols its inputs bind, and the rule set bound to %, which declares
// the positions an invoked predicate requires bound and so where the
// invocation is scheduled.
type Bindings struct {
	Symbols map[Symbol]bool
	Rules   *RuleSet
}
