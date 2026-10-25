package query

// Bindings is what a caller holds when it asks for a plan, beyond the query:
// the symbols its inputs bind, the rule set bound to %, which declares the
// positions an invoked predicate requires bound and so where the invocation
// is scheduled, and which of the scope's invocations are held.
type Bindings struct {
	Symbols map[Symbol]bool
	Rules   *RuleSet
	Held    Held
}

// Held says which invocations of the scope being planned read a relation
// something else supplies, so the planner plans no body for them. Under a
// query none is held: each invocation is derived at its clause, and the
// planner plans the bodies its evaluation runs. Under a body a fixpoint
// runs, every predicate without required positions is held by the fixpoint,
// and a predicate with required positions is derived at its clause. Under
// the rewrite's reading of a scope's order every invocation is held, since
// the rewrite decides each site itself.
type Held int

const (
	HeldNone Held = iota
	HeldByFixpoint
	HeldEvery
)
