package planner

import (
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Bindings is what a caller holds when it asks for a plan, beyond the query:
// the symbols its inputs bind, and the rule set bound to %, which declares
// the positions an invoked predicate requires bound and so where the
// invocation is scheduled.
type Bindings struct {
	Symbols map[query.Symbol]bool
	Rules   *query.RuleSet
}

// QueryPlanner is the interface for query planners
type QueryPlanner interface {
	// PlanQuery creates an optimized query plan under the caller's bindings.
	// handler (may be nil) carries request-scoped annotation observability
	// through planning; it is not stored.
	PlanQuery(q *query.Query, bindings Bindings, handler annotations.Handler) (*RealizedPlan, error)

	// ExplainPlan plans under the caller's bindings without executing and
	// returns the algebra explanation as values — the compiled tree, every
	// rewrite decision, the optimized tree, the rewritten query — alongside
	// the physical plan. Bypasses the plan cache in both directions.
	ExplainPlan(q *query.Query, bindings Bindings) (*AlgebraExplanation, error)

	// Options returns the planner options
	Options() PlannerOptions

	// SetCache sets the query plan cache
	SetCache(cache *PlanCache)
}

// Ensure ClauseBasedPlanner implements the interface
var _ QueryPlanner = (*ClauseBasedPlanner)(nil)

// PlanQuery implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) PlanQuery(q *query.Query, bindings Bindings, handler annotations.Handler) (*RealizedPlan, error) {
	return p.Plan(q, bindings, handler)
}

// Options implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) Options() PlannerOptions {
	return p.options
}

// SetCache implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) SetCache(cache *PlanCache) {
	p.cache = cache
}

// CreatePlanner creates a query planner with the given options
func CreatePlanner(stats *Statistics, options PlannerOptions) QueryPlanner {
	return NewClauseBasedPlanner(stats, options)
}
