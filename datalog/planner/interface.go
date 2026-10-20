package planner

import (
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// QueryPlanner is the interface for query planners
type QueryPlanner interface {
	// PlanQuery creates an optimized query plan under the caller's bindings.
	// handler (may be nil) carries request-scoped annotation observability
	// through planning; it is not stored.
	PlanQuery(q *query.Query, bindings query.Bindings, handler annotations.Handler) (*RealizedPlan, error)

	// PlanScope plans a nested scope, a header over a clause list, framed as
	// the query that finds the header and opens with scope/required over the
	// required run, under the caller's bindings.
	PlanScope(s Scope, bindings query.Bindings, handler annotations.Handler) (*RealizedPlan, error)

	// ExplainPlan plans under the caller's bindings without executing and
	// returns the algebra explanation as values — the compiled tree, every
	// rewrite decision, the optimized tree, the rewritten query — alongside
	// the physical plan. Bypasses the plan cache in both directions.
	ExplainPlan(q *query.Query, bindings query.Bindings) (*AlgebraExplanation, error)

	// Options returns the planner options
	Options() PlannerOptions

	// SetCache sets the query plan cache
	SetCache(cache *PlanCache)
}

// Ensure ClauseBasedPlanner implements the interface
var _ QueryPlanner = (*ClauseBasedPlanner)(nil)

// PlanQuery implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) PlanQuery(q *query.Query, bindings query.Bindings, handler annotations.Handler) (*RealizedPlan, error) {
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
