package planner

import (
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// QueryPlanner is the interface for query planners
type QueryPlanner interface {
	// PlanQuery creates an optimized query plan. handler (may be nil) carries
	// request-scoped annotation observability through planning; it is not stored.
	PlanQuery(q *query.Query, handler annotations.Handler) (*RealizedPlan, error)

	// PlanQueryWithBindings creates an optimized query plan with initial bindings.
	PlanQueryWithBindings(q *query.Query, initialBindings map[query.Symbol]bool, handler annotations.Handler) (*RealizedPlan, error)

	// Options returns the planner options
	Options() PlannerOptions

	// SetCache sets the query plan cache
	SetCache(cache *PlanCache)
}

// Ensure ClauseBasedPlanner implements the interface
var _ QueryPlanner = (*ClauseBasedPlanner)(nil)

// PlanQuery implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) PlanQuery(q *query.Query, handler annotations.Handler) (*RealizedPlan, error) {
	return p.Plan(q, handler)
}

// PlanQueryWithBindings implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) PlanQueryWithBindings(q *query.Query, initialBindings map[query.Symbol]bool, handler annotations.Handler) (*RealizedPlan, error) {
	return p.PlanWithBindings(q, initialBindings, handler)
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
