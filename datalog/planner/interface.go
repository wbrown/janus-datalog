package planner

import (
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// QueryPlanner is the interface for query planners
type QueryPlanner interface {
	// PlanQuery creates an optimized query plan
	PlanQuery(q *query.Query) (*RealizedPlan, error)

	// PlanQueryWithBindings creates an optimized query plan with initial bindings
	PlanQueryWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*RealizedPlan, error)

	// Options returns the planner options
	Options() PlannerOptions

	// SetCache sets the query plan cache
	SetCache(cache *PlanCache)

	// SetHandler sets the annotation handler for query planning observability
	SetHandler(h annotations.Handler)
}

// Ensure ClauseBasedPlanner implements the interface
var _ QueryPlanner = (*ClauseBasedPlanner)(nil)

// PlanQuery implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) PlanQuery(q *query.Query) (*RealizedPlan, error) {
	return p.Plan(q)
}

// PlanQueryWithBindings implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) PlanQueryWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*RealizedPlan, error) {
	return p.PlanWithBindings(q, initialBindings)
}

// Options implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) Options() PlannerOptions {
	return p.options
}

// SetCache implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) SetCache(cache *PlanCache) {
	p.cache = cache
}

// SetHandler implements QueryPlanner for ClauseBasedPlanner
func (p *ClauseBasedPlanner) SetHandler(h annotations.Handler) {
	p.handler = h
}

// CreatePlanner creates a query planner with the given options
func CreatePlanner(stats *Statistics, options PlannerOptions) QueryPlanner {
	return NewClauseBasedPlanner(stats, options)
}
