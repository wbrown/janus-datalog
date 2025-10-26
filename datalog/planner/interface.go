package planner

import "github.com/wbrown/janus-datalog/datalog/query"

// QueryPlanner is the interface that both old and new planners implement
type QueryPlanner interface {
	// PlanQuery creates an optimized query plan
	PlanQuery(q *query.Query) (*RealizedPlan, error)

	// PlanQueryWithBindings creates an optimized query plan with initial bindings
	PlanQueryWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*RealizedPlan, error)

	// Options returns the planner options
	Options() PlannerOptions

	// SetCache sets the query plan cache
	SetCache(cache *PlanCache)
}

// Ensure both planners implement the interface
var _ QueryPlanner = (*PlannerAdapter)(nil)
var _ QueryPlanner = (*ClauseBasedPlanner)(nil)

// PlannerAdapter adapts the old Planner to the QueryPlanner interface
type PlannerAdapter struct {
	planner *Planner
}

// NewPlannerAdapter wraps the old planner
func NewPlannerAdapter(stats *Statistics, options PlannerOptions) *PlannerAdapter {
	return &PlannerAdapter{
		planner: NewPlanner(stats, options),
	}
}

// PlanQuery implements QueryPlanner
func (pa *PlannerAdapter) PlanQuery(q *query.Query) (*RealizedPlan, error) {
	plan, err := pa.planner.Plan(q)
	if err != nil {
		return nil, err
	}
	return plan.Realize(), nil
}

// PlanQueryWithBindings implements QueryPlanner
func (pa *PlannerAdapter) PlanQueryWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*RealizedPlan, error) {
	plan, err := pa.planner.PlanWithBindings(q, initialBindings)
	if err != nil {
		return nil, err
	}
	return plan.Realize(), nil
}

// Options implements QueryPlanner
func (pa *PlannerAdapter) Options() PlannerOptions {
	return pa.planner.Options()
}

// SetCache implements QueryPlanner
func (pa *PlannerAdapter) SetCache(cache *PlanCache) {
	pa.planner.SetCache(cache)
}

// GetUnderlyingPlanner returns the wrapped old planner (for testing/migration)
func (pa *PlannerAdapter) GetUnderlyingPlanner() *Planner {
	return pa.planner
}

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

// CreatePlanner creates the appropriate planner based on options
func CreatePlanner(stats *Statistics, options PlannerOptions) QueryPlanner {
	if options.UseClauseBasedPlanner {
		return NewClauseBasedPlanner(stats, options)
	}
	return NewPlannerAdapter(stats, options)
}
