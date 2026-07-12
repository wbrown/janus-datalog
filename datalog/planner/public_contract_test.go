package planner_test

import (
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

var (
	_ planner.QueryPlanner                                                          = (*planner.ClauseBasedPlanner)(nil)
	_ func(*planner.Statistics, planner.PlannerOptions) *planner.ClauseBasedPlanner = planner.NewClauseBasedPlanner

	_ = planner.RealizedPlan{Query: &query.Query{}}
	_ = planner.RealizedPhase{Query: &query.Query{}}
	_ = planner.PlannerOptions{}
)
