package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestOptimizeViaAlgebraReturnsNestedDatalogWithoutMutatingInput(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?payload
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]
		         [?entity :item/payload ?payload]
		  :order-by [[?payload :asc]]
		  :limit 10]`,
	)
	require.NoError(t, err)
	before := parsed.String()

	var eventNames []string
	handler := func(event annotations.Event) {
		eventNames = append(eventNames, event.Name)
	}
	optimized, err := optimizeViaAlgebra(
		parsed,
		PlannerOptions{
			EnableAlgebraOptimizer:     true,
			EnableJoinProjectInsertion: true,
		},
		handler,
		nil,
	)
	require.NoError(t, err)
	require.NotSame(t, parsed, optimized)
	require.Equal(t, before, parsed.String(), "optimization must not mutate its Datalog input")
	require.Equal(t, parsed.Find, optimized.Find)
	require.Equal(t, parsed.In, optimized.In)
	require.Equal(t, parsed.OrderBy, optimized.OrderBy)
	require.Equal(t, parsed.Limit, optimized.Limit)

	require.Len(t, optimized.Where, 2)
	projected, ok := optimized.Where[0].(*query.SubqueryPattern)
	require.True(t, ok)
	require.Equal(t,
		[]query.FindElement{query.FindVariable{Symbol: entity}},
		projected.Query.Find,
	)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity}},
		projected.Binding,
	)
	_, ok = optimized.Where[1].(*query.DataPattern)
	require.True(t, ok)
	require.Contains(t, eventNames, "algebra/bridge-complete")
	require.NotContains(t, eventNames, "algebra/emitted")
}

func TestClauseBasedPlannerPlansOptimizedDatalog(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?count
		  :where [?entity :item/type :item.type/scored]
		         [(q [:find (count ?child)
		              :in $ ?parent
		              :where [?child :item/parent ?parent]]
		             $ ?entity) [[?count]]]]`,
	)
	require.NoError(t, err)

	var names []string
	handler := func(event annotations.Event) {
		names = append(names, event.Name)
	}
	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).
		Plan(parsed, handler)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Contains(t, names, "algebra/bridge-complete")
	require.NotContains(t, names, "algebra/emitted")
}

func TestClauseBasedPlannerLowersJoinProjectIntoNestedDatalog(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	payload := datalog.NewSymbol("?payload")
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?payload
		  :where [?entity :item/score ?score]
		         [(> ?score 90)]
		         [?entity :item/payload ?payload]]`,
	)
	require.NoError(t, err)

	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{
		EnableAlgebraOptimizer:     true,
		EnableJoinProjectInsertion: true,
	}).
		Plan(parsed, nil)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Len(t, plan.Phases, 1)
	require.Equal(t, []query.Symbol{entity, payload}, plan.Phases[0].Provides)
	require.Empty(t, plan.Phases[0].Keep)
	require.Len(t, plan.Phases[0].Query.Where, 2)
	var projected *query.SubqueryPattern
	var hasPattern bool
	for _, clause := range plan.Phases[0].Query.Where {
		switch typed := clause.(type) {
		case *query.SubqueryPattern:
			projected = typed
		case *query.DataPattern:
			hasPattern = true
		}
	}
	require.NotNil(t, projected, "logical Project must remain a nested Datalog query")
	require.Equal(t,
		[]query.FindElement{query.FindVariable{Symbol: entity}},
		projected.Query.Find,
	)
	require.Equal(t,
		query.RelationBinding{Variables: []query.Symbol{entity}},
		projected.Binding,
	)
	require.True(t, hasPattern)
}

func TestClauseBasedPlannerKeepsInputSymbolsLiveAcrossJoinPlanning(t *testing.T) {
	targetTeam := datalog.NewSymbol("?target-team")
	parsed, err := parser.ParseQuery(
		`[:find ?name
		  :in $ ?target-team
		  :where [?entity :person/name ?name]
		         [?entity :person/team ?target-team]]`,
	)
	require.NoError(t, err)

	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).
		Plan(parsed, nil)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Len(t, plan.Phases, 1)
	require.Contains(t, plan.Phases[0].Available, targetTeam)
	require.Len(t, plan.Phases[0].Query.Where, 2)
	_, isPattern := plan.Phases[0].Query.Where[1].(*query.DataPattern)
	require.True(t, isPattern,
		"input-bound pattern must remain in the Datalog query that receives its binding")
}
