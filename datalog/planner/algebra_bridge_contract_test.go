package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The bridge answers a new query: its input is unchanged, the find, the
// inputs, the order and the limit are carried over, every clause is carried,
// and the bridge reports its completion.
func TestOptimizeViaAlgebraReturnsANewQueryWithoutMutatingItsInput(t *testing.T) {
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
		parsed, query.Bindings{},
		PlannerOptions{EnableAlgebraOptimizer: true},
		handler,
		nil)

	require.NoError(t, err)
	require.NotSame(t, parsed, optimized)
	require.Equal(t, before, parsed.String(), "optimization must not mutate its Datalog input")
	require.Equal(t, parsed.Find, optimized.Find)
	require.Equal(t, parsed.In, optimized.In)
	require.Equal(t, parsed.OrderBy, optimized.OrderBy)
	require.Equal(t, parsed.Limit, optimized.Limit)
	require.Len(t, optimized.Where, 3, "the three clauses are carried")
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
		Plan(parsed, query.Bindings{}, handler)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Contains(t, names, "algebra/bridge-complete")
	require.NotContains(t, names, "algebra/emitted")
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
		Plan(parsed, query.Bindings{}, nil)
	require.NoError(t, err)
	require.NoError(t, plan.Validate())
	require.Len(t, plan.Phases, 1)
	require.Contains(t, plan.Phases[0].Available, targetTeam)
	require.Len(t, plan.Phases[0].Query.Where, 2)
	_, isPattern := plan.Phases[0].Query.Where[1].(*query.DataPattern)
	require.True(t, isPattern,
		"input-bound pattern must remain in the Datalog query that receives its binding")
}
