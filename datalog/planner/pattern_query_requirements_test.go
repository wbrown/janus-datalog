package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestSinglePatternPhaseCarriesSafeOrderAndLimit(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?tx
	                              :where [?e :event/value ?v ?tx]
	                              :order-by [[?tx :desc] [?e :asc]]
	                              :limit 10]`)
	require.NoError(t, err)

	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).Plan(q, nil)
	require.NoError(t, err)
	require.Len(t, plan.Phases, 1)
	require.Equal(t, q.OrderBy, plan.Phases[0].Query.OrderBy)
	require.NotNil(t, plan.Phases[0].Query.Limit)
	require.Equal(t, 10, *plan.Phases[0].Query.Limit)
}

func TestPatternPhaseDeclinesUnsafeOrderAndLimitPushdown(t *testing.T) {
	tests := []string{
		`[:find ?e ?name
		   :where [?e :person/name ?name]
		          [?e :person/active true]
		   :order-by [[?name :asc]]
		   :limit 10]`,
		`[:find ?name (count ?e)
		   :where [?e :person/name ?name]
		   :order-by [[?name :asc]]
		   :limit 10]`,
	}

	for _, queryText := range tests {
		q, err := parser.ParseQuery(queryText)
		require.NoError(t, err)
		plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).Plan(q, nil)
		require.NoError(t, err)
		for _, phase := range plan.Phases {
			require.Empty(t, phase.Query.OrderBy)
			require.Nil(t, phase.Query.Limit)
		}
	}
}
