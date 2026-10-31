package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Plan reports each plan it makes through the handler
// (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md P8): a plan made on a
// cache miss is reported once, with the query planned and the bindings it
// was made under, typed, under the report's own key, and nothing under the
// scan family's "bound", which names a run's bound positions; a plan the
// cache serves is reported not at all.
func TestPlan_ReportsEachPlanItMakes(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?a :where [?e :t/k ?a]]`)
	require.NoError(t, err)
	prepared, err := query.Prepare(q, nil)
	require.NoError(t, err)
	var made []annotations.Event
	handler := func(e annotations.Event) {
		if e.Name == annotations.PlannerPlanMade {
			made = append(made, e)
		}
	}
	p := NewClauseBasedPlanner(nil, PlannerOptions{Cache: NewPlanCache(10, 0)})
	bindings := query.Bindings{Held: query.HeldByFixpoint}
	_, err = p.Plan(prepared, bindings, handler)
	require.NoError(t, err)
	require.Len(t, made, 1, "a plan made on a cache miss is reported once")
	require.Same(t, prepared, made[0].Data[annotations.KeyQuery], "the report carries the query planned")
	require.Equal(t, bindings, made[0].Data["bindings"], "the report carries the bindings the plan was made under, typed")
	_, bound := made[0].Data[annotations.KeyBound]
	require.False(t, bound, "the report carries nothing under the scan family's bound")
	_, err = p.Plan(prepared, bindings, handler)
	require.NoError(t, err)
	require.Len(t, made, 1, "a plan the cache serves is reported not at all")
}
