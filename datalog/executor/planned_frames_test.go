package executor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// plansMade collects the plans the planner reports making, as their queries
// rendered, while recording is on.
type plansMade struct {
	recording bool
	queries   []string
}

func (m *plansMade) handle(e annotations.Event) {
	if m.recording && e.Name == annotations.PlannerPlanMade {
		m.queries = append(m.queries, e.Data[annotations.KeyQuery].(*query.Query).String())
	}
}

// A run of a query the planner has planned makes no plan: every frame a
// windowed clause reads was planned at PlanQuery
// (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md T2, T4), for a pattern
// after a join and for an invocation after a join. The planning under the
// same handler reports the plans it makes, so the run's silence is a
// verdict the fixture can fail.
func TestAWindowedClauseReadsItsFrameFromThePlan(t *testing.T) {
	const pages = 10
	datoms := chainDatoms(pages)
	rs := mustRules(t, followsRulesText)
	cases := []struct{ name, text string }{
		{"a pattern after a join", `[:find ?x ?z :in $ % :where [?x :page/kind "leaf"] [?x :page/next ?y] [?y :page/next ?z]]`},
		{"an invocation after a join", `[:find ?x ?z :in $ % :where [?x :page/kind "leaf"] [?x :page/next ?y] (follows ?y ?z)]`},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				made := &plansMade{recording: true}
				opts := mode.plannerOptions()
				opts.Handler = made.handle
				exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts)
				exec.SetPlanCache(planner.NewPlanCache(64, 0))
				q := mustParse(t, c.text)
				prepared, err := query.Prepare(q, rs)
				require.NoError(t, err)
				_, err = exec.GetPlanner().PlanQuery(prepared, query.Bindings{Rules: rs}, made.handle)
				require.NoError(t, err)
				require.NotEmpty(t, made.queries, "planning the query reports the plans it makes")

				made.queries = nil
				rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
				require.NoError(t, err)
				got, err := CollectTuples(rel, nil)
				require.NoError(t, err)
				require.NotEmpty(t, got, "the windowed clause answers")
				require.Empty(t, made.queries, "the run of a planned query makes no plan; the plans made:\n%s", strings.Join(made.queries, "\n"))
			})
		}
	}
}
