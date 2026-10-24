package planner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// No headerless form reaches the planner after Prepare, and a skipped
// preparation surfaces at once: PlanQuery on a parsed query holding an or or
// a not panics as ScopeOf does, naming the clause type, on both planner
// paths (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.1).
func TestPlanQuery_PanicsOnAHeaderlessForm(t *testing.T) {
	forms := []struct{ name, src string }{
		{"or", `[:find ?x :where [?x :a ?v] (or [?x :b 1] [?x :c 2])]`},
		{"not", `[:find ?x :where [?x :a ?v] (not [?x :b 1])]`},
	}
	for _, algebra := range []bool{false, true} {
		for _, form := range forms {
			t.Run(fmt.Sprintf("%s/algebra=%v", form.name, algebra), func(t *testing.T) {
				q, err := parser.ParseQuery(form.src)
				require.NoError(t, err)
				p := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra})
				defer func() {
					recovered := recover()
					require.NotNil(t, recovered, "PlanQuery returned on a headerless form")
					require.Contains(t, fmt.Sprint(recovered), "unknown clause type", "the panic names the clause the planner does not take")
				}()
				_, _ = p.PlanQuery(q, query.Bindings{}, nil)
			})
		}
	}
}
