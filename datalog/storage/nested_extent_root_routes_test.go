package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Each caller route over the nested-scope rule set plans: the free query,
// the bound query and Explain of the bound query.
func TestRules_TheNestedExtentRootRuleSetPlansOnEveryRoute(t *testing.T) {
	x1 := datalog.NewIdentity("extentroot/x1")
	pairs := `[(pairs [?r] ?m ?q) [?r :page/next ?q] [?m :page/next ?q]]`
	rules := []struct{ name, rule string }{
		{"an or-join branch inside an or-join branch", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] (or-join [[?r ?x] ?q] (and [?x :page/next ?m] [?r :page/link ?m] (pairs ?r ?m ?q)) [?r :page/other ?q]) [?r :page/other ?q])]`},
		{"a subquery inside a subquery's inner query", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] [(q [:find ?w :in $ % ?s ?y :where [(q [:find ?v :in $ % ?t ?z :where [?z :page/next ?m] [?t :page/link ?m] (pairs ?t ?m ?v)] $ % ?s ?y) [[?w] ...]]] $ % ?r ?x) [[?q] ...]]]`},
		{"a subquery inside an or-join branch", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] [(q [:find ?w :in $ % ?s ?y :where [?y :page/next ?m] [?s :page/link ?m] (pairs ?s ?m ?w)] $ % ?r ?x) [[?q] ...]] [?r :page/other ?q])]`},
		{"an or-join branch three scopes deep", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] (or-join [[?r ?x] ?q] (or-join [[?r ?x] ?q] (and [?x :page/next ?m] [?r :page/link ?m] (pairs ?r ?m ?q)) [?r :page/other ?q]) [?r :page/other ?q]) [?r :page/other ?q])]`},
	}
	for _, mode := range optimizerModes {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		for _, r := range rules {
			rs := mustRuleSet(t, "["+pairs+" "+r.rule+"]")
			free, err := executor.CollectTuples(d.Query(`[:find ?x ?q :in $ % :where (p ?x ?q)]`, rs))
			assert.NoError(t, err, "%s %s: free query", mode.name, r.name)
			assert.Empty(t, free, "%s %s: the store holds no facts, so the free query answers no pairs", mode.name, r.name)
			bound, err := executor.CollectTuples(d.Query(`[:find ?q :in $ % ?x :where (p ?x ?q)]`, rs, x1))
			assert.NoError(t, err, "%s %s: bound query", mode.name, r.name)
			assert.Empty(t, bound, "%s %s: the store holds no facts, so the bound query answers nothing", mode.name, r.name)
			plan, err := d.Explain(`[:find ?q :in $ % ?x :where (p ?x ?q)]`, rs, x1)
			if assert.NoError(t, err, "%s %s: explain", mode.name, r.name) && assert.NotNil(t, plan) {
				assert.NoError(t, plan.Validate(), "%s %s: the explained plan's contracts", mode.name, r.name)
			}
		}
	}
}
