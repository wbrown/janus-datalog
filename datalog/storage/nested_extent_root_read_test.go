package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A bound invocation of p, whose required predicate pairs is invoked inside
// a scope at the root of [?r :page/kind "root"] beside a symbol the scope's
// seed-connected clause provides, derives exactly what the free evaluation
// of p derives at that binding (REWRITE_ORDER_PROOF.md T1,
// A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md T1), and its
// evaluation holds magic.bbf/pairs. A case is the scope p's body invokes
// pairs in.
func TestRules_ARequiredInvocationAtAnExtentRootInsideNestedScopesDerivesWhatTheFreeEvaluationDerives(t *testing.T) {
	x1, x2 := datalog.NewIdentity("extentroot/x1"), datalog.NewIdentity("extentroot/x2")
	m1, m2 := datalog.NewIdentity("extentroot/m1"), datalog.NewIdentity("extentroot/m2")
	q1, q2, q3 := datalog.NewIdentity("extentroot/q1"), datalog.NewIdentity("extentroot/q2"), datalog.NewIdentity("extentroot/q3")
	r1, r2 := datalog.NewIdentity("extentroot/r1"), datalog.NewIdentity("extentroot/r2")
	type fact struct {
		e datalog.Identity
		a string
		v interface{}
	}
	facts := []fact{
		{x1, ":name", "x1"}, {x2, ":name", "x2"}, {m1, ":name", "m1"}, {m2, ":name", "m2"},
		{q1, ":name", "q1"}, {q2, ":name", "q2"}, {q3, ":name", "q3"}, {r1, ":name", "r1"}, {r2, ":name", "r2"},
		{x1, ":page/n", int64(1)}, {x2, ":page/n", int64(2)},
		{x1, ":page/next", m1}, {x2, ":page/next", m2},
		{m1, ":page/next", q1}, {m2, ":page/next", q2},
		{r1, ":page/kind", "root"}, {r2, ":page/kind", "root"},
		{r1, ":page/next", q1}, {r2, ":page/next", q2}, {r2, ":page/next", q1},
		{r1, ":page/other", q3},
		{r1, ":page/link", m1}, {r2, ":page/link", m2}, {r2, ":page/link", m1},
	}
	pairs := `[(pairs [?r] ?m ?q) [?r :page/next ?q] [?m :page/next ?q]]`
	cases := []struct {
		name, rule string
	}{
		{"one or-join branch whose clause mentions the root", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] (and [?x :page/next ?m] [?r :page/link ?m] (pairs ?r ?m ?q)) [?r :page/other ?q])]`},
		{"an or-join branch inside an or-join branch", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] (or-join [[?r ?x] ?q] (and [?x :page/next ?m] [?r :page/link ?m] (pairs ?r ?m ?q)) [?r :page/other ?q]) [?r :page/other ?q])]`},
		{"an or-join branch inside a subquery's inner query", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] [(q [:find ?w :in $ % ?s ?y :where (or-join [[?s ?y] ?w] (and [?y :page/next ?m] [?s :page/link ?m] (pairs ?s ?m ?w)) [?s :page/other ?w])] $ % ?r ?x) [[?q] ...]]]`},
		{"a subquery inside a subquery's inner query", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] [(q [:find ?w :in $ % ?s ?y :where [(q [:find ?v :in $ % ?t ?z :where [?z :page/next ?m] [?t :page/link ?m] (pairs ?t ?m ?v)] $ % ?s ?y) [[?w] ...]]] $ % ?r ?x) [[?q] ...]]]`},
		{"a subquery inside an or-join branch", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] [(q [:find ?w :in $ % ?s ?y :where [?y :page/next ?m] [?s :page/link ?m] (pairs ?s ?m ?w)] $ % ?r ?x) [[?q] ...]] [?r :page/other ?q])]`},
		{"an or-join branch three scopes deep", `[(p ?x ?q) [?x :page/n _] [?r :page/kind "root"] (or-join [[?r ?x] ?q] (or-join [[?r ?x] ?q] (or-join [[?r ?x] ?q] (and [?x :page/next ?m] [?r :page/link ?m] (pairs ?r ?m ?q)) [?r :page/other ?q]) [?r :page/other ?q]) [?r :page/other ?q])]`},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var strata []string
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				if e.Name == annotations.RulesStratumBegin {
					strata = append(strata, fmt.Sprint(e.Data["predicates"]))
				}
			}})
			tx := d.NewTransaction()
			for _, f := range facts {
				require.NoError(t, tx.Add(f.e, kw(f.a), f.v))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			named, err := d.Query(`[:find ?e ?n :where [?e :name ?n]]`)
			require.NoError(t, err)
			display := map[datalog.Identity]string{}
			require.NoError(t, executor.ForEach(named, func(tuple executor.Tuple) error {
				display[tuple[0].(datalog.Identity)] = tuple[1].(string)
				return nil
			}))
			render := func(ids []datalog.Identity) string {
				parts := make([]string, len(ids))
				for i, id := range ids {
					parts[i] = display[id]
				}
				sort.Strings(parts)
				return strings.Join(parts, " ")
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rules := mustRuleSet(t, "["+pairs+" "+c.rule+"]")
					free, err := d.Query(`[:find ?x ?q :in $ % :where (p ?x ?q)]`, rules)
					require.NoError(t, err)
					want := map[datalog.Identity][]datalog.Identity{}
					require.NoError(t, executor.ForEach(free, func(tuple executor.Tuple) error {
						x := tuple[0].(datalog.Identity)
						want[x] = append(want[x], tuple[1].(datalog.Identity))
						return nil
					}))
					for _, x := range []datalog.Identity{x1, x2} {
						strata = nil
						got, err := d.Query(`[:find ?q :in $ % ?x :where (p ?x ?q)]`, rules, x)
						require.NoError(t, err)
						var answered []datalog.Identity
						require.NoError(t, executor.ForEach(got, func(tuple executor.Tuple) error {
							answered = append(answered, tuple[0].(datalog.Identity))
							return nil
						}))
						require.Equal(t, render(want[x]), render(answered), "p at %s", display[x])
						t.Logf("p at %s strata %v", display[x], strata)
					}
				})
			}
		})
	}
}
