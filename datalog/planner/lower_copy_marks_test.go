package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Every lower entry a stratum carries is a copy, marked on a seed-restricted
// predicate of a lower stratum and invoking that predicate's delta symbol
// once, and a lower occurrence has a copy exactly when the delta walk reaches
// it: at the top level, inside a union's branch and inside a subquery's inner
// query, and not under a negation or a fallback form, whose body reads the
// predicate whole (docs/proposals/DELTA_COPY_RUNS_ON_ITS_DELTA_PROOF.md, P2,
// T4). Each case names the adornments of near the rewrite gives the
// occurrence, and the adorned bodies are checked to invoke them, so a case
// expecting no copy expects it of an occurrence that is there.
func TestPlanStratum_EveryLowerCopyInvokesTheDeltaItIsMarkedOn(t *testing.T) {
	far, near := sym("far"), sym("near")
	bf := query.AdornedSymbol(near, []bool{true, false})
	bb := query.AdornedSymbol(near, []bool{true, true})
	fb := query.AdornedSymbol(near, []bool{false, true})
	cases := []struct {
		name  string
		rules string
		near  []query.Symbol
		copy  bool
	}{
		{"a lower occurrence at the top level", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?b) (near ?a ?b)]
		    [(far ?a ?c) (near ?a ?b) (far ?b ?c)]]`, []query.Symbol{bf}, true},
		{"a lower occurrence inside a union's branch", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?c) [?a :page/next ?b] (or-join [[?b] ?c] (near ?b ?c) [?b :page/next ?c])]
		    [(far ?a ?c) [?a :page/next ?b] (far ?b ?c)]]`, []query.Symbol{bf}, true},
		{"a lower occurrence inside a subquery's inner query", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?c) [?a :page/next ?b] [(q [:find ?c :in $ % ?b :where (near ?b ?c)] $ % ?b) [[?c] ...]]]
		    [(far ?a ?c) [?a :page/next ?b] (far ?b ?c)]]`, []query.Symbol{bf}, true},
		{"a lower occurrence under not", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?b) [?a :page/next ?b] (not (near ?b ?a))]
		    [(far ?a ?c) [?a :page/next ?b] (far ?b ?c) (not (near ?c ?a))]]`, []query.Symbol{bb, fb}, false},
		{"a lower occurrence under not-join", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?b) [?a :page/next ?b] (not-join [?a ?b] (near ?b ?a))]
		    [(far ?a ?c) [?a :page/next ?b] (far ?b ?c) (not-join [?a ?c] (near ?c ?a))]]`, []query.Symbol{bb, fb}, false},
		{"a lower occurrence under or-default-join", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?k) [?a :page/next ?b] (or-default-join [[?a] ?k] (near ?a ?k) [(ground 0) ?k])]
		    [(far ?a ?c) [?a :page/next ?b] (far ?b ?c)]]`, []query.Symbol{bf}, false},
		{"a lower occurrence under or-default", `[[(near ?a ?b) [?a :page/next ?b]]
		    [(far ?a ?k) [?a :page/next ?b] (or-default (near ?a ?k) [(ground 0) ?k])]
		    [(far ?a ?c) [?a :page/next ?b] (far ?b ?c)]]`, []query.Symbol{bf}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rs, err := parser.ParseRules(c.rules)
			require.NoError(t, err)
			p := NewClauseBasedPlanner(nil, PlannerOptions{})
			magic, err := Rewrite(p, rs, far, []bool{true, false}, nil)
			require.NoError(t, err)
			adornedFar := query.AdornedSymbol(far, []bool{true, false})
			invoked := map[query.Symbol]bool{}
			for _, rule := range magic.Rules.Rules(adornedFar) {
				for _, inv := range query.Invocations(rule.Body) {
					invoked[inv.Name] = true
				}
			}
			for _, adorned := range c.near {
				require.True(t, invoked[adorned], "the bodies of %s invoke %s", adornedFar, adorned)
			}
			onNear := false
			for _, stratum := range magic.Rules.Strata() {
				bodies, err := PlanStratum(p, magic.Rules, stratum, nil)
				require.NoError(t, err)
				for _, body := range bodies.Lower {
					var invokes []query.Symbol
					for _, inv := range query.Invocations(body.Plan.Query.Where) {
						if query.IsDeltaSymbol(inv.Name) {
							invokes = append(invokes, inv.Name)
						}
					}
					if body.Marked == nil || len(invokes) != 1 || invokes[0] != query.DeltaSymbol(body.Marked) {
						t.Errorf("the lower copy of %s is marked on %q and invokes the deltas %v", body.Rule.Name, body.Marked, invokes)
					}
					if body.Rule.Name == adornedFar && query.ContainsSymbol(c.near, body.Marked) {
						onNear = true
					}
				}
			}
			require.Equal(t, c.copy, onNear, "a lower copy of %s marked on an adornment of near", adornedFar)
		})
	}
}
