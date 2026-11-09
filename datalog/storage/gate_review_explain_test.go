package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// On the algebra legs, Explain on a history handle plans a bound rule body's
// get-else as the per-tuple lookup whether or not the database ran the text
// before: the evaluated set's body holds the get-else, not the
// or-default-join the scan rewrite decompiles to.
func TestGateReview_ExplainOnAHistoryHandlePlansTheRuleBodysGetElseAsTheLookup(t *testing.T) {
	one := datalog.NewIdentity("gate-review-explain:one")
	text := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	cases := []struct {
		name        string
		latestFirst bool
	}{
		{"on a database that ran nothing before", false},
		{"after the database's Query ran the text", true},
	}
	for _, mode := range legsWhere(legsFor(optimizerModes, algebraOption, fusionOption), func(m optimizerMode) bool { return m.algebra }) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := gateReviewRuleBodyDatabase(t, mode, one)
				rs, err := parser.ParseRules(`[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`)
				require.NoError(t, err)
				if c.latestFirst {
					latest, err := gateReviewAnswer(d.Query(text, rs))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{one, "new"}}, latest)
				}
				plan, err := d.History().Explain(text, rs)
				require.NoError(t, err)
				var fallbacks int
				for _, evaluation := range plan.Evaluations {
					for _, rule := range evaluation.Magic.Rules.Rules(evaluation.Magic.Predicate) {
						for _, clause := range rule.Body {
							if _, isFallback := clause.(*query.OrDefaultJoinClause); isFallback {
								fallbacks++
							}
						}
					}
				}
				require.Zero(t, fallbacks, "the evaluated body holds no or-default-join on the history handle")
			})
		}
	}
}
