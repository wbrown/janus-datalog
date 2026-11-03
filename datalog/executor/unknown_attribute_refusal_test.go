package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// An executor over a router with a refusing source refuses an undeclared
// attribute named on that source and answers a query naming declared ones,
// and an executor over sources that refuse nothing runs any attribute
// (NESTED_SCOPES_AS_QUERIES.md §5.5, the roster paragraph).
func TestNewExecutor_CollectsARosterWhenAnySourceRefuses(t *testing.T) {
	other := datalog.NewSymbol("$other")
	known := datalog.NewKeyword(":t/known")
	refusing := func() PatternMatcher {
		return refusingDatabase(t, known)
	}
	parse := func(text string) *query.Query {
		q, err := parser.ParseQuery(text)
		require.NoError(t, err)
		return q
	}
	typoOnOther := parse(`[:find ?v :in $ $other :where [$other ?e :t/strnig ?v]]`)
	knownOnOther := parse(`[:find ?v :in $ $other :where [$other ?e :t/known ?v]]`)
	typoOnDefault := parse(`[:find ?v :where [?e :t/strnig ?v]]`)
	knownOnDefault := parse(`[:find ?v :where [?e :t/known ?v]]`)
	cases := []struct {
		name    string
		sources map[query.Symbol]PatternMatcher
		refused *query.Query
		runs    *query.Query
	}{
		{"a refusing named source beside a plain default source",
			map[query.Symbol]PatternMatcher{datalog.SymDollar: factsDatabase(t, nil), other: refusing()}, typoOnOther, knownOnOther},
		{"a refusing default source",
			map[query.Symbol]PatternMatcher{datalog.SymDollar: refusing()}, typoOnDefault, knownOnDefault},
		{"sources that refuse nothing",
			map[query.Symbol]PatternMatcher{datalog.SymDollar: factsDatabase(t, nil), other: factsDatabase(t, nil)}, nil, typoOnOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exec := NewExecutor(NewSourceRouter(c.sources), nil)
			if c.refused != nil {
				var refused error
				require.NotPanics(t, func() {
					_, refused = CollectTuples(exec.Execute(c.refused))
				})
				var unknown *schema.UnknownAttributeError
				require.ErrorAs(t, refused, &unknown, "the source named refuses the attribute it does not declare")
				require.Same(t, datalog.NewKeyword(":t/strnig"), unknown.Attribute)
			}
			var answered error
			require.NotPanics(t, func() {
				_, answered = CollectTuples(exec.Execute(c.runs))
			})
			require.NoError(t, answered)
		})
	}
}
