package executor

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// declaredVocabulary is a source declaring the attributes it holds and
// refusing every other, under any name the query gives it.
type declaredVocabulary []datalog.Keyword

func (declaredVocabulary) RefusesUnknownAttributes(query.Symbol) bool { return true }

func (v declaredVocabulary) UnknownAttributes(_ query.Symbol, attrs []datalog.Keyword) error {
	for _, attr := range attrs {
		if !slices.Contains(v, attr) {
			return &schema.UnknownAttributeError{Attribute: attr}
		}
	}
	return nil
}

// The vocabulary check reads the roster the plan holds, collected once at
// planning, and walks the query again nowhere: a plan carrying another
// query's roster is refused by that roster's attribute, which the plan's own
// query never names (NESTED_SCOPES_AS_QUERIES.md §5.5, the roster paragraph).
func TestRefuseUnknownAttributes_ReadsThePlansRoster(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?v :where [?e :t/known ?v]]`)
	require.NoError(t, err)
	other, err := parser.ParseQuery(`[:find ?v :where [?e :t/strnig ?v]]`)
	require.NoError(t, err)
	plan := &planner.RealizedPlan{Query: q, Rosters: query.CollectRosters(other)}
	err = refuseUnknownAttributes(declaredVocabulary{datalog.NewKeyword(":t/known")}, NewContext(), plan)
	var unknown *schema.UnknownAttributeError
	require.ErrorAs(t, err, &unknown, "the plan's roster names :t/strnig, and the check reads the plan's roster")
	require.Same(t, datalog.NewKeyword(":t/strnig"), unknown.Attribute)
}

// refusingSource is a source answering patterns from an in-memory matcher
// and refusing every attribute outside its declared vocabulary.
type refusingSource struct {
	PatternMatcher
	declaredVocabulary
}

// A matcher routes to a source that refuses unknown attributes when it is a
// router holding a refusing source at $ or at a name, or a refusing source
// itself. A router of sources without the capability, and a matcher without
// it, route to none.
func TestAnySourceRefusesUnknownAttributes(t *testing.T) {
	other := datalog.NewSymbol("$other")
	refusing := refusingSource{NewMemoryPatternMatcher(nil), declaredVocabulary{datalog.NewKeyword(":t/known")}}
	plain := NewMemoryPatternMatcher(nil)
	require.True(t, anySourceRefusesUnknownAttributes(refusing), "a refusing source asked as itself")
	require.False(t, anySourceRefusesUnknownAttributes(plain), "a matcher without the capability")
	atDefault := NewSourceRouter(map[query.Symbol]PatternMatcher{datalog.SymDollar: refusing, other: plain})
	atName := NewSourceRouter(map[query.Symbol]PatternMatcher{datalog.SymDollar: plain, other: refusing})
	none := NewSourceRouter(map[query.Symbol]PatternMatcher{datalog.SymDollar: plain, other: plain})
	require.True(t, atDefault.refusesAnySource(), "a router asked holds a refusing source at $")
	require.True(t, atName.refusesAnySource(), "a router asked holds a refusing source at a name")
	require.False(t, none.refusesAnySource(), "a router asked holds no refusing source")
	require.True(t, anySourceRefusesUnknownAttributes(atDefault), "a router whose default source refuses")
	require.True(t, anySourceRefusesUnknownAttributes(atName), "a router whose named source refuses")
	require.False(t, anySourceRefusesUnknownAttributes(none), "a router of sources that refuse nothing")
}

// An executor over a router with a refusing source refuses an undeclared
// attribute named on that source and answers a query naming declared ones,
// and an executor over sources that refuse nothing runs any attribute
// (NESTED_SCOPES_AS_QUERIES.md §5.5, the roster paragraph).
func TestNewExecutor_CollectsARosterWhenAnySourceRefuses(t *testing.T) {
	other := datalog.NewSymbol("$other")
	known := datalog.NewKeyword(":t/known")
	refusing := func() PatternMatcher {
		return refusingSource{NewMemoryPatternMatcher(nil), declaredVocabulary{known}}
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
			map[query.Symbol]PatternMatcher{datalog.SymDollar: NewMemoryPatternMatcher(nil), other: refusing()}, typoOnOther, knownOnOther},
		{"a refusing default source",
			map[query.Symbol]PatternMatcher{datalog.SymDollar: refusing()}, typoOnDefault, knownOnDefault},
		{"sources that refuse nothing",
			map[query.Symbol]PatternMatcher{datalog.SymDollar: NewMemoryPatternMatcher(nil), other: NewMemoryPatternMatcher(nil)}, nil, typoOnOther},
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
