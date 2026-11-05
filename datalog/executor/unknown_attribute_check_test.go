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

// The vocabulary check asks each source the query names an attribute on,
// whether or not its :in declares the source: a get-else and a data pattern
// on $other, which the :in does not declare, are refused there, and of two
// undeclared sources the one the query names first is refused first.
func TestRefuseUnknownAttributes_AsksEverySourceTheQueryNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		text    string
		refused string
	}{
		{"a get-else on an undeclared source", `[:find ?v :where [?e :t/known ?v] [(get-else $other ?e :t/strnig "x") ?w]]`, ":t/strnig"},
		{"a data pattern on an undeclared source", `[:find ?v :where [?e :t/known ?v] [$other ?e :t/strnig ?w]]`, ":t/strnig"},
		{"two undeclared sources", `[:find ?v :where [?e :t/known ?v] [$b ?e :t/strnig-b ?w] [$a ?e :t/strnig-a ?x]]`, ":t/strnig-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tc.text)
			require.NoError(t, err)
			plan := &planner.RealizedPlan{Query: q, Rosters: query.CollectRosters(q)}
			err = refuseUnknownAttributes(declaredVocabulary{datalog.NewKeyword(":t/known")}, NewContext(), plan)
			var unknown *schema.UnknownAttributeError
			require.ErrorAs(t, err, &unknown, "the query names %s on a source its :in does not declare", tc.refused)
			require.Same(t, datalog.NewKeyword(tc.refused), unknown.Attribute)
		})
	}
}

// The vocabulary check asks each source the rule set names an attribute on,
// though the query names nothing there.
func TestRefuseUnknownAttributes_AsksEverySourceTheRuleSetNames(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?v :in $ % :where [?e :t/known ?v] (probe ?e ?w)]`)
	require.NoError(t, err)
	rs, err := parser.ParseRules(`[[(probe ?e ?w) [?e :t/known _] [(get-else $other ?e :t/strnig "x") ?w]]]`)
	require.NoError(t, err)
	plan := &planner.RealizedPlan{Query: q, Rosters: query.CollectRosters(q)}
	err = refuseUnknownAttributes(declaredVocabulary{datalog.NewKeyword(":t/known")}, NewContext().WithRules(rs), plan)
	var unknown *schema.UnknownAttributeError
	require.ErrorAs(t, err, &unknown, "the rule set names :t/strnig on $other")
	require.Same(t, datalog.NewKeyword(":t/strnig"), unknown.Attribute)
}

// refusingSource is a source answering patterns through the matcher it
// embeds and refusing every attribute outside its declared vocabulary.
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
	refusing := refusingSource{nil, declaredVocabulary{datalog.NewKeyword(":t/known")}}
	var plain PatternMatcher
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
