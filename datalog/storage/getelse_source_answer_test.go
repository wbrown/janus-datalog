package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database passed as a named source answers, through the router, whether a
// match on it answers its lookup's one value per entity, as the matcher it
// mints answers: true at an attribute it declares cardinality-one, false at
// one it declares cardinality-many, and false on its history handle.
func TestADatabasePassedAsANamedSourceAnswersWhetherAMatchAnswersItsLookup(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			outer := createOptimizerModeDB(t, mode, DatabaseOptions{})
			one := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityOne)})
			many := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityMany)})
			oneSource := datalog.NewSymbol("$one")
			manySource := datalog.NewSymbol("$many")
			historySource := datalog.NewSymbol("$history")
			router := executor.NewSourceRouter(map[query.Symbol]executor.PatternMatcher{
				datalog.SymDollar: outer,
				oneSource:         one,
				manySource:        many,
				historySource:     one.History(),
			})

			require.True(t, router.CanFuseAttributeFetch(oneSource, kw(":t/a")),
				"a database passed as a named source, at an attribute it declares cardinality-one")
			require.False(t, router.CanFuseAttributeFetch(manySource, kw(":t/a")),
				"a database passed as a named source, at an attribute it declares cardinality-many")
			require.False(t, router.CanFuseAttributeFetch(historySource, kw(":t/a")),
				"a database's history handle passed as a named source")
		})
	}
}

// ExplainAlgebra of a get-else on a history handle passed as a named source
// records the get-else scan rewrite declined, with its reason. The handle's
// match answers every datom at the attribute, and its lookup answers the
// latest value.
func TestExplainAlgebraDeclinesTheGetElseRewriteOnAHistoryHandle(t *testing.T) {
	for _, mode := range pinned(optimizerModes, true) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityOne)})

			expl, err := db.ExplainAlgebra(
				`[:find ?e ?v :in $ $h :where [?e :t/name _] [(get-else $h ?e :t/a "none") ?v]]`,
				WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$h"): db.History()}))
			require.NoError(t, err)

			var declined []string
			for _, r := range expl.Rewrites {
				if r.Pass == "get-else-scan-rewrite" && r.Action == algebra.RewriteDeclined {
					declined = append(declined, r.Reason)
				}
			}
			require.Equal(t, []string{"the source's match does not answer its lookup's one value per entity"}, declined,
				"rewrites=%v", expl.Rewrites)
		})
	}
}

// Explain and Analyze plan with the query's WithSources: for a get-else on a
// history handle passed as a named source, the plan each returns keeps the
// get-else as a per-tuple lookup, as the plan the query runs does.
func TestExplainAndAnalyzePlanWithTheQuerysSources(t *testing.T) {
	text := `[:find ?e ?v :in $ $h :where [?e :t/name _] [(get-else $h ?e :t/a "none") ?v]]`
	for _, mode := range pinned(optimizerModes, true) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: getElseFixtureSchema(schema.CardinalityOne)})
			sources := WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$h"): db.History()})

			explained, err := db.Explain(text, sources)
			require.NoError(t, err)
			require.Equal(t, [2]int{1, 0}, planGetElseForms(explained),
				"Explain's plan keeps the get-else as a lookup")

			analyzed, err := db.Analyze(text, sources)
			require.NoError(t, err)
			require.Equal(t, [2]int{1, 0}, planGetElseForms(analyzed.Plan),
				"Analyze's plan keeps the get-else as a lookup")
		})
	}
}

// planGetElseForms counts, among a plan's phase clauses, the get-else
// expressions a declined rewrite leaves as per-tuple lookups and the
// or-default-joins an applied rewrite decompiles to.
func planGetElseForms(plan *planner.RealizedPlan) [2]int {
	var lookups, rewritten int
	for _, phase := range plan.Phases {
		for _, clause := range phase.Query.Where {
			switch typed := clause.(type) {
			case *query.Expression:
				if _, isGetElse := typed.Function.(*query.GetElseFunction); isGetElse {
					lookups++
				}
			case *query.OrDefaultJoinClause:
				rewritten++
			}
		}
	}
	return [2]int{lookups, rewritten}
}
