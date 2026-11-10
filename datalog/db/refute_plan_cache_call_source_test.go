package db_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A database opened on disk through db.Open holds :t/a, declared
// cardinality-one, at "old" and then "new" on the entity named "one". A bound
// invocation's body holds a subquery that invokes a rule and reads a get-else
// of :t/a on its inner $, which the call binds to $src, the history handle.
// Through Query, Analyze and QueryInto, every leg answers what the reference
// leg answers.
func TestRefutePlanCache_AnOnDiskDatabasesRuleBodySubqueryReadsTheHistoryHandleItsCallPasses(t *testing.T) {
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	one := datalog.NewIdentity("refute-plan-cache-db:one")
	text := `[:find ?e ?v :in $ $src % :where [?e :t/name _] (r ?e ?v)]`
	rules := `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $src % "one") [[?v]]]] [(s ?y) [?y :t/name _]]]`
	type answer struct {
		E datalog.Identity `datalog:"?e"`
		V string           `datalog:"?v"`
	}
	routes := []string{"Query", "Analyze", "QueryInto"}
	// answers is what the text answers on a mode through each route, on a
	// database opened on disk, each route's tuples sorted by value.
	answers := func(t *testing.T, mode optimizerMode) map[string][][]interface{} {
		d, err := db.Open(t.TempDir(), db.WithSchema(s), db.WithPlannerOptions(mode.plannerOptions()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(one, name, "one"))
		require.NoError(t, tx.Add(one, a, "old"))
		_, err = tx.Commit()
		require.NoError(t, err)
		tx = d.NewTransaction()
		require.NoError(t, tx.Add(one, a, "new"))
		_, err = tx.Commit()
		require.NoError(t, err)
		rs, err := parser.ParseRules(rules)
		require.NoError(t, err)
		sources := storage.WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): d.History()})
		byRoute := map[string][][]interface{}{}
		queried, err := executor.CollectTuples(d.Query(text, rs, sources))
		require.NoError(t, err)
		byRoute["Query"] = queried
		analyzed, err := d.Analyze(text, rs, sources)
		require.NoError(t, err)
		analyzedTuples, err := executor.CollectTuples(analyzed.Result, nil)
		require.NoError(t, err)
		byRoute["Analyze"] = analyzedTuples
		var into []answer
		require.NoError(t, d.QueryInto(&into, text, rs, sources))
		var intoTuples [][]interface{}
		for _, x := range into {
			intoTuples = append(intoTuples, []interface{}{x.E, x.V})
		}
		byRoute["QueryInto"] = intoTuples
		for route := range byRoute {
			slices.SortFunc(byRoute[route], func(x, y []interface{}) int { return datalog.CompareValues(x[1], y[1]) })
		}
		return byRoute
	}
	reference := answers(t, referenceMode)
	for _, mode := range legsFor(t, optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			got := answers(t, mode)
			for _, route := range routes {
				require.Equal(t, reference[route], got[route], "%s answers what the reference leg answers", route)
			}
		})
	}
}
