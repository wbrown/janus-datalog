package db

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A database opened on disk through db.Open holds :t/a, declared cardinality-one, at "old" and then
// "new" on the entity named "one". A bound invocation's body holds a
// subquery that invokes a rule and reads a get-else of :t/a on its inner $,
// which the call binds to $src, the history handle. Through Query, Analyze
// and QueryInto, the optimizer-on path answers what the optimizer-off path
// answers.
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
	answers := map[bool]map[string][][]interface{}{}
	for _, algebra := range []bool{false, true} {
		opts := storage.DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = algebra
		d, err := Open(t.TempDir(), WithSchema(s), WithPlannerOptions(opts))
		require.NoError(t, err)
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
		require.NoError(t, err, "algebra %v", algebra)
		byRoute["Query"] = queried
		analyzed, err := d.Analyze(text, rs, sources)
		require.NoError(t, err, "algebra %v", algebra)
		analyzedTuples, err := executor.CollectTuples(analyzed.Result, nil)
		require.NoError(t, err)
		byRoute["Analyze"] = analyzedTuples
		var into []answer
		require.NoError(t, d.QueryInto(&into, text, rs, sources), "algebra %v", algebra)
		var intoTuples [][]interface{}
		for _, x := range into {
			intoTuples = append(intoTuples, []interface{}{x.E, x.V})
		}
		byRoute["QueryInto"] = intoTuples
		for route := range byRoute {
			slices.SortFunc(byRoute[route], func(x, y []interface{}) int { return datalog.CompareValues(x[1], y[1]) })
		}
		answers[algebra] = byRoute
		require.NoError(t, d.Close())
	}
	differ := map[string]string{}
	for _, route := range []string{"Query", "Analyze", "QueryInto"} {
		if !reflect.DeepEqual(answers[false][route], answers[true][route]) {
			differ[route] = fmt.Sprintf("off %v, on %v", answers[false][route], answers[true][route])
		}
	}
	require.Empty(t, differ, "routes where the optimizer-on answer differs from the optimizer-off answer")
}
