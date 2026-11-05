package db

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A memory database runs a bound rule invocation's text on its latest view
// and then on its history handle. On the optimizer-on path the second run
// answers what the optimizer-off path answers, the get-else's lookup of the
// latest value.
func TestGateReview_AMemoryDatabasesHistoryHandleAnswersARuleBodysGetElseAsTheLookup(t *testing.T) {
	one := datalog.NewIdentity("gate-review-db:one")
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	text := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	for _, algebra := range []bool{false, true} {
		opts := storage.DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = algebra
		d, err := OpenMemory(WithSchema(s), WithPlannerOptions(opts))
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
		rs, err := parser.ParseRules(`[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`)
		require.NoError(t, err)
		latest, err := executor.CollectTuples(d.Query(text, rs))
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{one, "new"}}, latest, "algebra %v", algebra)
		history, err := executor.CollectTuples(d.History().Query(text, rs))
		require.NoError(t, err)
		slices.SortFunc(history, func(x, y []interface{}) int { return datalog.CompareValues(x[1], y[1]) })
		require.Equal(t, [][]interface{}{{one, "new"}}, history, "algebra %v", algebra)
		require.NoError(t, d.Close())
	}
}
