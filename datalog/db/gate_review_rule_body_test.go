package db_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A memory database runs a bound rule invocation's text on its latest view
// and then on its history handle. The second run answers the get-else's
// lookup of the latest value on the reference leg, and every leg answers
// what the reference leg answers.
func TestGateReview_AMemoryDatabasesHistoryHandleAnswersARuleBodysGetElseAsTheLookup(t *testing.T) {
	one := datalog.NewIdentity("gate-review-db:one")
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	text := `[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`
	// answers is what the text answers on a mode, on the latest view and on
	// the history handle.
	answers := func(t *testing.T, mode optimizerMode) (latest, history [][]interface{}) {
		d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(mode.plannerOptions()))
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
		rs, err := parser.ParseRules(`[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`)
		require.NoError(t, err)
		latest, err = executor.CollectTuples(d.Query(text, rs))
		require.NoError(t, err)
		history, err = executor.CollectTuples(d.History().Query(text, rs))
		require.NoError(t, err)
		slices.SortFunc(history, func(x, y []interface{}) int { return datalog.CompareValues(x[1], y[1]) })
		return latest, history
	}
	referenceLatest, referenceHistory := answers(t, referenceMode)
	require.Equal(t, [][]interface{}{{one, "new"}}, referenceLatest, "the reference leg's latest view")
	require.Equal(t, [][]interface{}{{one, "new"}}, referenceHistory, "the reference leg's history handle answers the lookup of the latest value")
	for _, mode := range legsFor(t, optimizerModes, algebraOption, fusionOption) {
		t.Run(mode.name, func(t *testing.T) {
			latest, history := answers(t, mode)
			require.Equal(t, referenceLatest, latest, "the latest view answers what the reference leg answers")
			require.Equal(t, referenceHistory, history, "the history handle answers what the reference leg answers")
		})
	}
}
