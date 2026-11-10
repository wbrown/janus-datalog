package db_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Through db.OpenMemory, d.Snapshot, d.TruncateTo and d.Query: holder asserts
// the unique value "y", the database snapshots, and claimant asserts "y"
// after it. Each form is asked once. TruncateTo the snapshot erases
// claimant's assertion, so holder holds "y" under its own assertion, as the
// attribute scan and the database with the EA cache off answer; every form
// answers it with the cache on as well.
func TestRefuteAUniqueValueAfterTruncationThroughThePublicAPI(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: code, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	for _, mode := range legsFor(t, optimizerModes, fusionOption) {
		for _, cache := range []struct {
			name string
			opts []db.Option
		}{{"cache", []db.Option{db.WithSchema(s)}}, {"without cache", []db.Option{db.WithSchema(s), db.WithoutCache()}}} {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				d, err := db.OpenMemory(append([]db.Option{db.WithPlannerOptions(mode.plannerOptions())}, cache.opts...)...)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				tx := d.NewTransaction()
				require.NoError(t, tx.Add(holder, code, "y"))
				_, err = tx.Commit()
				require.NoError(t, err)
				_, err = d.Snapshot("before the claim")
				require.NoError(t, err)
				asserted, err := executor.CollectTuples(d.History().Query(`[:find ?t :where [#id "item:holder" :item/code "y" ?t]]`))
				require.NoError(t, err)
				require.Len(t, asserted, 1)
				holderTx := asserted[0][0]

				tx2 := d.NewTransaction()
				require.NoError(t, tx2.Add(claimant, code, "y"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				forms := []struct {
					text string
					args []interface{}
					want [][]interface{}
				}{
					{`[:find ?e ?a :where [?e ?a "y"]]`, nil, [][]interface{}{{holder, code}}},
					{`[:find ?e ?a :in $ ?v :where [?e ?a ?v]]`, []interface{}{"y"}, [][]interface{}{{holder, code}}},
					{`[:find ?e ?a ?v :in $ ?t :where [?e ?a ?v ?t]]`, []interface{}{holderTx}, [][]interface{}{{holder, code, "y"}}},
					{`[:find ?e ?v :in $ ?t :where [?e :item/code ?v ?t]]`, []interface{}{holderTx}, [][]interface{}{{holder, "y"}}},
					{`[:find ?e :where [?e :item/code "y"]]`, nil, [][]interface{}{{holder}}},
					{`[:find ?v :in $ ?e :where [?e :item/code ?v]]`, []interface{}{holder}, [][]interface{}{{"y"}}},
					{`[:find ?e ?v :where [?e :item/code ?v]]`, nil, [][]interface{}{{holder, "y"}}},
				}
				for _, f := range forms {
					_, err := executor.CollectTuples(d.Query(f.text, f.args...))
					require.NoError(t, err, f.text)
				}

				require.NoError(t, d.TruncateTo("before the claim"))

				for _, f := range forms {
					got, err := executor.CollectTuples(d.Query(f.text, f.args...))
					require.NoError(t, err, f.text)
					assert.ElementsMatch(t, f.want, got, "after the truncation: %s", f.text)
				}
			})
		}
	}
}
