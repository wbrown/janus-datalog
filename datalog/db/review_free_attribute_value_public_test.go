package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Through db.OpenMemory, d.Query and d.AsOf: a constant value with a free
// attribute answers the entity and attribute exactly where the attribute's
// own match does. A unique attribute's value is what the unique walk settles
// (pull answers it), and a transaction constant selects the assertion it
// names. With the EA cache on and off.
func TestReviewAConstantValueWithAFreeAttributeThroughThePublicAPI(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	one := datalog.NewKeyword(":item/one")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: code, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	s.Add(&schema.AttributeDefinition{Ident: one, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})

	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	caches := []struct {
		name string
		opts []db.Option
	}{{"cache", []db.Option{db.WithSchema(s)}}, {"without cache", []db.Option{db.WithSchema(s), db.WithoutCache()}}}
	for _, mode := range legsFor(t, optimizerModes) {
		for _, cache := range caches {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				d, err := db.OpenMemory(append([]db.Option{db.WithPlannerOptions(mode.plannerOptions())}, cache.opts...)...)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()

				tx := d.NewTransaction()
				require.NoError(t, tx.Add(holder, code, "y"))
				require.NoError(t, tx.Add(holder, one, "x"))
				_, err = tx.Commit()
				require.NoError(t, err)
				tx2 := d.NewTransaction()
				require.NoError(t, tx2.Add(holder, code, "x"))
				require.NoError(t, tx2.Add(claimant, one, "x"))
				_, err = tx2.Commit()
				require.NoError(t, err)
				tx3 := d.NewTransaction()
				require.NoError(t, tx3.Add(claimant, code, "x"))
				target, err := tx3.Commit()
				require.NoError(t, err)

				asserted, err := executor.CollectTuples(d.Query(`[:find ?t :where [#id "item:holder" :item/one "x" ?t]]`))
				require.NoError(t, err)
				require.Len(t, asserted, 1)
				lamport := asserted[0][0].(*datalog.ElementID).Lamport

				for _, view := range []struct {
					name string
					d    *db.DB
				}{{"latest", d}, {"as of the last commit", d.AsOf(target)}} {
					pulled, err := view.d.Pull(holder, "[:item/code]")
					require.NoError(t, err)
					require.Equal(t, "y", pulled["item/code"], "%s: pull answers the value the unique walk settles", view.name)

					bound, err := executor.CollectTuples(view.d.Query(`[:find ?e :where [?e :item/code "y"]]`))
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{holder}}, bound, "%s: [?e :item/code \"y\"]", view.name)
					free, err := executor.CollectTuples(view.d.Query(`[:find ?e ?a :where [?e ?a "y"]]`))
					require.NoError(t, err)
					assert.ElementsMatch(t, [][]interface{}{{holder, code}}, free, "%s: [?e ?a \"y\"] answers holder under :item/code as [?e :item/code \"y\"] does", view.name)

					e, a := qb.NewVar("e"), qb.NewVar("a")
					built := qb.Query().Find(e, a).Where(qb.Pat(e, a, qb.V("y"))).MustBuild()
					fromBuilder, err := executor.CollectTuples(view.d.Query(built))
					require.NoError(t, err)
					assert.ElementsMatch(t, [][]interface{}{{holder, code}}, fromBuilder, "%s: the builder's [?e ?a \"y\"] answers holder under :item/code", view.name)

					selected, err := executor.CollectTuples(view.d.Query(fmt.Sprintf(`[:find ?e ?a :where [?e ?a "x" %d]]`, lamport)))
					require.NoError(t, err)
					assert.ElementsMatch(t, [][]interface{}{{holder, one}}, selected, "%s: [?e ?a \"x\" %d] answers the assertion the transaction names alone", view.name, lamport)
				}
			})
		}
	}
}
