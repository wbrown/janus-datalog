package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A vector literal at the value position of a pattern whose entity is bound
// from an input and whose attribute is open answers the attributes under
// which the bound entity's resolved collection equals it, in the current
// view, and nothing under History, where every datom is read as written.
func TestAVectorLiteralOnABoundEntityAnswersItsResolvedCollection(t *testing.T) {
	itemName := datalog.NewKeyword(":item/name")
	itemRank := datalog.NewKeyword(":item/rank")
	itemTags := datalog.NewKeyword(":item/tags")
	x := datalog.NewIdentity("item:x")
	tagged := datalog.NewIdentity("item:t")

	cases := []struct {
		entity  datalog.Identity
		history bool
		want    []datalog.Keyword
	}{
		{x, false, nil},
		{tagged, false, []datalog.Keyword{itemTags}},
		{x, true, nil},
		{tagged, true, nil},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":item/name").Type(schema.TypeString).Add().
				Attribute(":item/rank").Type(schema.TypeLong).Add().
				Attribute(":item/tags").Type(schema.TypeLong).Vector().Add().
				Build()
			require.NoError(t, err)
			d := tempDB(t, mode.backend, db.WithSchema(s), db.WithPlannerOptions(mode.plannerOptions()))
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(x, itemName, "x"))
			require.NoError(t, tx.Add(x, itemRank, int64(1)))
			require.NoError(t, tx.Add(tagged, itemName, "t"))
			require.NoError(t, tx.Set(tagged, itemTags, []int64{1, 2}))
			_, err = tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				var names []string
				require.NoError(t, d.QueryInto(&names, `[:find ?n :in $ ?e :where [?e :item/name ?n]]`, c.entity))
				h := d
				if c.history {
					h = d.History()
				}
				tuples, err := executor.CollectTuples(h.Query(`[:find ?a :in $ ?e :where [?e ?a [1 2]]]`, c.entity))
				require.NoError(t, err)
				var got []datalog.Keyword
				for _, tuple := range tuples {
					got = append(got, tuple[0].(datalog.Keyword))
				}
				require.ElementsMatchf(t, c.want, got, "entity %v history=%v answered %v", names, c.history, got)
			}
		})
	}
}
