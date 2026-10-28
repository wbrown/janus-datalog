package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A vector at the value position under an open attribute answers the
// attributes under which the pattern's own entity carries that collection. A
// pattern that writes its entity as a constant names that entity alone: the
// bound pattern answers what the unbound pattern [?e ?a ?v] answers that
// agrees with the binding at every bound position, entity included. The
// store carries x, holding the long 1 at :item/rank, and t, holding the
// vector [1 2] at :item/tags, declared a vector of longs; each carries its
// display name at :item/name.
func TestAVectorUnderAnOpenAttributeOnAConstantEntityAnswersThatEntityAlone(t *testing.T) {
	itemName := datalog.NewKeyword(":item/name")
	itemRank := datalog.NewKeyword(":item/rank")
	itemTags := datalog.NewKeyword(":item/tags")
	x := datalog.NewIdentity("item:x")
	tagged := datalog.NewIdentity("item:t")
	vector := []interface{}{int64(1), int64(2)}

	type form int
	const (
		written form = iota // the vector written into the pattern text
		input               // the vector bound from an input
		built               // the vector a builder constant
	)
	formNames := map[form]string{written: "written", input: "input", built: "builder"}

	cases := []struct {
		entity datalog.Identity
		form   form
		want   []datalog.Keyword
	}{
		{x, written, nil},
		{x, input, nil},
		{x, built, nil},
		{tagged, written, []datalog.Keyword{itemTags}},
		{tagged, input, []datalog.Keyword{itemTags}},
		{tagged, built, []datalog.Keyword{itemTags}},
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
			committed, err := tx.Commit()
			require.NoError(t, err)

			views := []struct {
				name string
				h    *db.DB
			}{
				{"current", d},
				{"as-of", d.AsOf(committed)},
			}
			for _, view := range views {
				t.Run(view.name, func(t *testing.T) {
					for _, c := range cases {
						var names []string
						require.NoError(t, d.QueryInto(&names, `[:find ?n :in $ ?e :where [?e :item/name ?n]]`, c.entity))
						t.Run(fmt.Sprintf("%v/%s", names, formNames[c.form]), func(t *testing.T) {
							var rel executor.Relation
							var err error
							switch c.form {
							case written:
								rel, err = view.h.Query(fmt.Sprintf(`[:find ?a :where [#identity "%s" ?a [1 2]]]`, c.entity.L85()))
							case input:
								rel, err = view.h.Query(fmt.Sprintf(`[:find ?a :in $ ?v :where [#identity "%s" ?a ?v]]`, c.entity.L85()), vector)
							case built:
								a := qb.NewVar("a")
								q, buildErr := qb.Query().Find(a).Where(qb.Pat(c.entity, a, vector)).Build()
								require.NoError(t, buildErr)
								rel, err = view.h.Query(q)
							}
							require.NoError(t, err)
							tuples, err := executor.CollectTuples(rel, err)
							require.NoError(t, err)
							var got []datalog.Keyword
							for _, tuple := range tuples {
								kw, ok := tuple[0].(datalog.Keyword)
								require.True(t, ok, "?a binds a keyword, got %T", tuple[0])
								got = append(got, kw)
							}
							require.ElementsMatchf(t, c.want, got,
								"entity %v carries the vector under %v and answered %v", names, c.want, got)
						})
					}
				})
			}
		})
	}
}
