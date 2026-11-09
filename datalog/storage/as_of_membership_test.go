package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// In an as-of view, a cardinality-many value match answers the membership as
// of the view's target: a member withdrawn after the target is a member, and
// one added after it is not. The entity free and bound, with the EA cache on
// and off.
func TestAnAsOfMembershipMatchAnswersTheMembershipAsOfItsTarget(t *testing.T) {
	many := datalog.NewKeyword(":item/many")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: many, ValueType: schema.TypeString, Cardinality: schema.CardinalityMany})

	seeds := map[string]datalog.Identity{}
	for _, seed := range []string{"member:held", "member:withdrawn", "member:added"} {
		seeds[seed] = datalog.NewIdentity(seed)
	}
	held, withdrawn, added := seeds["member:held"], seeds["member:withdrawn"], seeds["member:added"]

	for _, mode := range optimizerModes {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(held, many, "x"))
				require.NoError(t, tx.Add(withdrawn, many, "x"))
				first, err := tx.Commit()
				require.NoError(t, err)
				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Remove(withdrawn, many, "x"))
				require.NoError(t, tx2.Add(added, many, "x"))
				_, err = tx2.Commit()
				require.NoError(t, err)

				views := []struct {
					name    string
					db      *Database
					members map[datalog.Identity]bool
				}{
					{"latest", db, map[datalog.Identity]bool{held: true, added: true}},
					{"as of the first commit", db.AsOf(first), map[datalog.Identity]bool{held: true, withdrawn: true}},
				}
				for _, view := range views {
					require.Equal(t, view.members, entitySet(t, view.db, `[:find ?e :where [?e :item/many "x"]]`),
						"%s: the entity-free match answers the members as of the view", view.name)
					for seed, e := range seeds {
						tuples, err := executor.CollectTuples(view.db.Query(
							fmt.Sprintf(`[:find ?k :where [#id %q :item/many "x"] [(ground :member) ?k]]`, seed)))
						require.NoError(t, err)
						require.Equal(t, view.members[e], len(tuples) == 1,
							"%s: the entity-bound match answers whether %s holds \"x\" as of the view", view.name, seed)
					}
				}
			})
		}
	}
}
