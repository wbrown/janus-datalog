package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Holder asserts :item/code "y", then "x"; claimant asserts "x" after it. The
// unique rule's walk settles holder's value at "y": [?e :item/code "y"]
// answers holder. A free-attribute match of the constant "y" answers holder
// on every entry point that reads the pattern: $ directly, a named source, a
// rule body, a not-join and an or-join, in the latest view and as of the
// claimant's commit, with the EA cache on and off.
func TestRefuteAFreeAttributeUniqueValueOnEveryEntryPoint(t *testing.T) {
	code := datalog.NewKeyword(":item/code")
	name := datalog.NewKeyword(":item/name")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: code, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne, Unique: schema.UniqueIdentity})
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	holder := datalog.NewIdentity("item:holder")
	claimant := datalog.NewIdentity("item:claimant")

	for _, mode := range legsFor(optimizerModes, fusionOption) {
		for _, cacheMode := range cacheTestModes {
			t.Run(mode.name+"/"+cacheMode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cacheMode.disableCache})
				tx := db.NewTransaction()
				require.NoError(t, tx.Add(holder, name, "holder"))
				require.NoError(t, tx.Add(claimant, name, "claimant"))
				require.NoError(t, tx.Add(holder, code, "y"))
				_, err := tx.Commit()
				require.NoError(t, err)
				tx2 := db.NewTransaction()
				require.NoError(t, tx2.Add(holder, code, "x"))
				_, err = tx2.Commit()
				require.NoError(t, err)
				tx3 := db.NewTransaction()
				require.NoError(t, tx3.Add(claimant, code, "x"))
				third, err := tx3.Commit()
				require.NoError(t, err)

				for _, view := range []struct {
					name string
					db   *Database
				}{{"latest", db}, {"as of the claimant's commit", db.AsOf(third)}} {
					bound, err := executor.CollectTuples(view.db.Query(`[:find ?e :where [?e :item/code "y"]]`))
					require.NoError(t, err)
					require.ElementsMatch(t, [][]interface{}{{holder}}, bound, "%s: the bound attribute answers holder", view.name)

					cases := []struct {
						name string
						run  func() ([][]interface{}, error)
					}{
						{"$", func() ([][]interface{}, error) {
							return executor.CollectTuples(view.db.Query(`[:find ?e :where [?e ?a "y"]]`))
						}},
						{"named source", func() ([][]interface{}, error) {
							return executor.CollectTuples(db.Query(`[:find ?e :in $ $past :where [$past ?e ?a "y"]]`, sourceNamed("$past", view.db)))
						}},
						{"rule body", func() ([][]interface{}, error) {
							return executor.CollectTuples(view.db.Query(`[:find ?e :in $ % :where (holds ?e)]`, mustRuleSet(t, `[[(holds ?e) [?e ?a "y"]]]`)))
						}},
						{"not-join", func() ([][]interface{}, error) {
							return executor.CollectTuples(view.db.Query(`[:find ?e :where [?e :item/name _] (not-join [?e] [?e ?a "y"]) ]`))
						}},
						{"or-join", func() ([][]interface{}, error) {
							return executor.CollectTuples(view.db.Query(`[:find ?e :where [?e :item/name _] (or-join [?e] [?e ?a "y"] [?e :item/name "nobody"])]`))
						}},
					}
					for _, c := range cases {
						got, err := c.run()
						require.NoError(t, err, "%s, %s", view.name, c.name)
						want := [][]interface{}{{holder}}
						if c.name == "not-join" {
							want = [][]interface{}{{claimant}}
						}
						assert.ElementsMatch(t, want, got, "%s, %s: answered %d tuples", view.name, c.name, len(got))
					}
				}
			})
		}
	}
}
