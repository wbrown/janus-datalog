package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// dbIDPulled is a struct whose ID field is tagged with the engine's word for
// the entity's id, read through PullInto.
type dbIDPulled struct {
	ID   datalog.Identity `datalog:"db/id"`
	Name string           `datalog:"t/name"`
}

// A pull answers the entity under db/id when its pattern names :db/id or
// holds a wildcard, at every entry point and at every depth of a nested
// pull, and answers none when the pattern names neither. A wildcard's id
// rides beside the attributes the entity carries, so an identity carrying
// nothing answers nil under a wildcard, and an explicit :db/id is answered
// for any identity.
func TestPull_DBIDAnswersTheEntityAtEveryEntryPoint(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(":t/name").Type(schema.TypeString).Add().
				Attribute(":t/ref").Type(schema.TypeRef).Add().
				Build()
			require.NoError(t, err)
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			a, b, c := datalog.NewIdentity("dbid:a"), datalog.NewIdentity("dbid:b"), datalog.NewIdentity("dbid:c")
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(a, kw(":t/name"), "n"))
			require.NoError(t, tx.Add(a, kw(":t/ref"), b))
			require.NoError(t, tx.Add(b, kw(":t/name"), "m"))
			_, err = tx.Commit()
			require.NoError(t, err)

			named, err := d.Pull(a, `[:db/id :t/name]`)
			require.NoError(t, err)
			require.Equal(t, a, named[query.DBIDKey], "Database.Pull with :db/id named answers the entity")
			require.Equal(t, "n", named["t/name"])

			wildcard, err := d.Pull(a, `[*]`)
			require.NoError(t, err)
			require.Equal(t, a, wildcard[query.DBIDKey], "a wildcard answers the id beside the carried attributes")
			require.Equal(t, "n", wildcard["t/name"])
			require.Equal(t, b, wildcard["t/ref"])

			unnamed, err := d.Pull(a, `[:t/name]`)
			require.NoError(t, err)
			require.NotContains(t, unnamed, query.DBIDKey, "a pattern naming neither :db/id nor * answers no id")
			require.Equal(t, "n", unnamed["t/name"])

			nested, err := d.Pull(a, `[:t/name {:t/ref [:db/id :t/name]}]`)
			require.NoError(t, err)
			require.NotContains(t, nested, query.DBIDKey, "the outer pattern names no id")
			referent, ok := nested["t/ref"].(map[string]interface{})
			require.True(t, ok, "the nested pull is a map")
			require.Equal(t, b, referent[query.DBIDKey], "the referent's id at the nested depth")
			require.Equal(t, "m", referent["t/name"])

			bare, err := d.Pull(c, `[:db/id :t/name]`)
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{query.DBIDKey: c}, bare, "an identity carrying nothing answers its id alone when the pattern names it")
			nothing, err := d.Pull(c, `[*]`)
			require.NoError(t, err)
			require.Nil(t, nothing, "and nil under a wildcard, which reads what the entity carries")

			findNamed, err := executor.CollectTuples(d.Query(`[:find (pull ?e [:db/id :t/name]) :where [?e :t/name "n"]]`))
			require.NoError(t, err)
			require.Len(t, findNamed, 1)
			require.Equal(t, a, findNamed[0][0].(map[string]interface{})[query.DBIDKey], "a find expression naming :db/id answers the entity")

			findUnnamed, err := executor.CollectTuples(d.Query(`[:find (pull ?e [:t/name]) :where [?e :t/name "n"]]`))
			require.NoError(t, err)
			require.Len(t, findUnnamed, 1)
			require.NotContains(t, findUnnamed[0][0].(map[string]interface{}), query.DBIDKey, "a find expression naming neither :db/id nor * answers no id")

			findWildcard, err := executor.CollectTuples(d.Query(`[:find (pull ?e [*]) :where [?e :t/name _]]`))
			require.NoError(t, err)
			require.Len(t, findWildcard, 2)
			byName := map[interface{}]datalog.Identity{"n": a, "m": b}
			for _, tuple := range findWildcard {
				pulled := tuple[0].(map[string]interface{})
				require.Equal(t, byName[pulled["t/name"]], pulled[query.DBIDKey], "a find expression's wildcard answers each entity its own id through the batch path")
			}

			var into dbIDPulled
			require.NoError(t, d.PullInto(a, &into))
			require.Equal(t, a, into.ID, "PullInto sets the field tagged db/id")
			require.Equal(t, "n", into.Name)
		})
	}
}
