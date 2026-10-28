package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A keyword wider than storage holds names no stored attribute, so a pattern
// whose attribute position a binding fills with one answers nothing under it,
// on every route a binding at the attribute position takes: with the entity
// bound beside it and with the entity open, with the cache on and with it
// off, in both optimizer modes. The 32-byte attribute sharing its first 32
// bytes is never answered in its place.
func TestAKeywordWiderThanStorageHoldsBoundAtTheAttributePositionNamesNoAttribute(t *testing.T) {
	short := ":t/aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	wide := short + "z"
	require.Len(t, short, datalog.MaxAttributeBytes)

	alice := datalog.NewIdentity("wide-routes-alice")
	pointer := datalog.NewIdentity("wide-routes-pointer")
	queries := []struct {
		name string
		text string
	}{
		{"a ground, the entity bound", `[:find ?e ?v :where [?e :t/name "alice"] [(ground ` + wide + `) ?a] [?e ?a ?v]]`},
		{"a ground, the entity open", `[:find ?e ?v :where [(ground ` + wide + `) ?a] [?e ?a ?v]]`},
		{"a stored keyword value, the entity bound", `[:find ?e ?v :where [?p :t/names ?a] [?p :t/target ?e] [?e ?a ?v]]`},
		{"a stored keyword value, the entity open", `[:find ?e ?v :where [?p :t/names ?a] [?e ?a ?v]]`},
	}
	caches := []struct {
		name    string
		options []db.Option
	}{
		{"the cache on", nil},
		{"the cache off", []db.Option{db.WithoutCache()}},
	}
	for _, mode := range optimizerModes {
		for _, cache := range caches {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				options := append([]db.Option{db.WithPlannerOptions(mode.plannerOptions())}, cache.options...)
				d := tempDB(t, mode.backend, options...)
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
				require.NoError(t, tx.Add(alice, datalog.NewKeyword(short), "short"))
				require.NoError(t, tx.Add(pointer, datalog.NewKeyword(":t/name"), "pointer"))
				require.NoError(t, tx.Add(pointer, datalog.NewKeyword(":t/names"), datalog.NewKeyword(wide)))
				require.NoError(t, tx.Add(pointer, datalog.NewKeyword(":t/target"), alice))
				_, err := tx.Commit()
				require.NoError(t, err)

				for _, q := range queries {
					t.Run(q.name, func(t *testing.T) {
						answered, err := executor.CollectTuples(d.Query(q.text))
						require.NoError(t, err, q.text)
						require.Empty(t, answered, "no attribute named %s is stored, so %s answers nothing", wide, q.text)
					})
				}
			})
		}
	}
}
