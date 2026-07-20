package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

func TestCountRepro_WithVector(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           t.TempDir(),
				DisableCache:   true, // cache_disabled
				PlannerOptions: &popts,
			})
			require.NoError(t, err)
			defer db.Close()
			db.SetSchema(eaCacheBypassSchema())

			person1 := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")
			contentAttr := datalog.NewKeyword(":doc/content")

			tx := db.NewTransaction()
			tx.Set(person1, nameAttr, "Alice")
			_, err = tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			tx2.Set(person1, contentAttr, []interface{}{"a", "b", "c"})
			_, err = tx2.Commit()
			require.NoError(t, err)

			var events []annotations.Event
			db.SetAnnotationHandler(func(e annotations.Event) {
				events = append(events, e)
			})

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person1, contentAttr},
				}))
			require.NoError(t, err)

			db.SetAnnotationHandler(nil)

			t.Logf("Got %d results: %v", len(results), results)
			for _, e := range events {
				if e.Name == "pattern/hash-join-complete" || e.Name == "matches->relations" ||
					e.Name == "storage/reuse-strategy" || e.Name == "storage/join-strategy" {
					t.Logf("EVENT: %s %v", e.Name, e.Data)
				}
			}
			require.Len(t, results, 2, "Should get 2 results")
		})
	}
}
