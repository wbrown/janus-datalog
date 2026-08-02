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
			// Registered at open: everything the database builds is constructed
			// with it. The loop below filters by event name and only logs, so the
			// fixture's own events are harmless here.
			var events []annotations.Event
			db := createOptimizerModeDB(t, mode, DatabaseOptions{
				DisableCache: true, // cache_disabled
				AnnotationHandler: func(e annotations.Event) {
					events = append(events, e)
				},
			})
			db.SetSchema(eaCacheBypassSchema())

			person1 := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")
			contentAttr := datalog.NewKeyword(":doc/content")

			tx := db.NewTransaction()
			tx.Set(person1, nameAttr, "Alice")
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			tx2.Set(person1, contentAttr, []interface{}{"a", "b", "c"})
			_, err = tx2.Commit()
			require.NoError(t, err)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person1, contentAttr},
				}))
			require.NoError(t, err)

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
