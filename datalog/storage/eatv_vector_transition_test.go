package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestEATV_VectorTransitionDropsDatom reproduces the CRDT vector group
// transition bug. When scanning EATV for an entity that has both a Vector
// attribute and a non-Vector attribute, the CRDTResolvingIterator drops the
// first datom of the non-Vector group at the cardinality boundary.
//
// Forces EATV via collection input [?e ...] with A as a free variable.
// Single bound position (E) + A not constant → EATV (matcher_strategy.go:118-122).
// Each entity's EATV scan crosses from :doc/content (Vector) to :person/name (One),
// triggering the vector group transition.
func TestEATV_VectorTransitionDropsDatom(t *testing.T) {
	db, cleanup := createEACacheTestDB(t, true) // cache disabled
	defer cleanup()

	nameAttr := datalog.NewKeyword(":person/name")
	contentAttr := datalog.NewKeyword(":doc/content")

	entities := make([]datalog.Identity, 3)
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("person-%d", i))

		tx := db.NewTransaction()
		tx.Set(entities[i], nameAttr, fmt.Sprintf("Name-%d", i))
		_, err := tx.Commit()
		require.NoError(t, err)

		tx2 := db.NewTransaction()
		tx2.Set(entities[i], contentAttr, []any{"x", "y"})
		_, err = tx2.Commit()
		require.NoError(t, err)
	}

	var events []annotations.Event
	db.SetAnnotationHandler(func(e annotations.Event) {
		events = append(events, e)
	})

	// Collection input [?e ...] — only E is bound, A is free → forces EATV
	entitySlice := []any{entities[0], entities[1], entities[2]}
	results, err := executor.CollectTuples(db.Query(
		`[:find ?e ?a ?v :in $ [?e ...] :where [?e ?a ?v]]`,
		entitySlice))
	require.NoError(t, err)

	db.SetAnnotationHandler(nil)

	// Verify EATV was selected
	usedEATV := false
	for _, e := range events {
		if e.Name == "storage/reuse-strategy" {
			t.Logf("EVENT: %s %v", e.Name, e.Data)
			if idx, ok := e.Data["index"]; ok && fmt.Sprint(idx) == "EATV" {
				usedEATV = true
			}
		}
	}
	require.True(t, usedEATV, "Strategy should select EATV for E-only binding with free A")

	t.Logf("Got %d results:", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	// Each entity has :person/name + :doc/content = 2 results each
	require.Len(t, results, 6, "3 entities × 2 attributes = 6 results")
}
