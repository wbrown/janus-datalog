package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestGetElseWithAnnotationHandler reproduces a bug where get-else silently
// returns no results when the database has an annotation handler enabled.
//
// Root cause: AnnotatedMatcher wraps BadgerMatcher but does not implement
// EntityLookupMatcher. When the executor checks for EntityLookupMatcher via
// type assertion, the wrapper fails the check. get-else then falls through
// to Eval() (instead of EvalWithLookup), which returns an error that is
// silently swallowed — every tuple is skipped, producing 0 results.
//
// This bug only manifests with annotation handlers because without one,
// WrapMatcher returns the unwrapped BadgerMatcher directly.
func TestGetElseWithAnnotationHandler(t *testing.T) {
	dir, err := os.MkdirTemp("", "annotated-get-else-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Open database WITH annotation handler — this is the critical difference
	var events []annotations.Event
	handler := func(e annotations.Event) {
		events = append(events, e)
	}

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              dir,
		AnnotationHandler: handler,
	})
	require.NoError(t, err)
	defer db.Close()

	// Create test data
	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")

	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice"))
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":person/age"), int64(30)))
	require.NoError(t, tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob"))
	// Bob has no age — get-else should return the default

	_, err = tx.Commit()
	require.NoError(t, err)

	t.Run("get-else with existing attribute", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(
			`[:find ?name ?age
			  :where
			  [?e :person/name ?name]
			  [(get-else $ ?e :person/age 0) ?age]]`,
		))
		require.NoError(t, err)
		require.Len(t, results, 2, "should return results for both Alice and Bob")

		// Build map for order-independent assertion
		byName := make(map[string]interface{})
		for _, tuple := range results {
			byName[tuple[0].(string)] = tuple[1]
		}
		assert.Equal(t, int64(30), byName["Alice"], "Alice should have age 30")
		assert.Equal(t, int64(0), byName["Bob"], "Bob should have default age 0")
	})

	t.Run("get-else with missing attribute uses default", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(
			`[:find ?name ?nick
			  :where
			  [?e :person/name ?name]
			  [(get-else $ ?e :person/nickname "unknown") ?nick]]`,
		))
		require.NoError(t, err)
		require.Len(t, results, 2, "should return results for both entities")

		for _, tuple := range results {
			assert.Equal(t, "unknown", tuple[1], "%s should have default nickname", tuple[0])
		}
	})

	t.Run("missing? with annotation handler", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(
			`[:find ?name ?missing
			  :where
			  [?e :person/name ?name]
			  [(missing? $ ?e :person/age) ?missing]]`,
		))
		require.NoError(t, err)
		require.Len(t, results, 2)

		byName := make(map[string]bool)
		for _, tuple := range results {
			byName[tuple[0].(string)] = tuple[1].(bool)
		}
		assert.False(t, byName["Alice"], "Alice has age, should not be missing")
		assert.True(t, byName["Bob"], "Bob has no age, should be missing")
	})

	t.Run("get-else with constant entity from input", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(
			`[:find ?age
			  :in $ ?entity
			  :where
			  [(get-else $ ?entity :person/age -1) ?age]]`,
			alice,
		))
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, int64(30), results[0][0])
	})
}
