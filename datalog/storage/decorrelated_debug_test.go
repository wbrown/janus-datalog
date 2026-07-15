//go:build !(js && wasm)

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestDecorrelatedOrJoinBranchResults traces what each or-join branch produces
// to debug why scenario 2 (no tasks) is missing from results.
func TestDecorrelatedOrJoinBranchResults(t *testing.T) {
	dir, err := os.MkdirTemp("", "decorrelated-debug-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "or/branch.complete" {
				t.Logf("[ANNOTATION] %s: %v", e.Name, e.Data)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1")
	tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2")
	tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1)
	tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Exact query from TestOrWithGetElseInsideSubquery_E2E
	results, err := executor.CollectTuples(db.Query(`
		[:find ?scenario ?taskCount ?totalTokens
		 :where
		 [?scenario :scenario/id ?id]
		 (or-default [(q [:find (count ?t) (sum ?tok)
		          :in $ ?s
		          :where [?t :task/scenario ?s]
		                 [?t :task/status :status/complete]
		                 [(get-else $ ?t :task/token-count 0) ?tok]]
		         $ ?scenario) [[?taskCount ?totalTokens]]]
		     (and [(ground 0) ?taskCount]
		          [(ground 0) ?totalTokens]))]`))
	require.NoError(t, err)
	t.Logf("Results: %v", results)
	require.Len(t, results, 2, "should return both scenarios")
}
