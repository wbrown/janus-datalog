package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestPrefetchDoesNotBreakMaterialize reproduces a panic where the prefetch
// code materializes a StreamingRelation after the first DataPattern, then
// materializeRelationsForPattern tries to materialize it again during the
// second pattern, hitting "Materialize() called after iteration began".
//
// The query has two patterns sharing a variable — the second pattern triggers
// materializeRelationsForPattern on the first pattern's result.
func TestPrefetchDoesNotBreakMaterialize(t *testing.T) {
	dir, err := os.MkdirTemp("", "prefetch-materialize-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	// Create enough entities to trigger prefetch (>50)
	tx := db.NewTransaction()
	for i := 0; i < 100; i++ {
		e := datalog.NewIdentity("entity:" + string(rune('a'+i/26)) + string(rune('a'+i%26)))
		tx.Add(e, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/thing"))
		tx.Add(e, datalog.NewKeyword(":entity/name"), "Name")
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with subquery that triggers nested Execute() via
	// ExecuteWithRelations → ScanSharingMatcher → Iterator consumption.
	// The outer query then tries to materialize the same relation.
	q := `[:find ?e ?name
	       :where
	       [?e :entity/type :type/thing]
	       [?e :entity/name ?name]
	       (not [?e :entity/type :type/deleted])]`

	rel, err := db.Query(q)
	require.NoError(t, err, "query should not panic on double materialization")
	results, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)
	require.Equal(t, 100, len(results), "should return all 100 entities")

	// Reproduce empty-cache materialization bug: multiple filter queries on
	// an empty DB. Each query goes through ScanSharingMatcher which intercepts
	// unbound patterns, wraps in LazySeq. On empty results, the CachingIterator
	// completes with cache=nil but cacheReady=true. A subsequent Materialize()
	// must check cacheReady, not just cache!=nil.
	emptyDir2, err := os.MkdirTemp("", "prefetch-empty-*")
	require.NoError(t, err)
	defer os.RemoveAll(emptyDir2)

	emptyDB, err := NewDatabaseWithOptions(DatabaseOptions{Path: emptyDir2})
	require.NoError(t, err)
	defer emptyDB.Close()

	parent := datalog.NewIdentity("parent:1")

	// Run multiple filter queries in sequence — each shares variables
	// with the :in binding and has multiple patterns triggering materialization.
	filterQueries := []string{
		`[:find ?e :in $ ?parent :where [?e :entity/type :type/a] [?e :entity/group ?g] [?parent :entity/group ?g]]`,
		`[:find ?e :in $ ?parent :where [?e :entity/type :type/b] [?e :entity/group ?g] [?parent :entity/group ?g]]`,
		`[:find ?e :in $ ?parent :where [?e :entity/type :type/c] [?e :entity/group ?g] [?parent :entity/group ?g] [(!= ?e ?parent)]]`,
		`[:find ?e :in $ ?parent :where [?parent :entity/group ?via] [?via :entity/link ?e]]`,
	}

	for i, fq := range filterQueries {
		rel, err := emptyDB.Query(fq, parent)
		require.NoError(t, err, "filter query %d should not panic", i)
		results, err := executor.CollectTuples(rel, nil)
		require.NoError(t, err)
		t.Logf("filter %d: %d results", i, len(results))
	}
}
