package storage

import (
	"os"
	"testing"

	"strings"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestDecorrelationCache_SingleEntity tests the decorrelation cache with a
// single outer entity. This reproduces the OHLC test crash where the cache
// filter returned empty for a single-entity correlation.
func TestDecorrelationCache_SingleEntity(t *testing.T) {
	dir, err := os.MkdirTemp("", "decor-cache-single-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var cacheEvents []string
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if strings.HasPrefix(e.Name, "subquery/") {
				t.Logf("[%s] %v", e.Name, e.Data)
				cacheEvents = append(cacheEvents, e.Name)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	// Setup: one symbol with multiple price bars
	tx := db.NewTransaction()
	sym := datalog.NewIdentity("sym:CRWV")
	tx.Add(sym, datalog.NewKeyword(":symbol/ticker"), "CRWV")

	for i := 0; i < 10; i++ {
		bar := datalog.NewIdentity("bar:" + string(rune('a'+i)))
		tx.Add(bar, datalog.NewKeyword(":bar/symbol"), sym)
		tx.Add(bar, datalog.NewKeyword(":bar/high"), float64(100+i))
		tx.Add(bar, datalog.NewKeyword(":bar/low"), float64(90+i))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query: single entity with correlated aggregate subqueries
	results, err := executor.CollectTuples(db.Query(`
		[:find ?high ?low
		 :where
		 [?s :symbol/ticker "CRWV"]
		 [(q [:find (max ?h) :in $ ?sym
		      :where [?b :bar/symbol ?sym] [?b :bar/high ?h]]
		     $ ?s) [[?high]]]
		 [(q [:find (min ?l) :in $ ?sym
		      :where [?b :bar/symbol ?sym] [?b :bar/low ?l]]
		     $ ?s) [[?low]]]]`))
	require.NoError(t, err)
	require.Len(t, results, 1)
	t.Logf("Results: %v", results)
	t.Logf("Cache events: %v", cacheEvents)

	assert.Equal(t, float64(109), results[0][0]) // max high
	assert.Equal(t, float64(90), results[0][1])  // min low
}

// TestDecorrelationCache_MultipleEntities tests the cache with multiple
// outer entities, verifying correct filtering per entity.
func TestDecorrelationCache_MultipleEntities(t *testing.T) {
	dir, err := os.MkdirTemp("", "decor-cache-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              dir,
		AnnotationHandler: func(e annotations.Event) {},
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	s1 := datalog.NewIdentity("scenario:1")
	s2 := datalog.NewIdentity("scenario:2")
	s3 := datalog.NewIdentity("scenario:3")
	tx.Add(s1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/scenario"))
	tx.Add(s2, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/scenario"))
	tx.Add(s3, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/scenario"))

	// s1: 3 tasks, s2: 1 task, s3: 0 tasks
	for i := 0; i < 3; i++ {
		t := datalog.NewIdentity("task:1:" + string(rune('a'+i)))
		tx.Add(t, datalog.NewKeyword(":task/root"), s1)
		tx.Add(t, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
	}
	t1 := datalog.NewIdentity("task:2:a")
	tx.Add(t1, datalog.NewKeyword(":task/root"), s2)
	tx.Add(t1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with OR-fallback (the production pattern)
	results, err := executor.CollectTuples(db.Query(`
		[:find ?e ?count
		 :where
		 [?e :entity/type :type/scenario]
		 (or [(q [:find (count ?t) :in $ ?s
		          :where [?t :task/root ?s] [?t :task/status :status/complete]]
		         $ ?e) [[?count]]]
		     [(ground 0) ?count])]`))
	require.NoError(t, err)
	require.Len(t, results, 3, "should return all 3 scenarios")
	t.Logf("Results: %v", results)

	byID := make(map[string]int64)
	for _, row := range results {
		id := row[0].(datalog.Identity)
		byID[id.String()] = row[1].(int64)
	}
	assert.Equal(t, int64(3), byID["scenario:1"])
	assert.Equal(t, int64(1), byID["scenario:2"])
	assert.Equal(t, int64(0), byID["scenario:3"])
}

// TestDecorrelationCache_NoCache tests that non-aggregate subqueries
// are NOT cached (they don't have aggregates in :find).
func TestDecorrelationCache_NoCache(t *testing.T) {
	dir, err := os.MkdirTemp("", "decor-cache-nocache-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var cacheEvents []string
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "subquery/decorrelation-cached" {
				cacheEvents = append(cacheEvents, e.Name)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	s1 := datalog.NewIdentity("scenario:1")
	tx.Add(s1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/scenario"))
	tx.Add(s1, datalog.NewKeyword(":scenario/name"), "First")
	_, err = tx.Commit()
	require.NoError(t, err)

	// Non-aggregate subquery — should NOT be cached
	results, err := executor.CollectTuples(db.Query(`
		[:find ?name
		 :where
		 [?e :entity/type :type/scenario]
		 [(q [:find ?n :in $ ?s :where [?s :scenario/name ?n]]
		     $ ?e) [[?name]]]]`))
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "First", results[0][0])
	assert.Empty(t, cacheEvents, "non-aggregate subqueries should not be cached")
}
