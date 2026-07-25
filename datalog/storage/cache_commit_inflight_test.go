package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// These tests cover the cache stale-read window: Transaction.Commit marks the
// touched (E, A) keys in-flight before the storage commit, so a reader resolving
// one of them in the window between stx.Commit() returning and the cache update
// never sees the pre-commit value.

func inFlightSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":counter/value"),
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityOne,
	})
	return s
}

func openInFlightDB(t *testing.T) *Database {
	t.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      t.TempDir(),
		Schema:    inFlightSchema(),
		ReplicaID: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func setAndCommit(t *testing.T, db *Database, e datalog.Identity, a datalog.Keyword, v any) {
	t.Helper()
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, v))
	_, err := tx.Commit()
	require.NoError(t, err)
}

// resolveOne reads (e, a) through exactly the cache path the bug lives in
// (Cache.GetOrResolve), returning the resolved CardinalityOne value.
func resolveOne(db *Database, e datalog.Identity, a datalog.Keyword) any {
	var aBytes Attribute
	copy(aBytes[:], a.String())
	key := CacheKey{E: Entity(e.Hash()), A: aBytes}
	matcher := NewPatternMatcher(db.store)
	matcher.SetSchema(db.schema)
	entry := db.cache.GetOrResolve(key, matcher, nil)
	if entry == nil {
		return nil
	}
	return entry.OneValue()
}

// TestCommit_NoStaleCachedReadAfterCommitReturns is the core regression. It warms
// the cache for (e, k) at v1, then overwrites to v2 and — via the test-only
// onCommitWindow hook, which fires after the storage commit but before the cache
// update — resolves (e, k) in exactly the window where a stale cache hit was once
// possible. With the in-flight fix the read sees v2; without it (maxVersions not
// yet bumped, entry still v1) it would see the stale v1.
func TestCommit_NoStaleCachedReadAfterCommitReturns(t *testing.T) {
	db := openInFlightDB(t)
	e := datalog.NewIdentity("e1")
	k := datalog.NewKeyword(":counter/value")

	setAndCommit(t, db, e, k, int64(1))
	require.Equal(t, int64(1), resolveOne(db, e, k), "cache should be warmed at v1")

	var windowValue any
	db.onCommitWindow = func() { windowValue = resolveOne(db, e, k) }
	setAndCommit(t, db, e, k, int64(2))
	db.onCommitWindow = nil

	assert.Equal(t, int64(2), windowValue,
		"a cache read in the post-commit window must see the committed value, not the stale entry")

	// And the cache serves v2 normally afterward.
	assert.Equal(t, int64(2), resolveOne(db, e, k))
}

// TestCommit_ConcurrentReadersSeeCommittedValue hammers the in-flight machinery:
// readers continuously resolve (e, k) through the cache while a writer overwrites
// it with a monotonically increasing value. Values must always be valid and never
// move backward for a given reader. Run under -race to catch data races in the
// sentinel/CAS paths.
func TestCommit_ConcurrentReadersSeeCommittedValue(t *testing.T) {
	db := openInFlightDB(t)
	e := datalog.NewIdentity("e1")
	k := datalog.NewKeyword(":counter/value")

	setAndCommit(t, db, e, k, int64(0))
	resolveOne(db, e, k) // warm

	const readers = 8
	stop := make(chan struct{})
	errCh := make(chan error, readers)
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int64 = -1
			for {
				select {
				case <-stop:
					return
				default:
				}
				v := resolveOne(db, e, k)
				if v == nil {
					continue
				}
				iv, ok := v.(int64)
				if !ok {
					errCh <- fmt.Errorf("non-int64 value %T from cache", v)
					return
				}
				if iv < last {
					errCh <- fmt.Errorf("reader saw %d after %d (value moved backward)", iv, last)
					return
				}
				last = iv
			}
		}()
	}

	for n := int64(1); n <= 200; n++ {
		setAndCommit(t, db, e, k, n)
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestCommit_RollbackPathDoesNotPoisonCache exercises the commit-failure cleanup:
// Commit marks keys in-flight before stx.Commit(), and on failure clears them via
// Invalidate WITHOUT bumping maxVersions. This drives that exact sequence (storage
// unchanged) and asserts the cache rebuilds the correct pre-"commit" value and
// isn't left poisoned for the next real commit.
func TestCommit_RollbackPathDoesNotPoisonCache(t *testing.T) {
	db := openInFlightDB(t)
	e := datalog.NewIdentity("e1")
	k := datalog.NewKeyword(":counter/value")

	setAndCommit(t, db, e, k, int64(7))
	require.Equal(t, int64(7), resolveOne(db, e, k), "cache warmed at v7")

	var aBytes Attribute
	copy(aBytes[:], k.String())
	key := CacheKey{E: Entity(e.Hash()), A: aBytes}

	// Mimic Commit's failure path: mark in-flight, then (commit fails, storage
	// unchanged) Invalidate — no UpdateMaxVersion.
	db.cache.BeginInFlight([]CacheKey{key})
	db.cache.Invalidate([]CacheKey{key})

	// Storage still holds v7, and maxVersions was never bumped, so the cache must
	// rebuild and serve v7 — no stale value, no permanent bypass.
	assert.Equal(t, int64(7), resolveOne(db, e, k), "rollback must leave the pre-commit value")

	// A subsequent real commit still takes effect (no lingering poison).
	setAndCommit(t, db, e, k, int64(8))
	assert.Equal(t, int64(8), resolveOne(db, e, k))
}
