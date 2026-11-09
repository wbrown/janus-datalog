package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestSessionBoundedCacheRead pins the review's two-reader tear: a sessioned
// query must not be served cache content newer than its snapshot, even after
// a concurrent commit and another latest reader re-warm the slot. The cache
// path (GetOrResolve behind LookupAttribute) must agree with the session's
// storage path (ResolveLWW) on the same matcher.
func TestSessionBoundedCacheRead(t *testing.T) {
	for _, mode := range legsFor(backendModes) {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})

			e := datalog.NewIdentity("bound:e1")
			attr := datalog.NewKeyword(":bound/value")

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(e, attr, int64(1)))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Sessioned matcher pinned at the X=1 snapshot.
			matcher, session, err := d.sessionMatcher(d.effectivePlannerOptions())
			require.NoError(t, err)
			defer session.Close()
			sessioned := matcher.(*PatternMatcher)

			// First read through the session establishes X.
			v, found, err := sessioned.LookupAttribute(e, attr)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(1), v)

			// Concurrent commit lands Y after the session opened...
			tx2 := d.NewTransaction()
			require.NoError(t, tx2.Add(e, attr, int64(2)))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// ...and another latest reader warms the cache slot to Y.
			latest := d.Matcher().(*PatternMatcher)
			v, found, err = latest.LookupAttribute(e, attr)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(2), v, "the latest reader must see the new value")

			// The sessioned matcher's cache path must still answer from its snapshot,
			// agreeing with its own storage path.
			v, found, err = sessioned.LookupAttribute(e, attr)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(1), v,
				"a sessioned read must not be served cache content newer than its snapshot")

			report := &scanReport{}
			raw, _, present, err := sessioned.ResolveLWW(e, attr, sessioned.schema.GetAttribute(attr), report)
			require.NoError(t, err)
			require.True(t, present, "a datom exists within the session's bound")
			require.Equal(t, int64(1), raw, "storage path defines the snapshot answer")
			require.Positive(t, report.scanned,
				"the storage path read the index to answer; an intake of zero would mean it did not")

			// The latest reader is unaffected by the session's bound.
			v, found, err = latest.LookupAttribute(e, attr)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(2), v)
		})
	}
}

// TestStoreGateKeepsFresherEntry pins the anti-clobber gate: a rebuild from
// an older snapshot must not replace a fresher cached entry — the cache never
// regresses, it only declines to serve.
func TestStoreGateKeepsFresherEntry(t *testing.T) {
	// The entries' versions were drawn before the cache was made, so the
	// cache's floor, its read point when made, lies above them.
	clock := NewLamportClock(1)
	clock.Restore(datalog.ElementID{Lamport: 3, ReplicaID: 1})
	c := NewCache(clock)
	floor := c.ReadPoint()
	key := CacheKey{E: datalog.NewIdentity("gate"), A: datalog.NewKeyword(":gate/value")}

	fresh := &CacheEntry{version: datalog.ElementID{Lamport: 2, ReplicaID: 1}}
	require.True(t, c.storeIfNotInFlight(key, fresh, c.ReadPoint()))

	stale := &CacheEntry{version: datalog.ElementID{Lamport: 1, ReplicaID: 1}}
	require.False(t, c.storeIfNotInFlight(key, stale, c.ReadPoint()),
		"an older-versioned entry must not replace a fresher one")

	slot, ok := c.slots.Load(key)
	require.True(t, ok)
	require.Same(t, fresh, slot.entry, "the fresher entry must survive")
	require.Equal(t, floor, slot.version,
		"a store into an absent slot records the higher of the floor and the entry's version")

	// Equal or newer versions still store.
	newer := &CacheEntry{version: datalog.ElementID{Lamport: 3, ReplicaID: 1}}
	require.True(t, c.storeIfNotInFlight(key, newer, c.ReadPoint()))
	slot, ok = c.slots.Load(key)
	require.True(t, ok)
	require.Same(t, newer, slot.entry)
}

// TestCacheDropRefusesAResolutionReadBeforeIt pins the floor each drop raises
// (raiseFloor): once a key's slot is dropped, a resolution whose read point was
// taken before the drop is not stored for the key, and one whose read point was
// taken after it is.
func TestCacheDropRefusesAResolutionReadBeforeIt(t *testing.T) {
	key := CacheKey{E: datalog.NewIdentity("floor"), A: datalog.NewKeyword(":floor/value")}

	drops := []struct {
		name string
		drop func(c *Cache)
	}{
		{"clearing the cache", func(c *Cache) { c.Clear() }},
		{"dropping the key's attribute", func(c *Cache) { c.InvalidateAttribute(key.A) }},
		{"dropping the key on a rewind", func(c *Cache) { c.InvalidateRewind([]CacheKey{key}) }},
	}
	for _, d := range drops {
		t.Run(d.name, func(t *testing.T) {
			c := NewCache(NewLamportClock(1))
			require.True(t, c.storeIfNotInFlight(key, &CacheEntry{}, c.ReadPoint()),
				"a resolution read with nothing arrived on the key is stored")

			before := c.ReadPoint()
			d.drop(c)
			require.False(t, c.storeIfNotInFlight(key, &CacheEntry{}, before),
				"a resolution read before the drop may lack what the drop's change brought")
			_, ok := c.slots.Load(key)
			require.False(t, ok, "the refused resolution leaves the key without a slot")

			require.True(t, c.storeIfNotInFlight(key, &CacheEntry{}, c.ReadPoint()),
				"a resolution read after the drop is stored")
		})
	}
}
