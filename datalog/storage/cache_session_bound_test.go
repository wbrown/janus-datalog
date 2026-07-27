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
	d, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer d.Close()

	e := datalog.NewIdentity("bound:e1")
	attr := datalog.NewKeyword(":bound/value")

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(e, attr, int64(1)))
	_, err = tx.Commit()
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

	var eBytes Entity
	copy(eBytes[:], e.Bytes())
	var aBytes Attribute
	copy(aBytes[:], attr.String())
	raw, _, scanned, err := sessioned.ResolveLWW(eBytes, aBytes)
	require.NoError(t, err)
	require.Equal(t, int64(1), raw, "storage path defines the snapshot answer")
	require.Positive(t, scanned,
		"the storage path read the index to answer; an intake of zero would mean it did not")

	// The latest reader is unaffected by the session's bound.
	v, found, err = latest.LookupAttribute(e, attr)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), v)
}

// TestStoreGateKeepsFresherEntry pins the anti-clobber gate: a rebuild from
// an older snapshot must not replace a fresher cached entry — the cache never
// regresses, it only declines to serve.
func TestStoreGateKeepsFresherEntry(t *testing.T) {
	c := NewCache()
	key := CacheKey{}
	copy(key.A[:], ":gate/value")

	fresh := &CacheEntry{version: datalog.ElementID{Lamport: 2, ReplicaID: 1}}
	require.True(t, c.storeIfNotInFlight(key, fresh))

	stale := &CacheEntry{version: datalog.ElementID{Lamport: 1, ReplicaID: 1}}
	require.False(t, c.storeIfNotInFlight(key, stale),
		"an older-versioned entry must not replace a fresher one")

	slot, ok := c.slots.Load(key)
	require.True(t, ok)
	require.Same(t, fresh, slot.entry, "the fresher entry must survive")
	require.Equal(t, fresh.version, slot.version)

	// Equal or newer versions still store.
	newer := &CacheEntry{version: datalog.ElementID{Lamport: 3, ReplicaID: 1}}
	require.True(t, c.storeIfNotInFlight(key, newer))
	slot, ok = c.slots.Load(key)
	require.True(t, ok)
	require.Same(t, newer, slot.entry)
}
