package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Reproducer for BUG_VECTOR_HELPERS_DISABLECACHE_PANIC: GetVectorNth and
// GetVectorLength dereferenced d.cache unconditionally, so a database opened
// with DisableCache:true (d.cache == nil) panicked. Cache-disabled mode must
// behave the same as cache-enabled mode. These tests run every case under both
// modes; the cache-disabled subtests panic against the unfixed code.

func vectorAccessorSchema() schema.SchemaProvider {
	return schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Attribute(":character/name").Type(schema.TypeString).Add(). // cardinality-one
		MustBuild()
}

// newVectorAccessorDB opens a DB in the requested cache mode and writes a
// three-element :character/skills vector on character:alice.
func newVectorAccessorDB(t *testing.T, disableCache bool) (*Database, datalog.Identity) {
	t.Helper()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         t.TempDir(),
		Schema:       vectorAccessorSchema(),
		ReplicaID:    1,
		DisableCache: disableCache,
	})
	require.NoError(t, err)
	e := datalog.NewIdentity("character:alice")
	skills := datalog.NewKeyword(":character/skills")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, skills, "stealth"))
	require.NoError(t, tx.Add(e, skills, "archery"))
	require.NoError(t, tx.Add(e, skills, "lockpicking"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return db, e
}

func eachCacheMode(t *testing.T, fn func(t *testing.T, disableCache bool)) {
	t.Helper()
	for _, disable := range []bool{false, true} {
		name := "cache_enabled"
		if disable {
			name = "cache_disabled"
		}
		t.Run(name, func(t *testing.T) { fn(t, disable) })
	}
}

func TestGetVectorNth_BothCacheModes(t *testing.T) {
	skills := datalog.NewKeyword(":character/skills")
	eachCacheMode(t, func(t *testing.T, disable bool) {
		db, e := newVectorAccessorDB(t, disable)
		defer db.Close()

		v0, err := db.GetVectorNth(e, skills, 0)
		require.NoError(t, err)
		require.Equal(t, "stealth", v0)

		v2, err := db.GetVectorNth(e, skills, 2)
		require.NoError(t, err)
		require.Equal(t, "lockpicking", v2)

		oob, err := db.GetVectorNth(e, skills, 99)
		require.NoError(t, err)
		require.Nil(t, oob, "out-of-bounds index returns nil")
	})
}

func TestGetVectorLength_BothCacheModes(t *testing.T) {
	skills := datalog.NewKeyword(":character/skills")
	eachCacheMode(t, func(t *testing.T, disable bool) {
		db, e := newVectorAccessorDB(t, disable)
		defer db.Close()

		n, err := db.GetVectorLength(e, skills)
		require.NoError(t, err)
		require.Equal(t, int64(3), n)
	})
}

func TestGetVector_MissingVector_BothCacheModes(t *testing.T) {
	skills := datalog.NewKeyword(":character/skills")
	eachCacheMode(t, func(t *testing.T, disable bool) {
		db, _ := newVectorAccessorDB(t, disable)
		defer db.Close()

		// A different entity with no skills written.
		empty := datalog.NewIdentity("character:nobody")

		v, err := db.GetVectorNth(empty, skills, 0)
		require.NoError(t, err)
		require.Nil(t, v, "missing vector: nth returns nil")

		n, err := db.GetVectorLength(empty, skills)
		require.NoError(t, err)
		require.Equal(t, int64(0), n, "missing vector: length returns 0")
	})
}

func TestGetVector_NonVectorAttribute_BothCacheModes(t *testing.T) {
	name := datalog.NewKeyword(":character/name")
	eachCacheMode(t, func(t *testing.T, disable bool) {
		db, e := newVectorAccessorDB(t, disable)
		defer db.Close()

		tx := db.NewTransaction()
		require.NoError(t, tx.Add(e, name, "Alice"))
		_, err := tx.Commit()
		require.NoError(t, err)

		_, err = db.GetVectorNth(e, name, 0)
		require.Error(t, err, "non-vector attribute must return an error, not panic")
		_, err = db.GetVectorLength(e, name)
		require.Error(t, err, "non-vector attribute must return an error, not panic")
	})
}
