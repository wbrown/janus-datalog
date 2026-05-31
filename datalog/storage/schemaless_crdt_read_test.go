package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Regression for BUG_SCHEMALESS_READ_COLLAPSES_CRDT_VECTOR_TO_LWW.
//
// A CardinalityVector / CardinalityMany attribute written WITH a schema (so its
// datoms carry OpRGAInsert / OpCRDTAdd) must read back identically whether or
// not the reader has a schema: the data is self-describing via its CRDT ops.
// The bug: a schemaless read collapses a vector to its last element (LWW) and a
// set to a single member, silently.
//
// The invariant asserted here is read-equality — schemaless read == schema-aware
// read of the same bytes — so it does not hard-code the value representation,
// only that the schemaless path must not lose data the schema-aware path keeps.
//
// NOTE: (count ?v) cannot detect this collapse: a vector binds ?v to one list
// value, so count is 1 either way. These tests inspect the bound value.

// readVectorAsStrings runs `[:find ?v :where [?e <attr> ?v]]` (E bound via :in
// when bindE), requires exactly one ?v row, and returns the binding normalized
// to []string. A correctly-resolved vector binds ?v to one ordered list value
// ([]string or []interface{}); a collapsed read binds ?v to a single scalar,
// which fails here — that failure IS the bug.
func readVectorAsStrings(t *testing.T, db *Database, e datalog.Identity, attrPat string, bindE bool) []string {
	t.Helper()
	q := fmt.Sprintf(`[:find ?v :where [?e %s ?v]]`, attrPat)
	args := []interface{}{}
	if bindE {
		q = fmt.Sprintf(`[:find ?v :in $ ?e :where [?e %s ?v]]`, attrPat)
		args = append(args, e)
	}
	rel, err := db.Query(q, args...)
	require.NoError(t, err)
	it := rel.Iterator()
	defer it.Close()
	require.True(t, it.Next(), "expected one ?v row")
	v := it.Tuple()[0]
	require.False(t, it.Next(), "expected exactly one ?v row")
	require.NoError(t, it.Error())
	return asStringList(t, v)
}

// asStringList normalizes a vector ?v binding to []string, failing if v is a
// scalar (the schemaless collapse) or contains non-strings.
func asStringList(t *testing.T, v interface{}) []string {
	t.Helper()
	switch vv := v.(type) {
	case []string:
		return vv
	case []interface{}:
		out := make([]string, len(vv))
		for i, x := range vv {
			s, ok := x.(string)
			require.Truef(t, ok, "vector element %d not a string: %T", i, x)
			out[i] = s
		}
		return out
	default:
		t.Fatalf("expected a vector value ([]string/[]interface{}), got %T (%v) — "+
			"this is the schemaless collapse: ?v bound to a single element", v, v)
		return nil
	}
}

// writeVectorDB writes n ordered elements of a CardinalityVector attribute
// :doc/lines under a schema, then closes. Returns the db path.
func writeVectorDB(t *testing.T, n int, e datalog.Identity) string {
	t.Helper()
	dir := t.TempDir()
	s := schema.NewBuilder().
		Attribute(":doc/lines").Type(schema.TypeString).Vector().Add().
		MustBuild()
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, Schema: s, ReplicaID: 1})
	require.NoError(t, err)
	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		require.NoError(t, tx.Add(e, datalog.NewKeyword(":doc/lines"), fmt.Sprintf("line %d", i)))
	}
	_, err = tx.Commit()
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return dir
}

// vectorSchema rebuilds the schema used by writeVectorDB (for schema-aware reopen).
func vectorSchema() schema.SchemaProvider {
	return schema.NewBuilder().
		Attribute(":doc/lines").Type(schema.TypeString).Vector().Add().
		MustBuild()
}

// TestSchemalessRead_VectorMatchesSchemaAware is the core regression: reopening
// a vector-bearing database WITHOUT a schema must return the same ordered list
// as reopening it WITH the schema. Covered for the unbound-E (streaming
// CRDTResolvingIterator) and bound-E (cache) query paths, cache enabled and
// disabled.
func TestSchemalessRead_VectorMatchesSchemaAware(t *testing.T) {
	const n = 5
	e := datalog.NewIdentity("doc-1")
	dir := writeVectorDB(t, n, e)

	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("line %d", i)
	}

	// Establish the schema-aware truth on the same bytes.
	schemaDB, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, Schema: vectorSchema(), ReplicaID: 1})
	require.NoError(t, err)
	require.Equal(t, want, readVectorAsStrings(t, schemaDB, e, ":doc/lines", false),
		"schema-aware unbound must see the full ordered vector")
	require.Equal(t, want, readVectorAsStrings(t, schemaDB, e, ":doc/lines", true),
		"schema-aware bound must see the full ordered vector")
	require.NoError(t, schemaDB.Close())

	cases := []struct {
		name    string
		opts    DatabaseOptions
		openDef bool // use NewDatabase(path) (cache on, schemaless)
	}{
		{name: "schemaless_cache_on", openDef: true},
		{name: "schemaless_cache_off", opts: DatabaseOptions{Path: dir, ReplicaID: 1, DisableCache: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var db *Database
			var err error
			if tc.openDef {
				db, err = NewDatabase(dir)
			} else {
				db, err = NewDatabaseWithOptions(tc.opts)
			}
			require.NoError(t, err)
			defer db.Close()

			require.Equal(t, want, readVectorAsStrings(t, db, e, ":doc/lines", false),
				"schemaless unbound-E read must equal schema-aware read, not collapse to the last element")
			require.Equal(t, want, readVectorAsStrings(t, db, e, ":doc/lines", true),
				"schemaless bound-E read must equal schema-aware read, not collapse to the last element")
		})
	}
}

// TestSchemalessLookupAttribute_VectorMatchesSchemaAware pins the direct
// LookupAttribute resolution path (used by Pull). The matcher is obtained via
// db.Matcher() — the production path — so on a schemaless reopen it carries the
// schema reconstructed at open from the stored CRDT ops, and must return the
// full vector rather than collapsing to the last element.
func TestSchemalessLookupAttribute_VectorMatchesSchemaAware(t *testing.T) {
	const n = 5
	e := datalog.NewIdentity("doc-1")
	dir := writeVectorDB(t, n, e)
	attr := datalog.NewKeyword(":doc/lines")

	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("line %d", i)
	}

	// Schema-aware truth.
	schemaDB, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, Schema: vectorSchema(), ReplicaID: 1})
	require.NoError(t, err)
	sval, sfound := schemaDB.Matcher().(*BadgerMatcher).LookupAttribute(e, attr)
	require.True(t, sfound)
	require.Equal(t, want, asStringList(t, sval), "schema-aware LookupAttribute must return full vector")
	require.NoError(t, schemaDB.Close())

	// Schemaless reopen: NewDatabase reconstructs the schema from stored ops, so
	// the production matcher (db.Matcher()) resolves the vector correctly.
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()
	val, found := db.Matcher().(*BadgerMatcher).LookupAttribute(e, attr)
	require.True(t, found)
	require.Equal(t, want, asStringList(t, val),
		"schemaless LookupAttribute must reconstruct the vector via the open-time schema, not collapse to the last element")
}

// TestSchemalessRead_ManyMatchesSchemaAware is the CardinalityMany counterpart:
// a schemaless read must return the full set, not a single member.
func TestSchemalessRead_ManyMatchesSchemaAware(t *testing.T) {
	dir := t.TempDir()
	e := datalog.NewIdentity("doc-1")
	attr := datalog.NewKeyword(":doc/tags")
	members := []string{"alpha", "beta", "gamma", "delta"}

	manySchema := func() schema.SchemaProvider {
		return schema.NewBuilder().
			Attribute(":doc/tags").Type(schema.TypeString).Many().Add().
			MustBuild()
	}

	dbw, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, Schema: manySchema(), ReplicaID: 1})
	require.NoError(t, err)
	tx := dbw.NewTransaction()
	for _, m := range members {
		require.NoError(t, tx.Add(e, attr, m))
	}
	_, err = tx.Commit()
	require.NoError(t, err)
	require.NoError(t, dbw.Close())

	collectSet := func(t *testing.T, db *Database) []string {
		t.Helper()
		rel, err := db.Query(`[:find ?v :in $ ?e :where [?e :doc/tags ?v]]`, e)
		require.NoError(t, err)
		it := rel.Iterator()
		defer it.Close()
		var got []string
		for it.Next() {
			s, ok := it.Tuple()[0].(string)
			require.True(t, ok)
			got = append(got, s)
		}
		require.NoError(t, it.Error())
		sort.Strings(got)
		return got
	}

	want := append([]string(nil), members...)
	sort.Strings(want)

	schemaDB, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, Schema: manySchema(), ReplicaID: 1})
	require.NoError(t, err)
	require.Equal(t, want, collectSet(t, schemaDB), "schema-aware must see full set")
	require.NoError(t, schemaDB.Close())

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, want, collectSet(t, db),
		"schemaless read must return the full add-wins set, not a single member")
}

// TestSchemalessReopen_WriteUsesInferredCardinality pins the write-path
// consequence of installing the inferred schema into d.schema: a schemaless
// reopen of an existing database must write NEW values for an existing
// vector attribute with RGA semantics (the inferred cardinality), not as an
// OpNone LWW assertion. The proof is the round trip — appending then reading
// back the full ordered list. If the write had collapsed to OpNone, that entry
// would carry the highest Tx and the read would reclassify the group as
// cardinality-one and return only the new element.
func TestSchemalessReopen_WriteUsesInferredCardinality(t *testing.T) {
	const n = 5
	e := datalog.NewIdentity("doc-1")
	dir := writeVectorDB(t, n, e) // :doc/lines written as a vector, with schema

	// Reopen WITHOUT a schema; cardinality is reconstructed from stored ops.
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	// Append a new element through the schemaless handle.
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, datalog.NewKeyword(":doc/lines"), fmt.Sprintf("line %d", n)))
	_, err = tx.Commit()
	require.NoError(t, err)

	want := make([]string, n+1)
	for i := range want {
		want[i] = fmt.Sprintf("line %d", i)
	}
	require.Equal(t, want, readVectorAsStrings(t, db, e, ":doc/lines", false),
		"schemaless append to an inferred vector attribute must extend the vector (RGA), not LWW-overwrite it")
}

// TestSchemalessPrefetch_VectorNotCollapsed pins the EnableEntityPrefetch path
// (the 5th cardinality-decision site): PrefetchEntities warms the EA cache by
// resolving each (E,A) group, and must classify a schemaless vector as Vector
// from its CRDT ops, not collapse it to a single LWW value. Pre-fix it called
// GetCardinality (CardinalityOne for schemaless) and cached the last element.
func TestSchemalessPrefetch_VectorNotCollapsed(t *testing.T) {
	const n = 5
	e := datalog.NewIdentity("doc-1")
	dir := writeVectorDB(t, n, e)
	attr := datalog.NewKeyword(":doc/lines")

	db, err := NewDatabase(dir) // schemaless, cache enabled
	require.NoError(t, err)
	defer db.Close()

	m := db.Matcher().(*BadgerMatcher)
	// Warm the EA cache the way EnableEntityPrefetch does.
	m.PrefetchEntities([]datalog.Identity{e})

	// The entry prefetch populated must be a full vector. PopulateFromDatoms set
	// the entry's version to the group's max ElementID, so GetOrResolve returns
	// the prefetched entry without rebuilding — i.e. this asserts what prefetch
	// itself cached.
	var eEnt Entity
	copy(eEnt[:], e.Bytes())
	aStorage := ToStorageDatom(datalog.Datom{A: attr}).A
	var aAttr Attribute
	copy(aAttr[:], aStorage[:])
	key, ok := m.cacheKey(eEnt, aAttr)
	require.True(t, ok)
	entry := db.cache.GetOrResolve(key, m)
	require.NotNil(t, entry)
	require.Equal(t, schema.CardinalityVector, entry.Cardinality(),
		"prefetch must classify a schemaless vector as Vector, not collapse to One")
	require.Len(t, entry.VectorList(), n,
		"prefetch must cache all vector elements, not just the last")
}
