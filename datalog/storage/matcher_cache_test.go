package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Note: fmt, executor, query imports used by benchmarks at end of file

// Helper to create a temp database for matcher-cache tests
func createMatcherCacheTestDatabase(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

func TestMatcherCacheCardinalityOne(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").
		Type(schema.TypeString).
		One().
		Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher with cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	// Verify cache returns correct LWW value
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityOne, entry.Cardinality())
	assert.Equal(t, "Alice", entry.OneValue())
}

func TestMatcherCacheCardinalityMany(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/tags").
		Type(schema.TypeString).
		Many().
		Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "veteran")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher with cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	// Verify cache returns correct set
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/tags")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityMany, entry.Cardinality())
	assert.True(t, entry.ManySet()["warrior"])
	assert.True(t, entry.ManySet()["veteran"])
}

func TestMatcherCacheCardinalityVector(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":character/skills").
		Type(schema.TypeString).
		Vector().
		Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("character1")
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "stealth")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "archery")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher with cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	// Verify cache returns correct vector
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":character/skills")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityVector, entry.Cardinality())
	assert.Equal(t, []any{"stealth", "archery"}, entry.VectorList())
}

func TestMatcherCacheConsistentWithDirectScan(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get value via cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	cachedValue := entry.OneValue()

	// Get value via direct lookup (LookupAttribute)
	directValue, found := matcher.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)

	// Both should return the same value
	assert.Equal(t, cachedValue, directValue)
}

func TestMatcherCacheInvalidationOnWrite(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add initial data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify initial value via cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry1 := db.Cache().GetOrResolve(key, matcher)
	assert.Equal(t, "Alice", entry1.OneValue())

	// Update data
	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":person/name"), "Bob")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Cache should be invalidated, next resolve should see new value
	entry2 := db.Cache().GetOrResolve(key, matcher)
	assert.Equal(t, "Bob", entry2.OneValue())
}

func TestMatcherCacheWithSchemalessAttribute(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// No schema set - should default to cardinality-one

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("entity1")
	// Set works without schema (defaults to cardinality-one)
	err := tx.Set(e, datalog.NewKeyword(":unknown/attr"), "value")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher without schema
	matcher := NewBadgerMatcher(db.Store())

	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":unknown/attr")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	// Should default to cardinality-one
	assert.Equal(t, schema.CardinalityOne, entry.Cardinality())
	assert.Equal(t, "value", entry.OneValue())
}

func TestMatcherCacheLWWResolution(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Write multiple values - highest ElementID should win
	tx1 := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx1.Set(e, datalog.NewKeyword(":person/name"), "First")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":person/name"), "Second")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	err = tx3.Set(e, datalog.NewKeyword(":person/name"), "Third")
	require.NoError(t, err)
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Cache should resolve to "Third" (highest ElementID)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, "Third", entry.OneValue())
}

func TestMatcherCacheAddWinsResolution(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e := datalog.NewIdentity("person1")

	// Add a tag
	tx1 := db.NewTransaction()
	err = tx1.Add(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Remove the tag
	tx2 := db.NewTransaction()
	err = tx2.Remove(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Add it back
	tx3 := db.NewTransaction()
	err = tx3.Add(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Cache should resolve to "warrior" being in set (add-wins with latest add)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/tags")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.True(t, entry.ManySet()["warrior"], "warrior should be in set after add-remove-add")
}

// =============================================================================
// BENCHMARKS - Cache Path Tuple Building
// =============================================================================

// BenchmarkCachePathTupleBuilding measures the allocation overhead of the cache
// path tuple building, which currently creates an intermediate Datom struct
// just to extract fields into a tuple.
func BenchmarkCachePathTupleBuilding(b *testing.B) {
	// Setup
	dir := b.TempDir()
	db, err := NewDatabase(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	db.SetSchema(s)

	// Create 100 entities with names
	for i := 0; i < 100; i++ {
		tx := db.NewTransaction()
		e := datalog.NewIdentity("person" + string(rune('0'+i/10)) + string(rune('0'+i%10)))
		tx.Set(e, datalog.NewKeyword(":person/name"), "Name"+string(rune('0'+i/10))+string(rune('0'+i%10)))
		tx.Commit()
	}

	// Warm cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	matcher.SetCache(db.Cache())

	b.Run("Query_CacheHit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, _ := db.ExecuteQuery(`[:find ?e ?name :where [?e :person/name ?name]]`)
			if len(result) != 100 {
				b.Fatalf("expected 100 results, got %d", len(result))
			}
		}
	})
}

// BenchmarkCacheResolutionOverhead measures the overhead of cache resolution
// vs direct storage access.
func BenchmarkCacheResolutionOverhead(b *testing.B) {
	dir := b.TempDir()
	db, err := NewDatabase(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	db.SetSchema(s)

	// Create entity
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	tx.Commit()

	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	b.Run("CacheGetOrResolve", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			entry := db.Cache().GetOrResolve(key, matcher)
			if entry == nil {
				b.Fatal("cache miss")
			}
			_ = entry.OneValue()
		}
	})

	b.Run("DirectLookupAttribute", func(b *testing.B) {
		b.ReportAllocs()
		attr := datalog.NewKeyword(":person/name")
		for i := 0; i < b.N; i++ {
			val, found := matcher.LookupAttribute(e, attr)
			if !found {
				b.Fatal("not found")
			}
			_ = val
		}
	})
}

// BenchmarkCachePathWithBindings exercises the matchWithBindingsFromCache code path
// in matcher_relations.go:612-739. This path is triggered when:
// 1. A pattern has E bound from a previous join (bindings exist)
// 2. A is a constant attribute
// 3. Cache is enabled
//
// The buildTuple closure at line 657-660 creates a *Datom just to pass to
// BuildTupleInterned - this benchmark measures that allocation overhead.
func BenchmarkCachePathWithBindings(b *testing.B) {
	dir := b.TempDir()
	db, err := NewDatabase(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	s, _ := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Build()
	db.SetSchema(s)

	// Create 100 entities with name and age
	for i := 0; i < 100; i++ {
		tx := db.NewTransaction()
		e := datalog.NewIdentity(fmt.Sprintf("person%d", i))
		tx.Set(e, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		tx.Set(e, datalog.NewKeyword(":person/age"), int64(20+i))
		tx.Commit()
	}

	// This query triggers matchWithBindingsFromCache:
	// - First pattern [?e :person/name ?name] binds ?e
	// - Second pattern [?e :person/age ?age] has ?e from bindings, :person/age constant
	// The second pattern goes through the cache path with bindings
	b.Run("JoinQuery_CachePath", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := db.ExecuteQuery(`[:find ?e ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
			if err != nil {
				b.Fatal(err)
			}
			if len(result) != 100 {
				b.Fatalf("expected 100 results, got %d", len(result))
			}
		}
	})

	// Direct matcher.Match call to isolate the cache path more precisely
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	matcher.SetCache(db.Cache())

	// Create binding relation simulating output from first pattern
	var bindingTuples []executor.Tuple
	for i := 0; i < 100; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("person%d", i))
		bindingTuples = append(bindingTuples, executor.Tuple{e})
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		bindingTuples,
	)

	// Pattern for second clause: [?e :person/age ?age]
	agePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: datalog.NewSymbol("?age")},
		},
	}
	columns := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?age")}

	b.Run("MatcherMatch_WithBindings", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := matcher.Match(agePattern, executor.Relations{bindingRel})
			if err != nil {
				b.Fatal(err)
			}
			// Consume iterator to trigger all tuple building
			count := 0
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
				count++
			}
			it.Close()
			if count != 100 {
				b.Fatalf("expected 100 results, got %d", count)
			}
		}
	})

	_ = columns // Used in pattern setup
}
