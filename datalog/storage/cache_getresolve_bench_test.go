package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	getResolveSink    *CacheEntry
	getResolveErrSink error
)

// cacheWithOneFreshEntry is a cache holding one fresh entry for the returned
// key, built through the returned resolver over a store on backend holding the
// key's one datom, so a rebuild over that store reads the index where a hit
// reads none.
func cacheWithOneFreshEntry(tb testing.TB, backend Backend) (*Cache, CacheKey, datalog.Keyword, *PatternMatcher) {
	tb.Helper()
	s, err := schema.NewBuilder().
		Attribute(":bench/attr").Type(schema.TypeLong).One().Add().
		Build()
	if err != nil {
		tb.Fatal(err)
	}
	db := openBackendDB(tb, backend, DatabaseOptions{Schema: s})
	entity := datalog.NewIdentity("bench")
	attr := datalog.NewKeyword(":bench/attr")
	tx := db.NewTransaction()
	if err := tx.Set(entity, attr, int64(42)); err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	resolver := db.Matcher().(*PatternMatcher)
	var a Attribute
	copy(a[:], attr.String())
	key := CacheKey{E: Entity(entity.Hash()), A: a}
	card := resolver.GetCardinality(attr)

	// The build's report shows what a rebuild over this store reads, so a pin
	// that a hit reads none tells a hit from a fallthrough to the rebuild path.
	c := NewCache()
	build := &scanReport{}
	if _, err := c.GetOrResolve(key, card, resolver, nil, nil, build); err != nil {
		tb.Fatal(err)
	}
	if build.scanned == 0 {
		tb.Fatal("the build read no index entry, so a rebuild could pass as a hit")
	}
	return c, key, card, resolver
}

// BenchmarkGetOrResolve_FreshHit measures the cache-hit hot path, including the
// entry.inFlight sentinel check and the reader's rule checks, one leg per
// backend. A fresh hit resolves nothing, so the resolver's resolution count
// stays at zero. Alloc-free, which TestGetOrResolveFreshHitAllocatesNothing
// pins.
func BenchmarkGetOrResolve_FreshHit(b *testing.B) {
	for _, backend := range AvailableBackends() {
		b.Run(backend.Name, func(b *testing.B) {
			c, key, card, resolver := cacheWithOneFreshEntry(b, backend)

			// Prove the fresh-hit path resolves cleanly before measuring it. The
			// check is here rather than in the loop because the loop exists to
			// measure a single branch, and a per-iteration error test would be
			// measuring itself.
			hit := &scanReport{}
			if _, err := c.GetOrResolve(key, card, resolver, nil, nil, hit); err != nil {
				b.Fatal(err)
			}
			if hit.scanned != 0 {
				b.Fatalf("the fresh hit read %d index entries, want none", hit.scanned)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				getResolveSink, getResolveErrSink = c.GetOrResolve(key, card, resolver, nil, nil, DiscardIntake)
			}
		})
	}
}

// TestGetOrResolveFreshHitAllocatesNothing pins that a fresh hit is the trie
// walk and the reader's rule checks and nothing else: it allocates nothing,
// so a reader pays no decode on the cache's hot path.
func TestGetOrResolveFreshHitAllocatesNothing(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			c, key, card, resolver := cacheWithOneFreshEntry(t, backend)
			allocs := testing.AllocsPerRun(100, func() {
				getResolveSink, getResolveErrSink = c.GetOrResolve(key, card, resolver, nil, nil, DiscardIntake)
			})
			if getResolveErrSink != nil {
				t.Fatal(getResolveErrSink)
			}
			if allocs != 0 {
				t.Errorf("a fresh hit allocates %v per call, want none", allocs)
			}
		})
	}
}

var getCardinalitySink datalog.Keyword

// BenchmarkPatternMatcherGetCardinality measures the matcher's cardinality
// read, which a lookup makes once before it asks the cache: the attribute
// decoded, the keyword interned, the schema consulted. A fresh hit reads none.
func BenchmarkPatternMatcherGetCardinality(b *testing.B) {
	s, err := schema.NewBuilder().
		Attribute(":bench/attr").Type(schema.TypeString).Many().Add().
		Build()
	if err != nil {
		b.Fatal(err)
	}
	m := NewPatternMatcher(NewMemoryStore(&BinaryKeyEncoder{}))
	m.SetSchema(s)
	attr := datalog.NewKeyword(":bench/attr")
	if card := m.GetCardinality(attr); card != schema.CardinalityMany {
		b.Fatalf("the matcher reads %s for a declared set", card)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getCardinalitySink = m.GetCardinality(attr)
	}
}

// TestPatternMatcherGetCardinalityAllocatesNothing pins that the matcher's
// cardinality read is a schema lookup and nothing else: it allocates
// nothing, so a cache call site pays no decode on a hit.
func TestPatternMatcherGetCardinalityAllocatesNothing(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":bench/attr").Type(schema.TypeString).Many().Add().
		Build()
	if err != nil {
		t.Fatal(err)
	}
	m := NewPatternMatcher(NewMemoryStore(&BinaryKeyEncoder{}))
	m.SetSchema(s)
	attr := datalog.NewKeyword(":bench/attr")
	allocs := testing.AllocsPerRun(100, func() {
		getCardinalitySink = m.GetCardinality(attr)
	})
	if allocs != 0 {
		t.Errorf("the cardinality read allocates %v per call, want none", allocs)
	}
	if getCardinalitySink != schema.CardinalityMany {
		t.Errorf("the matcher reads %s for a declared set", getCardinalitySink)
	}
}
