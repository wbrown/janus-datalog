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

// BenchmarkGetOrResolve_FreshHit measures the cache-hit hot path, including the
// entry.inFlight sentinel check and the reader's cardinality read. A fresh hit
// resolves nothing, so the resolver's resolution count stays at zero.
// Alloc-free.
func BenchmarkGetOrResolve_FreshHit(b *testing.B) {
	c := NewCache()
	var e Entity
	copy(e[:], "0123456789abcdef0123")
	var a Attribute
	copy(a[:], ":bench/attr")
	key := CacheKey{E: e, A: a}

	ver := datalog.ElementID{}
	c.slots.Store(key, cacheSlot{
		entry: &CacheEntry{
			version:     ver,
			cardinality: schema.CardinalityOne,
			oneValue:    int64(42),
		},
		version: ver,
	})
	resolver := NewPatternMatcher(NewMemoryStore(&BinaryKeyEncoder{}))

	// Prove the fresh-hit path resolves cleanly before measuring it. The check
	// is here rather than in the loop because the loop exists to measure a
	// single branch, and a per-iteration error test would be measuring itself.
	// It also pins that the hit is real: a fallthrough to the rebuild path
	// would read the index.
	hit := &scanReport{}
	if _, err := c.GetOrResolve(key, schema.CardinalityOne, resolver, nil, nil, hit); err != nil {
		b.Fatal(err)
	}
	if hit.scanned != 0 {
		b.Fatalf("the fresh hit read %d index entries, want none", hit.scanned)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getResolveSink, getResolveErrSink = c.GetOrResolve(key, schema.CardinalityOne, resolver, nil, nil, DiscardIntake)
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
