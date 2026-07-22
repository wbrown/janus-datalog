package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var getResolveSink *CacheEntry

// BenchmarkGetOrResolve_FreshHit measures the cache-hit hot path — the path the
// in-flight stale-read fix added a field read to (entry.inFlight). The resolver
// is never invoked on a fresh hit, so a nil resolver is safe. Alloc-free; a
// before/after benchstat shows whether the sentinel check costs anything.
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

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getResolveSink = c.GetOrResolve(key, nil, nil)
	}
}
