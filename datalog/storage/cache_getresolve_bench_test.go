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
// entry.inFlight sentinel check. The resolver is never invoked on a fresh hit,
// so a nil resolver is safe. Alloc-free.
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

	// Prove the fresh-hit path resolves cleanly before measuring it. The check
	// is here rather than in the loop because the loop exists to measure a
	// single branch, and a per-iteration error test would be measuring itself.
	// It also pins that the hit is real: on a miss the nil resolver panics
	// rather than erroring, so a silent fallthrough to the rebuild path cannot
	// masquerade as a hit.
	if _, err := c.GetOrResolve(key, nil, nil, nil, DiscardIntake); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getResolveSink, getResolveErrSink = c.GetOrResolve(key, nil, nil, nil, DiscardIntake)
	}
}
