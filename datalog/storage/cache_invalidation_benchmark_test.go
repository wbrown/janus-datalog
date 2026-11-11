package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// benchmarkCacheKeys returns the keys of a cache holding entries for a tenth
// as many entities as entries, each entity under the attributes :bench/attr0
// to :bench/attr9, so a tenth of the entries carry each attribute.
func benchmarkCacheKeys(entries int) []CacheKey {
	attrs := make([]datalog.Keyword, 10)
	for i := range attrs {
		attrs[i] = datalog.NewKeyword(fmt.Sprintf(":bench/attr%d", i))
	}
	keys := make([]CacheKey, 0, entries)
	for e := 0; e < entries/len(attrs); e++ {
		entity := datalog.NewIdentity(fmt.Sprintf("bench:entity:%d", e))
		for _, a := range attrs {
			keys = append(keys, CacheKey{E: entity, A: a})
		}
	}
	return keys
}

// storeBenchmarkEntries stores one entry per key through the cache's store
// path at the clock's read point, and fails the benchmark on a key the cache
// refuses.
func storeBenchmarkEntries(b *testing.B, c *Cache, keys []CacheKey) {
	readPoint := c.ReadPoint()
	for _, key := range keys {
		if !c.storeIfNotInFlight(key, &CacheEntry{version: datalog.ElementID{Lamport: 1}}, readPoint) {
			b.Fatalf("the cache refused to store %v", key)
		}
	}
}

// cachedEntryCount counts the cache's entries by walking them.
func cachedEntryCount(c *Cache) int {
	n := 0
	c.slots.Range(func(CacheKey, cacheSlot) bool {
		n++
		return true
	})
	return n
}

// BenchmarkInvalidateAttribute measures the walk a unique-attribute
// invalidation makes over a cache of N entries: every entry is visited and
// those under the attribute are deleted. Under "absent" no entry carries the
// attribute, so the walk deletes nothing and the cache stands across the
// iterations. Under "tenth" the attribute is one of the ten every entity
// carries, so each iteration deletes a tenth of the entries, which are stored
// again outside the timer.
func BenchmarkInvalidateAttribute(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			keys := benchmarkCacheKeys(entries)
			c := NewCache(NewLamportClock(1))
			storeBenchmarkEntries(b, c, keys)
			if n := cachedEntryCount(c); n != entries {
				b.Fatalf("the cache holds %d entries, want %d", n, entries)
			}

			b.Run("absent", func(b *testing.B) {
				absent := datalog.NewKeyword(":bench/absent")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					c.InvalidateAttribute(absent)
				}
				b.StopTimer()
				if n := cachedEntryCount(c); n != entries {
					b.Fatalf("the walk over an attribute no entry carries left %d entries, want %d", n, entries)
				}
			})

			b.Run("tenth", func(b *testing.B) {
				attr := keys[0].A
				var under []CacheKey
				for _, key := range keys {
					if key.A == attr {
						under = append(under, key)
					}
				}
				c.InvalidateAttribute(attr)
				if n := cachedEntryCount(c); n != entries-len(under) {
					b.Fatalf("the walk over %v left %d entries, want %d", attr, n, entries-len(under))
				}
				storeBenchmarkEntries(b, c, under)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					c.InvalidateAttribute(attr)
					b.StopTimer()
					storeBenchmarkEntries(b, c, under)
					b.StartTimer()
				}
				b.StopTimer()
				if n := cachedEntryCount(c); n != entries {
					b.Fatalf("the cache holds %d entries after the last refill, want %d", n, entries)
				}
			})
		})
	}
}

// BenchmarkCacheClear measures Clear over a cache of N entries. Clear stores a
// fresh root in place of the trie, so its own work does not grow with N; the
// garbage it leaves does. The entries are stored again outside the timer, so a
// run is bounded by its iteration count: pass -benchtime=<n>x.
func BenchmarkCacheClear(b *testing.B) {
	for _, entries := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			keys := benchmarkCacheKeys(entries)
			c := NewCache(NewLamportClock(1))
			storeBenchmarkEntries(b, c, keys)
			if n := cachedEntryCount(c); n != entries {
				b.Fatalf("the cache holds %d entries, want %d", n, entries)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Clear()
				b.StopTimer()
				storeBenchmarkEntries(b, c, keys)
				b.StartTimer()
			}
			b.StopTimer()
			if n := cachedEntryCount(c); n != entries {
				b.Fatalf("the cache holds %d entries after the last refill, want %d", n, entries)
			}
		})
	}
}
