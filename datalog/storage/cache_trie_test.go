package storage

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// trieKey builds a CacheKey directly from raw bytes. Tests construct keys
// byte-wise because routing is deterministic and public: collision coverage
// crafts real colliding keys instead of injecting an artificial hash.
func trieKey(e byte, attr string) CacheKey {
	var key CacheKey
	key.E[19] = e
	copy(key.A[:], attr)
	return key
}

// collidingKeys returns n distinct keys with identical routing values: they
// share E's first word (zero) and A, differing only in E's last byte, which
// the routing fold never reads.
func collidingKeys(n int) []CacheKey {
	keys := make([]CacheKey, n)
	for i := range keys {
		keys[i] = trieKey(byte(i+1), ":collide/attr")
	}
	return keys
}

// slotFor returns a deterministic slot per version: cacheSlot equality
// compares the entry pointer, so equal versions must share one CacheEntry
// for slot comparisons (CAS, CompareAndDelete, Load assertions) to mean
// anything. Guarded because the concurrency tests call it from goroutines.
var (
	slotEntriesMu sync.Mutex
	slotEntries   = map[uint64]*CacheEntry{}
)

func slotFor(version uint64) cacheSlot {
	slotEntriesMu.Lock()
	entry, ok := slotEntries[version]
	if !ok {
		entry = &CacheEntry{version: datalog.ElementID{Lamport: version}}
		slotEntries[version] = entry
	}
	slotEntriesMu.Unlock()
	return cacheSlot{entry: entry, version: datalog.ElementID{Lamport: version}}
}

func TestCacheKeyRoutingSeparatesSameEntityAttributes(t *testing.T) {
	// Same entity, same-namespace keywords that agree through byte 7 —
	// the case a first-word-only fold would collide.
	name := trieKey(1, ":person/name")
	age := trieKey(1, ":person/age")
	if name.routing() == age.routing() {
		t.Error("same-entity same-namespace attributes must route apart — the fold must consume every attribute word")
	}
	// And the collision harness really collides.
	keys := collidingKeys(3)
	if keys[0].routing() != keys[1].routing() || keys[1].routing() != keys[2].routing() {
		t.Error("collidingKeys must produce identical routing values")
	}
}

func TestCacheTrieBasicOps(t *testing.T) {
	ht := newCacheTrie()
	a := trieKey(1, ":person/name")
	b := trieKey(2, ":person/age")

	if _, ok := ht.Load(a); ok {
		t.Error("empty trie must not load")
	}
	s1 := slotFor(1)
	if _, loaded := ht.LoadOrStore(a, s1); loaded {
		t.Error("first LoadOrStore must store")
	}
	if got, ok := ht.Load(a); !ok || got != s1 {
		t.Errorf("Load after store: got %+v ok=%v", got, ok)
	}
	if got, loaded := ht.LoadOrStore(a, slotFor(9)); !loaded || got != s1 {
		t.Error("second LoadOrStore must load the existing slot")
	}

	s2 := slotFor(2)
	ht.Store(b, s2)
	if prev, loaded := ht.Swap(b, slotFor(3)); !loaded || prev != s2 {
		t.Error("Swap must return the previous slot")
	}

	s3 := slotFor(3)
	if ht.CompareAndSwap(b, s2, slotFor(4)) {
		t.Error("CAS with stale old slot must fail")
	}
	if !ht.CompareAndSwap(b, s3, slotFor(4)) {
		t.Error("CAS with current slot must succeed")
	}

	if ht.CompareAndDelete(b, slotFor(3)) {
		t.Error("CompareAndDelete with stale slot must fail")
	}
	if !ht.CompareAndDelete(b, slotFor(4)) {
		t.Error("CompareAndDelete with current slot must succeed")
	}
	if _, ok := ht.Load(b); ok {
		t.Error("deleted key must not load")
	}
	ht.Delete(a)
	if _, ok := ht.Load(a); ok {
		t.Error("deleted key must not load")
	}
}

func TestCacheTrieRoutingCollisions(t *testing.T) {
	ht := newCacheTrie()
	keys := collidingKeys(12)

	for i, key := range keys {
		ht.Store(key, slotFor(uint64(i+1)))
	}
	for i, key := range keys {
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+1)) {
			t.Fatalf("chained key %d: got %+v ok=%v", i, got, ok)
		}
	}

	// Delete from the middle of the chain; the rest must survive.
	ht.Delete(keys[5])
	if _, ok := ht.Load(keys[5]); ok {
		t.Error("deleted chained key must not load")
	}
	for i, key := range keys {
		if i == 5 {
			continue
		}
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+1)) {
			t.Fatalf("chain broken after middle delete at key %d", i)
		}
	}

	// CAS on a chained entry.
	if !ht.CompareAndSwap(keys[9], slotFor(10), slotFor(100)) {
		t.Error("CAS on chained entry must succeed")
	}
	if got, _ := ht.Load(keys[9]); got != slotFor(100) {
		t.Error("CAS on chained entry must be visible")
	}

	// CompareAndDelete on a chained entry: stale fails, current deletes,
	// and the rest of the chain survives.
	if ht.CompareAndDelete(keys[7], slotFor(99)) {
		t.Error("chained CompareAndDelete with stale slot must fail")
	}
	if !ht.CompareAndDelete(keys[7], slotFor(8)) {
		t.Error("chained CompareAndDelete with current slot must succeed")
	}
	if _, ok := ht.Load(keys[7]); ok {
		t.Error("chained CompareAndDelete must delete the key")
	}
	if got, _ := ht.Load(keys[9]); got != slotFor(100) {
		t.Error("chain must survive a chained CompareAndDelete")
	}

	// Delete every chained key; all must read absent afterward.
	for _, key := range keys {
		ht.Delete(key)
	}
	for i, key := range keys {
		if _, ok := ht.Load(key); ok {
			t.Fatalf("key %d survived chain teardown", i)
		}
	}
}

func TestCacheTrieRange(t *testing.T) {
	ht := newCacheTrie()
	keys := map[CacheKey]cacheSlot{}
	for i := 0; i < 64; i++ {
		key := trieKey(byte(i), fmt.Sprintf(":range/attr%d", i))
		keys[key] = slotFor(uint64(i + 1))
		ht.Store(key, keys[key])
	}

	seen := map[CacheKey]cacheSlot{}
	ht.Range(func(key CacheKey, slot cacheSlot) bool {
		if _, dup := seen[key]; dup {
			t.Fatalf("Range visited key twice: %v", key)
		}
		seen[key] = slot
		return true
	})
	if len(seen) != len(keys) {
		t.Fatalf("Range visited %d keys, want %d", len(seen), len(keys))
	}
	for key, want := range keys {
		if seen[key] != want {
			t.Errorf("Range slot mismatch for %v", key)
		}
	}

	// Early stop.
	visits := 0
	ht.Range(func(CacheKey, cacheSlot) bool {
		visits++
		return false
	})
	if visits != 1 {
		t.Errorf("Range must stop when yield returns false, visited %d", visits)
	}
}

func TestCacheTrieConcurrent(t *testing.T) {
	ht := newCacheTrie()
	// A mix of well-distributed and deliberately colliding keys.
	keys := collidingKeys(8)
	for i := 0; i < 24; i++ {
		keys = append(keys, trieKey(byte(i), fmt.Sprintf(":spread/attr%d", i)))
	}

	workers := runtime.GOMAXPROCS(-1)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for round := 0; round < 200; round++ {
				for _, key := range keys {
					ht.LoadOrStore(key, slotFor(uint64(id+1)))
					if slot, ok := ht.Load(key); ok {
						// CAS may succeed or fail against other workers;
						// either way the trie must stay consistent.
						ht.CompareAndSwap(key, slot, slotFor(slot.version.Lamport+1))
					}
					if round%50 == 49 {
						ht.Delete(key)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	// Every key must be readable or absent — never panic, never corrupt.
	for _, key := range keys {
		if slot, ok := ht.Load(key); ok && slot.entry == nil && slot.version == (datalog.ElementID{}) {
			t.Errorf("zero slot stored for %v", key)
		}
	}
}

func TestCacheTrieConcurrentClear(t *testing.T) {
	ht := newCacheTrie()
	keys := collidingKeys(16)
	for i, key := range keys {
		ht.Store(key, slotFor(uint64(i+1)))
	}

	var wg sync.WaitGroup
	for w := 0; w < runtime.GOMAXPROCS(-1); w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for _, key := range keys {
				ht.CompareAndDelete(key, slotFor(uint64(id)))
				ht.CompareAndSwap(key, slotFor(uint64(id)), slotFor(uint64(id+1)))
			}
		}(w)
	}
	runtime.Gosched()
	ht.Clear()
	wg.Wait()

	ht.Clear()
	for _, key := range keys {
		if _, ok := ht.Load(key); ok {
			t.Errorf("key %v survived Clear", key)
		}
	}
}
