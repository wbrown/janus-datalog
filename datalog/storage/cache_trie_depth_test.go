package storage

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// routedKey crafts a key with an exact routing value: A is zero, so the fold
// reduces to E's first word. tag keeps keys with equal routing distinct.
// Routing is public and deterministic (owner ruling), so trie-shape tests
// construct the exact towers and chains they mean to exercise.
func routedKey(r uint64, tag byte) CacheKey {
	var key CacheKey
	binary.BigEndian.PutUint64(key.E[0:8], r)
	key.E[19] = tag
	return key
}

// towerKeys returns 16 keys whose routing values share the top 60 bits and
// differ only in the lowest nibble: identical routing down 15 trie levels,
// splitting at the deepest level, so expand builds a full indirect tower and
// deletes exercise the multi-level collapse walk.
func towerKeys(base uint64) []CacheKey {
	keys := make([]CacheKey, 16)
	for i := range keys {
		keys[i] = routedKey(base&^uint64(0xF)|uint64(i), 0)
	}
	return keys
}

func TestCacheTrieDeepTowerBuildAndCollapse(t *testing.T) {
	ht := newCacheTrie()
	keys := towerKeys(0xABCDEF0123456780)

	for i, key := range keys {
		ht.Store(key, slotFor(uint64(i+1)))
	}
	for i, key := range keys {
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+1)) {
			t.Fatalf("tower key %d unreadable after build", i)
		}
	}

	// Delete one at a time; after every single delete, every survivor must
	// still load and the deleted keys must not. This drives the collapse
	// walk at every occupancy of the deep indirects.
	for i, key := range keys {
		if _, loaded := ht.LoadAndDelete(key); !loaded {
			t.Fatalf("tower key %d must delete", i)
		}
		for j, other := range keys {
			got, ok := ht.Load(other)
			if j <= i {
				if ok {
					t.Fatalf("deleted tower key %d resurfaced after deleting %d", j, i)
				}
				continue
			}
			if !ok || got != slotFor(uint64(j+1)) {
				t.Fatalf("tower key %d lost after deleting %d", j, i)
			}
		}
	}

	// The fully-collapsed trie must accept reinsertion.
	for i, key := range keys {
		ht.Store(key, slotFor(uint64(i+100)))
	}
	for i, key := range keys {
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+100)) {
			t.Fatalf("tower key %d unreadable after rebuild", i)
		}
	}
}

func TestCacheTrieChainAtTowerBottom(t *testing.T) {
	ht := newCacheTrie()
	// Four keys with IDENTICAL routing, distinct tags: an overflow chain
	// hanging at the bottom of a full 16-level tower once a near-colliding
	// neighbor forces the expansion.
	const r = 0x123456789ABCDEF0
	chain := make([]CacheKey, 4)
	for i := range chain {
		chain[i] = routedKey(r, byte(i+1))
	}
	neighbor := routedKey(r^1, 0) // differs in the lowest routing bit

	for i, key := range chain {
		ht.Store(key, slotFor(uint64(i+1)))
	}
	ht.Store(neighbor, slotFor(99))

	for i, key := range chain {
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+1)) {
			t.Fatalf("chained key %d unreadable at tower bottom", i)
		}
	}

	// Middle-of-chain CompareAndDelete at full depth.
	if !ht.CompareAndDelete(chain[1], slotFor(2)) {
		t.Fatal("chained CompareAndDelete at tower bottom must succeed")
	}
	if _, ok := ht.Load(chain[1]); ok {
		t.Fatal("deleted chained key resurfaced")
	}
	for _, i := range []int{0, 2, 3} {
		if got, ok := ht.Load(chain[i]); !ok || got != slotFor(uint64(i+1)) {
			t.Fatalf("chain neighbor %d lost after middle delete", i)
		}
	}

	// Tear down the chain, then the neighbor: the tower must collapse and
	// every key must read absent.
	for _, i := range []int{0, 2, 3} {
		ht.Delete(chain[i])
	}
	ht.Delete(neighbor)
	for i, key := range chain {
		if _, ok := ht.Load(key); ok {
			t.Fatalf("chained key %d survived teardown", i)
		}
	}
	if _, ok := ht.Load(neighbor); ok {
		t.Fatal("neighbor survived teardown")
	}
}

func TestCacheTrieBranchDepths(t *testing.T) {
	ht := newCacheTrie()
	// Pairs branching at each level: for level L, two keys sharing routing
	// above bit 64-4L and differing at that level's nibble.
	var keys []CacheKey
	for level := 0; level < 16; level++ {
		shift := uint(60 - 4*level)
		base := uint64(0x1111111111111111)
		keys = append(keys,
			routedKey(base, byte(level)),
			routedKey(base^(0x1<<shift), byte(level)),
		)
	}
	for i, key := range keys {
		ht.Store(key, slotFor(uint64(i+1)))
	}
	for i, key := range keys {
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+1)) {
			t.Fatalf("branch-depth key %d unreadable", i)
		}
	}
	// Delete in reverse, checking survivors each time.
	for i := len(keys) - 1; i >= 0; i-- {
		ht.Delete(keys[i])
		for j := 0; j < i; j++ {
			if got, ok := ht.Load(keys[j]); !ok || got != slotFor(uint64(j+1)) {
				t.Fatalf("branch-depth key %d lost after deleting %d", j, i)
			}
		}
	}
}

func TestCacheTrieConcurrentTowerChurn(t *testing.T) {
	ht := newCacheTrie()
	keys := towerKeys(0xFEDCBA9876543210)
	const r = 0xFEDCBA9876543210
	for i := byte(1); i <= 4; i++ {
		keys = append(keys, routedKey(r, i)) // chained at the tower bottom
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id + 1)))
			for round := 0; round < 400; round++ {
				key := keys[rng.Intn(len(keys))]
				switch rng.Intn(5) {
				case 0:
					ht.LoadOrStore(key, slotFor(uint64(id+1)))
				case 1:
					ht.Store(key, slotFor(uint64(id+10)))
				case 2:
					if slot, ok := ht.Load(key); ok {
						ht.CompareAndSwap(key, slot, slotFor(slot.version.Lamport+1))
					}
				case 3:
					ht.LoadAndDelete(key)
				case 4:
					if slot, ok := ht.Load(key); ok {
						ht.CompareAndDelete(key, slot)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	// The tower must be structurally intact: every key loads or is absent,
	// and a full rebuild reads back cleanly.
	for i, key := range keys {
		ht.Store(key, slotFor(uint64(i+1000)))
	}
	for i, key := range keys {
		if got, ok := ht.Load(key); !ok || got != slotFor(uint64(i+1000)) {
			t.Fatalf("tower key %d corrupt after concurrent churn", i)
		}
	}
}

// TestCacheTrieOracle drives long random operation sequences against a plain
// map oracle. The key universe mixes spread keys, exact-collision chains,
// near-collision towers, and chains at tower bottoms, so the comparison
// covers every structural regime; every return value must match the oracle.
func TestCacheTrieOracle(t *testing.T) {
	var universe []CacheKey
	for i := 0; i < 10; i++ {
		universe = append(universe, trieKey(byte(i), fmt.Sprintf(":oracle/spread%d", i)))
	}
	for i := byte(1); i <= 6; i++ {
		universe = append(universe, routedKey(0x5555AAAA5555AAAA, i)) // exact collisions
	}
	universe = append(universe, towerKeys(0x7777777777777770)...) // near collisions
	for i := byte(1); i <= 4; i++ {
		universe = append(universe, routedKey(0x7777777777777770, i)) // chain at tower bottom
	}

	for seed := int64(1); seed <= 5; seed++ {
		rng := rand.New(rand.NewSource(seed))
		ht := newCacheTrie()
		oracle := map[CacheKey]cacheSlot{}

		for op := 0; op < 50000; op++ {
			key := universe[rng.Intn(len(universe))]
			want, inOracle := oracle[key]
			switch rng.Intn(8) {
			case 0:
				got, ok := ht.Load(key)
				if ok != inOracle || (ok && got != want) {
					t.Fatalf("seed %d op %d: Load mismatch", seed, op)
				}
			case 1:
				next := slotFor(uint64(rng.Intn(64) + 1))
				ht.Store(key, next)
				oracle[key] = next
			case 2:
				next := slotFor(uint64(rng.Intn(64) + 1))
				got, loaded := ht.LoadOrStore(key, next)
				if loaded != inOracle {
					t.Fatalf("seed %d op %d: LoadOrStore loaded mismatch", seed, op)
				}
				if inOracle && got != want {
					t.Fatalf("seed %d op %d: LoadOrStore value mismatch", seed, op)
				}
				if !inOracle {
					oracle[key] = next
				}
			case 3:
				next := slotFor(uint64(rng.Intn(64) + 1))
				prev, loaded := ht.Swap(key, next)
				if loaded != inOracle || (loaded && prev != want) {
					t.Fatalf("seed %d op %d: Swap mismatch", seed, op)
				}
				oracle[key] = next
			case 4:
				old := slotFor(uint64(rng.Intn(64) + 1))
				if inOracle && rng.Intn(2) == 0 {
					old = want // correct old half the time
				}
				next := slotFor(uint64(rng.Intn(64) + 1))
				swapped := ht.CompareAndSwap(key, old, next)
				wantSwap := inOracle && old == want
				if swapped != wantSwap {
					t.Fatalf("seed %d op %d: CAS outcome mismatch", seed, op)
				}
				if wantSwap {
					oracle[key] = next
				}
			case 5:
				got, loaded := ht.LoadAndDelete(key)
				if loaded != inOracle || (loaded && got != want) {
					t.Fatalf("seed %d op %d: LoadAndDelete mismatch", seed, op)
				}
				delete(oracle, key)
			case 6:
				old := slotFor(uint64(rng.Intn(64) + 1))
				if inOracle && rng.Intn(2) == 0 {
					old = want
				}
				deleted := ht.CompareAndDelete(key, old)
				wantDelete := inOracle && old == want
				if deleted != wantDelete {
					t.Fatalf("seed %d op %d: CompareAndDelete outcome mismatch", seed, op)
				}
				if wantDelete {
					delete(oracle, key)
				}
			case 7:
				if rng.Intn(200) == 0 {
					ht.Clear()
					oracle = map[CacheKey]cacheSlot{}
				}
			}

			// Periodically compare complete contents through Range.
			if op%2500 == 2499 {
				seen := map[CacheKey]cacheSlot{}
				ht.Range(func(k CacheKey, v cacheSlot) bool {
					if _, dup := seen[k]; dup {
						t.Fatalf("seed %d op %d: Range revisited a key", seed, op)
					}
					seen[k] = v
					return true
				})
				if len(seen) != len(oracle) {
					t.Fatalf("seed %d op %d: Range saw %d keys, oracle holds %d", seed, op, len(seen), len(oracle))
				}
				for k, v := range oracle {
					if seen[k] != v {
						t.Fatalf("seed %d op %d: Range content mismatch", seed, op)
					}
				}
			}
		}
	}
}
