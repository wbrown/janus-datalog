// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the GO-LICENSE file in this directory.

// Ported from Go 1.26.3 internal/sync/hashtriemap.go, fully specialized to the
// EA cache by owner ruling (2026-07-21). The concurrency logic — the
// find/expand protocol, per-indirect mutexes, dead-node collapse, overflow
// chains — is upstream's, verbatim. The departures all remove generality the
// one consumer does not need:
//
//  1. Keys are CacheKey and values are cacheSlot; no type parameters. The
//     resolved entry and its max-version high-water mark live in ONE slot, so
//     the cache's hot path pays one trie walk where the two-map layout paid
//     two.
//  2. Routing bits come from CacheKey.routing() — the entity hash's own
//     uniform bits folded with the attribute words. The key is already a SHA1
//     content address, so no hash function and no seed. Routing is
//     deterministic and unseeded by owner ruling: these lookups are
//     performance-critical, and the embedded threat model does not include
//     hostile writers grinding SHA1 preimages; engineered routing collisions
//     only lengthen overflow chains — lookups always compare full keys.
//  3. Slot values are compared with ==, replacing the runtime's
//     unsafe.Pointer value-equality function.
//  4. Construction is explicit (newCacheTrie), so upstream's lazy-init
//     machinery for zero-value usability is deleted.
//
// Routing values are 64-bit on every platform (upstream uses the pointer
// width); on 32-bit targets the trie can simply run deeper. The struct-prefix
// casts in cacheTrieNode.entry/cacheTrieNode.indirect are upstream's node
// polymorphism and are kept verbatim.

package storage

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/wbrown/janus-datalog/datalog"
)

// cacheSlot is the combined per-(E,A) cache state: the resolved view (nil when
// invalidated or not yet resolved; the shared in-flight sentinel while a
// commit owns the key) and the max-version high-water mark that freshness
// checks compare against. Keeping both in one slot makes "fresh iff
// entry.version == version" checkable from a single trie walk.
type cacheSlot struct {
	entry   *CacheEntry
	version datalog.ElementID
}

// routing returns the trie routing bits for a key: the entity hash's first
// word folded with all four attribute words. E is a SHA1 content address, so
// its bits are uniform by construction; folding every attribute word in
// separates same-entity attributes even when keyword names share long
// prefixes (":person/name" vs ":person/age" agree through byte 7).
func (k CacheKey) routing() uint64 {
	return binary.BigEndian.Uint64(k.E[0:8]) ^
		binary.BigEndian.Uint64(k.A[0:8]) ^
		binary.BigEndian.Uint64(k.A[8:16]) ^
		binary.BigEndian.Uint64(k.A[16:24]) ^
		binary.BigEndian.Uint64(k.A[24:32])
}

// cacheTrie is a concurrent hash-trie from CacheKey to cacheSlot, designed
// around frequent loads, with decent performance for stores and deletes.
type cacheTrie struct {
	root atomic.Pointer[cacheTrieIndirect]
}

// newCacheTrie returns an empty cacheTrie.
func newCacheTrie() *cacheTrie {
	ht := &cacheTrie{}
	ht.root.Store(newCacheTrieIndirect(nil))
	return ht
}

const cacheTrieRoutingBits = 64

// Load returns the slot stored in the map for a key, or the zero slot if no
// slot is present. The ok result indicates whether the slot was found.
func (ht *cacheTrie) Load(key CacheKey) (value cacheSlot, ok bool) {
	hash := key.routing()

	i := ht.root.Load()
	hashShift := uint(cacheTrieRoutingBits)
	for hashShift != 0 {
		hashShift -= nCacheTrieChildrenLog2

		n := i.children[(hash>>hashShift)&nCacheTrieChildrenMask].Load()
		if n == nil {
			return cacheSlot{}, false
		}
		if n.isEntry {
			return n.entry().lookup(key)
		}
		i = n.indirect()
	}
	panic("storage.cacheTrie: ran out of hash bits while iterating")
}

// LoadOrStore returns the existing slot for the key if present.
// Otherwise, it stores and returns the given slot.
// The loaded result is true if the slot was loaded, false if stored.
func (ht *cacheTrie) LoadOrStore(key CacheKey, value cacheSlot) (result cacheSlot, loaded bool) {
	hash := key.routing()
	var i *cacheTrieIndirect
	var hashShift uint
	var slot *atomic.Pointer[cacheTrieNode]
	var n *cacheTrieNode
	for {
		// Find the key or a candidate location for insertion.
		i = ht.root.Load()
		hashShift = cacheTrieRoutingBits
		haveInsertPoint := false
		for hashShift != 0 {
			hashShift -= nCacheTrieChildrenLog2

			slot = &i.children[(hash>>hashShift)&nCacheTrieChildrenMask]
			n = slot.Load()
			if n == nil {
				// We found a nil slot which is a candidate for insertion.
				haveInsertPoint = true
				break
			}
			if n.isEntry {
				// We found an existing entry, which is as far as we can go.
				// If it stays this way, we'll have to replace it with an
				// indirect node.
				if v, ok := n.entry().lookup(key); ok {
					return v, true
				}
				haveInsertPoint = true
				break
			}
			i = n.indirect()
		}
		if !haveInsertPoint {
			panic("storage.cacheTrie: ran out of hash bits while iterating")
		}

		// Grab the lock and double-check what we saw.
		i.mu.Lock()
		n = slot.Load()
		if (n == nil || n.isEntry) && !i.dead.Load() {
			// What we saw is still true, so we can continue with the insert.
			break
		}
		// We have to start over.
		i.mu.Unlock()
	}
	// N.B. This lock is held from when we broke out of the outer loop above.
	// We specifically break this out so that we can use defer here safely.
	// One option is to break this out into a new function instead, but
	// there's so much local iteration state used below that this turns out
	// to be cleaner.
	defer i.mu.Unlock()

	var oldEntry *cacheTrieEntry
	if n != nil {
		oldEntry = n.entry()
		if v, ok := oldEntry.lookup(key); ok {
			// Easy case: by loading again, it turns out exactly what we wanted is here!
			return v, true
		}
	}
	newEntry := newCacheTrieEntry(key, value)
	if oldEntry == nil {
		// Easy case: create a new entry and store it.
		slot.Store(&newEntry.cacheTrieNode)
	} else {
		// We possibly need to expand the entry already there into one or more new nodes.
		//
		// Publish the node last, which will make both oldEntry and newEntry visible. We
		// don't want readers to be able to observe that oldEntry isn't in the tree.
		slot.Store(ht.expand(oldEntry, newEntry, hash, hashShift, i))
	}
	return value, false
}

// expand takes oldEntry and newEntry whose hashes conflict from bit 64 down to
// hashShift and produces a subtree of indirect nodes to hold the two new entries.
func (ht *cacheTrie) expand(oldEntry, newEntry *cacheTrieEntry, newHash uint64, hashShift uint, parent *cacheTrieIndirect) *cacheTrieNode {
	// Check for a hash collision.
	oldHash := oldEntry.key.routing()
	if oldHash == newHash {
		// Store the old entry in the new entry's overflow list, then store
		// the new entry.
		newEntry.overflow.Store(oldEntry)
		return &newEntry.cacheTrieNode
	}
	// We have to add an indirect node. Worse still, we may need to add more than one.
	newIndirect := newCacheTrieIndirect(parent)
	top := newIndirect
	for {
		if hashShift == 0 {
			panic("storage.cacheTrie: ran out of hash bits while inserting (colliding routing values must chain, not expand)")
		}
		hashShift -= nCacheTrieChildrenLog2 // hashShift is for the level parent is at. We need to go deeper.
		oi := (oldHash >> hashShift) & nCacheTrieChildrenMask
		ni := (newHash >> hashShift) & nCacheTrieChildrenMask
		if oi != ni {
			newIndirect.children[oi].Store(&oldEntry.cacheTrieNode)
			newIndirect.children[ni].Store(&newEntry.cacheTrieNode)
			break
		}
		nextIndirect := newCacheTrieIndirect(newIndirect)
		newIndirect.children[oi].Store(&nextIndirect.cacheTrieNode)
		newIndirect = nextIndirect
	}
	return &top.cacheTrieNode
}

// Store sets the slot for a key.
func (ht *cacheTrie) Store(key CacheKey, new cacheSlot) {
	_, _ = ht.Swap(key, new)
}

// Swap swaps the slot for a key and returns the previous slot if any.
// The loaded result reports whether the key was present.
func (ht *cacheTrie) Swap(key CacheKey, new cacheSlot) (previous cacheSlot, loaded bool) {
	hash := key.routing()
	var i *cacheTrieIndirect
	var hashShift uint
	var slot *atomic.Pointer[cacheTrieNode]
	var n *cacheTrieNode
	for {
		// Find the key or a candidate location for insertion.
		i = ht.root.Load()
		hashShift = cacheTrieRoutingBits
		haveInsertPoint := false
		for hashShift != 0 {
			hashShift -= nCacheTrieChildrenLog2

			slot = &i.children[(hash>>hashShift)&nCacheTrieChildrenMask]
			n = slot.Load()
			if n == nil || n.isEntry {
				// We found a nil slot which is a candidate for insertion,
				// or an existing entry that we'll replace.
				haveInsertPoint = true
				break
			}
			i = n.indirect()
		}
		if !haveInsertPoint {
			panic("storage.cacheTrie: ran out of hash bits while iterating")
		}

		// Grab the lock and double-check what we saw.
		i.mu.Lock()
		n = slot.Load()
		if (n == nil || n.isEntry) && !i.dead.Load() {
			// What we saw is still true, so we can continue with the insert.
			break
		}
		// We have to start over.
		i.mu.Unlock()
	}
	// N.B. This lock is held from when we broke out of the outer loop above.
	// We specifically break this out so that we can use defer here safely.
	// One option is to break this out into a new function instead, but
	// there's so much local iteration state used below that this turns out
	// to be cleaner.
	defer i.mu.Unlock()

	var oldEntry *cacheTrieEntry
	if n != nil {
		// Swap if the keys compare.
		oldEntry = n.entry()
		newEntry, old, swapped := oldEntry.swap(key, new)
		if swapped {
			slot.Store(&newEntry.cacheTrieNode)
			return old, true
		}
	}
	// The keys didn't compare, so we're doing an insertion.
	newEntry := newCacheTrieEntry(key, new)
	if oldEntry == nil {
		// Easy case: create a new entry and store it.
		slot.Store(&newEntry.cacheTrieNode)
	} else {
		// We possibly need to expand the entry already there into one or more new nodes.
		//
		// Publish the node last, which will make both oldEntry and newEntry visible. We
		// don't want readers to be able to observe that oldEntry isn't in the tree.
		slot.Store(ht.expand(oldEntry, newEntry, hash, hashShift, i))
	}
	return cacheSlot{}, false
}

// CompareAndSwap swaps the old and new slots for key
// if the slot stored in the map is equal to old.
func (ht *cacheTrie) CompareAndSwap(key CacheKey, old, new cacheSlot) (swapped bool) {
	hash := key.routing()

	// Find a node with the key and compare with it. n != nil if we found the node.
	i, _, slot, n := ht.find(key, hash, true, old)
	if i != nil {
		defer i.mu.Unlock()
	}
	if n == nil {
		return false
	}

	// Try to swap the entry.
	e, swapped := n.entry().compareAndSwap(key, old, new)
	if !swapped {
		// Nothing was actually swapped, which means the node is no longer there.
		return false
	}
	// Store the entry back because it changed.
	slot.Store(&e.cacheTrieNode)
	return true
}

// LoadAndDelete deletes the slot for a key, returning the previous slot if any.
// The loaded result reports whether the key was present.
func (ht *cacheTrie) LoadAndDelete(key CacheKey) (value cacheSlot, loaded bool) {
	hash := key.routing()

	// Find a node with the key. n != nil if we found the node.
	i, hashShift, slot, n := ht.find(key, hash, false, cacheSlot{})
	if n == nil {
		if i != nil {
			i.mu.Unlock()
		}
		return cacheSlot{}, false
	}

	// Try to delete the entry.
	v, e, loaded := n.entry().loadAndDelete(key)
	if !loaded {
		// Nothing was actually deleted, which means the node is no longer there.
		i.mu.Unlock()
		return cacheSlot{}, false
	}
	if e != nil {
		// We didn't actually delete the whole entry, just one entry in the chain.
		// Nothing else to do, since the parent is definitely not empty.
		slot.Store(&e.cacheTrieNode)
		i.mu.Unlock()
		return v, true
	}
	// Delete the entry.
	slot.Store(nil)

	// Check if the node is now empty (and isn't the root), and delete it if able.
	for i.parent != nil && i.empty() {
		if hashShift == cacheTrieRoutingBits {
			panic("storage.cacheTrie: ran out of hash bits while iterating")
		}
		hashShift += nCacheTrieChildrenLog2

		// Delete the current node in the parent.
		parent := i.parent
		parent.mu.Lock()
		i.dead.Store(true)
		parent.children[(hash>>hashShift)&nCacheTrieChildrenMask].Store(nil)
		i.mu.Unlock()
		i = parent
	}
	i.mu.Unlock()
	return v, true
}

// Delete deletes the slot for a key.
func (ht *cacheTrie) Delete(key CacheKey) {
	_, _ = ht.LoadAndDelete(key)
}

// CompareAndDelete deletes the entry for key if its slot is equal to old.
//
// If there is no current slot for key in the map, CompareAndDelete returns
// false.
func (ht *cacheTrie) CompareAndDelete(key CacheKey, old cacheSlot) (deleted bool) {
	hash := key.routing()

	// Find a node with the key. n != nil if we found the node.
	i, hashShift, slot, n := ht.find(key, hash, false, cacheSlot{})
	if n == nil {
		if i != nil {
			i.mu.Unlock()
		}
		return false
	}

	// Try to delete the entry.
	e, deleted := n.entry().compareAndDelete(key, old)
	if !deleted {
		// Nothing was actually deleted, which means the node is no longer there.
		i.mu.Unlock()
		return false
	}
	if e != nil {
		// We didn't actually delete the whole entry, just one entry in the chain.
		// Nothing else to do, since the parent is definitely not empty.
		slot.Store(&e.cacheTrieNode)
		i.mu.Unlock()
		return true
	}
	// Delete the entry.
	slot.Store(nil)

	// Check if the node is now empty (and isn't the root), and delete it if able.
	for i.parent != nil && i.empty() {
		if hashShift == cacheTrieRoutingBits {
			panic("storage.cacheTrie: ran out of hash bits while iterating")
		}
		hashShift += nCacheTrieChildrenLog2

		// Delete the current node in the parent.
		parent := i.parent
		parent.mu.Lock()
		i.dead.Store(true)
		parent.children[(hash>>hashShift)&nCacheTrieChildrenMask].Store(nil)
		i.mu.Unlock()
		i = parent
	}
	i.mu.Unlock()
	return true
}

// find searches the tree for a node that contains key (hash must be the
// routing value of key). If checkValue is set, then it will also enforce that
// the slot stored equals the given slot.
//
// Returns a non-nil node, which will always be an entry, if found.
//
// If i != nil then i.mu is locked, and it is the caller's responsibility to
// unlock it.
func (ht *cacheTrie) find(key CacheKey, hash uint64, checkValue bool, value cacheSlot) (i *cacheTrieIndirect, hashShift uint, slot *atomic.Pointer[cacheTrieNode], n *cacheTrieNode) {
	for {
		// Find the key or return if it's not there.
		i = ht.root.Load()
		hashShift = cacheTrieRoutingBits
		found := false
		for hashShift != 0 {
			hashShift -= nCacheTrieChildrenLog2

			slot = &i.children[(hash>>hashShift)&nCacheTrieChildrenMask]
			n = slot.Load()
			if n == nil {
				// Nothing to compare with. Give up.
				i = nil
				return
			}
			if n.isEntry {
				// We found an entry. Check if it matches.
				if _, ok := n.entry().lookupWithValue(key, value, checkValue); !ok {
					// No match, comparison failed.
					i = nil
					n = nil
					return
				}
				// We've got a match. Prepare to perform an operation on the key.
				found = true
				break
			}
			i = n.indirect()
		}
		if !found {
			panic("storage.cacheTrie: ran out of hash bits while iterating")
		}

		// Grab the lock and double-check what we saw.
		i.mu.Lock()
		n = slot.Load()
		if !i.dead.Load() && (n == nil || n.isEntry) {
			// Either we've got a valid node or the node is now nil under the lock.
			// In either case, we're done here.
			return
		}
		// We have to start over.
		i.mu.Unlock()
	}
}

// Range calls yield sequentially for each key and slot present in the map.
// If yield returns false, range stops the iteration.
//
// The iteration does not necessarily correspond to any consistent snapshot of
// the map's contents: no key will be visited more than once, but if the slot
// for any key is stored or deleted concurrently (including by yield), the
// iteration may reflect any mapping for that key from any point during
// iteration. Iteration does not block other methods on the receiver; even
// yield itself may call any method on the map.
func (ht *cacheTrie) Range(yield func(CacheKey, cacheSlot) bool) {
	ht.iter(ht.root.Load(), yield)
}

func (ht *cacheTrie) iter(i *cacheTrieIndirect, yield func(key CacheKey, value cacheSlot) bool) bool {
	for j := range i.children {
		n := i.children[j].Load()
		if n == nil {
			continue
		}
		if !n.isEntry {
			if !ht.iter(n.indirect(), yield) {
				return false
			}
			continue
		}
		e := n.entry()
		for e != nil {
			if !yield(e.key, e.value) {
				return false
			}
			e = e.overflow.Load()
		}
	}
	return true
}

// Clear deletes all the entries, resulting in an empty map.
func (ht *cacheTrie) Clear() {
	// It's sufficient to just drop the root on the floor, but the root
	// must always be non-nil.
	ht.root.Store(newCacheTrieIndirect(nil))
}

const (
	// 16 children. This seems to be the sweet spot for
	// load performance: any smaller and we lose out on
	// 50% or more in CPU performance. Any larger and the
	// returns are minuscule (~1% improvement for 32 children).
	nCacheTrieChildrenLog2 = 4
	nCacheTrieChildren     = 1 << nCacheTrieChildrenLog2
	nCacheTrieChildrenMask = nCacheTrieChildren - 1
)

// cacheTrieIndirect is an internal node in the hash-trie.
type cacheTrieIndirect struct {
	cacheTrieNode
	dead     atomic.Bool
	mu       sync.Mutex // Protects mutation to children and any children that are entry nodes.
	parent   *cacheTrieIndirect
	children [nCacheTrieChildren]atomic.Pointer[cacheTrieNode]
}

func newCacheTrieIndirect(parent *cacheTrieIndirect) *cacheTrieIndirect {
	return &cacheTrieIndirect{cacheTrieNode: cacheTrieNode{isEntry: false}, parent: parent}
}

func (i *cacheTrieIndirect) empty() bool {
	for j := range i.children {
		if i.children[j].Load() != nil {
			return false
		}
	}
	return true
}

// cacheTrieEntry is a leaf node in the hash-trie.
type cacheTrieEntry struct {
	cacheTrieNode
	overflow atomic.Pointer[cacheTrieEntry] // Overflow for routing collisions.
	key      CacheKey
	value    cacheSlot
}

func newCacheTrieEntry(key CacheKey, value cacheSlot) *cacheTrieEntry {
	return &cacheTrieEntry{
		cacheTrieNode: cacheTrieNode{isEntry: true},
		key:           key,
		value:         value,
	}
}

func (e *cacheTrieEntry) lookup(key CacheKey) (cacheSlot, bool) {
	for e != nil {
		if e.key == key {
			return e.value, true
		}
		e = e.overflow.Load()
	}
	return cacheSlot{}, false
}

func (e *cacheTrieEntry) lookupWithValue(key CacheKey, value cacheSlot, checkValue bool) (cacheSlot, bool) {
	for e != nil {
		if e.key == key && (!checkValue || e.value == value) {
			return e.value, true
		}
		e = e.overflow.Load()
	}
	return cacheSlot{}, false
}

// swap replaces an entry in the overflow chain if keys compare equal. Returns
// the new entry chain, the old slot, and whether or not anything was swapped.
//
// swap must be called under the mutex of the indirect node which e is a child of.
func (head *cacheTrieEntry) swap(key CacheKey, new cacheSlot) (*cacheTrieEntry, cacheSlot, bool) {
	if head.key == key {
		// Return the new head of the list.
		e := newCacheTrieEntry(key, new)
		if chain := head.overflow.Load(); chain != nil {
			e.overflow.Store(chain)
		}
		return e, head.value, true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key {
			eNew := newCacheTrieEntry(key, new)
			eNew.overflow.Store(e.overflow.Load())
			i.Store(eNew)
			return head, e.value, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return head, cacheSlot{}, false
}

// compareAndSwap replaces an entry in the overflow chain if both the key and
// slot compare equal. Returns the new entry chain and whether or not anything
// was swapped.
//
// compareAndSwap must be called under the mutex of the indirect node which e
// is a child of.
func (head *cacheTrieEntry) compareAndSwap(key CacheKey, old, new cacheSlot) (*cacheTrieEntry, bool) {
	if head.key == key && head.value == old {
		// Return the new head of the list.
		e := newCacheTrieEntry(key, new)
		if chain := head.overflow.Load(); chain != nil {
			e.overflow.Store(chain)
		}
		return e, true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key && e.value == old {
			eNew := newCacheTrieEntry(key, new)
			eNew.overflow.Store(e.overflow.Load())
			i.Store(eNew)
			return head, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return head, false
}

// loadAndDelete deletes an entry in the overflow chain by key. Returns the
// slot for the key, the new entry chain and whether or not anything was
// loaded (and deleted).
//
// loadAndDelete must be called under the mutex of the indirect node which e
// is a child of.
func (head *cacheTrieEntry) loadAndDelete(key CacheKey) (cacheSlot, *cacheTrieEntry, bool) {
	if head.key == key {
		// Drop the head of the list.
		return head.value, head.overflow.Load(), true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key {
			i.Store(e.overflow.Load())
			return e.value, head, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return cacheSlot{}, head, false
}

// compareAndDelete deletes an entry in the overflow chain if both the key and
// slot compare equal. Returns the new entry chain and whether or not anything
// was deleted.
//
// compareAndDelete must be called under the mutex of the indirect node which e
// is a child of.
func (head *cacheTrieEntry) compareAndDelete(key CacheKey, value cacheSlot) (*cacheTrieEntry, bool) {
	if head.key == key && head.value == value {
		// Drop the head of the list.
		return head.overflow.Load(), true
	}
	i := &head.overflow
	e := i.Load()
	for e != nil {
		if e.key == key && e.value == value {
			i.Store(e.overflow.Load())
			return head, true
		}
		i = &e.overflow
		e = e.overflow.Load()
	}
	return head, false
}

// cacheTrieNode is the header for a node. It's polymorphic and
// is actually either a cacheTrieEntry or a cacheTrieIndirect.
type cacheTrieNode struct {
	isEntry bool
}

func (n *cacheTrieNode) entry() *cacheTrieEntry {
	if !n.isEntry {
		panic("called entry on non-entry node")
	}
	return (*cacheTrieEntry)(unsafe.Pointer(n))
}

func (n *cacheTrieNode) indirect() *cacheTrieIndirect {
	if n.isEntry {
		panic("called indirect on entry node")
	}
	return (*cacheTrieIndirect)(unsafe.Pointer(n))
}
