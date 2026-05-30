package datalog

import (
	"crypto/sha1"
	"encoding/binary"

	"github.com/wbrown/janus-datalog/datalog/codec"
)

// identity is the unexported base type for entity identifiers.
//
// Design: entities are CONTENT-ADDRESSED. An identity IS the SHA1 of its string
// name; there is no separate auto-assigned entity id (unlike Datomic). The
// original string is kept in `str` only as a convenience for identities
// constructed in-process via NewIdentity; it is NOT persisted. Identities
// reconstructed from storage (NewIdentityFromHash, the decode path) have an
// empty `str`, so String() falls back to the L85 of the hash. Two consequences
// callers must know:
//
//   - After a fresh open, query results render entities as L85 hashes, not their
//     original names. To recover a human name, store it as an attribute (e.g.
//     :person/name) and read that back — the hash alone does not round-trip to
//     the string.
//   - Because interning is keyed by the 20-byte hash, two DISTINCT name strings
//     that collide in SHA1 would intern to the SAME identity and silently alias
//     to one entity. This is astronomically unlikely and SHA1 is used here for
//     content addressing, not security. Note the Equal/Compare panic guards the
//     INTERNING invariant (no two live pointers share a hash); it does not — and
//     cannot — detect a genuine hash collision, since the colliding names become
//     one pointer before any comparison happens.
type identity struct {
	value [20]byte // SHA1 hash (the content address; this IS the entity's identity)
	str   string   // Original string, in-process only; empty when decoded from storage
}

// Identity is the exported pointer type, always interned. See the identity
// struct doc for content-addressing semantics (names are not persisted; reads
// surface L85 hashes unless the name is stored as an attribute).
type Identity = *identity

// NewIdentity creates an interned identity from a string.
// Always returns an interned instance for pointer equality.
func NewIdentity(s string) Identity {
	hash := sha1.Sum([]byte(s))

	// Fast path: check cache first before allocating
	if val, ok := identityIntern.cache.Load(hash); ok {
		return val.(Identity)
	}

	// Slow path: allocate and intern. The identity is immutable; L85 is a pure
	// function of value computed on demand, so there is no cached field to race
	// on (nor one kept resident for identities that never need it).
	id := &identity{
		value: hash,
		str:   s,
	}
	actual, _ := identityIntern.cache.LoadOrStore(hash, id)
	return actual.(Identity)
}

// NewIdentityFromHash creates an interned identity from a hash.
// Always returns an interned instance for pointer equality.
func NewIdentityFromHash(hash [20]byte) Identity {
	return InternIdentityFromHash(hash)
}

// Hash returns the raw hash value
func (i Identity) Hash() [20]byte {
	if i == nil {
		return [20]byte{}
	}
	return i.value
}

// L85 returns the L85-encoded representation
func (i Identity) L85() string {
	if i == nil {
		return ""
	}
	// Pure function of the immutable hash, computed on demand. Not cached: L85 is
	// no longer used on hot paths (joins, dedup, and the in-memory indices key on
	// the interned identity/keyword pointers), so a cache would only add memory
	// and a field to race on.
	return codec.EncodeL85(i.value[:])
}

// String returns a string representation
func (i Identity) String() string {
	if i == nil {
		return ""
	}
	if i.str != "" {
		return i.str
	}
	// If we don't know the original string, show the L85
	return i.L85()
}

// ID returns a numeric ID (first 8 bytes as uint64, like Clojure)
func (i Identity) ID() uint64 {
	if i == nil {
		return 0
	}
	return binary.BigEndian.Uint64(i.value[:8])
}

// Compare compares two identities by their hash bytes.
// Since all Identities are interned, pointer equality implies value equality.
// Panics if two different pointers have the same hash (indicates interning bug).
func (i Identity) Compare(other Identity) int {
	// Handle nil cases
	if i == nil && other == nil {
		return 0
	}
	if i == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	// Pointer equality means same identity
	if i == other {
		return 0
	}
	// Compare hash bytes directly - faster than string comparison
	for idx := 0; idx < 20; idx++ {
		if i.value[idx] < other.value[idx] {
			return -1
		} else if i.value[idx] > other.value[idx] {
			return 1
		}
	}
	// Same hash but different pointers - interning is broken
	panic("BUG: two Identity pointers with same hash in Compare - interning failed")
}

// Equal checks if two identities are equal using pointer comparison.
// Since all Identities are interned, pointer equality implies value equality.
// Panics if two different pointers have the same hash (indicates interning bug).
func (i Identity) Equal(other Identity) bool {
	// Pointer equality handles nil == nil too
	if i == other {
		return true
	}
	// If either is nil (but not both), they're not equal
	if i == nil || other == nil {
		return false
	}
	// Different pointers - if they have the same hash, interning is broken
	if i.value == other.value {
		panic("BUG: two Identity pointers with same hash in Equal - interning failed")
	}
	return false
}

// Bytes returns the raw hash bytes
func (i Identity) Bytes() []byte {
	if i == nil {
		return nil
	}
	return i.value[:]
}
