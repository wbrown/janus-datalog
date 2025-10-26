package datalog

import (
	"sync"
)

// KeywordIntern provides keyword interning to avoid repeated allocations
// Uses sync.Map for lock-free concurrent reads
type KeywordIntern struct {
	cache sync.Map // map[string]*Keyword
}

// Global keyword intern instance
var keywordIntern = &KeywordIntern{}

// InternKeyword returns an interned keyword instance
func InternKeyword(s string) *Keyword {
	// Fast path: load existing (lock-free)
	if val, ok := keywordIntern.cache.Load(s); ok {
		return val.(*Keyword)
	}

	// Slow path: create and store
	kw := &Keyword{value: s}
	actual, _ := keywordIntern.cache.LoadOrStore(s, kw)
	return actual.(*Keyword)
}

// IdentityIntern provides identity interning to avoid repeated allocations
// Uses sync.Map for lock-free concurrent reads
type IdentityIntern struct {
	cache sync.Map // map[[20]byte]*Identity
}

// Global identity intern instance
var identityIntern = &IdentityIntern{}

// InternIdentity returns an interned identity instance
func InternIdentity(id Identity) *Identity {
	hash := id.Hash()

	// Fast path: load existing (lock-free)
	if val, ok := identityIntern.cache.Load(hash); ok {
		return val.(*Identity)
	}

	// Slow path: create and store
	idCopy := id // Make a copy
	actual, _ := identityIntern.cache.LoadOrStore(hash, &idCopy)
	return actual.(*Identity)
}

// InternIdentityFromHash returns an interned identity from a hash
func InternIdentityFromHash(hash [20]byte) *Identity {
	// Fast path: load existing (lock-free)
	if val, ok := identityIntern.cache.Load(hash); ok {
		return val.(*Identity)
	}

	// Slow path: create and store
	// CRITICAL: Compute L85 eagerly so comparisons work correctly
	// Identities from storage have empty str field, so we use L85 representation
	id := NewIdentityFromHash(hash) // Use constructor to ensure proper initialization
	idPtr := &id
	actual, _ := identityIntern.cache.LoadOrStore(hash, idPtr)
	return actual.(*Identity)
}

// ClearInterns clears both keyword and identity intern caches
// Useful for testing or when memory needs to be reclaimed
func ClearInterns() {
	keywordIntern = &KeywordIntern{}
	identityIntern = &IdentityIntern{}
}
