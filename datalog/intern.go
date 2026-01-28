package datalog

import (
	"strings"
	"sync"

	"github.com/wbrown/janus-datalog/datalog/codec"
)

// keywordInternCache provides keyword interning to avoid repeated allocations
// Uses sync.Map for lock-free concurrent reads
// Cache key is [32]byte (null-padded) to match storage format
type keywordInternCache struct {
	cache sync.Map // map[[32]byte]Keyword
}

// Global keyword intern instance
var keywordIntern = &keywordInternCache{}

// InternKeyword returns an interned keyword instance.
// Pads the string to 32 bytes for cache key lookup.
func InternKeyword(s string) Keyword {
	// Pad to 32 bytes for cache key (matches storage format)
	var key [32]byte
	copy(key[:], s)

	// Fast path: load existing (lock-free)
	if val, ok := keywordIntern.cache.Load(key); ok {
		return val.(Keyword)
	}

	// Slow path: create and store
	kw := &keyword{value: s}
	actual, _ := keywordIntern.cache.LoadOrStore(key, kw)
	return actual.(Keyword)
}

// InternKeywordFromBytes returns an interned keyword from storage bytes.
// Uses the 32-byte key directly without conversion.
func InternKeywordFromBytes(key [32]byte) Keyword {
	// Fast path: load existing (lock-free)
	if val, ok := keywordIntern.cache.Load(key); ok {
		return val.(Keyword)
	}

	// Slow path: trim null padding to get string, create and store
	str := strings.TrimRight(string(key[:]), "\x00")
	kw := &keyword{value: str}
	actual, _ := keywordIntern.cache.LoadOrStore(key, kw)
	return actual.(Keyword)
}

// Kw creates an interned keyword from namespace and name parts.
// Kw("scenario", "premise") → :scenario/premise
// Kw("", "foo") → :foo
func Kw(ns, name string) Keyword {
	if ns == "" {
		return InternKeyword(":" + name)
	}
	return InternKeyword(":" + ns + "/" + name)
}

// Kws creates multiple interned keywords from strings.
// Kws(":user/name", ":user/age") → []Keyword{...}
func Kws(strs ...string) []Keyword {
	result := make([]Keyword, len(strs))
	for i, s := range strs {
		result[i] = NewKeyword(s)
	}
	return result
}

// identityInternCache provides identity interning to avoid repeated allocations
// Uses sync.Map for lock-free concurrent reads
type identityInternCache struct {
	cache sync.Map // map[[20]byte]Identity (Identity is *identity)
}

// Global identity intern instance
var identityIntern = &identityInternCache{}

// InternIdentity returns an interned identity instance.
// Since all Identity constructors now intern automatically, this is effectively
// an identity function kept for backward compatibility.
// It will still intern if somehow given an uninterned identity.
func InternIdentity(id Identity) Identity {
	if id == nil {
		return nil
	}
	hash := id.Hash()

	// Fast path: load existing (lock-free)
	if val, ok := identityIntern.cache.Load(hash); ok {
		return val.(Identity)
	}

	// Slow path: store the identity pointer directly
	actual, _ := identityIntern.cache.LoadOrStore(hash, id)
	return actual.(Identity)
}

// InternIdentityFromHash returns an interned identity from a hash.
// Since Identity is now a pointer type alias (*identity), we return Identity directly.
func InternIdentityFromHash(hash [20]byte) Identity {
	// Fast path: load existing (lock-free)
	if val, ok := identityIntern.cache.Load(hash); ok {
		return val.(Identity)
	}

	// Slow path: create and store
	// CRITICAL: Compute L85 eagerly so String() returns it correctly
	// Identities from storage have empty str field, so we use L85 representation
	id := &identity{
		value:       hash,
		l85:         codec.EncodeL85(hash[:]),
		str:         "", // Unknown - use L85 representation
		l85Computed: true,
	}
	actual, _ := identityIntern.cache.LoadOrStore(hash, id)
	return actual.(Identity)
}

// symbolInternCache provides symbol interning to avoid repeated allocations
// Uses sync.Map for lock-free concurrent reads
// Cache key is string (no storage format constraint for symbols)
type symbolInternCache struct {
	cache sync.Map // map[string]Symbol
}

// Global symbol intern instance
var symbolIntern = &symbolInternCache{}

// internSymbol returns an interned symbol instance.
// Uses string keys since symbols have no storage format constraint.
func internSymbol(s string) Symbol {
	// Fast path: load existing (lock-free)
	if val, ok := symbolIntern.cache.Load(s); ok {
		return val.(Symbol)
	}

	// Slow path: create and store
	sym := &symbol{value: s}
	actual, _ := symbolIntern.cache.LoadOrStore(s, sym)
	return actual.(Symbol)
}

// Pre-interned common symbols for hot paths
var SymDollar = internSymbol("$")

// ClearInterns clears keyword, identity, and symbol intern caches
// Useful for testing or when memory needs to be reclaimed
func ClearInterns() {
	keywordIntern = &keywordInternCache{}
	identityIntern = &identityInternCache{}
	symbolIntern = &symbolInternCache{}
	// Re-intern pre-interned symbols so they remain valid
	SymDollar = internSymbol("$")
}
