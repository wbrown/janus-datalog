package datalog

import (
	"strings"
	"sync"
)

// keywordInternCache provides keyword interning to avoid repeated allocations
// Uses sync.Map for lock-free concurrent reads
// Cache key is the full keyword string so distinct keywords never collide,
// regardless of length. (The 32-byte storage form is a separate concern,
// enforced by length validation on the write/schema paths.)
type keywordInternCache struct {
	cache sync.Map // map[string]Keyword
}

// Global keyword intern instance
var keywordIntern = &keywordInternCache{}

// InternKeyword returns an interned keyword instance.
// Keyed by the full string, so two distinct keywords never share a pointer
// even if their first 32 bytes match.
func InternKeyword(s string) Keyword {
	// Fast path: load existing (lock-free)
	if val, ok := keywordIntern.cache.Load(s); ok {
		return val.(Keyword)
	}

	// Slow path: create and store
	kw := &keyword{value: s}
	actual, _ := keywordIntern.cache.LoadOrStore(s, kw)
	return actual.(Keyword)
}

// InternKeywordFromBytes returns an interned keyword from storage bytes.
// The null padding is trimmed to recover the keyword string, which is the
// cache key — so a keyword decoded from storage shares the same interned
// pointer as one created via InternKeyword.
func InternKeywordFromBytes(key [32]byte) Keyword {
	str := strings.TrimRight(string(key[:]), "\x00")

	// Fast path: load existing (lock-free)
	if val, ok := keywordIntern.cache.Load(str); ok {
		return val.(Keyword)
	}

	// Slow path: create and store
	kw := &keyword{value: str}
	actual, _ := keywordIntern.cache.LoadOrStore(str, kw)
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

	// Slow path: create and store.
	id := &identity{value: hash}
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

// Pre-interned aggregate function symbols. FindAggregate.Function carries one
// of these; resolution is pointer equality against them.
var (
	SymCount = internSymbol("count")
	SymSum   = internSymbol("sum")
	SymAvg   = internSymbol("avg")
	SymMin   = internSymbol("min")
	SymMax   = internSymbol("max")
)

// Pre-interned comparison operator symbols. Comparison.Op and
// ChainedComparison.Op carry one of these; dispatch is pointer equality
// against them.
var (
	SymEQ  = internSymbol("=")
	SymNE  = internSymbol("!=")
	SymLT  = internSymbol("<")
	SymLTE = internSymbol("<=")
	SymGT  = internSymbol(">")
	SymGTE = internSymbol(">=")
)

// Pre-interned arithmetic operator symbols. ArithmeticFunction.Op carries
// one of these; dispatch is pointer equality against them.
var (
	SymAdd      = internSymbol("+")
	SymSubtract = internSymbol("-")
	SymMultiply = internSymbol("*")
	SymDivide   = internSymbol("/")
)

// Pre-interned time-extraction field symbols. TimeExtractionFunction.Field
// carries one of these; dispatch is pointer equality against them.
var (
	SymYear   = internSymbol("year")
	SymMonth  = internSymbol("month")
	SymDay    = internSymbol("day")
	SymHour   = internSymbol("hour")
	SymMinute = internSymbol("minute")
	SymSecond = internSymbol("second")
)

// ClearInterns clears keyword, identity, and symbol intern caches
// Useful for testing or when memory needs to be reclaimed
func ClearInterns() {
	keywordIntern = &keywordInternCache{}
	identityIntern = &identityInternCache{}
	symbolIntern = &symbolInternCache{}
	// Re-intern pre-interned symbols so they remain valid
	SymDollar = internSymbol("$")
	SymCount = internSymbol("count")
	SymSum = internSymbol("sum")
	SymAvg = internSymbol("avg")
	SymMin = internSymbol("min")
	SymMax = internSymbol("max")
	SymEQ = internSymbol("=")
	SymNE = internSymbol("!=")
	SymLT = internSymbol("<")
	SymLTE = internSymbol("<=")
	SymGT = internSymbol(">")
	SymGTE = internSymbol(">=")
	SymAdd = internSymbol("+")
	SymSubtract = internSymbol("-")
	SymMultiply = internSymbol("*")
	SymDivide = internSymbol("/")
	SymYear = internSymbol("year")
	SymMonth = internSymbol("month")
	SymDay = internSymbol("day")
	SymHour = internSymbol("hour")
	SymMinute = internSymbol("minute")
	SymSecond = internSymbol("second")
}
