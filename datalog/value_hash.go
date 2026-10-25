package datalog

import (
	"fmt"
	"math"
	"reflect"
	"time"
	"unsafe"
)

// HashValue hashes a single value without string conversion
func HashValue(v interface{}) uint64 {
	// Interned pointer types first — with interning, we see these constantly.
	switch ptr := v.(type) {
	case Identity:
		// Interned: equal identities share a pointer (Identity.Equal is
		// pointer comparison and panics on same-hash/different-pointer), so
		// the pointer address is a unique, stable key. This avoids FNV over
		// the 20-byte SHA1. The hash is in-memory only (never persisted); Go's
		// GC does not move heap objects, so the address is stable for the
		// identity's lifetime, and the map's collision check (Identity.Equal)
		// would catch any interning violation rather than silently mismatch.
		if ptr == nil {
			return 0
		}
		return uint64(uintptr(unsafe.Pointer(ptr)))
	case Keyword:
		// Interned the same way as Identity (Keyword.Equal is pointer
		// comparison with the same panic guard), so hash by pointer address
		// instead of FNV over the keyword string.
		if ptr == nil {
			return 0
		}
		return uint64(uintptr(unsafe.Pointer(ptr)))
	case Symbol:
		// Symbols use the same interned-pointer identity contract as Identity
		// and Keyword, so hashing remains O(1) with no string conversion.
		if ptr == nil {
			return 0
		}
		return uint64(uintptr(unsafe.Pointer(ptr)))
	}

	// Remaining value types (Identity/Keyword/Symbol handled above).
	switch val := v.(type) {
	case string:
		return hashString(val)

	case []byte:
		// Hash by content. Without this case, []byte falls through to the
		// default pointer-address hash below, so two equal byte slices get
		// different (nondeterministic) hashes and never collide in a
		// TupleKeyMap — breaking joins and dedup on byte-valued attributes.
		return hashBytes(val)

	case time.Time:
		// Hash by the absolute instant. Without this case, time.Time falls
		// through to the address-based default below, so equal times hash
		// differently across call sites and never match in a TupleKeyMap —
		// breaking joins and dedup on time-valued attributes (e.g. :created-at)
		// nondeterministically by platform/stack layout. Consistent with
		// datalog.ValuesEqual, which compares time.Time by instant (Equal).
		return hashTime(val)

	case int:
		return uint64(val)

	case int64:
		return uint64(val)

	case int8:
		// Integer widths hash by int64 magnitude so they collide with the
		// canonical int64 in a TupleKeyMap, matching datalog.ValuesEqual, which
		// treats integer widths as equal by magnitude.
		return uint64(int64(val))

	case int16:
		return uint64(int64(val))

	case int32:
		return uint64(int64(val))

	case uint64:
		return val

	case float64:
		// Go equality treats +0 and -0 as equal, so they must share a hash
		// bucket. Other floats hash by their canonical IEEE representation.
		if val == 0 {
			return 0
		}
		return math.Float64bits(val)

	case bool:
		if val {
			return 1
		}
		return 0

	case ElementID:
		return val.Lamport ^ (val.ReplicaID * 1099511628211)

	case *ElementID:
		// A pointer representation carries a value only when non-nil. Reported as
		// nil rather than as an unknown type: the type is in the domain, so naming
		// the type would state something false.
		if val == nil {
			panic(fmt.Sprintf("HashValue:%T is nil; nil is not a datalog value", v))
		}
		return val.Lamport ^ (val.ReplicaID * 1099511628211)

	case []interface{}:
		// Vectors are ordered (RGA semantics) — order-dependent content
		// hash, consistent with datalog.ValuesEqual's element-wise
		// comparison: equal vectors hash identically. See
		// docs/bugs/resolved/BUG_VECTOR_VALUES_DEGENERATE_HASHING.md.
		return HashValues(val)

	case nil:
		// Absence is not a member of the domain and has no hash. All three doors
		// of the domain — this one, datalog.ValuesEqual and datalog.CompareValues
		// — reject it alike, so nothing reaches a bucket that equality then
		// refuses to compare.
		panic("HashValue:nil is not a datalog value")

	default:
		// Typed slices (e.g. []string, produced by the storage
		// vector-matching path) are vectors by the equality layer's own
		// definition: ValuesEqual compares every slice kind element-wise
		// via reflection, so []string{"a"} equals []interface{}{"a"} and
		// must hash identically. Same order-dependent FNV accumulation as
		// HashValues, so cross-representation equal slices collide as
		// required. ([]byte never reaches here — its content case above
		// wins.)
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice {
			const prime = 1099511628211
			hash := uint64(14695981039346656037)
			for i := 0; i < rv.Len(); i++ {
				hash ^= HashValue(rv.Index(i).Interface())
				hash *= prime
			}
			return hash
		}

		// The value domain is closed (mirror datalog.Type()'s panic
		// convention). A type reaching here is not a datalog value — e.g.
		// a pulled map[string]interface{}, which is result presentation
		// and must never enter relational flow. Fail loudly: any
		// non-content fallback (address, identity) silently breaks joins
		// and deduplication for whatever type it swallows. See
		// docs/bugs/resolved/BUG_VECTOR_VALUES_DEGENERATE_HASHING.md.
		panic(fmt.Sprintf("HashValue:%T is not a datalog value type", v))
	}
}

// HashValues computes a hash for a slice of values without string conversion
func HashValues(values []interface{}) uint64 {
	// FNV-1a hash
	const prime = 1099511628211
	hash := uint64(14695981039346656037)

	for _, v := range values {
		// Hash based on type and value
		hash ^= HashValue(v)
		hash *= prime
	}

	return hash
}

// hashBytes hashes a byte slice
func hashBytes(b []byte) uint64 {
	const prime = 1099511628211
	hash := uint64(14695981039346656037)

	for _, byte := range b {
		hash ^= uint64(byte)
		hash *= prime
	}

	return hash
}

// hashTime hashes a time.Time by its absolute instant (seconds + nanoseconds),
// consistent with datalog.ValuesEqual's instant-based comparison: two times that
// compare Equal hash identically. Uses Unix()+Nanosecond() rather than UnixNano()
// to avoid int64 overflow for out-of-range times (e.g. the 0001-01-01 default).
func hashTime(t time.Time) uint64 {
	const prime = 1099511628211
	hash := uint64(14695981039346656037)
	hash ^= uint64(t.Unix())
	hash *= prime
	hash ^= uint64(t.Nanosecond())
	hash *= prime
	return hash
}

// hashString hashes a string without allocation
func hashString(s string) uint64 {
	const prime = 1099511628211
	hash := uint64(14695981039346656037)

	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}

	return hash
}
