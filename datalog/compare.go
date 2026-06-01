package datalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// typeRank assigns each value an ordering class so CompareValues is a total,
// antisymmetric order even across types. Values of different types are ordered
// by rank; same-rank values compare by value. int and float share rank 1 so
// they continue to compare by numeric magnitude (not split into separate
// classes) — the engine's existing, correct numeric cross-comparison. The rank
// order is internal to in-memory comparison; it is deliberately NOT the on-disk
// ValueType tag order (which separates int and float and would break numeric
// cross-compare). Unknown types share the top rank and fall back to string form.
func typeRank(v interface{}) int {
	switch v.(type) {
	case nil:
		return 0
	case int, int8, int16, int32, int64, uint64, *uint64, float64:
		return 1
	case bool:
		return 2
	case time.Time:
		return 3
	case string:
		return 4
	case []byte:
		return 5
	case Keyword:
		return 6
	case Symbol:
		return 7
	case Identity:
		return 8
	case ElementID, *ElementID:
		return 9
	default:
		return 10
	}
}

// compareByRank orders two values of different types by their type rank. It is
// only called once same-type comparison has been ruled out; equal ranks at this
// point mean an unknown/unhandled type pair, which falls back to string form so
// the result is still deterministic and antisymmetric.
func compareByRank(left, right interface{}) int {
	lr, rr := typeRank(left), typeRank(right)
	if lr != rr {
		return compareInt64s(int64(lr), int64(rr))
	}
	return strings.Compare(stringValue(left), stringValue(right))
}

// CompareValues compares two values and returns:
//
//	-1 if left < right
//	 0 if left == right
//	 1 if left > right
//
// Across different types it applies a stable type rank (see typeRank), so the
// result is a total antisymmetric order usable by sort/min/max/order-by. int and
// float still compare by numeric magnitude. This function handles all Datalog
// value types including:
// - Basic types: int, int64, float64, string, bool, time.Time
// - Datalog types: Identity, Keyword
// - Nil values (nil is less than any non-nil value)
// - Type conversions between numeric types
func CompareValues(left, right interface{}) int {
	// Handle nil
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	// Handle uint64 pointers by dereferencing
	if ptr, ok := left.(*uint64); ok {
		left = *ptr
	}
	if ptr, ok := right.(*uint64); ok {
		right = *ptr
	}

	// Handle Identity comparison - use the Compare method which handles nil
	if id1, ok := left.(Identity); ok {
		if id2, ok := right.(Identity); ok {
			return id1.Compare(id2)
		}
		// Identity vs non-Identity: order by type rank.
		return compareByRank(left, right)
	}

	// Handle Keyword comparison - use the Compare method which handles nil
	if kw1, ok := left.(Keyword); ok {
		if kw2, ok := right.(Keyword); ok {
			return kw1.Compare(kw2)
		}
		// Keyword vs non-Keyword: order by type rank.
		return compareByRank(left, right)
	}

	// Handle Symbol comparison - use the Compare method which handles nil
	if sym1, ok := left.(Symbol); ok {
		if sym2, ok := right.(Symbol); ok {
			return sym1.Compare(sym2)
		}
		// Symbol vs non-Symbol: order by type rank.
		return compareByRank(left, right)
	}

	// Handle ElementID comparison — dereference pointers, then compare by value
	if eid1, ok := DerefElementID(left); ok {
		if eid2, ok := DerefElementID(right); ok {
			return eid1.Compare(eid2)
		}
		// ElementID vs non-ElementID: order by type rank.
		return compareByRank(left, right)
	}

	// Integer widths normalize to int64 (the canonical representation) so
	// ordering agrees with ValuesEqual.
	if li, ok := asInt64(left); ok {
		return compareNumeric(li, right)
	}

	// Handle remaining numeric comparisons
	switch l := left.(type) {
	case uint64:
		return compareUint64(l, right)
	case float64:
		return compareFloat(l, right)
	case string:
		if r, ok := right.(string); ok {
			return strings.Compare(l, r)
		}
		// String vs non-string: order by type rank.
		return compareByRank(left, right)
	case []byte:
		if r, ok := right.([]byte); ok {
			return bytes.Compare(l, r)
		}
		// []byte vs non-[]byte: order by type rank.
		return compareByRank(left, right)
	case bool:
		if r, ok := right.(bool); ok {
			if !l && r {
				return -1
			} else if l && !r {
				return 1
			}
			return 0
		}
		// Bool vs non-bool: order by type rank.
		return compareByRank(left, right)
	case time.Time:
		if r, ok := right.(time.Time); ok {
			if l.Before(r) {
				return -1
			} else if l.After(r) {
				return 1
			}
			return 0
		}
		// Time vs non-time: order by type rank.
		return compareByRank(left, right)
	}

	// Unknown types: order by type rank (falls back to string form for
	// same-rank unknowns), keeping the comparator total and antisymmetric.
	return compareByRank(left, right)
}

// compareNumeric compares an int64 with another numeric value
func compareNumeric(left int64, right interface{}) int {
	if ri, ok := asInt64(right); ok {
		return compareInt64s(left, ri)
	}
	if r, ok := right.(float64); ok {
		return compareFloats(float64(left), r)
	}
	// Numeric (rank 1) vs non-numeric (higher rank): numeric sorts first. The
	// reverse direction (non-numeric left vs numeric right) reaches
	// compareByRank and yields +1, so this is antisymmetric.
	return -1
}

// compareFloat compares a float64 with another numeric value
func compareFloat(left float64, right interface{}) int {
	if ri, ok := asInt64(right); ok {
		return compareFloats(left, float64(ri))
	}
	if r, ok := right.(float64); ok {
		return compareFloats(left, r)
	}
	// Numeric (rank 1) vs non-numeric: numeric sorts first (see compareNumeric).
	return -1
}

// compareBytes compares two byte slices as numeric values
// For 20-byte hashes, we compare as 2 uint64s + 1 uint32
func compareBytes(a, b []byte) int {
	// Compare first 8 bytes as uint64
	a1 := binary.BigEndian.Uint64(a[0:8])
	b1 := binary.BigEndian.Uint64(b[0:8])
	if a1 < b1 {
		return -1
	}
	if a1 > b1 {
		return 1
	}

	// Compare second 8 bytes as uint64
	a2 := binary.BigEndian.Uint64(a[8:16])
	b2 := binary.BigEndian.Uint64(b[8:16])
	if a2 < b2 {
		return -1
	}
	if a2 > b2 {
		return 1
	}

	// Compare last 4 bytes as uint32
	a3 := binary.BigEndian.Uint32(a[16:20])
	b3 := binary.BigEndian.Uint32(b[16:20])
	if a3 < b3 {
		return -1
	}
	if a3 > b3 {
		return 1
	}

	return 0
}

// compareInt64s compares two int64 values
func compareInt64s(a, b int64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareFloats compares two float64 values
func compareFloats(a, b float64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// compareUint64 compares a uint64 with another numeric value
func compareUint64(left uint64, right interface{}) int {
	if ri, ok := asInt64(right); ok {
		if ri < 0 {
			return 1 // unsigned is always >= 0
		}
		return compareUint64s(left, uint64(ri))
	}
	switch r := right.(type) {
	case uint64:
		return compareUint64s(left, r)
	case float64:
		return compareFloats(float64(left), r)
	}
	// Numeric (rank 1) vs non-numeric: numeric sorts first (see compareNumeric).
	return -1
}

// compareUint64s compares two uint64 values
func compareUint64s(a, b uint64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// asInt64 returns the int64 magnitude of any signed integer width
// (int, int8, int16, int32, int64) and reports whether v was such a value.
// int64 is the engine's canonical integer representation — the EDN parser,
// storage decode, and schema validation all standardize on it — so this is the
// single place mixed integer widths are unified for comparison and hashing.
// int64 is listed first as the dominant case.
func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	}
	return 0, false
}

// ValuesEqual checks if two values are equal.
//
// The comparable hot-path types (interned Identity/Keyword/Symbol and the
// scalar primitives) return before any reflection: join keys are dominantly
// Identity, so paying reflect.ValueOf on every equality check was pure
// overhead. reflect is now reached only for typed slices, the rare slow
// path. The general == fallback at the end is panic-safe because slices have
// already been handled (== panics on two values sharing an uncomparable
// dynamic type).
func ValuesEqual(a, b interface{}) bool {
	// []byte/[]uint8 first ([]uint8 is an alias of []byte, so one assertion
	// covers both). Strings are stored as []uint8 internally, so this is the
	// most common slice value. Slices aren't comparable with == (would
	// panic), so they're handled before the == fast paths below.
	if ba, ok := a.([]byte); ok {
		bb, ok := b.([]byte)
		return ok && bytes.Equal(ba, bb)
	}

	// Dereference *uint64 so the primitive comparison below sees plain uint64.
	if ptr, ok := a.(*uint64); ok {
		a = *ptr
	}
	if ptr, ok := b.(*uint64); ok {
		b = *ptr
	}

	// Interned pointer types compare via their Equal method (which handles
	// nil). These are the dominant hash-join key shapes (entity references),
	// so they're checked first and never pay reflection.
	if id1, ok := a.(Identity); ok {
		id2, ok := b.(Identity)
		return ok && id1.Equal(id2)
	}
	if kw1, ok := a.(Keyword); ok {
		kw2, ok := b.(Keyword)
		return ok && kw1.Equal(kw2)
	}
	if sym1, ok := a.(Symbol); ok {
		sym2, ok := b.(Symbol)
		return ok && sym1.Equal(sym2)
	}

	// ElementID — dereference pointers, then compare by value.
	if eid1, ok := DerefElementID(a); ok {
		eid2, ok := DerefElementID(b)
		return ok && eid1.Equal(eid2)
	}

	// Comparable primitives. The == fast path is correct (and allocation-free)
	// whenever the two dynamic types match — the dominant case, and the only one
	// reached after boundary normalization makes user integers int64. Only when
	// == is false do we pay to unify integer width: a non-canonical width
	// (int/int8/int16/int32) compares by magnitude against any other integer
	// width, while int-vs-float stays strict (asInt64 reports false for float64),
	// so mixed-numeric values are never conflated.
	switch a.(type) {
	case int64, float64, string, bool, uint64, int, int8, int16, int32:
		if a == b {
			return true
		}
		if ia, ok := asInt64(a); ok {
			if ib, ok := asInt64(b); ok {
				return ia == ib
			}
		}
		return false
	}

	// time.Time uses Equal (instant equality, ignoring location).
	if av, ok := a.(time.Time); ok {
		bv, ok := b.(time.Time)
		return ok && av.Equal(bv)
	}

	// Slice comparison for typed slices ([]string, []int64) and
	// []interface{}, element-wise via reflect. Slow path; must precede the
	// general == fallback below, which would panic on two values that share
	// an uncomparable (slice) dynamic type.
	ra := reflect.ValueOf(a)
	rb := reflect.ValueOf(b)
	if ra.Kind() == reflect.Slice || rb.Kind() == reflect.Slice {
		if ra.Kind() != reflect.Slice || rb.Kind() != reflect.Slice {
			return false // one is a slice, the other isn't
		}
		if ra.Len() != rb.Len() {
			return false
		}
		for i := 0; i < ra.Len(); i++ {
			if !ValuesEqual(ra.Index(i).Interface(), rb.Index(i).Interface()) {
				return false
			}
		}
		return true
	}

	// General fallback for any remaining comparable type (and both-nil).
	// Safe: neither a nor b is a slice here, so == cannot panic.
	return a == b
}

// stringValue converts any value to a string for comparison
func stringValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case Identity:
		return val.String()
	case Keyword:
		return val.String()
	case Symbol:
		return val.String()
	default:
		// Use fmt.Sprintf for other types
		return fmt.Sprintf("%v", v)
	}
}

// DerefElementID extracts an ElementID from either ElementID or *ElementID.
func DerefElementID(v interface{}) (ElementID, bool) {
	switch e := v.(type) {
	case ElementID:
		return e, true
	case *ElementID:
		if e != nil {
			return *e, true
		}
		return ElementID{}, false
	default:
		return ElementID{}, false
	}
}
