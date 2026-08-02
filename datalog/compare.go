package datalog

import (
	"bytes"
	"fmt"
	"math"
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
// cross-compare).
//
// The domain is closed, so this enumerates it and panics on the rest, the way
// Type() and hashValue do. A catch-all rank is what let vectors — the domain's
// only composite — order by their rendered form: two vectors shared the unknown
// rank, and compareByRank resolved same-rank pairs through fmt.
//
// Vectors rank last. They are the only member that contains domain values, so
// they have no place among the scalars; every representation the equality layer
// treats as a vector ([]interface{} and typed slices) takes rankVector, which is
// why the arm is a reflect check rather than a type case. []byte is a scalar with
// its own rank and matches its exact case above.
func typeRank(v interface{}) int {
	switch t := v.(type) {
	case int, int8, int16, int32, int64, uint64, float64:
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
	case ElementID:
		return 9
	case *ElementID:
		// A pointer representation carries a value only when non-nil. A nil one
		// is absence, which is not a member of the domain and so has no rank —
		// ranking it would order a "latest" mode selector among real ElementIDs.
		// Reported as nil rather than as an unknown type: the type is in the
		// domain, so naming the type would send a reader to this taxonomy
		// instead of to whatever produced the nil.
		if t == nil {
			panic(fmt.Sprintf("typeRank: %T is nil; nil is not a datalog value", v))
		}
		return 9
	}
	if isVector(v) {
		return rankVector
	}
	panic(fmt.Sprintf("typeRank: %T is not a datalog value type", v))
}

// rankVector is the ordering class of the domain's composite. Named because it
// is referenced by the vector arm and by the tests that pin the ladder.
const rankVector = 10

// compareByRank orders two values of different types by their type rank. Equal
// ranks cannot reach here: every rank holds one type — save the numeric rank and
// the ElementID pair, whose same-rank comparisons are handled by CompareValues
// before it delegates — so an equal-rank pair here is a type CompareValues
// dispatched past without handling.
func compareByRank(left, right interface{}) int {
	lr, rr := typeRank(left), typeRank(right)
	if lr == rr {
		panic(fmt.Sprintf("compareByRank: %T and %T share rank %d but neither "+
			"was compared by value", left, right, lr))
	}
	return compareInt64s(int64(lr), int64(rr))
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
// - Type conversions between numeric types
//
// nil is not a value and has no rank: it reaches typeRank's panic, like anything
// else outside the domain.
func CompareValues(left, right interface{}) int {
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

	// Integer widths normalize to int64 (the canonical representation), so
	// every signed width orders and equates identically. Across the
	// int64/uint64/float64 split, ordering is by numeric magnitude while
	// equality stays representation-strict — cmp==0 does not imply ValuesEqual
	// within the numeric rank.
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

	// Vectors, the domain's composite. Element-wise so nesting works by
	// recursion and so ValuesEqual(a,b) ⇒ cmp == 0 holds: this is the traversal
	// ValuesEqual performs, so two vectors it calls equal — across
	// representations — compare zero here. A vector against a scalar orders by
	// rank; []byte is a scalar and left the switch above.
	if isVector(left) && isVector(right) {
		return compareVectors(reflect.ValueOf(left), reflect.ValueOf(right))
	}

	return compareByRank(left, right)
}

// CompareValuesTagOrder orders two values as a storage key lays them out: by
// ValueType tag, then by payload within the tag. Same-tag pairs order exactly
// as CompareValues orders them; different tags order by tag, which is not
// typeRank's order — the tag ladder separates int from float.
//
// One dispatch covers the canonical same-type pairs. Anything else — mixed
// tags, non-canonical integer widths, values Type refuses — takes the
// tag-then-CompareValues composition this fuses, so it orders and panics
// exactly as that composition does.
func CompareValuesTagOrder(a, b Value) int {
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return compareInt64s(av, bv)
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return compareFloats(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			switch {
			case !av && bv:
				return -1
			case av && !bv:
				return 1
			}
			return 0
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			switch {
			case av.Before(bv):
				return -1
			case av.After(bv):
				return 1
			}
			return 0
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Compare(av, bv)
		}
	case Identity:
		if bv, ok := b.(Identity); ok {
			return av.Compare(bv)
		}
	case Keyword:
		if bv, ok := b.(Keyword); ok {
			return av.Compare(bv)
		}
	case Symbol:
		if bv, ok := b.(Symbol); ok {
			return av.Compare(bv)
		}
	case ElementID, *ElementID:
		if ea, ok := DerefElementID(a); ok {
			if eb, ok := DerefElementID(b); ok {
				return ea.Compare(eb)
			}
		}
	}

	at, bt := Type(a), Type(b)
	switch {
	case at < bt:
		return -1
	case at > bt:
		return 1
	}
	return CompareValues(a, b)
}

// isVector reports whether v is the domain's composite. []byte is a Go slice but
// a domain scalar, with its own rank and its own comparison.
func isVector(v interface{}) bool {
	if _, ok := v.([]byte); ok {
		return false
	}
	rv := reflect.ValueOf(v)
	return rv.IsValid() && rv.Kind() == reflect.Slice
}

// compareVectors orders two vectors lexicographically: element-wise to the
// shorter length, then the shorter vector first. Length-first would also satisfy
// the equality implication but would order by a property the elements do not
// determine — [9] below [1,2].
func compareVectors(left, right reflect.Value) int {
	n := left.Len()
	if right.Len() < n {
		n = right.Len()
	}
	for i := 0; i < n; i++ {
		if c := CompareValues(left.Index(i).Interface(), right.Index(i).Interface()); c != 0 {
			return c
		}
	}
	return compareInt64s(int64(left.Len()), int64(right.Len()))
}

// compareNumeric compares an int64 with another numeric value
func compareNumeric(left int64, right interface{}) int {
	if ri, ok := asInt64(right); ok {
		return compareInt64s(left, ri)
	}
	if r, ok := right.(float64); ok {
		return compareInt64Float64(left, r)
	}
	if r, ok := right.(uint64); ok {
		if left < 0 {
			return -1 // unsigned is always >= 0
		}
		return compareUint64s(uint64(left), r)
	}
	// Numeric (rank 1) vs non-numeric (higher rank): numeric sorts first. The
	// reverse direction (non-numeric left vs numeric right) reaches
	// compareByRank and yields +1, so this is antisymmetric.
	return -1
}

// compareFloat compares a float64 with another numeric value
func compareFloat(left float64, right interface{}) int {
	if ri, ok := asInt64(right); ok {
		return -compareInt64Float64(ri, left)
	}
	if r, ok := right.(float64); ok {
		return compareFloats(left, r)
	}
	if r, ok := right.(uint64); ok {
		return -compareUint64Float64(r, left)
	}
	// Numeric (rank 1) vs non-numeric: numeric sorts first (see compareNumeric).
	return -1
}

// compareInt64Float64 compares an int64 with a float64 exactly. Routing the
// integer through float64 collapses adjacent values above 2^53; instead the
// float's integer part is compared as int64, with any fractional part
// breaking the tie.
//
// Precondition: f is not NaN. NaN is not a datalog value — it is rejected at
// the write, input, and expression-output boundaries — so it cannot reach a
// comparison. (compareFloats' exhaustive arms panic if that ever breaks.)
func compareInt64Float64(i int64, f float64) int {
	if f >= 9223372036854775808.0 { // 2^63: above every int64
		return -1
	}
	if f < -9223372036854775808.0 { // below every int64
		return 1
	}
	floor := math.Floor(f)
	fi := int64(floor)
	if i != fi {
		return compareInt64s(i, fi)
	}
	if f > floor {
		return -1 // i == floor(f) < f
	}
	return 0
}

// compareUint64Float64 compares a uint64 with a float64 exactly, mirroring
// compareInt64Float64 for the unsigned range (same NaN precondition).
func compareUint64Float64(u uint64, f float64) int {
	if f >= 18446744073709551616.0 { // 2^64: above every uint64
		return -1
	}
	if f < 0 {
		return 1
	}
	floor := math.Floor(f)
	fu := uint64(floor)
	if u != fu {
		return compareUint64s(u, fu)
	}
	if f > floor {
		return -1
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

// compareFloats compares two float64 values. The three arms are exhaustive
// for every valid float pair — only NaN, which is not a datalog value and is
// rejected at every boundary, can fall through — so the panic costs valid
// comparisons nothing.
func compareFloats(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	if a == b {
		return 0
	}
	panic("CompareValues: NaN is not a datalog value")
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
		return compareUint64Float64(left, r)
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
	// Classifying b against the domain is what validates it: a value outside the
	// domain has no rank, and typeRank's default panics. Ordering gets this on both
	// operands for free because compareByRank ranks each side; equality dispatches
	// on a alone, so b needs it here.
	//
	// Only b. a is the dispatch operand, so an out-of-domain a matches no arm and
	// reaches the domain check at the tail. b is only ever assertion-tested by
	// whichever arm a selected, and a failed assertion there cannot be told apart
	// from a legitimate type mismatch — so without this, an out-of-domain b is
	// reported merely unequal, which would make absence a value that differs from
	// every member of the domain.
	//
	// typeRank rather than a second enumeration: the domain is enumerated once.
	typeRank(b)

	// []byte/[]uint8 first ([]uint8 is an alias of []byte, so one assertion
	// covers both). Strings are stored as []uint8 internally, so this is the
	// most common slice value. Slices aren't comparable with == (would
	// panic), so they're handled before the == fast paths below.
	if ba, ok := a.([]byte); ok {
		bb, ok := b.([]byte)
		return ok && bytes.Equal(ba, bb)
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

	// The value domain is closed and every member of it was handled above, so a
	// value reaching here is outside the domain — a layering violation, e.g. a
	// pulled map[string]interface{}, which is result presentation and never a
	// relational value. Fail loudly naming the type, as Type() and hashValue do.
	//
	// This is a domain check, not a comparability check. Testing Comparable()
	// let any comparable non-domain value through to `a == b`: a pointer then
	// compared addresses, which is the silent hash-by-address fallback the value
	// rules name as worse than a panic, and it made equality the weakest of the
	// domain's three doors.
	//
	// A nil pointer is reported as nil rather than as an unknown type. The domain
	// admits *ElementID, so naming the type would state something false and send a
	// reader to the taxonomy instead of to whatever produced the nil.
	if ra.Kind() == reflect.Ptr && ra.IsNil() {
		panic(fmt.Sprintf("ValuesEqual: %T is nil; nil is not a datalog value", a))
	}
	panic(fmt.Sprintf("ValuesEqual: %T is not a datalog value type", a))
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
