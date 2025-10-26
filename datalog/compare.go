package datalog

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// CompareValues compares two values and returns:
//
//	-1 if left < right
//	 0 if left == right
//	 1 if left > right
//
// This function handles all Datalog value types including:
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

	// Handle pointers by dereferencing them
	if ptr, ok := left.(*Identity); ok {
		left = *ptr
	}
	if ptr, ok := right.(*Identity); ok {
		right = *ptr
	}
	if ptr, ok := left.(*Keyword); ok {
		left = *ptr
	}
	if ptr, ok := right.(*Keyword); ok {
		right = *ptr
	}
	if ptr, ok := left.(*uint64); ok {
		left = *ptr
	}
	if ptr, ok := right.(*uint64); ok {
		right = *ptr
	}

	// Handle Identity comparison
	if id1, ok := left.(Identity); ok {
		if id2, ok := right.(Identity); ok {
			// Compare raw bytes directly instead of L85 strings
			return compareBytes(id1.value[:], id2.value[:])
		}
		// Identity vs non-Identity: type mismatch
		return -1
	}

	// Handle Keyword comparison
	if kw1, ok := left.(Keyword); ok {
		if kw2, ok := right.(Keyword); ok {
			return strings.Compare(kw1.String(), kw2.String())
		}
		// Keyword vs non-Keyword: type mismatch
		return -1
	}

	// Handle numeric comparisons
	switch l := left.(type) {
	case int:
		return compareNumeric(int64(l), right)
	case int64:
		return compareNumeric(l, right)
	case float64:
		return compareFloat(l, right)
	case string:
		if r, ok := right.(string); ok {
			return strings.Compare(l, r)
		}
		// String vs non-string: type mismatch
		return -1
	case bool:
		if r, ok := right.(bool); ok {
			if !l && r {
				return -1
			} else if l && !r {
				return 1
			}
			return 0
		}
		// Bool vs non-bool: type mismatch
		return -1
	case time.Time:
		if r, ok := right.(time.Time); ok {
			if l.Before(r) {
				return -1
			} else if l.After(r) {
				return 1
			}
			return 0
		}
		// Time vs non-time: type mismatch
		return -1
	}

	// Fall back to string comparison for unknown types
	return strings.Compare(stringValue(left), stringValue(right))
}

// compareNumeric compares an int64 with another numeric value
func compareNumeric(left int64, right interface{}) int {
	switch r := right.(type) {
	case int:
		return compareInt64s(left, int64(r))
	case int64:
		return compareInt64s(left, r)
	case float64:
		return compareFloat(float64(left), right)
	}
	// Non-numeric: type mismatch
	return -1
}

// compareFloat compares a float64 with another numeric value
func compareFloat(left float64, right interface{}) int {
	switch r := right.(type) {
	case int:
		return compareFloats(left, float64(r))
	case int64:
		return compareFloats(left, float64(r))
	case float64:
		return compareFloats(left, r)
	}
	// Non-numeric: type mismatch
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

// ValuesEqual checks if two values are equal.
// It uses CompareValues for consistent equality checking.
func ValuesEqual(a, b interface{}) bool {
	// Quick pointer equality check for interned values
	if a == b {
		return true
	}

	// Handle pointers by dereferencing them first
	if ptr, ok := a.(*Identity); ok {
		a = *ptr
	}
	if ptr, ok := b.(*Identity); ok {
		b = *ptr
	}
	if ptr, ok := a.(*Keyword); ok {
		a = *ptr
	}
	if ptr, ok := b.(*Keyword); ok {
		b = *ptr
	}
	if ptr, ok := a.(*uint64); ok {
		a = *ptr
	}
	if ptr, ok := b.(*uint64); ok {
		b = *ptr
	}

	// Special handling for Identity types
	// CRITICAL: Must check this BEFORE general == comparison
	// because Identity struct equality compares ALL fields (value, l85, str)
	// but we only want to compare the hash (value field)
	if id1, ok := a.(Identity); ok {
		if id2, ok := b.(Identity); ok {
			// Direct byte comparison - much faster than string comparison
			return id1.value == id2.value
		}
		return false
	}

	// Special handling for Keyword types
	if kw1, ok := a.(Keyword); ok {
		if kw2, ok := b.(Keyword); ok {
			return kw1.String() == kw2.String()
		}
		return false
	}

	// Quick check for identity (after dereferencing and special type handling)
	if a == b {
		return true
	}

	// For numeric types and others, use direct comparison
	switch av := a.(type) {
	case int, int64, float64, string, bool, uint64:
		return a == b
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.Equal(bv)
		}
	}

	// Fall back to string comparison for unknown types
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
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
	default:
		// Use fmt.Sprintf for other types
		return fmt.Sprintf("%v", v)
	}
}
