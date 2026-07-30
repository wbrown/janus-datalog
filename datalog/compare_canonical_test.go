package datalog

import (
	"testing"
	"time"
)

// This file pins the value domain's semantics where the domain itself is
// defined: one total order (CompareValues), one equality (ValuesEqual).

// TestCompareValuesInt64Precision pins that int64 comparison is exact. The
// old range comparator coerced all numerics to float64, which erases
// differences above 2^53 and made adjacent large int64 values compare equal.
func TestCompareValuesInt64Precision(t *testing.T) {
	if CompareValues(int64(1<<53)+1, int64(1<<53)) <= 0 {
		t.Error("2^53+1 must be strictly greater than 2^53; float64 coercion must not erase the difference")
	}
	if CompareValues(int64(1<<53), int64(1<<53)) != 0 {
		t.Error("2^53 equals itself exactly")
	}
}

// TestCompareValuesTypedRangeOrdering pins same-type ordering across the
// non-numeric types range predicates see: an inclusive bound expressed
// through CompareValues behaves as a set membership test.
func TestCompareValuesTypedRangeOrdering(t *testing.T) {
	within := func(v, min, max interface{}) bool {
		return CompareValues(v, min) >= 0 && CompareValues(v, max) <= 0
	}

	if !within("banana", "apple", "mango") {
		t.Error("banana is within [apple, mango]")
	}
	if within("zebra", "apple", "mango") {
		t.Error("zebra is outside [apple, mango]")
	}

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	if !within(time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC), t1, t2) {
		t.Error("mid-year instant is within the year range")
	}
	if within(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), t1, t2) {
		t.Error("next-year instant is outside the year range")
	}
}

// TestValuesEqualCanonicalStrictness pins the one equality relation
// (ValuesEqual): type-strict across the whole domain. Strings become
// keywords, symbols, and entities only by boundary construction — never by
// comparison-time coercion — and mixed-numeric values are never conflated.
// CompareValues may still order int/float by magnitude — that is an ordering
// statement, not equality.
func TestValuesEqualCanonicalStrictness(t *testing.T) {
	kw := NewKeyword(":status/active")
	if ValuesEqual(kw, ":status/active") {
		t.Error("keyword must not equal its text; comparison-time string coercion must not exist")
	}
	if !ValuesEqual(kw, NewKeyword(":status/active")) {
		t.Error("keyword equals itself (interned)")
	}
	sym := NewSymbol("workflow/active")
	if ValuesEqual(sym, "workflow/active") {
		t.Error("symbol must not equal its text; comparison-time string coercion must not exist")
	}
	if !ValuesEqual(sym, NewSymbol("workflow/active")) {
		t.Error("symbol equals itself (interned)")
	}

	if ValuesEqual(int64(3), float64(3)) {
		t.Error("int64 and float64 are distinct values; equality is type-strict like join keys")
	}
	if ValuesEqual(int64(1<<53)+1, float64(1<<53)) {
		t.Error("2^53+1 must not equal float64 2^53")
	}

	id := NewIdentity("user:alice")
	if !ValuesEqual(id, id) {
		t.Error("identity equals itself")
	}
	if ValuesEqual(id, id.L85()) {
		t.Error("identity must not equal its L85 text")
	}

	now := time.Now()
	if !ValuesEqual(now, now) {
		t.Error("time equals itself")
	}
}
