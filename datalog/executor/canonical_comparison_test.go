package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Constraint evaluation and equality delegate to the canonical comparators in
// the datalog package: one total order (CompareValues), one equality
// (ValuesEqual), defined where the value domain is defined.

// TestRangeConstraintInt64Precision pins that range constraints compare int64
// values exactly. The old comparator coerced all numerics to float64, which
// erases differences above 2^53 and made adjacent large int64 values compare
// equal.
func TestRangeConstraintInt64Precision(t *testing.T) {
	c := &rangeConstraint{position: 2, min: int64(1 << 53), includeMin: false}
	d := &datalog.Datom{V: int64(1<<53) + 1}
	if !c.Evaluate(d) {
		t.Error("exclusive min: 2^53+1 must be strictly greater than 2^53; float64 coercion must not erase the difference")
	}

	equal := &datalog.Datom{V: int64(1 << 53)}
	if c.Evaluate(equal) {
		t.Error("exclusive min: 2^53 must not pass its own exclusive bound")
	}
}

// TestRangeConstraintTypedOrdering pins same-type range behavior across the
// non-numeric types range constraints see.
func TestRangeConstraintTypedOrdering(t *testing.T) {
	strRange := &rangeConstraint{position: 2, min: "apple", max: "mango", includeMin: true, includeMax: true}
	if !strRange.Evaluate(&datalog.Datom{V: "banana"}) {
		t.Error("banana is within [apple, mango]")
	}
	if strRange.Evaluate(&datalog.Datom{V: "zebra"}) {
		t.Error("zebra is outside [apple, mango]")
	}

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	timeRange := &rangeConstraint{position: 2, min: t1, max: t2, includeMin: true, includeMax: true}
	if !timeRange.Evaluate(&datalog.Datom{V: time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)}) {
		t.Error("mid-year instant is within the year range")
	}
	if timeRange.Evaluate(&datalog.Datom{V: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)}) {
		t.Error("next-year instant is outside the year range")
	}
}

// TestValuesEqualCanonicalDelegation pins valuesEqual's contract: canonical
// domain equality, plus the one deliberately retained extension — Keyword and
// Symbol match their string text (pending its own decision). Identity has no
// such coercion: strings become entities only by boundary construction.
func TestValuesEqualCanonicalDelegation(t *testing.T) {
	kw := datalog.NewKeyword(":status/active")
	if !valuesEqual(kw, ":status/active") {
		t.Error("keyword must match its text (retained coercion)")
	}
	sym := datalog.NewSymbol("workflow/active")
	if !valuesEqual(sym, "workflow/active") {
		t.Error("symbol must match its text (retained coercion)")
	}

	// Equality is type-strict on int-vs-float, matching join semantics
	// (TupleKeyMap keys on datalog.ValuesEqual): mixed-numeric values are
	// never conflated. CompareValues may still order them as ties — that is
	// an ordering statement, not equality.
	if valuesEqual(int64(3), float64(3)) {
		t.Error("int64 and float64 are distinct values; equality is type-strict like join keys")
	}
	if valuesEqual(int64(1<<53)+1, float64(1<<53)) {
		t.Error("2^53+1 must not equal float64 2^53")
	}

	id := datalog.NewIdentity("user:alice")
	if !valuesEqual(id, id) {
		t.Error("identity equals itself")
	}
	if valuesEqual(id, id.L85()) {
		t.Error("identity must not equal its L85 text")
	}

	now := time.Now()
	if !valuesEqual(now, now) {
		t.Error("time equals itself")
	}
}
