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

// TestFilterComparisonEqualityIsStrict pins the filter path's = and != to
// value equality (type-strict), with the ordering operators remaining the
// magnitude-based total order — the same split as the predicate layer.
func TestFilterComparisonEqualityIsStrict(t *testing.T) {
	if evaluateComparison("=", int64(3), float64(3)) {
		t.Error("filter (= 3 3.0) must be false: equality is type-strict like join keys")
	}
	if !evaluateComparison("!=", int64(3), float64(3)) {
		t.Error("filter (!= 3 3.0) must be true")
	}
	if !evaluateComparison("=", int64(3), int64(3)) {
		t.Error("filter (= 3 3) must be true")
	}
	if !evaluateComparison(">=", int64(3), float64(3)) {
		t.Error("filter (>= 3 3.0) must be true: ordering compares by magnitude")
	}
}

// TestValuesEqualCanonicalStrictness pins the one equality relation
// (datalog.ValuesEqual): type-strict across the whole domain. Strings become
// keywords, symbols, and entities only by boundary construction — never by
// comparison-time coercion — and mixed-numeric values are never conflated.
// CompareValues may still order int/float by magnitude — that is an ordering
// statement, not equality.
func TestValuesEqualCanonicalStrictness(t *testing.T) {
	kw := datalog.NewKeyword(":status/active")
	if datalog.ValuesEqual(kw, ":status/active") {
		t.Error("keyword must not equal its text; comparison-time string coercion must not exist")
	}
	if !datalog.ValuesEqual(kw, datalog.NewKeyword(":status/active")) {
		t.Error("keyword equals itself (interned)")
	}
	sym := datalog.NewSymbol("workflow/active")
	if datalog.ValuesEqual(sym, "workflow/active") {
		t.Error("symbol must not equal its text; comparison-time string coercion must not exist")
	}
	if !datalog.ValuesEqual(sym, datalog.NewSymbol("workflow/active")) {
		t.Error("symbol equals itself (interned)")
	}

	if datalog.ValuesEqual(int64(3), float64(3)) {
		t.Error("int64 and float64 are distinct values; equality is type-strict like join keys")
	}
	if datalog.ValuesEqual(int64(1<<53)+1, float64(1<<53)) {
		t.Error("2^53+1 must not equal float64 2^53")
	}

	id := datalog.NewIdentity("user:alice")
	if !datalog.ValuesEqual(id, id) {
		t.Error("identity equals itself")
	}
	if datalog.ValuesEqual(id, id.L85()) {
		t.Error("identity must not equal its L85 text")
	}

	now := time.Now()
	if !datalog.ValuesEqual(now, now) {
		t.Error("time equals itself")
	}
}
