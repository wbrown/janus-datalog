package datalog

import (
	"fmt"
	"strings"
	"testing"
)

// The value domain is closed: scalars, []byte, time.Time, Identity, Keyword,
// Symbol, ElementID, and vectors ([]interface{}). A type outside it reaching
// the equality layer is a layering violation — e.g. a pulled
// map[string]interface{}, which is result presentation, never a relational
// value (docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md). ValuesEqual must fail
// loudly naming the type, mirroring Type()'s panic convention — not crash
// with Go's cryptic "comparing uncomparable type" nor, worse, compare
// wrongly.
//
// nil is rejected on the same grounds: it is absence, not a member of the
// domain, so it is neither equal to another absence nor unequal to a value. A
// nil *ElementID is the same absence wearing an in-domain type — DerefElementID
// reports it with the same false it reports for a different type, which is what
// its matcher callers ask of it, so equality must separate the two itself.
//
// Both operand positions are covered because rejection that reads only the left
// operand lets the right one answer "not equal" instead of failing.
func TestValuesEqualRejectsNonValueTypes(t *testing.T) {
	eid := ElementID{Lamport: 1, ReplicaID: 1}
	cases := []struct {
		name string
		a, b interface{}
	}{
		{"map is presentation, not a value",
			map[string]interface{}{"k": "v"}, map[string]interface{}{"k": "v"}},
		{"map in the right position",
			"x", map[string]interface{}{"k": "v"}},
		{"untyped nil against untyped nil", nil, nil},
		{"untyped nil against a value", nil, "x"},
		{"untyped nil in the right position", "x", nil},
		{"nil *ElementID carries no ElementID", (*ElementID)(nil), eid},
		{"nil *ElementID in the right position", eid, (*ElementID)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected a value-domain panic")
				}
				if msg := fmt.Sprint(r); !strings.Contains(msg, "not a datalog value") {
					t.Fatalf("panic should name the value-domain violation, got: %v", r)
				}
			}()
			ValuesEqual(tc.a, tc.b)
		})
	}
}

// Ordering rejects the same non-members, and gets both operand positions for
// free: compareByRank classifies each side through typeRank, whose default
// panics. This is the door :order-by, min/max, the comparison predicates and the
// merge join's advance comparator run through, so a non-member reaching it must
// fail rather than be assigned a rank and silently ordered.
func TestCompareValuesRejectsNonValueTypes(t *testing.T) {
	eid := ElementID{Lamport: 1, ReplicaID: 1}
	cases := []struct {
		name string
		a, b interface{}
	}{
		{"map is presentation, not a value",
			map[string]interface{}{"k": "v"}, map[string]interface{}{"k": "v"}},
		{"map in the right position",
			"x", map[string]interface{}{"k": "v"}},
		{"untyped nil against untyped nil", nil, nil},
		{"untyped nil against a value", nil, "x"},
		{"untyped nil in the right position", "x", nil},
		{"nil *ElementID carries no ElementID", (*ElementID)(nil), eid},
		{"nil *ElementID in the right position", eid, (*ElementID)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("expected a value-domain panic")
				}
				if msg := fmt.Sprint(r); !strings.Contains(msg, "not a datalog value") {
					t.Fatalf("panic should name the value-domain violation, got: %v", r)
				}
			}()
			CompareValues(tc.a, tc.b)
		})
	}
}

// Vectors are values: element-wise equality, order-sensitive, recursive.
// (Pinning the existing slice handling that the domain guard must not
// disturb.)
func TestValuesEqualVectors(t *testing.T) {
	a := []interface{}{"x", int64(1), []interface{}{"nested"}}
	b := []interface{}{"x", int64(1), []interface{}{"nested"}}
	c := []interface{}{int64(1), "x", []interface{}{"nested"}}

	if !ValuesEqual(a, b) {
		t.Error("equal vectors must be ValuesEqual")
	}
	if ValuesEqual(a, c) {
		t.Error("vectors with different element order must not be ValuesEqual")
	}
}
