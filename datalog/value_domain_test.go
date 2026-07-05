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
func TestValuesEqualRejectsNonValueTypes(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected value-domain panic for map values")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "not a datalog value") {
			t.Fatalf("panic should name the value-domain violation, got: %v", r)
		}
	}()
	ValuesEqual(
		map[string]interface{}{"k": "v"},
		map[string]interface{}{"k": "v"},
	)
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
	if !ValuesEqual(nil, nil) {
		t.Error("nil values must remain equal through the domain guard")
	}
}
