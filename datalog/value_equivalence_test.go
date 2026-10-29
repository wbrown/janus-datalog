package datalog

import (
	"math"
	"testing"
	"time"
)

// TestValuesEqualIsAnEquivalenceAndEqualValuesHashAlike pins the two
// properties the join proofs read of ValuesEqual: it is an equivalence on
// the value domain, and values it holds equal hash alike under HashValue,
// so tuples it holds equal at every position hash alike under HashValues.
// The table partitions the domain's representations into classes, each
// class one value. ValuesEqual agreeing with a partition, equal within a
// class and unequal across classes in both directions, is reflexive,
// symmetric and transitive at once.
func TestValuesEqualIsAnEquivalenceAndEqualValuesHashAlike(t *testing.T) {
	noon := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	classes := [][]interface{}{
		{int64(5), int(5), int8(5), int16(5), int32(5)},
		{int64(-1), int(-1), int8(-1), int16(-1), int32(-1)},
		{int64(1)},
		{float64(1)},
		{uint64(5)},
		{uint64(math.MaxUint64)},
		{float64(0), math.Copysign(0, -1)},
		{math.Inf(1)},
		{math.Inf(-1)},
		{2.5},
		{"north"},
		{""},
		{[]byte{}},
		{[]interface{}{}, []string{}},
		{[]byte{1, 2}, []byte{1, 2}},
		{[]interface{}{"a", int64(1)}, []interface{}{"a", int(1)}},
		{[]string{"a"}, []interface{}{"a"}},
		{noon, noon.In(time.FixedZone("east", 3*60*60))},
		{true},
		{false},
		{NewIdentity("quire"), NewIdentity("quire")},
		{NewIdentity("press")},
		{NewKeyword(":press/north"), NewKeyword(":press/north")},
		{NewSymbol("?quire"), NewSymbol("?quire")},
		{ElementID{Lamport: 7, ReplicaID: 3}, &ElementID{Lamport: 7, ReplicaID: 3}},
	}

	for i, class := range classes {
		for _, a := range class {
			if !ValuesEqual(a, a) {
				t.Errorf("class %d: %T %v is unequal to itself", i, a, a)
			}
			for _, b := range class {
				if !ValuesEqual(a, b) {
					t.Errorf("class %d: %T %v and %T %v are one value and are unequal", i, a, a, b, b)
				}
				if HashValue(a) != HashValue(b) {
					t.Errorf("class %d: %T %v and %T %v are one value and hash apart", i, a, a, b, b)
				}
			}
		}
		for j := i + 1; j < len(classes); j++ {
			for _, a := range class {
				for _, b := range classes[j] {
					if ValuesEqual(a, b) {
						t.Errorf("classes %d and %d: %T %v and %T %v are different values and are equal", i, j, a, a, b, b)
					}
					if ValuesEqual(b, a) {
						t.Errorf("classes %d and %d: %T %v and %T %v are different values and are equal", j, i, b, b, a, a)
					}
				}
			}
		}
	}

	first := make([]interface{}, len(classes))
	last := make([]interface{}, len(classes))
	for i, class := range classes {
		first[i] = class[0]
		last[i] = class[len(class)-1]
	}
	if HashValues(first) != HashValues(last) {
		t.Errorf("two tuples equal at every position hash apart")
	}
}
