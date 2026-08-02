package datalog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tagOrderReference is the composition CompareValuesTagOrder fuses: ValueType
// tag first, payload by CompareValues within a tag. The pin holds the fused
// form to this, pair by pair.
func tagOrderReference(a, b Value) int {
	at, bt := Type(a), Type(b)
	switch {
	case at < bt:
		return -1
	case at > bt:
		return 1
	}
	return CompareValues(a, b)
}

// TestCompareValuesTagOrderMatchesReference checks every ordered pair over
// exemplars of the storable domain, including the pairs where a fast path
// could plausibly diverge: integer widths, equal instants in different
// locations, nil against empty []byte, and ElementID against its pointer form.
func TestCompareValuesTagOrderMatchesReference(t *testing.T) {
	instant := time.Date(2026, 3, 14, 9, 26, 53, 0, time.UTC)
	eid := ElementID{Lamport: 7, ReplicaID: 2}
	values := []Value{
		"", "a", "ab", "b",
		int64(-3), int64(0), int64(1), int64(42), int(1), int32(-3),
		float64(-2.5), float64(0), float64(0.5), float64(42),
		false, true,
		instant, instant.In(time.FixedZone("plus2", 7200)), instant.Add(time.Second),
		[]byte(nil), []byte{}, []byte{0x00}, []byte{0x01, 0x02},
		NewIdentity("tag-order:a"), NewIdentity("tag-order:b"),
		NewKeyword(":tag/a"), NewKeyword(":tag/b"),
		NewSymbol("sym/a"), NewSymbol("sym/b"),
		eid, &eid, ElementID{Lamport: 8, ReplicaID: 1},
	}

	for _, a := range values {
		for _, b := range values {
			want := tagOrderReference(a, b)
			got := CompareValuesTagOrder(a, b)
			require.Equal(t, want, got, "CompareValuesTagOrder(%#v, %#v)", a, b)
			require.Equal(t, -got, CompareValuesTagOrder(b, a), "antisymmetry (%#v, %#v)", a, b)
		}
	}
}

// TestCompareValuesTagOrderRejectsOutsideDomain: values Type refuses stay loud
// here too — the tag order is defined only on what a key can hold.
func TestCompareValuesTagOrderRejectsOutsideDomain(t *testing.T) {
	require.Panics(t, func() { CompareValuesTagOrder(uint64(1), int64(1)) })
	require.Panics(t, func() { CompareValuesTagOrder([]interface{}{int64(1)}, int64(1)) })
}
