package datalog

import (
	"bytes"
	"testing"
)

// These unit tests pin the integer-width policy directly, below the API
// boundary: NormalizeValue coerces to int64; ValuesEqual and CompareValues agree
// on integer widths (int(5) == int64(5)) while keeping int-vs-float strict; and
// Type/ValueBytes no longer panic on a bare Go int. The boundary tests in the
// storage package prove the user-facing behavior; these prove the machinery the
// boundary relies on (and the defense-in-depth for any path that bypasses it).

func TestNormalizeValue_CoercesIntegerWidths(t *testing.T) {
	cases := []struct {
		in   Value
		want Value
	}{
		{int(5), int64(5)},
		{int8(5), int64(5)},
		{int16(5), int64(5)},
		{int32(5), int64(5)},
		{int64(5), int64(5)},
		{int(-7), int64(-7)},
	}
	for _, c := range cases {
		got := NormalizeValue(c.in)
		if got != c.want {
			t.Errorf("NormalizeValue(%T %v) = %T %v, want %T %v",
				c.in, c.in, got, got, c.want, c.want)
		}
	}
	// Non-integers pass through unchanged (and keep their dynamic type).
	for _, v := range []Value{"s", 3.14, true, float64(5), int64(9)} {
		if got := NormalizeValue(v); got != v {
			t.Errorf("NormalizeValue(%T) changed it to %T %v", v, got, got)
		}
	}
}

func TestValuesEqual_CompareValues_AgreeOnIntegerWidths(t *testing.T) {
	widths := []Value{int(5), int8(5), int16(5), int32(5), int64(5)}
	for _, a := range widths {
		for _, b := range widths {
			if !ValuesEqual(a, b) {
				t.Errorf("ValuesEqual(%T(5), %T(5)) = false, want true", a, b)
			}
			if c := CompareValues(a, b); c != 0 {
				t.Errorf("CompareValues(%T(5), %T(5)) = %d, want 0", a, b, c)
			}
		}
	}
	// Different magnitudes across widths must NOT be equal, and order correctly.
	if ValuesEqual(int(5), int64(6)) {
		t.Error("ValuesEqual(int(5), int64(6)) = true, want false")
	}
	if c := CompareValues(int8(5), int64(6)); c != -1 {
		t.Errorf("CompareValues(int8(5), int64(6)) = %d, want -1", c)
	}
	if c := CompareValues(int32(9), int(2)); c != 1 {
		t.Errorf("CompareValues(int32(9), int(2)) = %d, want 1", c)
	}
}

func TestValuesEqual_IntVsFloatStaysStrict(t *testing.T) {
	// Policy: integer-vs-float stays strict in ValuesEqual so mixed-numeric join
	// keys aren't conflated, even though CompareValues orders them by magnitude.
	if ValuesEqual(int64(5), float64(5.0)) {
		t.Error("ValuesEqual(int64(5), float64(5.0)) = true, want false (int-vs-float strict)")
	}
	if ValuesEqual(int(5), float64(5.0)) {
		t.Error("ValuesEqual(int(5), float64(5.0)) = true, want false")
	}
	// CompareValues, by contrast, orders int and float by magnitude.
	if c := CompareValues(int64(5), float64(5.0)); c != 0 {
		t.Errorf("CompareValues(int64(5), float64(5.0)) = %d, want 0", c)
	}
	// float64 is still equal to itself.
	if !ValuesEqual(float64(5), float64(5)) {
		t.Error("ValuesEqual(float64(5), float64(5)) = false, want true")
	}
}

func TestType_And_ValueBytes_GoIntEncodeAsInt64(t *testing.T) {
	// Facet 2: Type/ValueBytes previously panicked on a bare int. They must now
	// coerce to the canonical int64 encoding.
	wantBytes := ValueBytes(int64(5))
	for _, v := range []Value{int(5), int8(5), int16(5), int32(5)} {
		if got := Type(v); got != TypeInt {
			t.Errorf("Type(%T) = %v, want TypeInt", v, got)
		}
		if got := ValueBytes(v); !bytes.Equal(got, wantBytes) {
			t.Errorf("ValueBytes(%T(5)) = %v, want %v (same as int64)", v, got, wantBytes)
		}
	}
	// And the decoded value is int64, not the original width.
	rt, err := ValueFromBytes(TypeInt, ValueBytes(int(5)))
	if err != nil {
		t.Fatal(err)
	}
	if rt != int64(5) {
		t.Errorf("round-trip int(5) -> %T %v, want int64(5)", rt, rt)
	}
}
