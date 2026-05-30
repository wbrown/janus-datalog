package datalog

import "testing"

// Allocation-free microbenchmarks for the comparison hot path touched by the
// integer-width unification. These isolate ValuesEqual/CompareValues from the
// GC- and thermal-dominated macro hash-join benchmarks, so a before/after
// benchstat reflects the function change rather than machine noise.

var (
	cmpEqualSink bool
	cmpOrderSink int
)

func BenchmarkValuesEqual_Int64Equal(b *testing.B) {
	var x, y Value = int64(1234567), int64(1234567)
	for i := 0; i < b.N; i++ {
		cmpEqualSink = ValuesEqual(x, y)
	}
}

func BenchmarkValuesEqual_Int64Unequal(b *testing.B) {
	var x, y Value = int64(1234567), int64(7654321)
	for i := 0; i < b.N; i++ {
		cmpEqualSink = ValuesEqual(x, y)
	}
}

func BenchmarkValuesEqual_StringEqual(b *testing.B) {
	var x, y Value = "the quick brown fox", "the quick brown fox"
	for i := 0; i < b.N; i++ {
		cmpEqualSink = ValuesEqual(x, y)
	}
}

// Identity is the dominant real join key and is resolved before the integer
// branch; this guards that the common case is untouched.
func BenchmarkValuesEqual_Identity(b *testing.B) {
	var x, y Value = NewIdentity("entity-1234567"), NewIdentity("entity-1234567")
	for i := 0; i < b.N; i++ {
		cmpEqualSink = ValuesEqual(x, y)
	}
}

func BenchmarkCompareValues_Int64(b *testing.B) {
	var x, y Value = int64(1234567), int64(7654321)
	for i := 0; i < b.N; i++ {
		cmpOrderSink = CompareValues(x, y)
	}
}
