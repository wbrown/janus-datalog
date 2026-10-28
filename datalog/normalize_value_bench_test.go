package datalog

import "testing"

// normalizeValueSink keeps each benchmark's answer live.
var normalizeValueSink Value

// BenchmarkNormalizeValue measures NormalizeValue on the values the write and
// input paths hand it, each with no signed zero and no integer to widen, so
// every case is the common one that answers the value as given: a string, the
// two vector types the static arms name, a named float vector and a vector of
// vectors.
func BenchmarkNormalizeValue(b *testing.B) {
	type embedding []float64
	strings := make([]string, 16)
	floats := make([]float64, 16)
	named := make(embedding, 16)
	values := make([]interface{}, 16)
	nested := make([][]float64, 4)
	for i := range strings {
		strings[i] = "a value"
		floats[i] = 1.5
		named[i] = 1.5
		values[i] = 1.5
	}
	for i := range nested {
		nested[i] = []float64{1.5, 2.5, 3.5, 4.5}
	}
	cases := []struct {
		name  string
		value Value
	}{
		{"string", "a value"},
		{"[]string/16", strings},
		{"[]float64/16", floats},
		{"[]interface{}/16", values},
		{"named-float/16", named},
		{"[][]float64/4x4", nested},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				normalizeValueSink = NormalizeValue(c.value)
			}
		})
	}
}
