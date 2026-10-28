package datalog

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// A vector's elements enter the domain as scalars do: a float's signed zero
// is 0.0 and an integer is int64, at any depth, in a []interface{} and in a
// []float64 alike. A vector whose elements are already of the domain is the
// same slice, not a copy.
func TestNormalizeValueTakesAVectorsElementsAsScalars(t *testing.T) {
	negativeZero := math.Copysign(0, -1)

	t.Run("a []float64 holding a signed zero", func(t *testing.T) {
		for _, held := range [][]float64{{negativeZero, 1.5}, {1.5, negativeZero}} {
			normalized, ok := NormalizeValue(held).([]float64)
			require.True(t, ok, "a []float64 normalizes to a []float64, got %T", NormalizeValue(held))
			require.Len(t, normalized, len(held))
			for i, f := range normalized {
				require.False(t, math.Signbit(f), "element %d of %v answers 0.0, got %v", i, held, f)
			}
			for i, f := range held {
				require.Equal(t, f == 0, normalized[i] == 0, "element %d keeps its magnitude", i)
			}
		}
	})

	t.Run("a []interface{} holding a signed zero, an int and a nested vector", func(t *testing.T) {
		held := []interface{}{negativeZero, int(1), []float64{negativeZero}}
		normalized, ok := NormalizeValue(held).([]interface{})
		require.True(t, ok, "a []interface{} normalizes to a []interface{}")
		require.Len(t, normalized, 3)
		first, ok := normalized[0].(float64)
		require.True(t, ok)
		require.False(t, math.Signbit(first), "the signed zero is 0.0")
		require.Equal(t, int64(1), normalized[1], "the int is int64")
		nested, ok := normalized[2].([]float64)
		require.True(t, ok, "the nested vector keeps its type, got %T", normalized[2])
		require.False(t, math.Signbit(nested[0]), "the nested signed zero is 0.0")
	})

	t.Run("a vector of domain values is the same slice", func(t *testing.T) {
		floats := []float64{0, 1.5}
		require.Same(t, &floats[0], &NormalizeValue(floats).([]float64)[0])
		values := []interface{}{int64(1), "s", float64(0)}
		require.Same(t, &values[0], &NormalizeValue(values).([]interface{})[0])
	})
}
