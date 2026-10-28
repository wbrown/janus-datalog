package datalog

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// A vector's elements enter the domain as scalars do: a float's signed zero
// is 0.0 and an integer is int64, at any depth, in every slice the domain
// admits: a []interface{}, a []float64, a []Value, a named type over any of
// them, and a vector of vectors, each answering its own type. A vector whose
// elements are already of the domain is the same slice, not a copy.
func TestNormalizeValueTakesAVectorsElementsAsScalars(t *testing.T) {
	type embedding []float64
	type row []interface{}
	negativeZero := math.Copysign(0, -1)

	t.Run("a named float vector holding a signed zero keeps its type", func(t *testing.T) {
		held := embedding{negativeZero, 1.5}
		normalized, ok := NormalizeValue(held).(embedding)
		require.True(t, ok, "a named float vector normalizes to its own type, got %T", NormalizeValue(held))
		require.Len(t, normalized, 2)
		require.False(t, math.Signbit(normalized[0]), "the signed zero is 0.0")
		require.Equal(t, 1.5, normalized[1])
		require.True(t, math.Signbit(held[0]), "the given slice is untouched")
	})

	t.Run("a vector of vectors copies from the first changed inner vector", func(t *testing.T) {
		first := []float64{1.5}
		second := []float64{negativeZero, 2.5}
		normalized, ok := NormalizeValue([][]float64{first, second}).([][]float64)
		require.True(t, ok, "a vector of vectors normalizes to its own type, got %T", NormalizeValue([][]float64{first, second}))
		require.Len(t, normalized, 2)
		require.Same(t, &first[0], &normalized[0][0], "an inner vector with no signed zero is the same slice")
		require.False(t, math.Signbit(normalized[1][0]), "the nested signed zero is 0.0")
		require.Equal(t, 2.5, normalized[1][1])
		require.True(t, math.Signbit(second[0]), "the given inner slice is untouched")
	})

	t.Run("a named interface vector holding a signed zero keeps its type", func(t *testing.T) {
		held := row{negativeZero, "s"}
		normalized, ok := NormalizeValue(held).(row)
		require.True(t, ok, "a named interface vector normalizes to its own type, got %T", NormalizeValue(held))
		require.Len(t, normalized, 2)
		require.False(t, math.Signbit(normalized[0].(float64)), "the signed zero is 0.0")
		require.Equal(t, "s", normalized[1])
	})

	t.Run("a vector of another element kind, or a named vector of domain values, is the same slice", func(t *testing.T) {
		named := embedding{0, 1.5}
		require.Same(t, &named[0], &NormalizeValue(named).(embedding)[0])
		strings := []string{"a", "b"}
		require.Same(t, &strings[0], &NormalizeValue(strings).([]string)[0])
		ints := []int64{1, 2}
		require.Same(t, &ints[0], &NormalizeValue(ints).([]int64)[0])
	})

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
		typed := []Value{int64(1), "s", float64(0)}
		require.Same(t, &typed[0], &NormalizeValue(typed).([]Value)[0])
	})

	t.Run("a []Value holding a signed zero answers a []Value", func(t *testing.T) {
		held := []Value{negativeZero, "s"}
		normalized, ok := NormalizeValue(held).([]Value)
		require.True(t, ok, "a []Value normalizes to a []Value, got %T", NormalizeValue(held))
		require.Len(t, normalized, 2)
		require.False(t, math.Signbit(normalized[0].(float64)), "the signed zero is 0.0")
		require.Equal(t, "s", normalized[1])
		require.True(t, math.Signbit(held[0].(float64)), "the given slice is untouched")
	})

	t.Run("a vector of []Value copies from the first changed inner vector", func(t *testing.T) {
		first := []Value{1.5}
		second := []Value{negativeZero, 2.5}
		normalized, ok := NormalizeValue([][]Value{first, second}).([][]Value)
		require.True(t, ok, "a vector of []Value normalizes to its own type, got %T", NormalizeValue([][]Value{first, second}))
		require.Len(t, normalized, 2)
		require.Same(t, &first[0], &normalized[0][0], "an inner vector with no signed zero is the same slice")
		require.False(t, math.Signbit(normalized[1][0].(float64)), "the nested signed zero is 0.0")
		require.Equal(t, 2.5, normalized[1][1])
		require.True(t, math.Signbit(second[0].(float64)), "the given inner slice is untouched")
	})

	t.Run("a []Value holding nil after a signed zero is refused, never a panic", func(t *testing.T) {
		held := []Value{negativeZero, nil}
		var err error
		require.NotPanics(t, func() { err = ValidateValue(held) })
		require.EqualError(t, err, "nil is not a datalog value")
	})
}
