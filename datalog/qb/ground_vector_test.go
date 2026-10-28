package qb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A vector a Ground embeds in a query carries its elements as the domain
// admits them, whatever Go slice the caller held: a float's signed zero is
// 0.0 and an integer is int64.
func TestGround_NormalizesAVectorsElements(t *testing.T) {
	x := NewVar("x")
	negativeZero := math.Copysign(0, -1)

	t.Run("a []float64 holding a signed zero", func(t *testing.T) {
		expr := Ground([]float64{negativeZero, 1.5}).As(x).toClause().(*query.Expression)
		held, ok := expr.Function.(*query.GroundFunction).Value.([]float64)
		require.True(t, ok, "the ground holds a []float64, got %T", expr.Function.(*query.GroundFunction).Value)
		require.False(t, math.Signbit(held[0]), "the first element is 0.0, got %v", held[0])
	})

	t.Run("a []interface{} holding a signed zero and an int", func(t *testing.T) {
		expr := Ground([]interface{}{negativeZero, int(2)}).As(x).toClause().(*query.Expression)
		held, ok := expr.Function.(*query.GroundFunction).Value.([]interface{})
		require.True(t, ok, "the ground holds a []interface{}, got %T", expr.Function.(*query.GroundFunction).Value)
		require.False(t, math.Signbit(held[0].(float64)), "the first element is 0.0, got %v", held[0])
		require.Equal(t, int64(2), held[1], "the int is int64")
	})
}
