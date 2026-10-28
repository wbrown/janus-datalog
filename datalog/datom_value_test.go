package datalog

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A datom's value is one scalar of the domain. ValidateDatomValue states it: a
// scalar and a non-value are answered as ValidateValue answers them, and a
// vector is refused, since a vector is a value of the query domain, an input,
// a constant or a resolved collection, and never one datom's. ValidateValue
// keeps admitting a vector of values at any depth and refusing a vector that
// holds a non-value.
func TestADatomsValueIsOneScalarOfTheDomain(t *testing.T) {
	eid := ElementID{Lamport: 1, ReplicaID: 1}

	t.Run("a scalar is a datom's value", func(t *testing.T) {
		scalars := []Value{
			"s", int64(1), 1.5, true, time.Now(), []byte{1},
			NewIdentity("datom-value-x"), NewKeyword(":t/a"), NewSymbol("sym"), eid, &eid,
			int(2), uint8(3),
		}
		for _, v := range scalars {
			require.NoError(t, ValidateDatomValue(v), "%T", v)
			require.NoError(t, ValidateValue(v), "%T", v)
		}
	})

	t.Run("a non-value is refused as ValidateValue refuses it", func(t *testing.T) {
		nonValues := []Value{
			nil, math.NaN(), uint64(math.MaxInt64) + 1, map[string]interface{}{"k": "v"},
			(*ElementID)(nil), Identity(nil),
		}
		for _, v := range nonValues {
			datom, value := ValidateDatomValue(v), ValidateValue(v)
			require.Error(t, datom, "%T", v)
			require.Error(t, value, "%T", v)
			require.EqualError(t, datom, value.Error(), "%T", v)
		}
	})

	t.Run("a vector is not a datom's value and is a value", func(t *testing.T) {
		vectors := []Value{
			[]interface{}{int64(1), int64(2)}, []float64{1.5}, []Value{"s"}, [][]float64{{1.5}},
		}
		for _, v := range vectors {
			err := ValidateDatomValue(v)
			require.Error(t, err, "%T", v)
			require.ErrorContains(t, err, "not a datom's value", "%T", v)
			require.NoError(t, ValidateValue(v), "%T", v)
		}
	})

	t.Run("a vector holding a non-value is refused naming it", func(t *testing.T) {
		err := ValidateValue([]interface{}{"s", map[string]interface{}{"k": "v"}})
		require.Error(t, err)
		require.ErrorContains(t, err, "map[string]interface {}")
	})
}
