package db_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// embedding is a caller's named float vector type: ValidateValue admits it as
// a vector of float64, the same as []float64.
type embedding []float64

// A signed zero inside every vector the domain admits answers as 0.0: a
// caller's named float vector and a vector of float vectors, as a built ground
// and as an input, answer their first float element as 0.0, as []float64 does.
func TestASignedZeroInsideEveryAdmittedVectorShapeIsOneValue(t *testing.T) {
	alice := datalog.NewIdentity("signed-zero-shapes-alice")
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	negativeZero := math.Copysign(0, -1)
	named := embedding{negativeZero, 1.5}
	nested := [][]float64{{negativeZero, 1.5}}
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	_, err = tx.Commit()
	require.NoError(t, err)
	require.NoError(t, datalog.ValidateValue(named))
	require.NoError(t, datalog.ValidateValue(nested))

	e, vec := qb.NewVar("e"), qb.NewVar("vec")
	ground := func(v interface{}) func() ([][]interface{}, error) {
		return func() ([][]interface{}, error) {
			built := qb.Query().Find(e, vec).
				Where(qb.Pat(e, qb.Kw(":t/name"), "alice"), qb.Ground(v).As(vec)).
				MustBuild()
			return executor.CollectTuples(d.Query(built))
		}
	}
	input := func(v interface{}) func() ([][]interface{}, error) {
		return func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?e ?vec :in $ ?vec :where [?e :t/name "alice"]]`, v))
		}
	}
	firstFloat := func(v interface{}) float64 {
		rv := reflect.ValueOf(v)
		for rv.Kind() == reflect.Slice || rv.Kind() == reflect.Interface {
			if rv.Kind() == reflect.Interface {
				rv = rv.Elem()
				continue
			}
			rv = rv.Index(0)
		}
		return rv.Float()
	}
	cases := []struct {
		name string
		run  func() ([][]interface{}, error)
	}{
		{"a named float vector as a built ground", ground(named)},
		{"a named float vector as an input", input(named)},
		{"a vector of float vectors as a built ground", ground(nested)},
		{"a vector of float vectors as an input", input(nested)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answered, err := c.run()
			require.NoError(t, err)
			require.Len(t, answered, 1)
			element := firstFloat(answered[0][1])
			require.False(t, math.Signbit(element), "the first float element answers 0.0, got %v of %#v", element, answered[0][1])
		})
	}
}
