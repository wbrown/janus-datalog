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

// A signed zero is one value at every place a value enters a query, a
// vector's element among them: a built vector ground or a vector input
// holding -0.0 answers that element as 0.0.
func TestASignedZeroInsideAVectorIsOneValue(t *testing.T) {
	alice := datalog.NewIdentity("signed-zero-vector-alice")
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	negativeZero := math.Copysign(0, -1)
	e, vec := qb.NewVar("e"), qb.NewVar("vec")
	cases := []struct {
		name string
		run  func() ([][]interface{}, error)
	}{
		{"a built vector ground", func() ([][]interface{}, error) {
			built := qb.Query().Find(e, vec).
				Where(qb.Pat(e, qb.Kw(":t/name"), "alice"), qb.Ground([]float64{negativeZero, 1.5}).As(vec)).
				MustBuild()
			return executor.CollectTuples(d.Query(built))
		}},
		{"a vector input", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?e ?vec :in $ ?vec :where [?e :t/name "alice"]]`,
				[]float64{negativeZero, 1.5}))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answered, err := c.run()
			require.NoError(t, err)
			require.Len(t, answered, 1)
			require.Equal(t, alice, answered[0][0])
			element := reflect.ValueOf(answered[0][1]).Index(0).Interface().(float64)
			require.False(t, math.Signbit(element), "the vector's first element answers 0.0, got %v of %#v", element, answered[0][1])
		})
	}
}
