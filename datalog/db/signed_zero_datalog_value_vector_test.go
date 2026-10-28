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
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A []datalog.Value is a vector ValidateValue admits; its signed zero answers
// 0.0 as a []interface{}'s does.
func TestASignedZeroInsideADatalogValueVectorIsOneValue(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	alice := datalog.NewIdentity("signed-zero-value-vector-alice")
	s := schema.NewBuilder().
		Attribute(":t/name").Type(schema.TypeString).Add().
		Attribute(":t/vals").Type(schema.TypeDouble).Vector().Add().
		MustBuild()
	d, err := db.OpenMemory(db.WithSchema(s))
	require.NoError(t, err)
	defer d.Close()
	held := []datalog.Value{negativeZero, 1.5}
	require.NoError(t, datalog.ValidateValue(held))
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	require.NoError(t, tx.Set(alice, datalog.NewKeyword(":t/vals"), []datalog.Value{negativeZero, 1.5}))
	_, err = tx.Commit()
	require.NoError(t, err)
	e, vec := qb.NewVar("e"), qb.NewVar("vec")
	first := func(v interface{}) float64 {
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
		{"NormalizeValue", func() ([][]interface{}, error) {
			return [][]interface{}{{nil, datalog.NormalizeValue(held)}}, nil
		}},
		{"as an input", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?e ?vec :in $ ?vec :where [?e :t/name "alice"]]`, held))
		}},
		{"as a built ground", func() ([][]interface{}, error) {
			built := qb.Query().Find(e, vec).
				Where(qb.Pat(e, qb.Kw(":t/name"), "alice"), qb.Ground(held).As(vec)).MustBuild()
			return executor.CollectTuples(d.Query(built))
		}},
		{"written to a vector attribute", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?e ?vec :where [?e :t/name "alice"] [?e :t/vals ?vec]]`))
		}},
		{"nested in a vector of vectors", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?e ?vec :in $ ?vec :where [?e :t/name "alice"]]`, [][]datalog.Value{held}))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answered, err := c.run()
			require.NoError(t, err)
			require.Len(t, answered, 1)
			f := first(answered[0][1])
			t.Logf("%#v", answered[0][1])
			require.False(t, math.Signbit(f), "got %v of %#v", f, answered[0][1])
		})
	}
}
